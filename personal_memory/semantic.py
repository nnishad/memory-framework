"""Rebuildable, chunked multilingual embeddings with a durable indexing journal.

SQLite is authoritative. HNSW is an in-memory accelerator rebuilt after restart;
no untrusted serialized Python index is loaded. Canonical filters run on every query.
"""
import importlib.metadata
import threading
import time
import hashlib
import json
from pathlib import Path

from .common import digest, now

DEFAULT_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Durable incremental work queue: canonical writers enqueue desired-state rows
# inside their own transaction, so background polling never rescans the archive.
# One shared row per record carries a monotonically increasing revision: coalescing
# overwrites the superseded signal instead of queueing a second one, and every
# acknowledgment is guarded by the revision it processed, so work that arrives
# during processing survives. The shared row is a signal to distribute, not
# proof any model finished: each registered model revision owns a durable
# per-model obligation that survives activation switches, another model's
# success and its own backoff until it completes or a retirement cancels it.
# The append-only change history stays for compatibility and diagnostics; no
# correctness depends on replaying it.
WORK_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_work(
  record_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
  seq INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  available_at REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS semantic_work_due ON semantic_work(kind, available_at, seq);
CREATE TABLE IF NOT EXISTS semantic_seq(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS semantic_history(
  seq INTEGER PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
  record_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS semantic_progress(
  model TEXT PRIMARY KEY, seq INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS semantic_bootstrap(
  model TEXT PRIMARY KEY, enqueued_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS semantic_models(
  model TEXT PRIMARY KEY, registered_at TEXT NOT NULL,
  registration_complete INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS semantic_model_work(
  model TEXT NOT NULL, record_id TEXT NOT NULL, seq INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL, PRIMARY KEY(model, record_id));
CREATE INDEX IF NOT EXISTS semantic_model_work_due ON semantic_model_work(model, available_at, seq);
"""

# The same statements, one per execute, so schema setup can run inside an explicit
# transaction: executescript would commit any transaction the caller already owns.
_WORK_STATEMENTS = tuple(
    statement.strip()
    for statement in WORK_SCHEMA.split(";")
    if statement.strip())

# Change history is retained at least this long; a model whose replay window
# was pruned reconciles from the canonical archive instead of guessing.
HISTORY_RETENTION = 50000


def _next_seq(db):
    # The UPDATE takes the write lock before the value is read, so concurrent
    # writers serialize and never observe the same revision twice.
    db.execute("INSERT OR IGNORE INTO semantic_seq VALUES(1,0)")
    db.execute("UPDATE semantic_seq SET value=value+1 WHERE id=1")
    return db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]


def _signal(db, kind, record_id, *, journal=True):
    seq = _next_seq(db)
    if journal:
        db.execute("INSERT OR REPLACE INTO semantic_history VALUES(?,?,?)", (seq, kind, record_id))
    db.execute("""INSERT INTO semantic_work(record_id,kind,seq,attempts,available_at,updated_at)
        VALUES(?,?,?,0,0,?) ON CONFLICT(record_id) DO UPDATE SET
        kind=excluded.kind,seq=excluded.seq,attempts=0,available_at=0,updated_at=excluded.updated_at""",
                 (record_id, kind, seq, now()))
    return seq


def enqueue(db, kind, record_ids):
    """Request semantic work for records inside the caller's transaction.

    Coalesces on the record: repeated signals for the same record overwrite
    the pending revision, so the newest desired state is never lost behind a
    superseded one and never duplicates embeddings.
    """
    for rid in record_ids:
        _signal(db, kind, rid, journal=True)


def _model_upsert(db, model, record_id, seq):
    """Raise one model's desired work for a record to the given revision.

    Coalescing never downgrades: an obligation already carrying a newer revision
    survives an older signal, and a strictly newer revision resets retry state so
    the fresh desired state is attempted immediately.
    """
    db.execute("""INSERT INTO semantic_model_work(model,record_id,seq,attempts,available_at,updated_at)
        VALUES(?,?,?,0,0,?) ON CONFLICT(model,record_id) DO UPDATE SET
        seq=excluded.seq,attempts=0,available_at=0,updated_at=excluded.updated_at
        WHERE semantic_model_work.seq<=excluded.seq""", (model, record_id, seq, now()))


def _ensure_seq_singleton(db):
    """Establish the revision clock as a schema invariant: the singleton row exists
    whenever queue setup completes, even for an archive with no records or signals.

    A new archive starts at zero. Repairing never lowers an existing clock: it is
    seeded from the highest durable revision across the work, history and progress
    tables, so the next signal is strictly newer than anything already journalled.
    """
    durable = db.execute("""SELECT COALESCE(MAX(seq),0) FROM (
            SELECT seq FROM semantic_work UNION ALL
            SELECT seq FROM semantic_history UNION ALL
            SELECT seq FROM semantic_progress)""").fetchone()[0]
    if db.execute("SELECT 1 FROM semantic_seq WHERE id=1").fetchone() is None:
        db.execute("INSERT INTO semantic_seq VALUES(1,?)", (durable,))
    else:
        db.execute("UPDATE semantic_seq SET value=max(value,?) WHERE id=1", (durable,))


def ensure_schema(db):
    """Create the queue schema and migrate the legacy two-kinds-per-record form.

    The migration is shape-detected and transactional: the newest legacy signal
    per record survives as that record's desired state, the legacy rowid order
    becomes the revision order, and existing per-model bootstrap markers seed
    progress so an upgraded archive neither re-bootstraps nor loses backlog.
    A legacy queue is reshaped before any index references the new seq column.
    No executescript is used, so setup can never implicitly commit a transaction
    the caller owns; when no caller transaction is open, the migration owns and
    commits its own. Either way, one atomic pass leaves the revision singleton
    in place; an injected mid-migration failure rolls back and simply reruns.
    """
    # Join the caller's transaction when one is already open; otherwise own the
    # migration transaction and commit it before returning.
    owned = not db.in_transaction
    if owned:
        db.execute("BEGIN IMMEDIATE")
    legacy = (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='semantic_work'").fetchone()
              and "seq" not in {r[1] for r in db.execute("PRAGMA table_info(semantic_work)")})
    if legacy:
        db.execute("""CREATE TABLE semantic_work_v2(
            record_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
            seq INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            available_at REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL)""")
        db.execute("""INSERT INTO semantic_work_v2(record_id,kind,seq,attempts,available_at,updated_at)
            SELECT record_id,kind,id,attempts,available_at,updated_at FROM (
            SELECT * FROM semantic_work ORDER BY id DESC) GROUP BY record_id""")
        db.execute("DROP TABLE semantic_work")
        db.execute("ALTER TABLE semantic_work_v2 RENAME TO semantic_work")
    for statement in _WORK_STATEMENTS:
        db.execute(statement)
    _ensure_seq_singleton(db)
    if legacy:
        # The migrated seq values already bound the durable revisions; bootstrap
        # markers seed per-model progress at the clock so an upgrade never rescans.
        db.execute("INSERT OR IGNORE INTO semantic_progress(model,seq,updated_at)"
                   " SELECT model,(SELECT value FROM semantic_seq WHERE id=1),? FROM semantic_bootstrap",
                   (now(),))
    if owned:
        db.commit()


class FastEmbedder:
    def __init__(self, config):
        from fastembed import TextEmbedding
        self.model = TextEmbedding(config.get("model", DEFAULT_MODEL),
                                   cache_dir=config.get("cache_dir"), threads=config.get("threads", 2),
                                   specific_model_path=config.get("model_path"))
        model_root=Path(self.model.model._model_dir)
        weights=hashlib.sha256()
        for file in sorted(model_root.rglob("*.onnx")):
            with file.open("rb") as stream:
                while block:=stream.read(1024*1024):weights.update(block)
        for name in ("tokenizer.json","config.json","tokenizer_config.json"):
            file=model_root/name
            if file.exists():weights.update(file.read_bytes())
        self.key = digest([config.get("model", DEFAULT_MODEL),weights.hexdigest(), importlib.metadata.version("fastembed"),
                           "token-chunks-v1", config.get("revision", "operator-default")])
        self.lock = threading.Lock()
        self.tokenizer = self.model.model.tokenizer

    def chunks(self, text):
        # Fit the actual tokenizer window, including special tokens. Offsets are
        # original Unicode character spans; overlap retains sentence boundaries.
        with self.lock:
            size = max(16, min(384, self.tokenizer.truncation["max_length"] - 8)) if self.tokenizer.truncation else 120
            self.tokenizer.no_truncation()
            try:
                offsets = self.tokenizer.encode(text, add_special_tokens=False).offsets
            finally:
                self.tokenizer.enable_truncation(max_length=size + 8)
        for i in range(0, len(offsets), max(1, size - min(24, size//4))):
            start, end = offsets[i][0], offsets[min(i+size, len(offsets))-1][1]
            if end > start:
                yield start, end, text[start:end]
            if i + size >= len(offsets):
                break

    def documents(self, texts):
        with self.lock:
            return list(self.model.passage_embed(texts, batch_size=16))

    def query(self, text):
        with self.lock:
            return next(self.model.query_embed(text))


class EndpointEmbedder:
    """Reuse an operator-configured OpenAI-compatible local embedding server."""
    def __init__(self, config):
        from .client import Client
        self.client=Client(config["url"],config.get("token",""),timeout=config.get("timeout",20))
        self.name=config["model"]
        self.window=int(config.get("chunk_chars",1000))
        if not 200<=self.window<=12000: raise ValueError("chunk_chars must be 200..12000")
        self.query_prefix=config.get("query_prefix","")
        self.passage_prefix=config.get("passage_prefix","")
        self.key=digest([self.name,config["url"],config.get("revision","operator-default"),self.window,self.query_prefix,self.passage_prefix])
    def chunks(self,text):
        for start in range(0,len(text),self.window-100):
            end=min(len(text),start+self.window)
            yield start,end,text[start:end]
            if end==len(text): break
    def _embed(self,texts):
        data=self.client.call("/embeddings",{"model":self.name,"input":texts,"encoding_format":"float"})
        rows=sorted(data["data"],key=lambda r:r["index"])
        if [r["index"] for r in rows]!=list(range(len(texts))): raise ValueError("Embedding indices/count mismatch")
        return [r["embedding"] for r in rows]
    def documents(self,texts):
        out=[]
        for start in range(0,len(texts),16): out.extend(self._embed([self.passage_prefix+t for t in texts[start:start+16]]))
        return out
    def query(self,text): return self._embed([self.query_prefix+text])[0]


class SemanticIndex:
    def __init__(self, store, config=None, embedder=None):
        import numpy as np
        self.np, self.store = np, store
        config=config or {}
        self.embedder = embedder or (EndpointEmbedder(config) if config.get("provider")=="http" else FastEmbedder(config))
        self.key = self.embedder.key
        self.lock, self.sync_lock = threading.RLock(), threading.Lock()
        self.index, self.rows, self.vectors = None, {}, {}
        self._index_unhealthy = False
        self.last_error = None
        self.dimension = None
        self.minimum_similarity=config.get("minimum_similarity")
        try:
            import hnswlib
            self.hnsw = hnswlib
        except ImportError:
            self.hnsw = None
        with store.lock, store.connect() as db:
            for statement in (
                    """CREATE TABLE IF NOT EXISTS vector_failures(
                  model TEXT,record_id TEXT,error TEXT,attempts INTEGER,next_retry REAL,PRIMARY KEY(model,record_id))""",
                    """CREATE TABLE IF NOT EXISTS vector_chunks(
                  id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT NOT NULL,
                  record_id TEXT NOT NULL REFERENCES records(id), start INTEGER NOT NULL,
                  end INTEGER NOT NULL, vector BLOB NOT NULL, UNIQUE(model,record_id,start))""",
                    """CREATE TABLE IF NOT EXISTS vector_done(
                  model TEXT NOT NULL, record_id TEXT NOT NULL REFERENCES records(id),
                  PRIMARY KEY(model,record_id))"""):
                db.execute(statement)
            ensure_schema(db)
            # Activating this model revision reconciles its position in the
            # durable change history inside one transaction: an interrupted
            # activation rolls back and simply reruns on the next open.
            db.execute("BEGIN IMMEDIATE")
            self._activate(db)
        with store.connect() as db:
            for row in db.execute("SELECT v.* FROM vector_chunks v JOIN records r ON r.id=v.record_id WHERE v.model=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)", (self.key,)):
                self._add(dict(row))

    def _activate(self, db):
        """Register this model revision and establish durable coverage for every
        revision the archive has ever seen.

        Distribution fans shared signals out to all registered models, so a
        registered revision can never lose work while it is inactive; a revision
        seen for the first time (including keys harvested from legacy progress,
        bootstrap and vector metadata) is reconciled against canonical state
        exactly once before it is declared covered. An interrupted registration
        rolls back with this transaction and simply reruns on the next open.
        Activation is not a completion watermark: unfinished per-model work
        survives reopen and resumes where it left off.
        """
        registered = now()
        db.execute("INSERT OR IGNORE INTO semantic_models(model,registered_at,registration_complete) VALUES(?,?,0)",
                   (self.key, registered))
        db.execute("""INSERT OR IGNORE INTO semantic_models(model,registered_at,registration_complete)
            SELECT model,?,0 FROM (SELECT model FROM semantic_progress UNION
            SELECT model FROM semantic_bootstrap UNION SELECT DISTINCT model FROM vector_done
            UNION SELECT DISTINCT model FROM vector_chunks)""", (registered,))
        for row in db.execute("SELECT model FROM semantic_models WHERE registration_complete=0 ORDER BY model").fetchall():
            # One-time reconciliation per known model: the legacy watermark may
            # already claim progress for work that was never completed.
            self._reconcile_model(db, row["model"])
            db.execute("UPDATE semantic_models SET registration_complete=1 WHERE model=?", (row["model"],))
        # The clock position is recorded for legacy inspection only; correctness
        # no longer depends on replaying the (pruned, diagnostic) change history.
        current = db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]
        db.execute("""INSERT INTO semantic_progress VALUES(?,?,?)
            ON CONFLICT(model) DO UPDATE SET seq=excluded.seq,updated_at=excluded.updated_at""",
                   (self.key, current, now()))
        db.execute("DELETE FROM semantic_history WHERE seq<?", (current - HISTORY_RETENTION,))

    def _reconcile_model(self, db, model):
        """Bounded canonical diff for one registered revision: adopt live records
        that lack durable completion under this model, and enqueue the global
        retirement of this model's vectors whose record is gone.

        Obligations land directly in the model's own queue - never the shared
        inbox - so registering one revision does not re-request work from the
        others. Revisions come from the same monotonic clock, so acknowledgment
        guards stay comparable. Valid durable vectors are preserved: only missing
        completion is enqueued. Never journals new history.
        """
        for row in db.execute("""SELECT r.id FROM records r WHERE r.deleted=0
                AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)
                AND NOT EXISTS(SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)""",
                              (model,)).fetchall():
            _model_upsert(db, model, row["id"], _next_seq(db))
        for row in db.execute("""SELECT DISTINCT v.record_id FROM
                (SELECT record_id FROM vector_chunks WHERE model=?
                 UNION SELECT record_id FROM vector_done WHERE model=?) v
                WHERE NOT EXISTS(SELECT 1 FROM records r WHERE r.id=v.record_id AND r.deleted=0
                AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1))""",
                (model, model)).fetchall():
            _signal(db, "retire", row["record_id"], journal=False)

    def _add(self, row):
        np = self.np
        vector = np.frombuffer(row["vector"], dtype=np.float32).copy()
        if not np.isfinite(vector).all() or not np.linalg.norm(vector):
            raise ValueError("Invalid embedding")
        vector /= np.linalg.norm(vector)
        if self.dimension is not None and len(vector)!=self.dimension:raise ValueError("Embedding dimension changed; select a new model revision")
        self.dimension=len(vector)
        label = row["id"]
        bookkeeping = (row["record_id"], row["start"], row["end"])
        with self.lock:
            if self.hnsw:
                self._insert_accelerator(label, vector)
            else:
                self.vectors[label] = vector
            # In-memory bookkeeping is published only after the backend holds the
            # item, so membership in the row map is trustworthy proof of
            # publication; a failed insertion leaves nothing half-published.
            self.rows[label] = bookkeeping

    def _insert_accelerator(self, label, vector):
        """Insert one vector into the running accelerator, under the index lock.

        A freshly built accelerator is adopted only after initialization and
        thread configuration succeed, so a broken object is never published.
        Any resize or insertion error is treated as ambiguous - the backend may
        mutate before it throws - so the running accelerator is marked
        unhealthy and the caller leaves the record genuinely unpublished and
        retryable; the next publication rebuilds it from durable chunks rather
        than inserting into a suspect object.
        """
        if self.index is None:
            index = self.hnsw.Index(space="cosine", dim=len(vector))
            index.init_index(max_elements=1024, ef_construction=160, M=24, random_seed=17)
            index.set_num_threads(2)
            self.index = index
        try:
            if self.index.get_current_count() >= self.index.get_max_elements():
                self.index.resize_index(self.index.get_max_elements()*2)
            self.index.add_items(vector.reshape(1,-1), [label])
        except Exception:
            self._index_unhealthy = True
            raise

    def _retire_record(self, db, record_id):
        """Erase every vector for one record across all model revisions.

        Retirement work is only created by canonical mutators, so processing a
        row is a targeted delete; no archive-wide sweep is required or run.
        Superseded per-model obligations and every model's completion/failure
        markers for the record are cancelled in the same transaction, so no
        revision later "finishes" work for evidence that is gone. A newer
        desired state on the shared row (a restore that raced ahead of
        processing) lives on the queue itself and is acknowledged only by
        revision, so it survives this erasure and re-creates fresh obligations.
        """
        db.execute("DELETE FROM vector_chunks WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_done WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_failures WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM semantic_model_work WHERE record_id=?", (record_id,))
        with self.lock:
            for label in [label for label, row in self.rows.items() if row[0] == record_id]:
                if self.index: self.index.mark_deleted(label)
                self.rows.pop(label, None); self.vectors.pop(label, None)

    def sync(self, batch=16):
        with self.sync_lock:
            processed = 0
            due = time.time()
            # Durable retirement work runs first: bounded, targeted erases that
            # span every model revision and cancel the obligations they supersede.
            with self.store.lock, self.store.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                retire_rows = db.execute(
                    "SELECT record_id,seq FROM semantic_work WHERE kind='retire' AND available_at<=?"
                    " ORDER BY seq LIMIT ?", (due, batch)).fetchall()
                for row in retire_rows:
                    self._retire_record(db, row["record_id"])
                    # Acknowledge only the revision this pass processed.
                    db.execute("DELETE FROM semantic_work WHERE record_id=? AND seq=?",
                               (row["record_id"], row["seq"]))
            processed += len(retire_rows)
            # Distribution: a shared index signal means every registered revision
            # must durably hold its own obligation before the signal is consumed.
            # With no model registered (activation never ran) the signal stays put
            # until activation establishes coverage.
            with self.store.lock, self.store.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                models = [r["model"] for r in db.execute("SELECT model FROM semantic_models")]
                if models:
                    signals = db.execute(
                        "SELECT record_id,seq FROM semantic_work WHERE kind='index' AND available_at<=?"
                        " ORDER BY seq LIMIT ?", (due, batch)).fetchall()
                    for row in signals:
                        for model in models:
                            if db.execute("SELECT 1 FROM vector_done WHERE model=? AND record_id=?",
                                          (model, row["record_id"])).fetchone():
                                continue  # never duplicate work for a durably complete immutable record
                            _model_upsert(db, model, row["record_id"], row["seq"])
                        # Obligations durable in this same transaction: the shared
                        # signal, guarded by the exact revision, may now go.
                        db.execute("DELETE FROM semantic_work WHERE record_id=? AND seq=?",
                                   (row["record_id"], row["seq"]))
            # The active model consumes only its own due work: one model's failure
            # or backoff never postpones another, and another's success never
            # erases it. Retry state lives on the model's own row.
            with self.store.connect() as db:
                rows = [dict(r) for r in db.execute(
                    "SELECT record_id,seq,attempts FROM semantic_model_work"
                    " WHERE model=? AND available_at<=? ORDER BY seq LIMIT ?", (self.key, due, batch))]
            for row in rows:
                try:
                    if self._index_record(row):
                        with self.store.connect() as db:
                            db.execute("DELETE FROM vector_failures WHERE model=? AND record_id=?",(self.key,row["record_id"]))
                except Exception as error:
                    with self.store.connect() as db:
                        db.execute("DELETE FROM vector_done WHERE model=? AND record_id=?",(self.key,row["record_id"]))
                        delay = min(3600.0, 30.0 * (2 ** row["attempts"]))
                        # A newer signal resets the row immediately; backing off
                        # the revision actually attempted must not undo it.
                        guarded = db.execute("UPDATE semantic_model_work SET attempts=attempts+1,available_at=?,updated_at=? WHERE model=? AND record_id=? AND seq=?",
                                   (time.time() + delay, now(), self.key, row["record_id"], row["seq"]))
                        if guarded.rowcount:
                            db.execute("INSERT INTO vector_failures VALUES(?,?,?,1,?) ON CONFLICT(model,record_id) DO UPDATE SET attempts=attempts+1,error=excluded.error,next_retry=excluded.next_retry",
                                       (self.key,row["record_id"],type(error).__name__,time.time()+delay))
                            db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                                       (row['record_id'],'semantic','failed',
                                        json.dumps({'error':type(error).__name__}),now()))
                processed += 1
            self.last_error = None
            return processed

    def _live(self, db, record_id):
        return bool(db.execute("""SELECT 1 FROM records r WHERE r.id=? AND r.deleted=0
            AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)""",
            (record_id,)).fetchone())

    def _rebuild_accelerator(self):
        """Reconstruct the running accelerator from durable vectors for live,
        visible records into a separate instance, then swap it in atomically.

        The suspect object is discarded because an ambiguous insertion may have
        mutated it. Only canonical-live chunks are rehydrated and their stored
        vectors are reused, so a publication-only failure never re-embeds. The
        swap happens under the index lock and replaces the row map wholesale,
        which also prunes any retired label, so a rebuild cannot resurrect
        evidence the canonical archive removed. A rebuild failure re-raises
        before the swap and leaves the accelerator unhealthy, so the triggering
        work stays retryable and readiness stays degraded.
        """
        with self.store.connect() as db:
            stored = [dict(r) for r in db.execute(
                "SELECT v.id,v.record_id,v.start,v.end,v.vector FROM vector_chunks v "
                "JOIN records r ON r.id=v.record_id WHERE v.model=? AND r.deleted=0 "
                "AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1) "
                "ORDER BY v.id", (self.key,))]
        np = self.np
        replacement = None
        rows = {}
        for row in stored:
            vector = np.frombuffer(row["vector"], dtype=np.float32).copy()
            vector /= np.linalg.norm(vector)
            if replacement is None:
                replacement = self.hnsw.Index(space="cosine", dim=len(vector))
                replacement.init_index(max_elements=1024, ef_construction=160, M=24, random_seed=17)
                replacement.set_num_threads(2)
            if replacement.get_current_count() >= replacement.get_max_elements():
                replacement.resize_index(replacement.get_max_elements()*2)
            replacement.add_items(vector.reshape(1,-1), [row["id"]])
            rows[row["id"]] = (row["record_id"], row["start"], row["end"])
        with self.lock:
            if replacement is not None:
                self.index = replacement
            self.rows = rows
            self._index_unhealthy = False

    def _publish(self, rows):
        """Add durable vectors to the running search index.

        Publication is distinct from persistence and idempotent: chunks already
        recorded in the accelerator are skipped. If an earlier insertion failed
        ambiguously, the accelerator is rebuilt from valid durable chunks first
        - reusing stored vectors, never re-embedding - so membership in the row
        map is trustworthy proof of publication again before the pending rows
        are added.
        """
        if self.hnsw and self._index_unhealthy:
            self._rebuild_accelerator()
        for row in rows:
            if row["id"] not in self.rows:
                self._add(row)

    def _index_record(self, row):
        """Resolve one model obligation against the canonical record, then bring
        the durable vectors, the running index and the model's queue to that
        decision.

        Returns True when the revision was fully consumed (indexed, retired or
        redundant). Raises on failure, which leaves the row retryable; the
        acknowledgment only ever consumes the revision actually processed, so a
        signal that arrived during embedding or publication survives - a stale
        attempt can never remove or mark complete a newer revision.
        """
        rid, seq = row["record_id"], row["seq"]
        with self.store.connect() as db:
            live = self._live(db, rid)
            done = live and db.execute("SELECT 1 FROM vector_done WHERE model=? AND record_id=?",
                                       (self.key, rid)).fetchone() is not None
            stored = []
            if live and not done:
                stored = [dict(r) for r in db.execute(
                    "SELECT id,record_id,start,end,vector FROM vector_chunks WHERE model=? AND record_id=? ORDER BY start",
                    (self.key, rid))]
            if not live:
                # Retired before this revision was ever taken: honor the newer
                # canonical state instead of indexing a ghost; the current
                # obligation for a later restoration is left to its own pass.
                with self.store.lock, self.store.connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    self._retire_record(db, rid)
                    db.execute("DELETE FROM semantic_model_work WHERE model=? AND record_id=? AND seq=?",
                               (self.key, rid, seq))
                return True
            if done or stored:
                # Durable vectors already exist: publication is idempotent and
                # never re-embeds, whether completion was recorded or a prior
                # attempt died between the chunk commit and the running index.
                published = [dict(r) for r in db.execute(
                    "SELECT id,record_id,start,end,vector FROM vector_chunks WHERE model=? AND record_id=? ORDER BY start",
                    (self.key, rid))]
            else:
                published = self._embed_record(db.execute(
                    "SELECT text FROM records WHERE id=?", (rid,)).fetchone()["text"], rid)
            self._publish(published)
        # Completion is only durable once the running index holds the vectors;
        # a publication failure above leaves the revision retryable and unacked.
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not self._live(db, rid):
                self._retire_record(db, rid)
            else:
                current = db.execute("SELECT seq FROM semantic_model_work WHERE model=? AND record_id=?",
                                     (self.key, rid)).fetchone()
                if current is None or current["seq"] == seq:
                    # Final visibility and revision check: only the obligation
                    # actually fulfilled is discharged, and only a live record
                    # is marked complete.
                    db.execute("INSERT OR IGNORE INTO vector_done VALUES(?,?)", (self.key, rid))
                    db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                               (rid, 'semantic', 'ready', None, now()))
                    db.execute("DELETE FROM semantic_model_work WHERE model=? AND record_id=? AND seq=?",
                               (self.key, rid, seq))
        return True

    def _embed_record(self, text, rid):
        """Embed, validate and persist durable vectors for one record.

        Nothing here marks completion or acknowledges the queue, so an earlier
        failure costs no derived state and a later publication failure stays
        retryable from the stored vectors without another embedding call.
        """
        chunks = list(self.embedder.chunks(text))
        vectors = self.embedder.documents([x[2] for x in chunks]) if chunks else []
        if len(vectors) != len(chunks):
            raise ValueError("Embedding count mismatch")
        dimension = self.dimension
        validated = []
        for vector in vectors:
            vector = self.np.asarray(vector, dtype=self.np.float32)
            if vector.ndim != 1 or not self.np.isfinite(vector).all() or not self.np.linalg.norm(vector):
                raise ValueError("Embedding must be a finite nonzero vector")
            if dimension is not None and len(vector) != dimension:
                raise ValueError("Embedding dimension changed; select a new model revision")
            dimension = len(vector)
            validated.append(vector)
        additions = []
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for (start, end, _), vector in zip(chunks, validated):
                cur = db.execute(
                    "INSERT OR REPLACE INTO vector_chunks(model,record_id,start,end,vector) VALUES(?,?,?,?,?)",
                    (self.key, rid, start, end, vector.tobytes()))
                additions.append({"id": cur.lastrowid, "record_id": rid, "start": start, "end": end,
                                  "vector": vector.tobytes()})
        return additions


    def candidates(self, query, limit, allowed=None):
        vector = self.np.asarray(self.embedder.query(query), dtype=self.np.float32)
        if vector.ndim!=1 or (self.dimension is not None and len(vector)!=self.dimension) or not self.np.isfinite(vector).all() or not self.np.linalg.norm(vector):
            raise ValueError("Invalid query embedding")
        vector /= self.np.linalg.norm(vector)
        with self.lock:
            eligible = None if allowed is None else {i for i,(rid,_,_) in self.rows.items() if rid in allowed}
            size=len(self.rows) if eligible is None else len(eligible)
            if not size:return []
            k = min(limit,size)
            if self.index:
                self.index.set_ef(max(100, k*3))
                labels, distances = self.index.knn_query(vector.reshape(1,-1), k=k,
                                                        filter=(None if eligible is None else lambda i: i in eligible), num_threads=1)
                ranked = zip(labels[0].tolist(), (1-distances[0]).tolist())
            else:
                ranked = sorted(((i,float(self.vectors[i] @ vector)) for i in (self.rows if eligible is None else eligible)),key=lambda x:-x[1])[:k]
            seen, results = set(), []
            for label, score in ranked:
                rid,start,end = self.rows[label]
                if self.minimum_similarity is not None and score<self.minimum_similarity:continue
                if rid not in seen:
                    results.append({"id":rid,"similarity":score,"span_start":start,"span_end":end})
                    seen.add(rid)
            return results

    def status(self):
        with self.store.connect() as db:
            # Pending is queue-backed: distribution makes every registered
            # revision's obligation durable before the shared signal goes, so a
            # pending count that ignored the model queues would report finished
            # work that was never done. The union counts each record once even
            # while a distribution transaction is mid-flight.
            remaining = db.execute("""SELECT count(*) FROM (
                SELECT record_id FROM semantic_model_work WHERE model=?
                UNION SELECT record_id FROM semantic_work WHERE kind='index')""",
                (self.key,)).fetchone()[0]
            # Unprocessed retirements mean durable vectors may still be serving
            # a record the canonical archive no longer wants indexed.
            retirements = db.execute("SELECT count(*) FROM semantic_work WHERE kind='retire'").fetchone()[0]
            # A revision still awaiting its one-time reconciliation is coverage
            # this archive cannot claim yet.
            registrations = db.execute("SELECT count(*) FROM semantic_models WHERE registration_complete=0").fetchone()[0]
            failures=db.execute("SELECT count(*) FROM vector_failures f JOIN records r ON r.id=f.record_id WHERE f.model=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)",(self.key,)).fetchone()[0]
        return {"enabled":True,"model_key":self.key,"failed_records":failures,"index":"hnsw" if self.hnsw else "exact_numpy",
                "pending_records":remaining,"pending_retirements":retirements,
                "pending_registrations":registrations,
                "accelerator_healthy":not self._index_unhealthy,
                "ready":remaining==0 and retirements==0 and registrations==0 and self.last_error is None and not self._index_unhealthy,
                "error":self.last_error}
