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
# One row per record carries a monotonically increasing revision: coalescing
# overwrites the superseded signal instead of queueing a second one, and every
# acknowledgment is guarded by the revision it processed, so work that arrives
# during processing survives. An append-only change history lets each model
# revision keep its own progress instead of a permanent bootstrap marker.
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
"""

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


def ensure_schema(db):
    """Create the queue schema and migrate the legacy two-kinds-per-record form.

    The migration is shape-detected and transactional: the newest legacy signal
    per record survives as that record's desired state, the legacy rowid order
    becomes the revision order, and existing per-model bootstrap markers seed
    progress so an upgraded archive neither re-bootstraps nor loses backlog.
    """
    db.executescript(WORK_SCHEMA)
    if "seq" in {r[1] for r in db.execute("PRAGMA table_info(semantic_work)")}:
        return
    # Join the caller's transaction when one is already open; otherwise own the
    # migration transaction and commit it before returning.
    owned = not db.in_transaction
    if owned:
        db.execute("BEGIN IMMEDIATE")
    db.execute("""CREATE TABLE semantic_work_v2(
        record_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
        seq INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL)""")
    db.execute("""INSERT INTO semantic_work_v2(record_id,kind,seq,attempts,available_at,updated_at)
        SELECT record_id,kind,id,attempts,available_at,updated_at FROM (
        SELECT * FROM semantic_work ORDER BY id DESC) GROUP BY record_id""")
    top = db.execute("SELECT COALESCE(MAX(id),0) FROM semantic_work").fetchone()[0]
    db.execute("DROP TABLE semantic_work")
    db.execute("ALTER TABLE semantic_work_v2 RENAME TO semantic_work")
    db.execute("CREATE INDEX IF NOT EXISTS semantic_work_due ON semantic_work(kind, available_at, seq)")
    db.execute("INSERT OR IGNORE INTO semantic_seq VALUES(1,?)", (top,))
    db.execute("UPDATE semantic_seq SET value=max(value,?) WHERE id=1", (top,))
    db.execute("INSERT OR IGNORE INTO semantic_progress(model,seq,updated_at) SELECT model,?,? FROM semantic_bootstrap",
               (top, now()))
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
        self.last_error = None
        self.dimension = None
        self.minimum_similarity=config.get("minimum_similarity")
        try:
            import hnswlib
            self.hnsw = hnswlib
        except ImportError:
            self.hnsw = None
        with store.lock, store.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS vector_failures(
                  model TEXT,record_id TEXT,error TEXT,attempts INTEGER,next_retry REAL,PRIMARY KEY(model,record_id));
                CREATE TABLE IF NOT EXISTS vector_chunks(
                  id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT NOT NULL,
                  record_id TEXT NOT NULL REFERENCES records(id), start INTEGER NOT NULL,
                  end INTEGER NOT NULL, vector BLOB NOT NULL, UNIQUE(model,record_id,start));
                CREATE TABLE IF NOT EXISTS vector_done(
                  model TEXT NOT NULL, record_id TEXT NOT NULL REFERENCES records(id),
                  PRIMARY KEY(model,record_id));
            """)
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
        """Bring the durable queue to this model revision's desired state.

        The work queue is shared across model revisions while completion is
        per-revision, so each model tracks its own position in the append-only
        change history. Activation replays exactly the changes that arrived
        since that position - no archive rescan per open, and no permanent
        bootstrap assumption across model switches. Only a brand-new revision
        or a history-starved one pays the bounded reconciliation diff.
        """
        row = db.execute("SELECT seq FROM semantic_progress WHERE model=?", (self.key,)).fetchone()
        oldest = db.execute("SELECT MIN(seq) FROM semantic_history").fetchone()[0]
        if row is None or (oldest is not None and oldest > row[0] + 1):
            self._reconcile(db)
        else:
            for change in db.execute("""SELECT h.record_id,h.kind FROM semantic_history h
                    WHERE h.seq>? AND h.seq=(SELECT MAX(seq) FROM semantic_history m
                                               WHERE m.record_id=h.record_id)
                    ORDER BY h.seq""", (row[0],)).fetchall():
                _signal(db, change["kind"], change["record_id"], journal=False)
        current = db.execute("SELECT value FROM semantic_seq").fetchone()[0]
        db.execute("""INSERT INTO semantic_progress VALUES(?,?,?)
            ON CONFLICT(model) DO UPDATE SET seq=excluded.seq,updated_at=excluded.updated_at""",
                   (self.key, current, now()))
        # Retention: never prune below the slowest tracked revision, and never
        # keep more than the bounded window; a pruned gap triggers reconciliation
        # above, so correctness survives any retention policy.
        floor = db.execute("SELECT COALESCE(MIN(seq),0) FROM semantic_progress").fetchone()[0]
        db.execute("DELETE FROM semantic_history WHERE seq<?", (max(floor, current - HISTORY_RETENTION),))

    def _reconcile(self, db):
        """Bounded canonical diff: adopt live records this revision lacks and
        retire vectors whose record is gone. Never journals new history."""
        for row in db.execute("""SELECT r.id FROM records r WHERE r.deleted=0
                AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)
                AND NOT EXISTS(SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)""",
                              (self.key,)).fetchall():
            _signal(db, "index", row["id"], journal=False)
        for row in db.execute("""SELECT DISTINCT v.record_id FROM
                (SELECT record_id FROM vector_chunks UNION SELECT record_id FROM vector_done) v
                WHERE NOT EXISTS(SELECT 1 FROM records r WHERE r.id=v.record_id AND r.deleted=0
                AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1))""").fetchall():
            _signal(db, "retire", row["record_id"], journal=False)

    def _add(self, row):
        np = self.np
        vector = np.frombuffer(row["vector"], dtype=np.float32).copy()
        if not np.isfinite(vector).all() or not np.linalg.norm(vector):
            raise ValueError("Invalid embedding")
        vector /= np.linalg.norm(vector)
        if self.dimension is not None and len(vector)!=self.dimension:raise ValueError("Embedding dimension changed; select a new model revision")
        self.dimension=len(vector)
        with self.lock:
            self.rows[row["id"]] = (row["record_id"], row["start"], row["end"])
            if self.hnsw:
                if self.index is None:
                    self.index = self.hnsw.Index(space="cosine", dim=len(vector))
                    self.index.init_index(max_elements=1024, ef_construction=160, M=24, random_seed=17)
                    self.index.set_num_threads(2)
                if self.index.get_current_count() >= self.index.get_max_elements():
                    self.index.resize_index(self.index.get_max_elements()*2)
                self.index.add_items(vector.reshape(1,-1), [row["id"]])
            else:
                self.vectors[row["id"]] = vector

    def _retire_record(self, db, record_id):
        """Erase every vector for one record across all model revisions.

        Retirement work is only created by canonical mutators, so processing a
        row is a targeted delete; no archive-wide sweep is required or run.
        A newer desired state on the same record (a restore that raced ahead of
        processing) lives on the queue row itself and is acknowledged only by
        revision, so it survives this erasure.
        """
        db.execute("DELETE FROM vector_chunks WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_done WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_failures WHERE record_id=?", (record_id,))
        with self.lock:
            for label in [label for label, row in self.rows.items() if row[0] == record_id]:
                if self.index: self.index.mark_deleted(label)
                self.rows.pop(label, None); self.vectors.pop(label, None)

    def sync(self, batch=16):
        with self.sync_lock:
            processed = 0
            due = time.time()
            # Durable retirement work runs first: bounded, targeted erases.
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
            # Index work due now; the canonical record decides what is actually
            # wanted. Retry state and backoff live on the queue row.
            with self.store.connect() as db:
                rows = [dict(r) for r in db.execute(
                    "SELECT record_id,seq,attempts FROM semantic_work"
                    " WHERE kind='index' AND available_at<=? ORDER BY seq LIMIT ?", (due, batch))]
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
                        guarded = db.execute("UPDATE semantic_work SET attempts=attempts+1,available_at=?,updated_at=? WHERE record_id=? AND seq=?",
                                   (time.time() + delay, now(), row["record_id"], row["seq"]))
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

    def _publish(self, rows):
        """Add durable vectors to the running search index.

        Publication is distinct from persistence and idempotent: chunks already
        present are skipped, so a partially published record completes on retry
        without duplicate index entries or another embedding request.
        """
        for row in rows:
            if row["id"] not in self.rows:
                self._add(row)

    def _index_record(self, row):
        """Resolve one index request against the canonical record, then bring the
        durable vectors, the running index and the queue to that decision.

        Returns True when the revision was fully consumed (indexed, retired or
        redundant). Raises on failure, which leaves the row retryable; the
        acknowledgment only ever deletes the revision actually processed, so a
        signal that arrived during embedding or publication survives.
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
                # canonical state instead of indexing a ghost.
                with self.store.lock, self.store.connect() as db:
                    db.execute("BEGIN IMMEDIATE")
                    self._retire_record(db, rid)
                    db.execute("DELETE FROM semantic_work WHERE record_id=? AND seq=?", (rid, seq))
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
                db.execute("INSERT OR IGNORE INTO vector_done VALUES(?,?)", (self.key, rid))
                db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                           (rid, 'semantic', 'ready', None, now()))
            # Acknowledge only the revision actually processed: a signal that
            # arrived during embedding or publication survives this ack.
            db.execute("DELETE FROM semantic_work WHERE record_id=? AND seq=?", (rid, seq))
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
            # Pending is queue-backed: canonical writers enqueue every change, so
            # the count stays O(backlog) instead of O(archive) per call.
            remaining = db.execute("""SELECT count(*) FROM semantic_work w WHERE w.kind='index' AND NOT EXISTS(
                SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=w.record_id)""", (self.key,)).fetchone()[0]
            # Unprocessed retirements mean durable vectors may still be serving
            # a record the canonical archive no longer wants indexed.
            retirements = db.execute("SELECT count(*) FROM semantic_work WHERE kind='retire'").fetchone()[0]
            failures=db.execute("SELECT count(*) FROM vector_failures f JOIN records r ON r.id=f.record_id WHERE f.model=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)",(self.key,)).fetchone()[0]
        return {"enabled":True,"model_key":self.key,"failed_records":failures,"index":"hnsw" if self.hnsw else "exact_numpy",
                "pending_records":remaining,"pending_retirements":retirements,
                "ready":remaining==0 and retirements==0 and self.last_error is None,
                "error":self.last_error}
