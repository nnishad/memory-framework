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

# Durable incremental work queue: canonical writers enqueue index/retire rows
# inside their own transaction, so background polling never rescans the archive.
WORK_SCHEMA = """
CREATE TABLE IF NOT EXISTS semantic_work(
  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
  record_id TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  available_at REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
  UNIQUE(kind, record_id));
CREATE INDEX IF NOT EXISTS semantic_work_due ON semantic_work(kind, available_at, id);
CREATE TABLE IF NOT EXISTS semantic_bootstrap(
  model TEXT PRIMARY KEY, enqueued_at TEXT NOT NULL);
"""


def enqueue(db, kind, record_ids):
    """Request semantic work for records inside the caller's transaction.

    Coalesces on (kind, record_id): repeated signals for the same record are
    one bounded unit of work, never duplicate embeddings.
    """
    for rid in record_ids:
        db.execute("INSERT OR IGNORE INTO semantic_work(kind,record_id,updated_at) VALUES(?,?,?)",
                   (kind, rid, now()))


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
            db.executescript(WORK_SCHEMA)
            # Bootstrap an archive created before the queue exactly once per model
            # revision; later opens consult only the durable marker.
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM semantic_bootstrap WHERE model=?", (self.key,)).fetchone():
                db.execute("""INSERT OR IGNORE INTO semantic_work(kind,record_id,updated_at)
                    SELECT 'index', r.id, ? FROM records r WHERE r.deleted=0
                    AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)
                    AND NOT EXISTS(SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)""",
                           (now(), self.key))
                db.execute("""INSERT OR IGNORE INTO semantic_work(kind,record_id,updated_at)
                    SELECT DISTINCT 'retire', v.record_id, ? FROM
                    (SELECT record_id FROM vector_chunks UNION SELECT record_id FROM vector_done) v
                    WHERE NOT EXISTS(SELECT 1 FROM records r WHERE r.id=v.record_id AND r.deleted=0
                    AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1))""",
                           (now(),))
                db.execute("INSERT OR IGNORE INTO semantic_bootstrap VALUES(?,?)", (self.key, now()))
        with store.connect() as db:
            for row in db.execute("SELECT v.* FROM vector_chunks v JOIN records r ON r.id=v.record_id WHERE v.model=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)", (self.key,)):
                self._add(dict(row))

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
        Index rows enqueued before this retirement are dropped as superseded;
        newer index requests (a restore that raced ahead of processing) survive
        and re-embed from scratch.
        """
        db.execute("DELETE FROM vector_chunks WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_done WHERE record_id=?", (record_id,))
        db.execute("DELETE FROM vector_failures WHERE record_id=?", (record_id,))
        cur = db.execute("SELECT id FROM semantic_work WHERE kind='retire' AND record_id=?", (record_id,)).fetchone()
        db.execute("DELETE FROM semantic_work WHERE kind='index' AND record_id=? AND id<?",
                   (record_id, cur[0] if cur else 1 << 62))
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
                    "SELECT id,record_id FROM semantic_work WHERE kind='retire' AND available_at<=?"
                    " ORDER BY id LIMIT ?", (due, batch)).fetchall()
                for row in retire_rows:
                    self._retire_record(db, row["record_id"])
                    db.execute("DELETE FROM semantic_work WHERE id=?", (row["id"],))
            processed += len(retire_rows)
            # Index work due now; retry state and backoff live on the queue row.
            with self.store.connect() as db:
                rows = [dict(r) for r in db.execute("""SELECT w.id AS work,w.attempts,r.id,r.text
                    FROM semantic_work w JOIN records r ON r.id=w.record_id
                    WHERE w.kind='index' AND w.available_at<=? AND r.deleted=0
                    AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)
                    AND NOT EXISTS(SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)
                    ORDER BY w.id LIMIT ?""", (due, self.key, batch))]
            for row in rows:
                try:
                    self._index_record(row)
                    with self.store.connect() as db:db.execute("DELETE FROM vector_failures WHERE model=? AND record_id=?",(self.key,row["id"]))
                except Exception as error:
                    with self.store.connect() as db:
                        db.execute("DELETE FROM vector_done WHERE model=? AND record_id=?",(self.key,row["id"]))
                        delay = min(3600.0, 30.0 * (2 ** row["attempts"]))
                        db.execute("UPDATE semantic_work SET attempts=attempts+1,available_at=?,updated_at=? WHERE id=?",
                                   (time.time() + delay, now(), row["work"]))
                        db.execute("INSERT INTO vector_failures VALUES(?,?,?,1,?) ON CONFLICT(model,record_id) DO UPDATE SET attempts=attempts+1,error=excluded.error,next_retry=excluded.next_retry",
                                   (self.key,row["id"],type(error).__name__,time.time()+delay))
                        db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                                   (row['id'],'semantic','failed',
                                    json.dumps({'error':type(error).__name__}),now()))
                processed += 1
            self.last_error = None
            return processed

    def _index_record(self,row):
        chunks = list(self.embedder.chunks(row["text"]))
        vectors = self.embedder.documents([x[2] for x in chunks]) if chunks else []
        if len(vectors) != len(chunks):
            raise ValueError("Embedding count mismatch")
        normalized=[]
        dimension=self.dimension
        for vector in vectors:
            vector=self.np.asarray(vector,dtype=self.np.float32)
            if vector.ndim!=1 or not self.np.isfinite(vector).all() or not self.np.linalg.norm(vector):
                raise ValueError("Embedding must be a finite nonzero vector")
            if dimension is not None and len(vector)!=dimension:
                raise ValueError("Embedding dimension changed; select a new model revision")
            dimension=len(vector)
            normalized.append(vector)
        additions = []
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM records r WHERE r.id=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)", (row["id"],)).fetchone():
                # Retired while embedding ran: publish nothing and drop the request.
                db.execute("DELETE FROM semantic_work WHERE id=?", (row["work"],))
                return
            for (start,end,_), vector in zip(chunks,normalized):
                vector = self.np.asarray(vector, dtype=self.np.float32)
                if vector.ndim != 1 or not self.np.isfinite(vector).all() or not self.np.linalg.norm(vector):
                    raise ValueError("Embedding must be a finite nonzero vector")
                cur = db.execute("INSERT OR REPLACE INTO vector_chunks(model,record_id,start,end,vector) VALUES(?,?,?,?,?)",
                                 (self.key,row["id"],start,end,vector.tobytes()))
                additions.append({"id":cur.lastrowid,"record_id":row["id"],"start":start,"end":end,"vector":vector.tobytes()})
            db.execute("INSERT OR IGNORE INTO vector_done VALUES(?,?)", (self.key,row["id"]))
            db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                       (row['id'],'semantic','ready',None,now()))
            # The queue row and the vectors commit (or roll back) together.
            db.execute("DELETE FROM semantic_work WHERE id=?", (row["work"],))
        for addition in additions:
            self._add(addition)

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
            failures=db.execute("SELECT count(*) FROM vector_failures f JOIN records r ON r.id=f.record_id WHERE f.model=? AND r.deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility z WHERE z.record_id=r.id AND z.hidden=1)",(self.key,)).fetchone()[0]
        return {"enabled":True,"model_key":self.key,"failed_records":failures,"index":"hnsw" if self.hnsw else "exact_numpy",
                "pending_records":remaining,"ready":remaining==0 and self.last_error is None,
                "error":self.last_error}
