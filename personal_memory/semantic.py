"""Rebuildable, chunked multilingual embeddings with a durable indexing journal.

SQLite is authoritative. HNSW is an in-memory accelerator rebuilt after restart;
no untrusted serialized Python index is loaded. Canonical filters run on every query.
"""
import importlib.metadata
import threading
import time
import hashlib
from pathlib import Path

from .common import digest

DEFAULT_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


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
        with store.connect() as db:
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
            for row in db.execute("SELECT v.* FROM vector_chunks v JOIN records r ON r.id=v.record_id WHERE v.model=? AND r.deleted=0", (self.key,)):
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

    def sync(self, batch=16):
        with self.sync_lock:
            with self.store.connect() as db:
                deleted={r[0] for r in db.execute("SELECT id FROM records WHERE deleted=1")}
                db.execute("DELETE FROM vector_chunks WHERE record_id IN (SELECT id FROM records WHERE deleted=1)")
                with self.lock:
                    for label in [label for label,row in self.rows.items() if row[0] in deleted]:
                        if self.index:self.index.mark_deleted(label)
                        self.rows.pop(label,None);self.vectors.pop(label,None)
                rows = [dict(r) for r in db.execute("""SELECT r.id,r.text FROM records r WHERE deleted=0
                    AND NOT EXISTS(SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)
                    AND NOT EXISTS(SELECT 1 FROM vector_failures f WHERE f.model=? AND f.record_id=r.id AND f.next_retry>?)
                    ORDER BY r.ingested_at,r.id LIMIT ?""", (self.key,self.key,time.time(),batch))]
            for row in rows:
                try:
                    self._index_record(row)
                    with self.store.connect() as db:db.execute("DELETE FROM vector_failures WHERE model=? AND record_id=?",(self.key,row["id"]))
                except Exception as error:
                    with self.store.connect() as db:
                        db.execute("DELETE FROM vector_done WHERE model=? AND record_id=?",(self.key,row["id"]))
                        db.execute("INSERT INTO vector_failures VALUES(?,?,?,1,?) ON CONFLICT(model,record_id) DO UPDATE SET attempts=attempts+1,error=excluded.error,next_retry=excluded.next_retry",
                                   (self.key,row["id"],type(error).__name__,time.time()+30))
            self.last_error = None
            return len(rows)

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
            if not db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0", (row["id"],)).fetchone():
                return
            for (start,end,_), vector in zip(chunks,normalized):
                vector = self.np.asarray(vector, dtype=self.np.float32)
                if vector.ndim != 1 or not self.np.isfinite(vector).all() or not self.np.linalg.norm(vector):
                    raise ValueError("Embedding must be a finite nonzero vector")
                cur = db.execute("INSERT OR REPLACE INTO vector_chunks(model,record_id,start,end,vector) VALUES(?,?,?,?,?)",
                                 (self.key,row["id"],start,end,vector.tobytes()))
                additions.append({"id":cur.lastrowid,"record_id":row["id"],"start":start,"end":end,"vector":vector.tobytes()})
            db.execute("INSERT OR IGNORE INTO vector_done VALUES(?,?)", (self.key,row["id"]))
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
            remaining = db.execute("""SELECT count(*) FROM records r WHERE deleted=0 AND NOT EXISTS(
                SELECT 1 FROM vector_done d WHERE d.model=? AND d.record_id=r.id)""", (self.key,)).fetchone()[0]
            failures=db.execute("SELECT count(*) FROM vector_failures f JOIN records r ON r.id=f.record_id WHERE f.model=? AND r.deleted=0",(self.key,)).fetchone()[0]
        return {"enabled":True,"model_key":self.key,"failed_records":failures,"index":"hnsw" if self.hnsw else "exact_numpy",
                "pending_records":remaining,"ready":remaining==0 and self.last_error is None,
                "error":self.last_error}
