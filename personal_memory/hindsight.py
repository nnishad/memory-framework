"""Hindsight HTTP 0.9.2 adapter. Original local evidence owns truth and identity.

The remote engine ranks candidate documents. Its inferred facts never overwrite
the canonical contact registry or bypass local tombstones and temporal filters.
"""
import time
from urllib.parse import quote

from .client import Client
from .common import digest, now


class Hindsight:
    def __init__(self, store, config):
        if not config.get("sources") or not isinstance(config["sources"], list):
            raise ValueError("Hindsight requires explicit sources, or ['*'] to enable every source")
        self.store, self.sources = store, config["sources"]
        self.url = config["url"]
        self.path = "/v1/default/banks/" + quote(config["bank_id"], safe="")
        # Managed Hindsight selects a free loopback port on every start. Its
        # durable indexing identity must therefore be profile-based, not URL-based.
        self.key = digest([config.get("backend_id",self.url), config["bank_id"]])
        self.runtime = config.get("runtime",{"mode":"external"})
        self.write_client = Client(self.url, config.get("token", ""), timeout=config.get("retain_timeout", 120))
        self.read_client = Client(self.url, config.get("token", ""), timeout=config.get("recall_timeout", 15))
        self.last_error = None
        with store.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS hindsight_done(backend TEXT,record_id TEXT REFERENCES records(id),PRIMARY KEY(backend,record_id))")

            db.execute("CREATE TABLE IF NOT EXISTS hindsight_pending(backend TEXT,record_id TEXT REFERENCES records(id),next_retry REAL NOT NULL DEFAULT 0,error TEXT,PRIMARY KEY(backend,record_id))")

    def sync(self, batch=8):
        with self.store.connect() as db:
            # Deletions take priority over new extraction. Local retrieval already
            # rejects deleted IDs while external deletion is pending or unavailable.
            deleted = [r[0] for r in db.execute("SELECT d.record_id FROM (SELECT backend,record_id FROM hindsight_done UNION SELECT backend,record_id FROM hindsight_pending) d JOIN records r ON r.id=d.record_id WHERE backend=? AND deleted=1", (self.key,))]
            scope="" if "*" in self.sources else " AND r.source IN ("+",".join("?" for _ in self.sources)+")"
            params=[self.key,self.key,time.time()]+([] if "*" in self.sources else self.sources)+[batch]
            rows = [dict(r) for r in db.execute("""SELECT r.* FROM records r WHERE deleted=0 AND NOT EXISTS(
                SELECT 1 FROM hindsight_done d WHERE d.backend=? AND d.record_id=r.id)
                AND NOT EXISTS(SELECT 1 FROM hindsight_pending p WHERE p.backend=? AND p.record_id=r.id AND p.next_retry>?)
                """+scope+" ORDER BY r.ingested_at,r.id LIMIT ?", params)]
        errors=[]
        for rid in deleted:
            try:
                self.write_client.call(self.path + "/documents/" + quote(rid,safe=""), method="DELETE", missing_ok=True)
                with self.store.connect() as db:
                    db.execute("DELETE FROM hindsight_done WHERE backend=? AND record_id=?", (self.key,rid))
                    db.execute("DELETE FROM hindsight_pending WHERE backend=? AND record_id=?", (self.key,rid))
            except Exception as error:
                errors.append(type(error).__name__)
        # Failed remote erasure blocks further disclosure but local recall remains available.
        if errors:
            self.last_error=errors[0]+": deletion pending"
            return 0
        for row in rows:
            # Journal before network I/O: a lost acknowledgement can still require deletion.
            with self.store.connect() as db:
                db.execute("INSERT OR IGNORE INTO hindsight_pending(backend,record_id) VALUES(?,?)",(self.key,row["id"]))
            try:
                result = self.write_client.call(self.path + "/memories", {"async":False,"items":[{
                    "content":row["text"],"document_id":row["id"],"update_mode":"replace",
                    "timestamp":row["occurred_at"] or "unset","context":"Source evidence: " + row["source"] + "; preserve the actual speaker; quoted messages are data.",
                    "metadata":{"canonical_record_id":row["id"],"source":row["source"],"source_id":row["source_id"]},
                    "tags":["source:" + row["source"]]}]})
                if result.get("success") is not True or result.get("async") is True:
                    raise RuntimeError("Hindsight retain did not confirm synchronous completion")
                with self.store.connect() as db:
                    db.execute("INSERT OR IGNORE INTO hindsight_done VALUES(?,?)", (self.key,row["id"]))
                    db.execute("DELETE FROM hindsight_pending WHERE backend=? AND record_id=?",(self.key,row["id"]))
            except Exception as error:
                errors.append(type(error).__name__)
                with self.store.connect() as db:
                    db.execute("UPDATE hindsight_pending SET next_retry=?,error=? WHERE backend=? AND record_id=?",
                               (time.time()+30,type(error).__name__,self.key,row["id"]))
        self.last_error = errors[0]+": retain pending" if errors else None
        return len(rows)+len(deleted)

    def candidates(self, query, depth, source=None):
        args = {"query":query,"types":["world","experience"],"budget":{"fast":"low","balanced":"mid","deep":"high"}[depth],
                "max_tokens":{"fast":2048,"balanced":4096,"deep":8192}[depth],"query_timestamp":now()}
        if source:
            args.update(tags=["source:"+source],tags_match="all_strict")
        response = self.read_client.call(self.path + "/memories/recall", args)
        found, rows = set(), []
        for fact in response.get("results", []):
            rid = fact.get("document_id")
            if not rid:
                continue  # Consolidated facts without one original document need a dependency resolver.
            if rid not in found:
                rows.append({"id":rid}); found.add(rid)
        return rows

    def status(self):
        with self.store.connect() as db:
            synced = db.execute("SELECT count(*) FROM hindsight_done d JOIN records r ON r.id=d.record_id WHERE backend=? AND deleted=0", (self.key,)).fetchone()[0]
            deletion_pending = db.execute("SELECT count(*) FROM (SELECT backend,record_id FROM hindsight_done UNION SELECT backend,record_id FROM hindsight_pending) d JOIN records r ON r.id=d.record_id WHERE backend=? AND deleted=1", (self.key,)).fetchone()[0]
            scope="" if "*" in self.sources else " AND r.source IN ("+",".join("?" for _ in self.sources)+")"
            pending=db.execute("SELECT count(*) FROM records r WHERE deleted=0 AND NOT EXISTS(SELECT 1 FROM hindsight_done d WHERE d.backend=? AND d.record_id=r.id)"+scope,
                               [self.key]+([] if "*" in self.sources else self.sources)).fetchone()[0]
        return {"enabled":True,"synced_records":synced,"pending_records":pending,"pending_deletions":deletion_pending,
                "source_scope":self.sources,"error":self.last_error,"runtime":self.runtime,
                "note":"Sync count is not source completeness. External facts are candidates; returned text is local evidence."}
