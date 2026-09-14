"""Commit locally before network delivery; retries are idempotent at the server."""
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from .common import digest, now
from . import __version__


class Outbox:
    def __init__(self, path, client):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.client = client
        self.stop = threading.Event()
        self.wake = threading.Event()
        self.delivery_lock = threading.Lock()
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS pending(id TEXT PRIMARY KEY,payload TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT,created_at TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS delivered(id TEXT PRIMARY KEY,delivered_at TEXT NOT NULL)")
            columns={r[1] for r in db.execute("PRAGMA table_info(pending)")}
            if "next_attempt" not in columns:
                db.execute("ALTER TABLE pending ADD COLUMN next_attempt REAL NOT NULL DEFAULT 0")
            if "fingerprint" not in columns:db.execute("ALTER TABLE pending ADD COLUMN fingerprint TEXT")
            if "fingerprint" not in {r[1] for r in db.execute("PRAGMA table_info(delivered)")}:
                db.execute("ALTER TABLE delivered ADD COLUMN fingerprint TEXT")
            db.execute("CREATE TABLE IF NOT EXISTS dead_letters(id TEXT PRIMARY KEY,payload TEXT NOT NULL,error TEXT NOT NULL,created_at TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS observation_receipts(
                id TEXT PRIMARY KEY,state TEXT NOT NULL,reason TEXT,record_ids TEXT NOT NULL,
                created_at TEXT NOT NULL,updated_at TEXT NOT NULL)""")
        self.path.chmod(0o600)
        self.thread = threading.Thread(target=self._run, daemon=True, name="personal-memory-outbox")
        self.thread.start()

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(self, items):
        from .ingestion import adapt_existing,validate_batch
        items=[i if "schema_version" in i else adapt_existing(i,connector_id="hermes.native",connector_version=__version__,
                source_locator="hermes://"+i["source_id"],observed_at=now()) for i in items]
        items=validate_batch(items)
        # Native captures generate a wall-clock event time on each host retry.
        # Source connectors must preserve their actual event timestamp.
        key = digest([(i["source"], i["source_id"], i.get("revision", "1")) for i in items])
        fingerprint=self._fingerprint(items)
        ids=["rec_"+digest([item["source"],item["source_id"],item["revision"]])[:32] for item in items]
        with self.connect() as db:
            if db.execute("SELECT 1 FROM dead_letters WHERE id=?",(key,)).fetchone():
                raise ValueError("Capture is quarantined; repair and replay its dead letter explicitly")
            for table in ("pending","delivered"):
                existing=db.execute("SELECT fingerprint FROM "+table+" WHERE id=?",(key,)).fetchone()
                if existing:
                    if existing[0] is not None and existing[0]!=fingerprint:raise ValueError("Outbox ID/revision reused with changed content")
                    stamp=now()
                    state="queued" if table=="pending" else "committed"
                    db.execute("INSERT OR IGNORE INTO observation_receipts VALUES(?,?,?,?,?,?)",
                               (key,state,None,json.dumps(ids),stamp,stamp))
                    return key
            db.execute("INSERT INTO pending(id,payload,created_at,fingerprint) VALUES(?,?,?,?)",
                       (key,json.dumps({"items":items, **({"epoch":self.client.write_epoch} if getattr(self.client,"write_epoch",None) is not None else {})},ensure_ascii=False),now(),fingerprint))
            stamp=now()
            db.execute("INSERT OR IGNORE INTO observation_receipts VALUES(?,?,?,?,?,?)",
                       (key,"queued",None,json.dumps(ids),stamp,stamp))
        self.wake.set()
        return key

    @staticmethod
    def _fingerprint(items):
        return digest([{k:v for k,v in item.items() if k != "observed_at" and
                       not (k == "occurred_at" and item["provenance"]["connector_id"] == "hermes.native")}
                       for item in items])

    def health(self):
        with self.connect() as db:
            return {"pending":db.execute("SELECT count(*) FROM pending").fetchone()[0],
                    "dead_letters":db.execute("SELECT count(*) FROM dead_letters").fetchone()[0],
                    "worker_alive":self.thread.is_alive(),
                    "oldest_pending":db.execute("SELECT min(created_at) FROM pending").fetchone()[0]}

    def mark_receipt(self,key,state,reason=None,record_ids=None):
        if state not in {"queued","committed","withheld","failed"}:raise ValueError("Invalid receipt state")
        with self.connect() as db:
            old=db.execute("SELECT record_ids,created_at FROM observation_receipts WHERE id=?",(key,)).fetchone()
            stamp=now();ids=json.dumps(record_ids if record_ids is not None else json.loads(old[0]) if old else [])
            db.execute("INSERT OR REPLACE INTO observation_receipts VALUES(?,?,?,?,?,?)",
                       (key,state,reason,ids,old[1] if old else stamp,stamp))

    def receipts(self,limit=50):
        with self.connect() as db:
            rows=db.execute("SELECT id,state,reason,record_ids,created_at,updated_at FROM observation_receipts ORDER BY updated_at DESC LIMIT ?",(limit,)).fetchall()
        return [{"id":row[0],"state":row[1],"reason":row[2],"record_ids":json.loads(row[3]),
                 "created_at":row[4],"updated_at":row[5]} for row in rows]

    def replay_dead_letter(self, entry_id):
        with self.delivery_lock,self.connect() as db:
            row=db.execute("SELECT payload FROM dead_letters WHERE id=?",(entry_id,)).fetchone()
            if not row: raise ValueError("Dead letter not found")
            items=json.loads(row[0])["items"]
            fingerprint=self._fingerprint(items) if all("provenance" in i for i in items) else None
            db.execute("INSERT OR IGNORE INTO pending(id,payload,created_at,fingerprint) VALUES(?,?,?,?)",(entry_id,row[0],now(),fingerprint))
            db.execute("DELETE FROM dead_letters WHERE id=?",(entry_id,))
            db.execute("UPDATE observation_receipts SET state='queued',reason=NULL,updated_at=? WHERE id=?",
                       (now(),entry_id))
        self.wake.set()

    def pending(self):
        with self.connect() as db:
            return db.execute("SELECT count(*) FROM pending").fetchone()[0]

    def _has_forgotten_parent(self, items):
        parents = sorted({p for item in items for p in item.get("provenance", {}).get("parent_record_ids", [])})
        for start in range(0, len(parents), 100):
            states = self.client.call("/v1/lineage/status", {"record_ids": parents[start:start+100]})["states"]
            if "forgotten" in states.values():
                return True
        return False

    def _discard_deleted(self, table, key, fingerprint):
        # Retain the idempotency marker, never the deleted payload. Logical
        # erasure only: filesystem/WAL/backups have a separate retention policy.
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO delivered VALUES(?,?,?)", (key, now(), fingerprint))
            db.execute("DELETE FROM " + table + " WHERE id=?", (key,))
        self.mark_receipt(key,"withheld","canonical evidence was forgotten")

    def _purge_forgotten_dead_letters(self):
        cursor = ""
        while not self.stop.is_set():
            with self.connect() as db:
                dead = db.execute("SELECT id,payload FROM dead_letters WHERE id>? ORDER BY id LIMIT 100", (cursor,)).fetchall()
            if not dead:
                return
            for key, raw in dead:
                try:
                    items = json.loads(raw)["items"]
                    if self._has_forgotten_parent(items):
                        self._discard_deleted("dead_letters", key, self._fingerprint(items))
                except Exception:
                    # Unavailability is not evidence of deletion. Retry later.
                    return
                cursor = key

    def flush(self, force=True):
        with self.delivery_lock:
            self._purge_forgotten_dead_letters()
            with self.connect() as db:
                rows = db.execute("SELECT id,payload,created_at,attempts,fingerprint FROM pending WHERE next_attempt<=? ORDER BY created_at LIMIT 20",(float("inf") if force else time.time(),)).fetchall()
            for key, raw, queued_at, attempts, fingerprint in rows:
                if self.stop.is_set():break
                try:
                    payload=json.loads(raw)
                    if any("schema_version" not in i for i in payload["items"]):
                        from .ingestion import adapt_existing
                        payload={"items":[adapt_existing(i,connector_id="hermes.legacy_outbox",connector_version="0.1.0",
                                   source_locator="hermes://"+i["source_id"],observed_at=queued_at) for i in payload["items"]]}
                    if self._has_forgotten_parent(payload["items"]):
                        self._discard_deleted("pending", key, fingerprint)
                        continue
                    self.client.call("/v1/ingest", payload)
                except Exception as error:
                    with self.connect() as db:
                        if getattr(error,"status",None) in {400,413,422} or isinstance(error,(ValueError,KeyError)):
                            db.execute("INSERT OR REPLACE INTO dead_letters VALUES(?,?,?,?)",(key,raw,type(error).__name__,now()))
                            db.execute("DELETE FROM pending WHERE id=?",(key,))
                            db.execute("UPDATE observation_receipts SET state='failed',reason=?,updated_at=? WHERE id=?",
                                       (type(error).__name__,now(),key))
                        else:
                            db.execute("UPDATE pending SET attempts=attempts+1,last_error=?,next_attempt=? WHERE id=?",
                                       (type(error).__name__,time.time()+min(300,2**min(attempts+1,8)),key))
                    continue
                with self.connect() as db:
                    db.execute("INSERT OR IGNORE INTO delivered VALUES(?,?,?)", (key, now(),fingerprint))
                    db.execute("DELETE FROM pending WHERE id=?", (key,))
                    db.execute("UPDATE observation_receipts SET state='committed',reason=NULL,updated_at=? WHERE id=?",(now(),key))
        return True

    def _run(self):
        while not self.stop.is_set():
            self.wake.wait(2)
            self.wake.clear()
            if not self.stop.is_set():
                try: self.flush(force=False)
                except Exception:
                    # Disk/locking failures must not terminate the durable worker.
                    self.stop.wait(2)

    def close(self):
        self.stop.set()
        self.wake.set()
        self.thread.join(timeout=35)
