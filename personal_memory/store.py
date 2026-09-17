"""Canonical records and derived claims. No LLM extraction runs implicitly."""
import json
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

from .common import digest, now, required_text, timestamp
from .catalog import Catalog, CATALOG_SCHEMA


class Store(Catalog):
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self.lock = threading.RLock()
        from .deletions import DeletionLedger
        self.deletions=DeletionLedger(path.with_name(path.stem+".deletions.db"))
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2, 3, 4, 5, 6, 7, 8):
                raise RuntimeError(f"Unsupported database version {version}")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS records(
                    id TEXT PRIMARY KEY, source TEXT NOT NULL, source_id TEXT NOT NULL,
                    revision TEXT NOT NULL, occurred_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
                    kind TEXT NOT NULL, text TEXT NOT NULL, metadata TEXT NOT NULL,
                    fingerprint TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(source, source_id, revision));
                CREATE TABLE IF NOT EXISTS record_visibility(record_id TEXT PRIMARY KEY,hidden INTEGER NOT NULL,replacement_id TEXT);
                CREATE VIRTUAL TABLE IF NOT EXISTS record_fts USING fts5(id UNINDEXED, text);
                CREATE TABLE IF NOT EXISTS entities(
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, label TEXT NOT NULL,
                    provisional INTEGER NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS entity_links(
                    entity_id TEXT NOT NULL REFERENCES entities(id),
                    record_id TEXT NOT NULL REFERENCES records(id),
                    relation TEXT NOT NULL, UNIQUE(entity_id, record_id, relation));
                CREATE TABLE IF NOT EXISTS claims(
                    id TEXT PRIMARY KEY, subject_id TEXT REFERENCES entities(id),
                    predicate TEXT NOT NULL, text TEXT NOT NULL, category TEXT NOT NULL,
                    evidence_kind TEXT NOT NULL, record_id TEXT NOT NULL REFERENCES records(id),
                    valid_from TEXT, valid_to TEXT, created_at TEXT NOT NULL,
                    supersedes TEXT REFERENCES claims(id), status TEXT NOT NULL DEFAULT 'active');
                CREATE VIRTUAL TABLE IF NOT EXISTS claim_fts USING fts5(id UNINDEXED, text);
                CREATE TABLE IF NOT EXISTS audit(
                    id INTEGER PRIMARY KEY, action TEXT NOT NULL, object_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, metadata TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sources(
                    source TEXT PRIMARY KEY, state TEXT NOT NULL, through_at TEXT,
                    note TEXT NOT NULL, updated_at TEXT NOT NULL);
            """)
            db.executescript(CATALOG_SCHEMA)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS ingestion_receipts(
                  id TEXT PRIMARY KEY,record_id TEXT REFERENCES records(id),observed_at TEXT NOT NULL,envelope TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS record_dependencies(
                  child_id TEXT REFERENCES records(id),parent_id TEXT REFERENCES records(id),PRIMARY KEY(child_id,parent_id));
                CREATE INDEX IF NOT EXISTS dependencies_parent ON record_dependencies(parent_id);
                CREATE INDEX IF NOT EXISTS receipts_record_time ON ingestion_receipts(record_id,observed_at);
                CREATE INDEX IF NOT EXISTS claims_record_status ON claims(record_id,status);
                CREATE INDEX IF NOT EXISTS records_ingested ON records(deleted,ingested_at,id);
                CREATE INDEX IF NOT EXISTS claims_subject_predicate ON claims(subject_id,predicate,status);
                CREATE TABLE IF NOT EXISTS connector_checkpoints(
                  connector_id TEXT,source TEXT,cursor TEXT,updated_at TEXT,PRIMARY KEY(connector_id,source));
            """)
            if "batch_hash" not in {r[1] for r in db.execute("PRAGMA table_info(connector_checkpoints)")}:
                db.execute("ALTER TABLE connector_checkpoints ADD COLUMN batch_hash TEXT")
            if "evidence_quote" not in {r[1] for r in db.execute("PRAGMA table_info(claims)")}:
                db.execute("ALTER TABLE claims ADD COLUMN evidence_quote TEXT")
            from .blobs import SCHEMA as BLOB_SCHEMA
            db.executescript(BLOB_SCHEMA)
            from .learning import SCHEMA
            db.executescript(SCHEMA)
            from .intelligence import SCHEMA as INTELLIGENCE_SCHEMA
            from .workflows import SCHEMA as WORKFLOW_SCHEMA
            db.executescript(INTELLIGENCE_SCHEMA)
            if "attempts" not in {r[1] for r in db.execute("PRAGMA table_info(task_events)")}:db.execute("ALTER TABLE task_events ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
            db.executescript(WORKFLOW_SCHEMA)
            from .curated import SCHEMA as CURATED_SCHEMA
            db.executescript(CURATED_SCHEMA)
            from .source_sync import SCHEMA as SOURCE_SYNC_SCHEMA
            db.executescript(SOURCE_SYNC_SCHEMA)
            # The semantic work queue must exist before any ingest can enqueue.
            from .semantic import WORK_SCHEMA
            db.executescript(WORK_SCHEMA)
            # Discovery retry state is interpreted against the configuration that
            # produced it; older databases only carried the deadline.
            if "config_key" not in {r[1] for r in db.execute("PRAGMA table_info(source_schedule)")}:
                db.execute("ALTER TABLE source_schedule ADD COLUMN config_key TEXT")
            from . import changes
            changes.ensure(db)
            from . import awareness
            awareness.ensure(db)
            for target in ("memory", "user"):
                db.execute("INSERT OR IGNORE INTO curated_heads VALUES(?,0,?)", (target, now()))
            db.execute("CREATE INDEX IF NOT EXISTS knowledge_subject ON learning_objects(kind,state,json_extract(payload,'$.subject_id'))")
            db.execute("PRAGMA user_version=8")
        path.chmod(0o600)
        for rid in self.deletions.ids():
            if rid.startswith('src_'):
                continue
            if rid.startswith(('done_','cancel_')):
                task_id='lrn_'+rid.split('_',1)[1]
                terminal='completed' if rid.startswith('done_') else 'cancelled'
                with self.connect() as db:
                    db.execute("UPDATE task_runtime SET state=?,version=version+1 WHERE object_id=? AND state NOT IN ('completed','cancelled')",(terminal,task_id))
                continue
            if rid.startswith('lrn_'):
                from .learning import invalidate
                with self.connect() as db:invalidate(db,objects=[rid],state='retracted')
                continue
            with self.connect() as db:live=db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0",(rid,)).fetchone()
            if live:self._forget(rid,journal=False)
        source_tombstones = {rid for rid in self.deletions.ids() if rid.startswith('src_')}
        if source_tombstones:
            # One bounded-memory pass, including historical revisions present
            # only in a restored backup. No content is read into this scan.
            cursor = ""
            while True:
                with self.connect() as db:
                    rows = db.execute("SELECT id,source,source_id FROM records WHERE deleted=0 AND id>? ORDER BY id LIMIT 500", (cursor,)).fetchall()
                if not rows: break
                for row in rows:
                    if self.source_key(row['source'], row['source_id']) in source_tombstones:
                        self._forget(row['id'], journal=False)
                cursor = rows[-1]['id']
        from .reset import initialize as initialize_reset
        initialize_reset(self)
        # Repair derived memory left stale by pre-unification retirement before
        # any background worker or recall request can observe it. The versioned
        # marker makes this a one-time, restartable migration per database.
        from . import lifecycle
        lifecycle.apply_upgrade(self)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=10000")
        try:
            with db:
                yield db
        finally:
            db.close()

    def audit(self, db, action, obj, metadata=None):
        db.execute("INSERT INTO audit(action,object_id,created_at,metadata) VALUES(?,?,?,?)",
                   (action, obj, now(), json.dumps(metadata or {})))

    def ingest_contract(self, items, checkpoint=None):
        from .ingestion import validate_batch
        records=validate_batch(items)  # Entire batch passes structural validation before any write.
        normalized=[]
        for item in records:
            normalized.append({"source":item["source"],"source_id":item["source_id"],"revision":item["revision"],
                "occurred_at":item["occurred_at"],"kind":item["kind"],"text":item["text"],
                "metadata":{"participants":item["participants"],"extensions":item["extensions"],
                            "origin":item["provenance"]["origin"],"parent_record_ids":item["provenance"]["parent_record_ids"]},
                "_contract":item})
        return self.ingest(normalized,checkpoint=checkpoint)

    def _receipt(self, db, rid, envelope):
        if envelope is None: return
        for parent in envelope["provenance"]["parent_record_ids"]:
            if not db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0",(parent,)).fetchone():
                raise ValueError("A parent_record_id does not resolve to live evidence")
            db.execute("INSERT OR IGNORE INTO record_dependencies VALUES(?,?)",(rid,parent))
        # Receipt provenance can change on re-observation without changing source content.
        receipt={k:envelope[k] for k in ("schema_version","observed_at","provenance")}
        db.execute("INSERT OR IGNORE INTO ingestion_receipts VALUES(?,?,?,?)",
                   (digest([rid,receipt]),rid,envelope["observed_at"],json.dumps(receipt,ensure_ascii=False)))

    def ingest(self, items, checkpoint=None):
        if not isinstance(items, list) or not 1 <= len(items) <= 100:
            raise ValueError("items must contain 1..100 records")
        results = []
        with self.lock, self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if checkpoint is not None:
                if not isinstance(checkpoint,dict) or set(checkpoint)!={"connector_id","source","expected_cursor","cursor"}:
                    raise ValueError("Invalid connector checkpoint")
                required_text(checkpoint["connector_id"],"connector_id",200)
                required_text(checkpoint["source"],"source",200)
                raw_cursor=json.dumps(checkpoint["cursor"],sort_keys=True,allow_nan=False)
                if len(raw_cursor)>8192:raise ValueError("Checkpoint cursor exceeds budget")
                for item in items:
                    if item["source"]!=checkpoint["source"] or item.get("_contract",{}).get("provenance",{}).get("connector_id")!=checkpoint["connector_id"]:
                        raise ValueError("Checkpoint scope must match every record")
                batch_hash=digest([{k:v for k,v in item.items() if k!="_contract"} for item in items])
                old_cursor=db.execute("SELECT cursor,batch_hash FROM connector_checkpoints WHERE connector_id=? AND source=?",
                                      (checkpoint["connector_id"],checkpoint["source"])).fetchone()
                current=json.loads(old_cursor[0]) if old_cursor else None
                if current!=checkpoint["expected_cursor"] and current!=checkpoint["cursor"]:
                    raise ValueError("Checkpoint conflict; another consumer advanced this source")
                if current==checkpoint["cursor"] and old_cursor and old_cursor[1]!=batch_hash:
                    raise ValueError("A committed checkpoint cannot be reused for a different batch")
            for item in items:
                prior = [row[0] for row in db.execute(
                    "SELECT id FROM records WHERE source=? AND source_id=? AND deleted=0",
                    (item["source"], item["source_id"]))]
                applied = self._apply_ingest_item(db, item)
                results.append(applied)
                if not applied["duplicate"]:
                    from . import changes
                    contract = item.get("_contract") or {}
                    provenance = contract.get("provenance") or {}
                    changes.append(
                        db, connection_id="direct:" + str(provenance.get("connector_id") or "import"),
                        source=item["source"], stream="direct", partition="", generation=1,
                        source_item_id=item["source_id"],
                        kind="content_updated" if prior else "created", origin_mode="backfill",
                        record_ids=[applied["id"]], previous_record_ids=prior,
                        coordinates={"arrival": "historical"}, occurred_at=item.get("occurred_at"))
            if checkpoint is not None:
                db.execute("INSERT INTO connector_checkpoints VALUES(?,?,?,?,?) ON CONFLICT(connector_id,source) DO UPDATE SET cursor=excluded.cursor,updated_at=excluded.updated_at,batch_hash=excluded.batch_hash",
                           (checkpoint["connector_id"],checkpoint["source"],raw_cursor,now(),batch_hash))
        return {"records": results,"checkpoint_committed":checkpoint is not None}

    def _apply_ingest_item(self, db, item):
        """Write one normalized ingest item inside the caller's transaction.

        Shared by the public ingest API and the source-sync atomic page commit;
        neither path may call the other in a nested committing transaction.
        """
        if not isinstance(item, dict):
            raise ValueError("record must be an object")
        source = required_text(item.get("source"), "source", 200)
        source_id = required_text(item.get("source_id"), "source_id", 1000)
        if self.deletions.contains(self.source_key(source, source_id)):
            raise ValueError("Source item was forgotten across all revisions")
        revision = required_text(item.get("revision", "1"), "revision", 200)
        text = required_text(item.get("text"), "text")
        when = "" if item.get("_contract") and item.get("occurred_at") is None else timestamp(item.get("occurred_at"))
        kind = required_text(item.get("kind", "episode"), "kind", 100)
        metadata = item.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be an object")
        fingerprint = digest([when, kind, text, metadata])
        old = db.execute("SELECT id,fingerprint,deleted FROM records WHERE source=? AND source_id=? AND revision=?",
                         (source, source_id, revision)).fetchone()
        if old:
            if old["deleted"]:
                raise ValueError("Record was forgotten; reimport is blocked for this source ID/revision")
            if old["fingerprint"] != fingerprint:
                raise ValueError("Source ID/revision already exists with different content; increment revision")
            self._receipt(db,old["id"],item.get("_contract"))
            return {"id": old["id"], "duplicate": True}
        rid = "rec_" + digest([source, source_id, revision])[:32]
        if self.deletions.contains(rid):raise ValueError("Record ID/revision was previously forgotten")
        db.execute("INSERT INTO records(id,source,source_id,revision,occurred_at,ingested_at,kind,text,metadata,fingerprint) VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (rid, source, source_id, revision, when, now(), kind, text,
                    json.dumps(metadata, ensure_ascii=False), fingerprint))
        db.execute("INSERT INTO record_fts(id,text) VALUES(?,?)", (rid, text))
        self._participants(db, rid, metadata, source)
        self._receipt(db,rid,item.get("_contract"))
        db.execute("INSERT OR IGNORE INTO sources VALUES(?, 'unknown', NULL, '', ?)", (source, now()))
        self.audit(db, "ingest", rid)
        from . import semantic
        semantic.enqueue(db, "index", [rid])
        return {"id": rid, "duplicate": False}

    def checkpoint(self,connector_id,source):
        with self.connect() as db:
            row=db.execute("SELECT cursor,updated_at FROM connector_checkpoints WHERE connector_id=? AND source=?",(connector_id,source)).fetchone()
            return {"connector_id":connector_id,"source":source,"cursor":json.loads(row[0]) if row else None,"updated_at":row[1] if row else None}

    def lineage_status(self, record_ids):
        if not isinstance(record_ids, list) or not 1 <= len(record_ids) <= 100:
            raise ValueError("record_ids must contain 1..100 IDs")
        for rid in record_ids:
            required_text(rid, "record_id", 100)
        with self.lock, self.connect() as db:
            states = {}
            for rid in record_ids:
                row = db.execute("SELECT deleted FROM records WHERE id=?", (rid,)).fetchone()
                states[rid] = "unknown" if row is None else "forgotten" if row[0] else "live"
        return {"states": states}

    @staticmethod
    def source_key(source, source_id):
        return "src_" + digest([source, source_id])

    def source_status(self, source, source_ids):
        required_text(source, "source", 200)
        if not isinstance(source_ids, list) or not 1 <= len(source_ids) <= 100:
            raise ValueError("source_ids must contain 1..100 identifiers")
        for sid in source_ids: required_text(sid, "source_id", 1000)
        blocked = self.deletions.contains_many(self.source_key(source, sid) for sid in source_ids)
        return {"states": {sid: "forgotten" if self.source_key(source, sid) in blocked else "open" for sid in source_ids}}

    def supersede(self,record_id,replacement_id=None):
        from . import lifecycle
        with self.lock,self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(record_id,)).fetchone():raise ValueError('Missing live retired record')
            if replacement_id and not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(replacement_id,)).fetchone():raise ValueError('Missing live replacement')
            hidden=lifecycle.retire(db,self,record_id,replacement=replacement_id,exclude_replacement=True)
            self.audit(db,'supersede',record_id)
        return {'retired':record_id,'replacement_id':replacement_id,'hidden_records':len(hidden),'history_preserved':True}

    def forget_source(self, source, source_id):
        required_text(source, "source", 200)
        required_text(source_id, "source_id", 1000)
        with self.lock:
            self.deletions.append([self.source_key(source, source_id)])
            with self.connect() as db:
                ids = [r[0] for r in db.execute("SELECT id FROM records WHERE source=? AND source_id=? AND deleted=0", (source, source_id))]
            affected = set()
            for rid in ids:
                affected.update(self._forget(rid, journal=True)["affected_records"])
        return {"source": source, "source_id": source_id, "affected_records": sorted(affected),
                "future_revisions_blocked": True, "scope": "Framework source item and tracked dependents; native files and external copies are not erased"}

    def evidence(self, record_id, compact=False, start=None, end=None):
        if not isinstance(compact,bool):raise ValueError("compact must be boolean")
        with self.connect() as db:
            row = db.execute("SELECT * FROM records WHERE id=? AND deleted=0", (record_id,)).fetchone()
            if not row:
                raise ValueError("Record not found or forgotten")
            result = dict(row)
            result["metadata"] = json.loads(result["metadata"])
            result["occurred_at"] = result["occurred_at"] or None
            text=result["text"]
            if start is not None or end is not None:
                start=0 if start is None else start;end=len(text) if end is None else end
                if (type(start) is not int or type(end) is not int or start<0 or end<start or
                        start>len(text) or end>len(text)):raise ValueError("Invalid evidence span")
                result["text"]=text[start:end]
                result.update(span_start=start,span_end=end,truncated=start>0 or end<len(text))
            if compact:
                keys=("id","source","source_id","revision","occurred_at","kind","text",
                      "span_start","span_end","truncated")
                return {k:result[k] for k in keys if k in result}
            receipt=db.execute("SELECT envelope FROM ingestion_receipts WHERE record_id=? ORDER BY observed_at DESC LIMIT 1",(record_id,)).fetchone()
            if receipt:
                result["ingestion_record"]={**json.loads(receipt[0]),
                    **{k:result[k] for k in ("source","source_id","revision","kind","occurred_at","text")},
                    "participants":result["metadata"]["participants"],"extensions":result["metadata"]["extensions"]}
            return result

    def entity(self, kind, label, provisional=True, entity_id=None, record_id=None, relation="mentioned"):
        required_text(kind, "kind", 100)
        required_text(label, "label", 500)
        required_text(relation, "relation", 100)
        if not isinstance(provisional, bool):
            raise ValueError("provisional must be boolean")
        with self.lock, self.connect() as db:
            eid = entity_id or "ent_" + uuid.uuid4().hex
            if entity_id:
                old = db.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
                if not old:
                    raise ValueError("Entity does not exist")
                # Updating a label is explicitly requested, never an automatic identity merge.
                self.audit(db, "entity_update", eid, dict(old))
                db.execute("UPDATE entities SET kind=?,label=?,provisional=? WHERE id=?",
                           (kind, label, int(provisional), eid))
            else:
                db.execute("INSERT INTO entities VALUES(?,?,?,?,?)", (eid, kind, label, int(provisional), now()))
                self.audit(db, "entity_create", eid)
            if record_id:
                if not db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0", (record_id,)).fetchone():
                    raise ValueError("Evidence record not found")
                db.execute("INSERT OR IGNORE INTO entity_links VALUES(?,?,?)", (eid, record_id, relation))
            return dict(db.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone())

    def claim(self, text, record_id, category="semantic", evidence_kind="inferred", subject_id=None,
              predicate="note", valid_from=None, valid_to=None, supersedes=None,evidence_quote=None):
        required_text(text, "text", 10000)
        required_text(predicate, "predicate", 200)
        if category not in {"semantic", "procedural", "prospective", "observation"}:
            raise ValueError("Unsupported memory category")
        if evidence_kind not in {"reported", "observed", "inferred"}:
            raise ValueError("Unsupported evidence_kind")
        if evidence_kind!="inferred" and not evidence_quote:
            raise ValueError("Reported/observed claims require an exact evidence_quote; otherwise use inferred")
        if evidence_quote is not None:required_text(evidence_quote,"evidence_quote",10000)
        start = timestamp(valid_from) if valid_from else None
        end = timestamp(valid_to) if valid_to else None
        if start and end and end <= start:
            raise ValueError("valid_to must follow valid_from")
        with self.lock, self.connect() as db:
            # Evidence validation and supersession share one write transaction, so a
            # retirement racing this creation either runs before validation (the claim
            # is rejected) or after the insert (the cascade retracts it).
            db.execute("BEGIN IMMEDIATE")
            from . import lifecycle
            cid = "clm_" + digest([text, record_id, category, evidence_kind, subject_id, predicate, start, end, supersedes,evidence_quote])[:32]
            # Replayed claims revalidate too: an idempotent retry must not smuggle
            # knowledge back in through evidence that has since retired.
            lifecycle.require_live_evidence(db, [record_id], message="A live evidence record is required")
            existing = db.execute("SELECT * FROM claims WHERE id=?", (cid,)).fetchone()
            if existing:
                return dict(existing)
            if evidence_quote is not None and evidence_quote not in db.execute("SELECT text FROM records WHERE id=?",(record_id,)).fetchone()[0]:
                raise ValueError("Evidence quote does not occur in the cited source")
            if subject_id and not db.execute("SELECT 1 FROM entities WHERE id=?", (subject_id,)).fetchone():
                raise ValueError("Subject entity does not exist")
            if supersedes:
                previous = db.execute("SELECT * FROM claims WHERE id=? AND status='active'", (supersedes,)).fetchone()
                if not previous:
                    raise ValueError("Only an active claim can be superseded")
                if previous["subject_id"] != subject_id or previous["predicate"] != predicate:
                    raise ValueError("Correction must address the same subject and predicate")
            db.execute("INSERT INTO claims(id,subject_id,predicate,text,category,evidence_kind,record_id,valid_from,valid_to,created_at,supersedes) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (cid, subject_id, predicate, text, category, evidence_kind, record_id, start, end, now(), supersedes))
            db.execute("INSERT INTO claim_fts(id,text) VALUES(?,?)", (cid, text))
            if evidence_quote is not None:db.execute("UPDATE claims SET evidence_quote=? WHERE id=?",(evidence_quote,cid))
            if supersedes:
                db.execute("UPDATE claims SET status='superseded' WHERE id=?", (supersedes,))
            if subject_id:
                db.execute("INSERT OR IGNORE INTO entity_links VALUES(?,?,?)", (subject_id, record_id, predicate))
            self.audit(db, "claim_create", cid, {"supersedes": supersedes})
            return dict(db.execute("SELECT * FROM claims WHERE id=?", (cid,)).fetchone())

    @staticmethod
    def query_terms(query):
        required_text(query, "query", 4000)
        terms = re.findall(r"[^\W_]+", query, flags=re.UNICODE)[:32]
        if not terms:
            raise ValueError("Search requires at least one word or number")
        return " OR ".join('"' + term.replace('"', '""') + '"' for term in terms)

    def search(self, query, limit=8, entity_id=None, source=None, after=None, before=None,
               include_history=False, allowed_ids=None):
        if type(limit) is not int or not 1 <= limit <= 300:
            raise ValueError("limit must be 1..300")
        terms = self.query_terms(query)
        clauses = ["r.deleted=0"]
        if not include_history:clauses.append("NOT EXISTS(SELECT 1 FROM record_visibility v WHERE v.record_id=r.id AND v.hidden=1)")
        params = []
        if source:
            clauses.append("r.source=?"); params.append(source)
        if entity_id:
            allowed_ids = self.related_ids(entity_id)
        if allowed_ids is not None:
            clauses.append("r.id IN (SELECT id FROM allowed)")
        if after:
            clauses.append("r.occurred_at>=?"); params.append(timestamp(after))
        if before:
            clauses.append("r.occurred_at<?"); params.append(timestamp(before))
        if after or before:
            clauses.append("r.occurred_at!=''")
        where = " AND ".join(clauses)
        with self.connect() as db:
            if allowed_ids is not None:
                db.execute("CREATE TEMP TABLE allowed(id TEXT PRIMARY KEY)")
                db.executemany("INSERT INTO allowed VALUES(?)", ((x,) for x in allowed_ids))
            episodes = [dict(r) for r in db.execute(f"""
                SELECT r.id,r.source,r.source_id,NULLIF(r.occurred_at,'') AS occurred_at,r.kind,
                       substr(r.text,1,2400) AS text,length(r.text)>2400 AS truncated
                FROM record_fts JOIN records r ON r.id=record_fts.id
                WHERE record_fts MATCH ? AND {where} ORDER BY rank LIMIT ?
                """, [terms, *params, limit])]
            # Superseded evidence is still searchable as episodes; claims carry explicit status.
            status = "c.status != 'retracted'" if include_history else "c.status='active'"
            claims = [dict(r) for r in db.execute(f"""
                SELECT c.*,r.source FROM claim_fts JOIN claims c ON c.id=claim_fts.id
                JOIN records r ON r.id=c.record_id
                WHERE claim_fts MATCH ? AND {where} AND {status} ORDER BY rank LIMIT ?
                """, [terms, *params, limit])]
        return {"episodes": episodes, "claims": claims,
                "retrieval": "sqlite_fts5_lexical", "coverage": self.status()["sources"],
                "warning": "Search is bounded and lexical. Empty results do not prove absence. Episodes may contain obsolete claims; inspect dates and claim status."}

    def timeline(self, entity_id, limit=30):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        with self.connect() as db:
            entity = db.execute("SELECT * FROM entities WHERE id=?", (entity_id,)).fetchone()
            if not entity:
                raise ValueError("Entity not found")
            records = [dict(r) for r in db.execute("""SELECT DISTINCT r.id,r.source,NULLIF(r.occurred_at,'') AS occurred_at,
                substr(r.text,1,2400) AS text,length(r.text)>2400 AS truncated FROM records r
                JOIN entity_links l ON l.record_id=r.id WHERE l.entity_id=? AND r.deleted=0
                ORDER BY r.occurred_at DESC LIMIT ?""", (entity_id, limit))]
            claims = [dict(r) for r in db.execute("""SELECT c.* FROM claims c JOIN records r ON r.id=c.record_id
                WHERE c.subject_id=? AND r.deleted=0 ORDER BY c.created_at DESC LIMIT ?""", (entity_id, limit))]
            return {"entity": dict(entity), "episodes": records, "claims": claims, "limit": limit,
                    "complete": False, "note": "Bounded latest entries; use filtered search to investigate further."}

    def entities(self, query):
        required_text(query, "query", 500)
        # Escape SQL LIKE wildcard characters so names are literal.
        escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self.connect() as db:
            pattern = "%" + escaped + "%"
            return {"entities": [dict(r) for r in db.execute("""SELECT e.*,a.namespace,a.address
                FROM entities e LEFT JOIN accounts a ON a.entity_id=e.id
                WHERE e.label LIKE ? ESCAPE '\\' OR a.address LIKE ? ESCAPE '\\'
                OR EXISTS(SELECT 1 FROM account_aliases n JOIN records r ON r.id=n.record_id
                  WHERE n.entity_id=e.id AND r.deleted=0 AND n.label LIKE ? ESCAPE '\\') LIMIT 50""", (pattern,pattern,pattern))]}

    def forget(self, record_id):
        return self._forget(record_id,journal=True)

    def _forget(self,record_id,*,journal):
        with self.lock, self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if not db.execute("SELECT 1 FROM records WHERE id=?", (record_id,)).fetchone():
                raise ValueError("Record not found")
            affected=[r[0] for r in db.execute("""WITH RECURSIVE affected(id) AS (
                SELECT ? UNION SELECT d.child_id FROM record_dependencies d JOIN affected a ON d.parent_id=a.id)
                SELECT id FROM affected""",(record_id,))]
            if journal:self.deletions.append(affected)
            ids=[]
            for rid in affected:
                from . import awareness
                awareness.invalidate_record(db, rid)
                db.execute("DELETE FROM source_jobs WHERE json_extract(payload,'$.record_id')=?",(rid,))
                from .learning import invalidate
                invalidate(db,record_id=rid)
                from .curated import invalidate as invalidate_curated
                invalidate_curated(db,self,rid)
                db.execute("DELETE FROM memory_feedback WHERE record_id=?",(rid,))
                claims=[r[0] for r in db.execute("SELECT id FROM claims WHERE record_id=?",(rid,))]
                ids.extend(claims)
                for cid in claims:
                    db.execute("DELETE FROM claim_fts WHERE id=?", (cid,))
                    db.execute("UPDATE claims SET text='',evidence_quote=NULL,status='retracted' WHERE id=?", (cid,))
                db.execute('DELETE FROM memory_blob_chunks WHERE blob_id IN (SELECT id FROM memory_blobs WHERE record_id=?)',(rid,))
                db.execute("UPDATE memory_blobs SET state='forgotten',filename='',mime='' WHERE record_id=?",(rid,))
                db.execute("DELETE FROM record_fts WHERE id=?", (rid,))
                db.execute("DELETE FROM entity_links WHERE record_id=?", (rid,))
                db.execute("UPDATE identity_edges SET status='revoked' WHERE record_id=?", (rid,))
                db.execute("DELETE FROM account_aliases WHERE record_id=?", (rid,))
                db.execute("DELETE FROM ingestion_receipts WHERE record_id=?", (rid,))
                db.execute("UPDATE records SET text='',metadata='{}',deleted=1 WHERE id=?", (rid,))
                self.audit(db, "forget", rid)
            from . import semantic
            semantic.enqueue(db, "retire", affected)
        return {"forgotten": record_id, "affected_records":affected,"retracted_claims": ids,
                "note": "Logical removal. Source identifiers/tombstone remain; backups and prior responses are outside this operation."}

    def coverage(self, source, state, through_at=None, note=""):
        required_text(source, "source", 200)
        if state not in {"unknown", "partial", "complete", "failed"}:
            raise ValueError("Invalid source state")
        if not isinstance(note, str) or len(note) > 2000:
            raise ValueError("Invalid coverage note")
        through = timestamp(through_at) if through_at else None
        if state == "complete" and (not through or not note):
            raise ValueError("Complete coverage requires a cutoff and explicit description of covered scope")
        with self.lock, self.connect() as db:
            db.execute("INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET state=excluded.state,through_at=excluded.through_at,note=excluded.note,updated_at=excluded.updated_at",
                       (source, state, through, note, now()))
            self.audit(db, "coverage", source)
        return self.status()

    def status(self):
        with self.connect() as db:
            curated = {r["target"]: {"version": r["version"], "entries": db.execute(
                       "SELECT count(*) FROM curated_entries WHERE target=? AND retired_version IS NULL",
                       (r["target"],)).fetchone()[0]} for r in db.execute("SELECT * FROM curated_heads")}
            return {"schema_version": 8, "ingestion_contract":"1.0", "backend": "sqlite_fts5", "semantic_embeddings": False,
                    "records": db.execute("SELECT count(*) FROM records WHERE deleted=0").fetchone()[0],
                    "active_claims": db.execute("SELECT count(*) FROM claims WHERE status='active'").fetchone()[0],
                    "entities": db.execute("SELECT count(*) FROM entities").fetchone()[0],
                    "sources": [dict(r) for r in db.execute("SELECT * FROM sources ORDER BY source")],
                    "curated_memory": curated,
                    "generation": db.execute("SELECT coalesce(max(id),0) FROM audit").fetchone()[0]}

    def generation(self):
        with self.connect() as db:
            return {"generation":db.execute("SELECT coalesce(max(id),0) FROM audit").fetchone()[0]}
