"""Persistent exposure ledger. IDs only; never a second copy of evidence text.

Session-wide dependencies are conservative: generated observations depend on
everything exposed to that session. Independent user observations do not.
"""
import json


def evidence_ids(value):
    found = set()
    def walk(node):
        if isinstance(node, dict):
            for key, child in node.items():
                if key == "record_id" and isinstance(child, str):
                    found.add(child)
                elif key in {"record_ids", "parent_record_ids"} and isinstance(child, list):
                    found.update(v for v in child if isinstance(v, str))
                elif key == "episodes" and isinstance(child, list):
                    found.update(r["id"] for r in child if isinstance(r, dict) and isinstance(r.get("id"), str))
                walk(child)
            if all(k in node for k in ("id", "source", "source_id", "text")) and isinstance(node["id"], str):
                found.add(node["id"])
        elif isinstance(node, list):
            for child in node:
                walk(child)
    walk(value)
    return found


class ExposureLedger:
    def __init__(self, outbox):
        self.outbox = outbox
        with outbox.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS exposures(session_id TEXT,record_id TEXT,PRIMARY KEY(session_id,record_id))")
            db.execute("CREATE TABLE IF NOT EXISTS untracked_exposure(session_id TEXT PRIMARY KEY,reason TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS capture_dependencies(capture_id TEXT PRIMARY KEY,parents TEXT NOT NULL)")

    def add(self, session_id, result):
        ids = evidence_ids(result)
        with self.outbox.connect() as db:
            db.executemany("INSERT OR IGNORE INTO exposures VALUES(?,?)", [(session_id, rid) for rid in ids])

    def exposed(self, session_id):
        """Whether any canonical record has been attributed to this session.

        Attribution is decided when a result is first observed, so a later replay that the host
        truncated has nothing left to attribute and must not widen the block.
        """
        with self.outbox.connect() as db:
            return db.execute("SELECT EXISTS(SELECT 1 FROM exposures WHERE session_id=?)",
                              (session_id,)).fetchone()[0] == 1

    def block(self, session_id, reason):
        with self.outbox.connect() as db:
            db.execute("INSERT OR REPLACE INTO untracked_exposure VALUES(?,?)", (session_id, reason))

    def inherit(self, session_id, parent_session_id):
        with self.outbox.connect() as db:
            db.execute("INSERT OR IGNORE INTO exposures SELECT ?,record_id FROM exposures WHERE session_id=?",
                       (session_id, parent_session_id))
            db.execute("INSERT OR IGNORE INTO untracked_exposure SELECT ?,reason FROM untracked_exposure WHERE session_id=?",
                       (session_id, parent_session_id))

    def bind(self, capture_id, parents):
        # Replaying an older observation must preserve the original exposure
        # boundary, even after the model has retrieved more evidence (including
        # the observation itself). Do not create cycles or mutate provenance.
        with self.outbox.connect() as db:
            db.execute("INSERT OR IGNORE INTO capture_dependencies VALUES(?,?)", (capture_id, json.dumps(parents)))
            return json.loads(db.execute("SELECT parents FROM capture_dependencies WHERE capture_id=?", (capture_id,)).fetchone()[0])

    def parents(self, session_id):
        with self.outbox.connect() as db:
            blocked = db.execute("SELECT reason FROM untracked_exposure WHERE session_id=?", (session_id,)).fetchone()
            ids = [r[0] for r in db.execute("SELECT record_id FROM exposures WHERE session_id=? ORDER BY record_id", (session_id,))]
        if blocked:
            raise ValueError("Generated capture withheld: " + blocked[0])
        if len(ids) > 100:
            raise ValueError("Generated capture withheld: dependency budget exceeded; lineage is never truncated")
        return ids

    def compact_parents(self, session_id):
        """Build immutable, canonical fan-in nodes; never drop dependencies.

        Every node has at most 100 parents. Retrying reuses the same node key.
        The service validates live leaves before accepting each node.
        """
        from .common import digest, now
        from .ingestion import adapt_existing
        with self.outbox.connect() as db:
            blocked = db.execute('SELECT reason FROM untracked_exposure WHERE session_id=?',(session_id,)).fetchone()
            ids = [r[0] for r in db.execute('SELECT record_id FROM exposures WHERE session_id=? ORDER BY record_id',(session_id,))]
        if blocked: raise ValueError('Generated capture withheld: ' + blocked[0])
        while len(ids) > 100:
            next_level=[]
            for offset in range(0,len(ids),100):
                parents=ids[offset:offset+100]
                item=adapt_existing({'source':'hermes-lineage','source_id':digest(parents),
                    'occurred_at':None,'text':'Provenance dependency group; not independent factual evidence.',
                    'metadata':{'lineage_only':True}}, connector_id='hermes.native',connector_version='1',
                    source_locator='hermes-lineage://'+digest(parents),observed_at=now())
                item['provenance'].update(origin='derived',parent_record_ids=parents)
                try:
                    result=self.outbox.client.call('/v1/ingest',{'items':[item]})
                except Exception as error:
                    raise ValueError('Generated capture withheld: provenance group could not be committed') from error
                next_level.append(result['records'][0]['id'])
            ids=next_level
        return ids
