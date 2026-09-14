"""Source-backed account identities and paginated evidence access."""
import json
import re

from .common import digest, now, required_text, timestamp


CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts(
 entity_id TEXT PRIMARY KEY REFERENCES entities(id), namespace TEXT NOT NULL,
 address TEXT NOT NULL, UNIQUE(namespace,address));
CREATE TABLE IF NOT EXISTS identity_edges(
 id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(entity_id),
 person_id TEXT NOT NULL REFERENCES entities(id), record_id TEXT NOT NULL REFERENCES records(id),
 status TEXT NOT NULL, valid_from TEXT, valid_to TEXT, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS account_aliases(
 entity_id TEXT REFERENCES accounts(entity_id),label TEXT,record_id TEXT REFERENCES records(id),
 PRIMARY KEY(entity_id,label,record_id));
CREATE INDEX IF NOT EXISTS records_source_time ON records(source,occurred_at,id);
CREATE INDEX IF NOT EXISTS links_record ON entity_links(record_id);
CREATE INDEX IF NOT EXISTS identity_person ON identity_edges(person_id,status);
"""


class Catalog:
    def _participants(self, db, rid, metadata, source):
        participants = metadata.get("participants", [])
        if not isinstance(participants, list) or len(participants) > 1000:
            raise ValueError("metadata.participants must be an array of at most 1000 observed accounts")
        for item in participants:
            namespace = required_text(item.get("namespace"), "namespace", 500)
            address = required_text(item.get("address"), "address", 500).strip()
            label = required_text(item.get("label", address), "label", 500)
            relation = required_text(item.get("relation", "participant"), "relation", 100)
            if namespace == "email":
                # Preserve local-part case; no Gmail-dot or plus-address guessing.
                local, sep, domain = address.rpartition("@")
                if not sep or not local or not domain:
                    raise ValueError("Invalid email account")
                address = local + "@" + domain.lower()
            if namespace == "phone":
                address = re.sub(r"[\s().-]", "", address)
                if not re.fullmatch(r"\+[1-9][0-9]{6,14}", address):
                    raise ValueError("Phone accounts require an explicit international country code")
            eid = "acct_" + digest([namespace, address])[:32]
            db.execute("INSERT OR IGNORE INTO entities VALUES(?, 'account', ?, 1, ?)", (eid, label, now()))
            db.execute("INSERT OR IGNORE INTO accounts VALUES(?,?,?)", (eid, namespace, address))
            db.execute("INSERT OR IGNORE INTO account_aliases VALUES(?,?,?)", (eid, label, rid))
            db.execute("INSERT OR IGNORE INTO entity_links VALUES(?,?,?)", (eid, rid, relation))

    def identity(self, account_id, person_id, record_id, status="candidate", valid_from=None, valid_to=None):
        if status not in {"candidate", "confirmed"}:
            raise ValueError("status must be candidate or confirmed; revoke with identity_revoke")
        start = timestamp(valid_from) if valid_from else None
        end = timestamp(valid_to) if valid_to else None
        if start and end and end <= start:
            raise ValueError("valid_to must follow valid_from")
        with self.lock, self.connect() as db:
            if not db.execute("SELECT 1 FROM accounts WHERE entity_id=?", (account_id,)).fetchone():
                raise ValueError("Unknown account")
            if not db.execute("SELECT 1 FROM entities WHERE id=? AND kind='person'", (person_id,)).fetchone():
                raise ValueError("Unknown person; create a person entity first")
            if not db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0", (record_id,)).fetchone():
                raise ValueError("Live identity evidence required")
            if status == "confirmed":
                overlap = db.execute("""SELECT 1 FROM identity_edges WHERE account_id=? AND status='confirmed'
                    AND person_id!=? AND (valid_to IS NULL OR ? IS NULL OR valid_to>?)
                    AND (? IS NULL OR valid_from IS NULL OR valid_from<?)""",
                    (account_id, person_id, start, start, end, end)).fetchone()
                if overlap:
                    raise ValueError("Conflicting account ownership interval; resolve or revoke the earlier link")
            edge_id = "idn_" + digest([account_id, person_id, record_id, status, start, end])[:32]
            db.execute("INSERT OR IGNORE INTO identity_edges VALUES(?,?,?,?,?,?,?,?)",
                       (edge_id, account_id, person_id, record_id, status, start, end, now()))
            self.audit(db, "identity_link", edge_id)
            return dict(db.execute("SELECT * FROM identity_edges WHERE id=?", (edge_id,)).fetchone())

    def identity_revoke(self, identity_id):
        with self.lock, self.connect() as db:
            if not db.execute("SELECT 1 FROM identity_edges WHERE id=?", (identity_id,)).fetchone():
                raise ValueError("Unknown identity link")
            db.execute("UPDATE identity_edges SET status='revoked' WHERE id=?", (identity_id,))
            self.audit(db, "identity_revoke", identity_id)
        return {"revoked": identity_id}

    def related_ids(self, entity_id):
        with self.connect() as db:
            return {r[0] for r in db.execute("""SELECT r.id FROM records r WHERE r.deleted=0 AND (
                EXISTS(SELECT 1 FROM entity_links l WHERE l.record_id=r.id AND l.entity_id=?) OR
                EXISTS(SELECT 1 FROM entity_links l JOIN identity_edges i ON i.account_id=l.entity_id
                  JOIN records evidence ON evidence.id=i.record_id AND evidence.deleted=0
                  WHERE l.record_id=r.id AND i.person_id=? AND i.status='confirmed'
                  AND (r.occurred_at!='' OR (i.valid_from IS NULL AND i.valid_to IS NULL))
                  AND (i.valid_from IS NULL OR r.occurred_at>=i.valid_from)
                  AND (i.valid_to IS NULL OR r.occurred_at<i.valid_to)))""", (entity_id, entity_id))}

    def connections(self, entity_id):
        with self.connect() as db:
            rows = [dict(r) for r in db.execute("""SELECT i.*, a.label AS account_label,p.label AS person_label
              FROM identity_edges i JOIN entities a ON a.id=i.account_id JOIN entities p ON p.id=i.person_id
              JOIN records r ON r.id=i.record_id WHERE (i.account_id=? OR i.person_id=?) AND r.deleted=0
              ORDER BY i.created_at DESC LIMIT 200""", (entity_id, entity_id))]
        return {"connections": rows, "note": "Only confirmed, time-valid links expand retrieval. Shared groups and names do not prove identity."}

    def browse(self, limit=50, cursor=None, entity_id=None, source=None, after=None, before=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be 1..100")
        clauses, args = ["deleted=0"], []
        if after or before: clauses.append("occurred_at!=''")
        for col, op, value in (("id", ">", cursor), ("source", "=", source),
                               ("occurred_at", ">=", timestamp(after) if after else None),
                               ("occurred_at", "<", timestamp(before) if before else None)):
            if value is not None:
                clauses.append(f"{col}{op}?"); args.append(value)
        # A temp table avoids SQLite parameter limits for years of entity history.
        with self.connect() as db:
            if entity_id:
                db.execute("CREATE TEMP TABLE allowed(id TEXT PRIMARY KEY)")
                db.executemany("INSERT INTO allowed VALUES(?)", ((x,) for x in self.related_ids(entity_id)))
                clauses.append("id IN (SELECT id FROM allowed)")
            rows = [dict(r) for r in db.execute("SELECT id,source,source_id,NULLIF(occurred_at,'') AS occurred_at,kind,substr(text,1,2400) AS text,length(text)>2400 AS truncated FROM records WHERE " + " AND ".join(clauses) + " ORDER BY id LIMIT ?", [*args, limit+1])]
        return {"episodes": rows[:limit], "next_cursor": rows[limit-1]["id"] if len(rows)>limit else None,
                "order": "stable_record_id", "complete_page": len(rows)<=limit,
                "note": "Continue next_cursor with identical filters. Exhaustion covers stored records, not missing source data. Concurrent imports require a fresh pass."}
