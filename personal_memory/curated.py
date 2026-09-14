"""Versioned curated memory used by Hermes's native memory surfaces.

Curated entries are deliberately separate from source records and claims.  They are
small, operator-visible prompt material with explicit source dependencies.  A single
SQLite transaction validates a complete edit, retires replaced entries, publishes the
new head and records the idempotency receipt.
"""
import json

from .common import digest, now, required_text


SCHEMA = """
CREATE TABLE IF NOT EXISTS curated_heads(
 target TEXT PRIMARY KEY, version INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS curated_entries(
 id TEXT PRIMARY KEY, target TEXT NOT NULL, position INTEGER NOT NULL,
 text TEXT NOT NULL, evidence_ids TEXT NOT NULL, actor TEXT NOT NULL,
 created_version INTEGER NOT NULL, retired_version INTEGER, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS curated_current
 ON curated_entries(target,retired_version,position);
CREATE TABLE IF NOT EXISTS curated_revisions(
 target TEXT NOT NULL, version INTEGER NOT NULL, parent_version INTEGER NOT NULL,
 request_id TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL, operation TEXT NOT NULL,
 actor TEXT NOT NULL, epoch INTEGER NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(target,version));
CREATE TABLE IF NOT EXISTS curated_requests(
 request_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, result TEXT NOT NULL);
"""

TARGETS = {"memory", "user"}
LIMITS = {"memory": 2200, "user": 1375}
ACTIONS = {"add", "replace", "remove", "clear"}


class VersionConflict(ValueError):
    """The caller edited a stale curated-memory version."""


def initialize(store):
    with store.connect() as db:
        db.executescript(SCHEMA)
        for target in sorted(TARGETS):
            db.execute("INSERT OR IGNORE INTO curated_heads VALUES(?,0,?)", (target, now()))


def _target(value):
    if value not in TARGETS:
        raise ValueError("target must be memory or user")
    return value


def _ids(db, values):
    if values is None:
        return []
    if not isinstance(values, list) or len(values) > 100:
        raise ValueError("evidence_ids must be a list of at most 100 record IDs")
    result = []
    for value in values:
        rid = required_text(value, "evidence_id", 100)
        if rid not in result:
            result.append(rid)
    for rid in result:
        if not db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0", (rid,)).fetchone():
            raise ValueError("Curated memory requires live canonical evidence")
    return result


def _current(db, target):
    rows = db.execute("""SELECT id,text,evidence_ids,position,created_version
        FROM curated_entries WHERE target=? AND retired_version IS NULL
        ORDER BY position,id""", (target,)).fetchall()
    return [{**dict(row), "evidence_ids": json.loads(row["evidence_ids"])} for row in rows]


def _usage(entries, target):
    characters = len("\n\n".join(entry["text"] for entry in entries)) if entries else 0
    return {"characters": characters, "limit": LIMITS[target],
            "percent": min(100, int(characters * 100 / LIMITS[target]))}


def read(store, target=None):
    targets = [_target(target)] if target is not None else ["memory", "user"]
    with store.connect() as db:
        result = {}
        for name in targets:
            head = db.execute("SELECT version,updated_at FROM curated_heads WHERE target=?", (name,)).fetchone()
            entries = _current(db, name)
            result[name] = {"target": name, "version": head["version"],
                            "updated_at": head["updated_at"], "entries": entries,
                            "entry_count": len(entries), "usage": _usage(entries, name),
                            "available": True}
    return {"stores": result, "contract_version": "1.0",
            "generation": store.generation()["generation"]}


def _match(entries, needle, position):
    matches = [index for index, entry in enumerate(entries) if needle in entry["text"]]
    if not matches:
        raise ValueError(f"Operation {position}: no entry matched {needle!r}")
    if len({entries[index]["text"] for index in matches}) > 1:
        raise ValueError(f"Operation {position}: old_text matched multiple distinct entries")
    return matches[0]


def _validate_operations(operations):
    if not isinstance(operations, list) or not 1 <= len(operations) <= 100:
        raise ValueError("operations must contain 1..100 edits")
    normalized = []
    for index, raw in enumerate(operations, 1):
        if not isinstance(raw, dict):
            raise ValueError(f"Operation {index} must be an object")
        action = raw.get("action")
        if action not in ACTIONS:
            raise ValueError(f"Operation {index}: action must be add, replace, remove or clear")
        allowed = {"action", "content", "old_text", "evidence_ids"}
        if set(raw) - allowed:
            raise ValueError(f"Operation {index}: unsupported fields")
        if action == "clear":
            if len(operations) != 1 or set(raw) != {"action"}:
                raise ValueError("clear must be the only operation and takes no other fields")
            normalized.append({"action": action})
            continue
        old = (raw.get("old_text") or "").strip()
        content = (raw.get("content") or "").strip()
        if action in {"replace", "remove"} and not old:
            raise ValueError(f"Operation {index}: old_text is required")
        if action in {"add", "replace"}:
            required_text(content, f"operation {index} content", LIMITS["memory"])
        normalized.append({"action": action, "old_text": old, "content": content,
                           "evidence_ids": raw.get("evidence_ids")})
    return normalized


def apply(store, *, target, expected_version, request_id, operations, epoch, actor,
          evidence_ids=None):
    target = _target(target)
    if type(expected_version) is not int or expected_version < 0:
        raise ValueError("expected_version must be a non-negative integer")
    if type(epoch) is not int or epoch < 0:
        raise ValueError("epoch must be a non-negative integer")
    request_id = required_text(request_id, "request_id", 200)
    operations = _validate_operations(operations)
    raw_digest = digest([target, expected_version, operations, evidence_ids, epoch, actor])
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute("SELECT request_digest,result FROM curated_requests WHERE request_id=?",
                              (request_id,)).fetchone()
        if previous:
            if previous["request_digest"] != raw_digest:
                raise ValueError("request_id was already used for a different edit")
            return json.loads(previous["result"])
        if db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0] != epoch:
            raise VersionConflict("Memory epoch changed; reload before editing")
        head = db.execute("SELECT version FROM curated_heads WHERE target=?", (target,)).fetchone()[0]
        if head != expected_version:
            raise VersionConflict(f"Curated memory version conflict: expected {expected_version}, current {head}")
        inherited = _ids(db, evidence_ids)
        working = _current(db, target)
        for index, operation in enumerate(operations, 1):
            action = operation["action"]
            if action == "clear":
                working = []
                continue
            refs = _ids(db, operation.get("evidence_ids")) if operation.get("evidence_ids") is not None else inherited
            if action == "add":
                if any(entry["text"] == operation["content"] for entry in working):
                    continue
                working.append({"id": None, "text": operation["content"], "evidence_ids": refs})
            else:
                selected = _match(working, operation["old_text"], index)
                if action == "remove":
                    working.pop(selected)
                else:
                    working[selected] = {"id": None, "text": operation["content"], "evidence_ids": refs}
        usage = _usage(working, target)
        if usage["characters"] > usage["limit"]:
            raise ValueError(f"Curated {target} exceeds {usage['limit']} characters")
        new_version = head + 1
        old_ids = {entry["id"] for entry in _current(db, target)}
        kept_ids = {entry["id"] for entry in working if entry.get("id")}
        retired = old_ids - kept_ids
        if retired:
            db.executemany("UPDATE curated_entries SET retired_version=? WHERE id=? AND retired_version IS NULL",
                           [(new_version, entry_id) for entry_id in sorted(retired)])
        for position, entry in enumerate(working):
            if entry.get("id"):
                db.execute("UPDATE curated_entries SET position=? WHERE id=?", (position, entry["id"]))
                continue
            entry_id = "cur_" + digest([target, request_id, position, entry["text"]])[:32]
            db.execute("INSERT INTO curated_entries VALUES(?,?,?,?,?,?,?,?,?)",
                       (entry_id, target, position, entry["text"],
                        json.dumps(entry["evidence_ids"], sort_keys=True), actor,
                        new_version, None, now()))
            entry["id"] = entry_id
        operation_name = operations[0]["action"] if len(operations) == 1 else "batch"
        db.execute("UPDATE curated_heads SET version=?,updated_at=? WHERE target=?",
                   (new_version, now(), target))
        db.execute("INSERT INTO curated_revisions VALUES(?,?,?,?,?,?,?,?,?)",
                   (target, new_version, head, request_id, raw_digest, operation_name,
                    actor, epoch, now()))
        store.audit(db, "curated_" + operation_name, target,
                    {"version": new_version, "request_id": request_id})
        result = {"success": True, "done": True, "target": target,
                  "version": new_version, "previous_version": head,
                  "entry_count": len(working), "usage": usage,
                  "entries": [{"id": entry["id"], "text": entry["text"],
                               "evidence_ids": entry["evidence_ids"]} for entry in working],
                  "generation": db.execute("SELECT max(id) FROM audit").fetchone()[0]}
        db.execute("INSERT INTO curated_requests VALUES(?,?,?)",
                   (request_id, raw_digest, json.dumps(result, ensure_ascii=False)))
        return result


def reset(store, *, scope, expected_versions, request_id, epoch, actor):
    if scope not in {"memory", "user", "curated"}:
        raise ValueError("scope must be memory, user or curated")
    if not isinstance(expected_versions, dict):
        raise ValueError("expected_versions must be an object")
    targets = [scope] if scope in TARGETS else ["memory", "user"]
    for target in targets:
        if target not in expected_versions:
            raise ValueError("expected_versions must include every reset target")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        raw_digest=digest([scope,expected_versions,epoch,actor])
        previous=db.execute("SELECT request_digest,result FROM curated_requests WHERE request_id=?",(request_id,)).fetchone()
        if previous:
            if previous[0]!=raw_digest:raise ValueError("request_id was already used for another reset")
            return json.loads(previous[1])
        if db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0] != epoch:
            raise VersionConflict("Memory epoch changed; reload before resetting")
        for target in targets:
            current = db.execute("SELECT version FROM curated_heads WHERE target=?", (target,)).fetchone()[0]
            if expected_versions[target] != current:
                raise VersionConflict(f"Curated memory version conflict for {target}")
        results={}
        for target in targets:
            old=expected_versions[target];new=old+1;stamp=now()
            db.execute("UPDATE curated_entries SET retired_version=? WHERE target=? AND retired_version IS NULL",(new,target))
            db.execute("UPDATE curated_heads SET version=?,updated_at=? WHERE target=?",(new,stamp,target))
            db.execute("INSERT INTO curated_revisions VALUES(?,?,?,?,?,?,?,?,?)",
                       (target,new,old,request_id+"/"+target,raw_digest,"clear",actor,epoch,stamp))
            store.audit(db,"curated_clear",target,{"version":new,"request_id":request_id})
            results[target]={"success":True,"target":target,"version":new,"previous_version":old,
                             "entry_count":0,"usage":_usage([],target)}
        result={"scope":scope,"results":results}
        db.execute("INSERT INTO curated_requests VALUES(?,?,?)",(request_id,raw_digest,json.dumps(result)))
        return result


def invalidate(db, store, record_id):
    """Retire current curated entries that depend on forgotten evidence."""
    targets=[row[0] for row in db.execute("""SELECT DISTINCT target FROM curated_entries
        WHERE retired_version IS NULL AND EXISTS(
          SELECT 1 FROM json_each(curated_entries.evidence_ids) WHERE value=?)""",(record_id,))]
    for target in targets:
        old=db.execute("SELECT version FROM curated_heads WHERE target=?",(target,)).fetchone()[0]
        new=old+1;stamp=now();request_id="evidence-forget/"+digest([target,record_id,new])
        db.execute("""UPDATE curated_entries SET retired_version=? WHERE target=? AND retired_version IS NULL
            AND EXISTS(SELECT 1 FROM json_each(curated_entries.evidence_ids) WHERE value=?)""",
            (new,target,record_id))
        db.execute("UPDATE curated_heads SET version=?,updated_at=? WHERE target=?",(new,stamp,target))
        db.execute("INSERT INTO curated_revisions VALUES(?,?,?,?,?,?,?,?,?)",
                   (target,new,old,request_id,digest([target,record_id]),"evidence_forget",
                    "system",db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0],stamp))
        store.audit(db,"curated_evidence_forget",target,{"version":new,"record_id":record_id})
