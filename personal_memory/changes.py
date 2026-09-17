"""Durable memory-change journal: the shared evidence feed every awareness
consumer scans. Appends are transaction-aware — the caller's SQLite transaction
commits canonical evidence, source state and the journal entry together, so a
rollback can never leave a notification without its evidence. Entries carry
references and minimal operational metadata only; content resolves through the
canonical store at read time, after visibility checks.
"""
import base64
import json
from datetime import datetime, timedelta, timezone

from .common import digest, now, required_text, timestamp

SCHEMA = '''
CREATE TABLE IF NOT EXISTS memory_changes(
  event_id TEXT PRIMARY KEY, sequence INTEGER UNIQUE NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1, memory_epoch INTEGER NOT NULL,
  connection_id TEXT NOT NULL, source TEXT NOT NULL, stream TEXT NOT NULL,
  partition TEXT NOT NULL DEFAULT '', generation INTEGER NOT NULL,
  source_item_id TEXT NOT NULL, transition_version INTEGER NOT NULL,
  kind TEXT NOT NULL, origin_mode TEXT NOT NULL, novelty TEXT NOT NULL,
  occurred_at TEXT, observed_at TEXT, committed_at TEXT NOT NULL,
  conversation_key TEXT, entity_ids TEXT NOT NULL DEFAULT '[]',
  cause_event_id TEXT, trace_id TEXT, classification_basis TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE(source, source_item_id, transition_version));
CREATE INDEX IF NOT EXISTS changes_scan ON memory_changes(sequence);
CREATE INDEX IF NOT EXISTS changes_item ON memory_changes(source,source_item_id,kind);
CREATE INDEX IF NOT EXISTS changes_cause ON memory_changes(cause_event_id);
CREATE TABLE IF NOT EXISTS memory_change_refs(
  event_id TEXT NOT NULL, record_id TEXT NOT NULL, role TEXT NOT NULL,
  PRIMARY KEY(event_id,record_id,role));
CREATE INDEX IF NOT EXISTS change_refs_record ON memory_change_refs(record_id);
CREATE TABLE IF NOT EXISTS changes_config(
  id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL,
  settings TEXT NOT NULL, start_sequence INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL);
'''

KINDS = ("created", "content_updated", "metadata_updated", "removed", "restored",
         "sync_gap", "backfill_progress", "backfill_complete", "enrichment")
ORIGIN_MODES = ("backfill", "incremental", "reconcile", "derived")
NOVELTIES = ("historical", "live", "recovered", "uncertain")
ARRIVAL_KINDS = ("created",)
MAX_INLINE_REFS = 64
MAX_LIMIT = 500
PLAUSIBLE_SKEW = timedelta(days=1)
_STREAM_ITEM = "__stream__"
_GAP_ITEM = "__gap__"


def record_id(source, source_id, revision):
    """Mirror the canonical ID derivation so references resolve without a lookup."""
    return "rec_" + digest([source, source_id, revision])[:32]


def ensure(db):
    """Schema mount for the store's own initialization transaction."""
    db.executescript(SCHEMA)
    db.execute("INSERT OR IGNORE INTO changes_config VALUES(1,0,?,0,?)", (json.dumps({}), now()))


def initialize(store):
    with store.connect() as db:
        ensure(db)


def configure(store, settings):
    """Operator-facing switch. Enabling over an existing archive records one
    atomic starting boundary; stored history is never replayed as live arrivals."""
    if not isinstance(settings, dict) or "journal" not in settings:
        raise ValueError("settings must carry an explicit journal section")
    journal = settings["journal"]
    if not isinstance(journal, dict) or not isinstance(journal.get("enabled"), bool):
        raise ValueError("journal.enabled must be boolean")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM changes_config WHERE id=1").fetchone()
        if row is None:
            initialize(store)
            row = db.execute("SELECT * FROM changes_config WHERE id=1").fetchone()
        enabled = int(journal["enabled"])
        start = row["start_sequence"]
        if enabled and not row["enabled"]:
            start = db.execute("SELECT COALESCE(MAX(sequence),0) FROM memory_changes").fetchone()[0]
        db.execute("UPDATE changes_config SET enabled=?,settings=?,start_sequence=?,updated_at=? WHERE id=1",
                   (enabled, json.dumps(settings, sort_keys=True), start, now()))
    return {"enabled": bool(enabled), "start_sequence": start}


def config(store):
    with store.connect() as db:
        row = db.execute("SELECT * FROM changes_config WHERE id=1").fetchone()
    if row is None:
        return {"enabled": False, "settings": {}, "start_sequence": 0}
    return {"enabled": bool(row["enabled"]), "settings": json.loads(row["settings"]),
            "start_sequence": row["start_sequence"]}


def _enabled(db):
    row = db.execute("SELECT enabled FROM changes_config WHERE id=1").fetchone()
    return bool(row and row["enabled"])


def classify(origin_mode, coordinates, occurred_at, moment=None):
    """Deterministic novelty from provider coordinates first, pass role second.
    Timestamps are supporting evidence only; implausible dates degrade to
    explicit uncertainty instead of ordering authority."""
    hint = coordinates.get("arrival") if isinstance(coordinates, dict) else None
    if hint not in ("fresh", "historical", None):
        hint = None
    basis_date = None
    if occurred_at:
        try:
            moment = datetime.now(timezone.utc) if moment is None else datetime.fromisoformat(moment)
            when = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
            if when > moment + PLAUSIBLE_SKEW:
                basis_date = "implausible-source-date"
        except ValueError:
            basis_date = "invalid-source-date"
    else:
        basis_date = "missing-source-date"
    if hint == "historical":
        return "historical", "provider-coordinate", None
    if hint == "fresh":
        if basis_date:
            return "uncertain", basis_date, None
        return "live", "provider-coordinate", occurred_at
    if origin_mode == "backfill":
        return "historical", "backfill-pass", occurred_at
    if origin_mode == "reconcile":
        return "recovered", "reconciliation-discovery", occurred_at
    return "uncertain", "no-arrival-coordinate", occurred_at


def append(db, *, connection_id, source, stream, partition, generation, source_item_id,
           kind, origin_mode, record_ids=(), previous_record_ids=(), coordinates=None,
           occurred_at=None, observed_at=None, conversation_key=None, cause_event_id=None,
           trace_id=None, entity_ids=(), novelty=None, classification_basis=None):
    """Journal one accepted transition inside the caller's transaction.

    Never commits, never touches the network or a model; when the journal is
    disabled it records nothing and returns None. Event identity is
    (source, source_item_id, transition counter): an identical accepted state
    is a no-op only when the caller does not describe a transition.
    """
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}")
    if origin_mode not in ORIGIN_MODES:
        raise ValueError(f"origin_mode must be one of {ORIGIN_MODES}")
    if not _enabled(db):
        return None
    coordinates = coordinates if isinstance(coordinates, dict) else {}
    if novelty is None or classification_basis is None:
        guessed, basis, safe_occurred = classify(origin_mode, coordinates, occurred_at)
        novelty = novelty or guessed
        classification_basis = classification_basis or basis
        occurred_at = safe_occurred
    if novelty not in NOVELTIES:
        raise ValueError(f"novelty must be one of {NOVELTIES}")
    required_text(source, "source", 200)
    required_text(source_item_id, "source_item_id", 1000)
    record_ids = [required_text(r, "record_id", 100) for r in record_ids]
    previous_record_ids = [required_text(r, "previous_record_id", 100) for r in previous_record_ids]
    version = 1 + db.execute(
        "SELECT COALESCE(MAX(transition_version),0) FROM memory_changes WHERE source=? AND source_item_id=?",
        (source, source_item_id)).fetchone()[0]
    committed = now()
    event_id = "evt_" + digest([source, source_item_id, version, kind])[:24]
    sequence = 1 + db.execute("SELECT COALESCE(MAX(sequence),0) FROM memory_changes").fetchone()[0]
    db.execute("INSERT INTO memory_changes(event_id,sequence,schema_version,memory_epoch,connection_id,"
               "source,stream,partition,generation,source_item_id,transition_version,kind,origin_mode,"
               "novelty,occurred_at,observed_at,committed_at,conversation_key,entity_ids,cause_event_id,"
               "trace_id,classification_basis,record_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (event_id, sequence, 1,
                db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0],
                connection_id, source, stream, partition, generation, source_item_id, version,
                kind, origin_mode, novelty, occurred_at, observed_at, committed,
                None if conversation_key is None else str(conversation_key)[:500],
                json.dumps(sorted(set(entity_ids))), cause_event_id, trace_id,
                str(classification_basis)[:2000], len(record_ids) + len(previous_record_ids)))
    db.executemany("INSERT OR IGNORE INTO memory_change_refs VALUES(?,?,?)",
                   [(event_id, rid, "current") for rid in record_ids] +
                   [(event_id, rid, "previous") for rid in previous_record_ids])
    db.execute("INSERT INTO audit(action,object_id,created_at,metadata) VALUES(?,?,?,?)",
               ("memory_change", event_id, committed, json.dumps({"kind": kind, "novelty": novelty})))
    return {"event_id": event_id, "sequence": sequence, "transition_version": version}


def _conversation_key(operation):
    coordinates = operation.get("coordinates") or {}
    metadata = operation.get("metadata") or {}
    return coordinates.get("conversation_key") or metadata.get("thread_id") or metadata.get("conversation")


def describe_transition(applied, head, action, connection, lease, operation, current, metadata_changed=False):
    """Decide the journal transition one accepted page operation represents.

    Returns None when the accepted state is identical to what was already
    stored: replays and converged backfill must not wake anything.
    """
    record_ids = [row["id"] for row in applied if not row["duplicate"]]
    all_ids = [row["id"] for row in applied]
    coordinates = operation.get("coordinates") or {}
    fresh_hint = coordinates.get("arrival") == "fresh"
    if head is None:
        return {"kind": "created", "record_ids": all_ids, "previous_record_ids": []}
    if record_ids:
        previous = sorted(current - set(all_ids))
        return {"kind": "content_updated", "record_ids": all_ids, "previous_record_ids": previous}
    if action == "metadata_update":
        if operation.get("metadata") is not None and metadata_changed:
            return {"kind": "metadata_updated", "record_ids": all_ids,
                    "previous_record_ids": [], "classification_basis": "explicit-metadata-operation"}
        return None
    if (fresh_hint and lease["role"] == "incremental" and head["origin_role"] == "backfill"
            and head["state"] == "live"):
        # Backfill won the database race; refine the classification through a
        # linked transition instead of losing the live arrival.
        return {"kind": "created", "record_ids": all_ids, "previous_record_ids": [],
                "refinement": True}
    return None


def journal_operation_transition(db, connection, lease, operation, head, applied, current, stored_metadata):
    """Append the accepted logical operation's transition inside commit_page's transaction."""
    metadata_changed = stored_metadata != json.dumps(operation.get("metadata") or {}, sort_keys=True)
    transition = describe_transition(applied, head, operation["action"], connection, lease,
                                     operation, current, metadata_changed=metadata_changed)
    if transition is None:
        return None
    origin_mode = lease["role"] if lease["role"] in ORIGIN_MODES else "derived"
    cause_event_id = None
    if transition.get("refinement"):
        prior = db.execute("SELECT event_id FROM memory_changes WHERE source=? AND source_item_id=?"
                           " AND kind='created' ORDER BY sequence DESC LIMIT 1",
                           (connection["source"], operation["source_id"])).fetchone()
        cause_event_id = None if prior is None else prior["event_id"]
    records = operation.get("records") or []
    occurred_at = records[0].get("occurred_at") if records else None
    observed_at = records[0].get("observed_at") if records else None
    basis = transition.get("classification_basis")
    return append(db, connection_id=connection["id"], source=connection["source"],
                  stream=lease["stream"], partition=lease["partition"], generation=lease["generation"],
                  source_item_id=operation["source_id"], kind=transition["kind"],
                  origin_mode=origin_mode, record_ids=transition["record_ids"],
                  previous_record_ids=transition["previous_record_ids"],
                  coordinates=operation.get("coordinates"),
                  occurred_at=occurred_at, observed_at=observed_at,
                  conversation_key=_conversation_key(operation),
                  cause_event_id=cause_event_id,
                  entity_ids=(operation.get("metadata") or {}).get("entity_ids", []),
                  classification_basis=basis,
                  novelty="historical" if basis == "explicit-metadata-operation" else None)


def journal_state_change(db, connection, lease, source_item_id, kind, record_ids=()):
    """Removed/restored transitions for an existing head."""
    return append(db, connection_id=connection["id"], source=connection["source"],
                  stream=lease["stream"], partition=lease["partition"], generation=lease["generation"],
                  source_item_id=source_item_id, kind=kind,
                  origin_mode=lease["role"] if lease["role"] in ORIGIN_MODES else "derived",
                  record_ids=list(record_ids), novelty="uncertain",
                  classification_basis="authoritative-state-transition")


def journal_stream_lifecycle(db, connection, lease, kind, *, version_hint=None):
    """Backfill progress/completion: bounded per pass, never per historical item."""
    return append(db, connection_id=connection["id"], source=connection["source"],
                  stream=lease["stream"], partition=lease["partition"], generation=lease["generation"],
                  source_item_id=_STREAM_ITEM, kind=kind,
                  origin_mode="backfill", novelty="historical",
                  classification_basis="sync-pass-lifecycle")


def journal_gap(db, connection, lease, note):
    """An explicit coverage gap is an event: consumers may have unread range loss."""
    return append(db, connection_id=connection["id"], source=connection["source"],
                  stream=lease["stream"], partition=lease["partition"], generation=lease["generation"],
                  source_item_id=_GAP_ITEM, kind="sync_gap",
                  origin_mode=lease["role"] if lease["role"] in ORIGIN_MODES else "derived",
                  novelty="uncertain", classification_basis=str(note or "coverage gap")[:2000])


def _window_bound(value, name):
    """Normalize one date-window bound to the stored ISO form; opaque to callers."""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).isoformat()
    except ValueError:
        raise ValueError(f"{name} must be an ISO-8601 date or timestamp") from None


def _filter_digest(principal, kinds, source, connection_id, since=None, until=None):
    return digest([principal, sorted(kinds or []) or None, source, connection_id, since, until])


def _encode_cursor(sequence, principal, filter_digest, epoch):
    raw = json.dumps({"s": sequence, "p": digest(principal)[:16], "f": filter_digest[:16], "e": epoch})
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _decode_cursor(value, principal, filter_digest, current_epoch):
    try:
        payload = json.loads(base64.urlsafe_b64decode(value.encode()).decode())
        sequence, marker, bound, epoch = payload["s"], payload["p"], payload["f"], payload["e"]
    except Exception:
        raise ValueError("Cursor is not readable") from None
    if not isinstance(sequence, int) or digest(principal)[:16] != marker or bound != filter_digest[:16] \
            or epoch != current_epoch:
        # One generic rejection: never leak which binding failed, nor any counts.
        raise ValueError("Cursor does not apply to this principal, filter or memory epoch")
    return sequence


def read(store, *, principal="changes-reader", cursor=None, limit=50, kinds=None, source=None,
         connection_id=None, since=None, until=None, epoch=None, budget_bytes=64 * 1024):
    """Authorized, paginated inspection. A read is never an acknowledgment:
    no consumer position moves here (C04)."""
    required_text(principal, "principal", 200)
    if type(limit) is not int or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be 1..{MAX_LIMIT}")
    if kinds is not None and (not isinstance(kinds, list) or not kinds or set(kinds) - set(KINDS)):
        raise ValueError(f"kinds must be a subset of {KINDS}")
    since = _window_bound(since, "since")
    until = _window_bound(until, "until")
    if since is not None and until is not None and since > until:
        raise ValueError("since must not be after until")
    settings = config(store)
    if not settings["enabled"]:
        return {"enabled": False, "events": [], "next_cursor": None, "has_more": False,
                "scanned_through": 0, "high_watermark": 0, "resync_required": False}
    with store.connect() as db:
        current_epoch = db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0]
        high = db.execute("SELECT COALESCE(MAX(sequence),0) FROM memory_changes"
                          " WHERE memory_epoch=?", (current_epoch,)).fetchone()[0]
        after = settings["start_sequence"]
        resync = False
        if epoch is None:
            epoch = current_epoch
        if epoch != current_epoch:
            return {"enabled": True, "events": [], "next_cursor": None, "has_more": False,
                    "scanned_through": after, "high_watermark": high, "resync_required": True}
        if cursor is not None:
            digest_value = _filter_digest(principal, kinds, source, connection_id, since, until)
            requested = _decode_cursor(cursor, principal, digest_value, current_epoch)
            oldest = db.execute("SELECT COALESCE(MIN(sequence),0) FROM memory_changes"
                                " WHERE memory_epoch=?", (current_epoch,)).fetchone()[0]
            if requested < oldest:
                resync = True  # forced compaction past this consumer: explicit, never silent
            after = max(after, requested)
        clauses = ["sequence>?", "sequence>=?", "memory_epoch=?"]
        params = [after, settings["start_sequence"], current_epoch]
        if kinds:
            clauses.append("kind IN (" + ",".join("?" * len(kinds)) + ")")
            params.extend(kinds)
        if source:
            clauses.append("source=?"); params.append(source)
        if connection_id:
            clauses.append("connection_id=?"); params.append(connection_id)
        if since is not None:
            clauses.append("COALESCE(occurred_at,committed_at)>=?"); params.append(since)
        if until is not None:
            clauses.append("COALESCE(occurred_at,committed_at)<=?"); params.append(until)
        rows = db.execute(f"SELECT * FROM memory_changes WHERE {' AND '.join(clauses)}"
                          f" ORDER BY sequence LIMIT ?", params + [limit + 1]).fetchall()
        has_more = len(rows) > limit
        events, scanned = [], after
        budget = budget_bytes
        for row in rows[:limit + 1] if not has_more else rows[:limit]:
            payload = _resolve(db, row)
            size = len(json.dumps(payload).encode())
            if events and size > budget:
                has_more = True
                break
            budget -= size
            events.append(payload)
            scanned = payload["sequence"]
        if has_more:
            next_cursor = _encode_cursor(scanned, principal,
                                         _filter_digest(principal, kinds, source, connection_id,
                                                        since, until),
                                         current_epoch)
        else:
            next_cursor = None
        if not resync and any(event["kind"] == "sync_gap" for event in events):
            resync = True
    return {"enabled": True, "events": events, "next_cursor": next_cursor, "has_more": has_more,
            "scanned_through": scanned, "high_watermark": high, "resync_required": resync}


def store_generation_epoch(store):
    with store.connect() as db:
        return db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0]


def _resolve(db, row):
    """References resolve against live, visible canonical evidence only. An
    oversized reference set stays complete in the refs table with an explicit
    flag and the paginated resolver; inline references are never silently cut."""
    event = dict(row)
    visible = [r[0] for r in db.execute(
        "SELECT ref.record_id FROM memory_change_refs ref"
        " JOIN records rec ON rec.id=ref.record_id AND rec.deleted=0"
        " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
        " WHERE ref.event_id=? AND ref.role='current' AND COALESCE(vis.hidden,0)=0"
        " ORDER BY ref.record_id", (event["event_id"],))]
    event["record_ids"] = visible[:MAX_INLINE_REFS]
    event["all_visible_refs"] = len(visible)
    event["entity_ids"] = json.loads(event["entity_ids"] or "[]")
    event["truncated_refs"] = event["record_count"] > len(event["record_ids"])
    redacted = db.execute(
        "SELECT 1 FROM memory_change_refs ref JOIN records rec ON rec.id=ref.record_id"
        " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
        " WHERE ref.event_id=? AND (rec.deleted!=0 OR COALESCE(vis.hidden,0)!=0) LIMIT 1",
        (event["event_id"],)).fetchone() is not None
    if redacted:
        event["source_item_id"] = None
        event["conversation_key"] = None
        event["entity_ids"] = []
        event["redacted"] = True
    for key in ("occurred_at", "observed_at", "cause_event_id", "trace_id", "conversation_key"):
        event[key] = event[key] or None
    return event


def event_refs(store, event_id):
    with store.connect() as db:
        rows = db.execute("SELECT ref.record_id,ref.role FROM memory_change_refs ref"
                          " JOIN records rec ON rec.id=ref.record_id AND rec.deleted=0"
                          " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
                          " WHERE ref.event_id=? AND COALESCE(vis.hidden,0)=0"
                          " ORDER BY ref.role,ref.record_id",
                          (event_id,)).fetchall()
        sequence = db.execute("SELECT record_count FROM memory_changes WHERE event_id=?",
                              (event_id,)).fetchone()
    if sequence is None:
        raise ValueError("Unknown change event")
    return {"event_id": event_id,
            "record_ids": [row["record_id"] for row in rows if row["role"] == "current"],
            "previous_record_ids": [row["record_id"] for row in rows if row["role"] == "previous"],
            "record_count": sequence["record_count"]}


def pending_arrivals(store):
    """One logical arrival per source item: the newest created-classification
    event wins, so backfill/live refinement races schedule exactly once."""
    with store.connect() as db:
        rows = db.execute("""SELECT m.source,m.source_item_id,m.novelty FROM memory_changes m
            JOIN (SELECT source,source_item_id,MAX(sequence) AS top FROM memory_changes
                  WHERE kind='created' GROUP BY source,source_item_id) latest
              ON m.source=latest.source AND m.source_item_id=latest.source_item_id
             AND m.sequence=latest.top ORDER BY m.source,m.source_item_id""").fetchall()
    return [(row["source"], row["source_item_id"], row["novelty"]) for row in rows]


def consumer_positions(store):
    """Awareness scan positions; empty until consumers exist (phase 2)."""
    with store.connect() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='awareness_consumers'").fetchone():
            return {}
        return {row[0]: row[1] for row in db.execute("SELECT id,cursor_seq FROM awareness_consumers")}


def status(store):
    settings = config(store)
    with store.connect() as db:
        counts = {row[0]: row[1] for row in db.execute(
            "SELECT kind,COUNT(*) FROM memory_changes GROUP BY kind")} if settings["enabled"] else {}
        novelty = {row[0]: row[1] for row in db.execute(
            "SELECT novelty,COUNT(*) FROM memory_changes GROUP BY novelty")} if settings["enabled"] else {}
        high = db.execute("SELECT COALESCE(MAX(sequence),0) FROM memory_changes").fetchone()[0] if settings["enabled"] else 0
    return {"enabled": settings["enabled"], "high_watermark": high, "by_kind": counts,
            "by_novelty": novelty, "start_sequence": settings["start_sequence"]}
