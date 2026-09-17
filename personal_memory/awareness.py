"""Awareness consumers: deterministic attention policy, grouping, budgets and
durable background work. The journal is the feed; this module decides what
deserves model attention, freezes batch membership at materialization time,
and leases it with the established fence/receipt discipline. Model execution
and delivery are separate state: a session exposure never acknowledges
processing, and processing never acknowledges delivery.
"""
import json
import re
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from .common import digest, now, required_text

SCHEMA = '''
CREATE TABLE IF NOT EXISTS awareness_consumers(
  id TEXT PRIMARY KEY,profile TEXT NOT NULL, purpose TEXT NOT NULL,
  policy TEXT NOT NULL DEFAULT '{}', sources TEXT NOT NULL DEFAULT '[]',
  cursor_seq INTEGER NOT NULL DEFAULT 0, done_through INTEGER NOT NULL DEFAULT 0,
  last_source TEXT, resync_required INTEGER NOT NULL DEFAULT 0,
  enabled INTEGER NOT NULL DEFAULT 1, delivery TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS awareness_batches(
  id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL REFERENCES awareness_consumers(id),
  decision TEXT NOT NULL, source TEXT NOT NULL, group_key TEXT NOT NULL,
  seq_from INTEGER NOT NULL, seq_to INTEGER NOT NULL, membership_digest TEXT NOT NULL,
  event_count INTEGER NOT NULL, payload_chars INTEGER NOT NULL DEFAULT 0,
  priority INTEGER NOT NULL DEFAULT 1, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT, lease_fence INTEGER NOT NULL DEFAULT 0, lease_until TEXT,
  next_eligible_at TEXT, memory_epoch INTEGER NOT NULL, reason TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS batch_claim ON awareness_batches(consumer_id,state,priority,seq_from);
CREATE TABLE IF NOT EXISTS awareness_batch_events(
  batch_id TEXT NOT NULL REFERENCES awareness_batches(id), sequence INTEGER NOT NULL,
  event_id TEXT NOT NULL, PRIMARY KEY(batch_id,sequence));
CREATE TABLE IF NOT EXISTS awareness_receipts(
  id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, kind TEXT NOT NULL,
  subject TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '{}',
  memory_epoch INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS receipt_lookup ON awareness_receipts(consumer_id,kind,subject);
CREATE TABLE IF NOT EXISTS awareness_results(
  id TEXT PRIMARY KEY, batch_id TEXT NOT NULL UNIQUE, consumer_id TEXT NOT NULL,
  summary TEXT NOT NULL, citations TEXT NOT NULL DEFAULT '[]', proposals TEXT NOT NULL DEFAULT '[]',
  memory_epoch INTEGER NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS awareness_deliveries(
  id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE, consumer_id TEXT NOT NULL,
  profile TEXT NOT NULL, destination TEXT, batch_id TEXT, decision TEXT NOT NULL,
  state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, receipt TEXT, uncertainty TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processing_readiness(
  record_id TEXT NOT NULL, stage TEXT NOT NULL, state TEXT NOT NULL, detail TEXT,
  updated_at TEXT NOT NULL, PRIMARY KEY(record_id,stage));
'''

PURPOSES = ("foreground", "background")
READINESS_STAGES = ("semantic", "hindsight", "consolidation", "review")
READINESS_STATES = ("disabled", "pending", "running", "ready", "failed", "partial")
DECISIONS = ("record_only", "next_turn", "background_digest", "background_prompt")
DEFAULTS = {"packet_groups": 8, "packet_max_chars": 4000, "background_groups": 20,
            "background_max_chars": 24000, "window_seconds": 300, "scan_limit": 2000,
            "max_attempts": 5, "on_source_pause": "hold"}
SOURCE_PAUSE_POLICIES = ("hold", "process")
_PRIORITY = {"background_prompt": 0, "next_turn": 0, "background_digest": 1, "record_only": 9}
_ARRIVAL_KINDS = ("created", "content_updated", "enrichment")
_PROPOSAL_KINDS = ("relationship", "task_change", "notification", "insufficient_evidence")
_CHARS_PER_TOKEN = 4
_SUMMARY_LIMIT = 20000
_RESULT_BUDGET = 64 * 1024


def ensure(db):
    db.executescript(SCHEMA)
    columns = {row[1] for row in db.execute("PRAGMA table_info(awareness_consumers)")}
    if "delivery" not in columns:  # additive migration for pre-delivery databases
        db.execute("ALTER TABLE awareness_consumers ADD COLUMN delivery TEXT NOT NULL DEFAULT '{}'")


def initialize(store):
    with store.connect() as db:
        ensure(db)


def _moment():
    return datetime.now(timezone.utc)


def _policy(stored):
    policy = dict(DEFAULTS)
    if isinstance(stored, dict):
        policy.update({k: v for k, v in stored.items() if k in DEFAULTS})
    return policy


def _delivery_config(delivery):
    if delivery is None:
        return None
    if not isinstance(delivery, dict) \
            or set(delivery) - {"enabled", "destination", "quiet_hours", "urgent_bypass"}:
        raise ValueError("delivery keys must be from enabled, destination, quiet_hours, urgent_bypass")
    for flag in ("enabled", "urgent_bypass"):
        if flag in delivery and not isinstance(delivery[flag], bool):
            raise ValueError(f"delivery {flag} must be boolean")
    destination = delivery.get("destination")
    if destination is not None and (not isinstance(destination, str) or not destination
                                     or len(destination) > 500):
        raise ValueError("delivery destination must be a non-empty bounded string")
    window = delivery.get("quiet_hours")
    if window is not None:
        if not isinstance(window, dict) or set(window) != {"start", "end"}:
            raise ValueError("quiet_hours requires start and end only")
        for edge in window.values():
            if not isinstance(edge, str) or not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", edge):
                raise ValueError("quiet_hours edges must be HH:MM")
    return delivery


def configure_consumer(store, *, id, profile, purpose, policy=None, sources=None, delivery=None):
    """Consumers are server-bound identities, never model-chosen arguments."""
    required_text(id, "id", 200)
    required_text(profile, "profile", 200)
    if purpose not in PURPOSES:
        raise ValueError(f"purpose must be one of {PURPOSES}")
    if policy is not None:
        if not isinstance(policy, dict) or any(k not in DEFAULTS for k in policy):
            raise ValueError(f"policy keys must be a subset of {sorted(DEFAULTS)}")
        for key, value in policy.items():
            if key == "on_source_pause":
                if value not in SOURCE_PAUSE_POLICIES:
                    raise ValueError(f"on_source_pause must be one of {SOURCE_PAUSE_POLICIES}")
            elif not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError("numeric policy values must be positive integers")
    if sources is not None and (not isinstance(sources, list)
                                or any(not isinstance(s, str) or not s for s in sources)):
        raise ValueError("sources must be a list of names")
    delivery = _delivery_config(delivery)
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM awareness_consumers WHERE id=?", (id,)).fetchone()
        stamp = now()
        if row is None:
            prior_epoch_end = db.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM memory_changes WHERE memory_epoch<>?",
                (_epoch(db),)).fetchone()[0]
            db.execute("INSERT INTO awareness_consumers(id,profile,purpose,policy,sources,cursor_seq,"
                       "done_through,last_source,resync_required,enabled,delivery,created_at,updated_at) "
                       "VALUES(?,?,?,?,?,?,?,NULL,0,1,?,?,?)",
                       (id, profile, purpose, json.dumps(policy or {}, sort_keys=True),
                        json.dumps(sources or []), prior_epoch_end, prior_epoch_end,
                        json.dumps(delivery or {}), stamp, stamp))
        else:
            kept = json.loads(row["delivery"]) if "delivery" in row.keys() else {}
            db.execute("UPDATE awareness_consumers SET profile=?,purpose=?,policy=?,sources=?,"
                       "delivery=?,updated_at=? WHERE id=?",
                       (profile, purpose, json.dumps(policy or {}, sort_keys=True),
                        json.dumps(sources or []),
                        json.dumps(delivery if delivery is not None else kept), stamp, id))
    return get_consumer(store, id)


def get_consumer(store, id):
    with store.connect() as db:
        return _consumer_row(db, id)


def _consumer_row(db, id):
    row = db.execute("SELECT * FROM awareness_consumers WHERE id=?", (id,)).fetchone()
    if row is None:
        raise ValueError("Unknown awareness consumer")
    consumer = dict(row)
    consumer["policy"] = _policy(json.loads(consumer["policy"]))
    consumer["sources"] = json.loads(consumer["sources"])
    consumer["delivery"] = json.loads(consumer.get("delivery") or "{}")
    consumer["resync_required"] = bool(consumer["resync_required"])
    consumer["enabled"] = bool(consumer["enabled"])
    return consumer


def decide(event, purpose, policy):
    """The four scheduling decisions, evaluated deterministically from the
    committed event alone. Novelty claims need evidence: uncertain discoveries
    never schedule a prompt run, and metadata churn stays out of model work."""
    kind, novelty = event["kind"], event["novelty"]
    if event["origin_mode"] == "derived" and kind != "enrichment":
        # Our own analyses flowing back must never re-wake the model (B08). A material
        # enrichment is an explicit linked finding, not captured self-feedback (R04).
        return "record_only"
    if kind in ("backfill_progress", "backfill_complete"):
        return "background_digest" if purpose == "background" else "record_only"
    if kind not in _ARRIVAL_KINDS:
        return "record_only"
    if novelty == "live":
        return "next_turn" if purpose == "foreground" else "background_prompt"
    if novelty == "recovered":
        return "background_digest" if purpose == "background" else "record_only"
    if novelty == "uncertain":
        # Catch-up digest, never a live claim: uncertainty may not assert freshness.
        return "background_digest" if purpose == "background" else "record_only"
    return "record_only"


def group_of(event):
    """Email by account + thread, chat by account + conversation, everything
    else by its logical source item. Names never merge across accounts."""
    if event.get("conversation_key"):
        return f"{event['source']}:{event['conversation_key']}"
    return f"{event['source']}:{event['stream']}:{event['source_item_id']}"


def _event_chars(event):
    # Reserve room for visible IDs and per-record readiness that prepare adds.
    # Exact packet size is checked again after those fields are resolved.
    return max(500, len(json.dumps({k: event.get(k) for k in
                                    ("kind", "source", "source_item_id", "novelty",
                                     "conversation_key")}, sort_keys=True))
               + 150 * min(int(event.get("record_count") or 0), 8))


def _epoch(db):
    return db.execute("SELECT value FROM memory_epoch WHERE id=1").fetchone()[0]


def _invalid_batch_evidence(db, batch_id):
    """An analysis is valid only while every cited source revision remains visible."""
    return db.execute(
        "SELECT 1 FROM awareness_batch_events m JOIN memory_change_refs ref"
        " ON ref.event_id=m.event_id JOIN records rec ON rec.id=ref.record_id"
        " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
        " WHERE m.batch_id=? AND (rec.deleted!=0 OR COALESCE(vis.hidden,0)!=0) LIMIT 1",
        (batch_id,)).fetchone() is not None


def invalidate_record(db, record_id):
    """Purge analyses derived from a hidden/forgotten revision in its write transaction."""
    batch_ids = [row[0] for row in db.execute(
        "SELECT DISTINCT m.batch_id FROM memory_change_refs ref"
        " JOIN awareness_batch_events m ON m.event_id=ref.event_id"
        " WHERE ref.record_id=?", (record_id,))]
    db.execute("DELETE FROM processing_readiness WHERE record_id=?", (record_id,))
    if batch_ids:
        db.executemany("DELETE FROM awareness_results WHERE batch_id=?",
                       [(batch_id,) for batch_id in batch_ids])
        db.executemany("UPDATE awareness_deliveries SET state='cancelled',updated_at=?"
                       " WHERE batch_id=? AND state IN ('queued','quiet_hold')",
                       [(now(), batch_id) for batch_id in batch_ids])


def pending(store, consumer_id):
    """Cheap scheduler probe: decide whether a run is worth initializing at all.
    An idle check must never touch a model (B01)."""
    with store.connect() as db:
        consumer = _consumer_row(db, consumer_id)
        stamp = now()
        claimable = int(db.execute(
            "SELECT EXISTS(SELECT 1 FROM awareness_batches WHERE consumer_id=?"
            " AND (state='pending' OR (state='retry_wait' AND next_eligible_at<=?)))",
            (consumer_id, stamp)).fetchone()[0])
        unscanned = int(db.execute(
            "SELECT EXISTS(SELECT 1 FROM memory_changes WHERE sequence>? AND memory_epoch=?)",
            (consumer["cursor_seq"], _epoch(db))).fetchone()[0])
    return {"pending": bool(claimable or unscanned or consumer["resync_required"]),
            "claimable": claimable, "unscanned": unscanned,
            "resync_required": consumer["resync_required"]}


def sweep(store, consumer_id):
    """Materialize or explicitly suppress everything through the scan position.
    The cursor advances only inside the same transaction that durably records
    the work, so a crash cannot lose pending awareness."""
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        consumer = _consumer_row(db, consumer_id)
        from . import changes
        settings = changes.config(store)
        if not settings["enabled"] or not consumer["enabled"]:
            return {"enabled": settings["enabled"], "scanned": 0, "batches": 0,
                    "suppressed": 0, "cursor_seq": consumer["cursor_seq"],
                    "resync_required": consumer["resync_required"]}
        start = max(consumer["cursor_seq"], settings["start_sequence"])
        oldest = db.execute("SELECT MIN(sequence) FROM memory_changes WHERE memory_epoch=?",
                            (_epoch(db),)).fetchone()[0]
        resync = consumer["resync_required"]
        if oldest is not None and oldest > start + 1:
            resync = True
        epoch = _epoch(db)
        rows = [dict(r) for r in db.execute(
            "SELECT * FROM memory_changes WHERE sequence>? AND memory_epoch=?"
            " ORDER BY sequence LIMIT ?",
            (start, epoch, consumer["policy"]["scan_limit"]))]
        groups = OrderedDict()
        suppressed = 0
        for event in rows:
            decision = decide(event, consumer["purpose"], consumer["policy"])
            if decision != "record_only" and consumer["sources"] \
                    and event["source"] not in consumer["sources"]:
                # Eligibility is operator-bound on the consumer, never self-granted.
                decision = "record_only"
            if decision == "record_only":
                suppressed += 1
                continue
            groups.setdefault((decision, event["source"], group_of(event)), []).append(event)
        cap = (consumer["policy"]["packet_groups"] if consumer["purpose"] == "foreground"
               else consumer["policy"]["background_groups"])
        budget = (consumer["policy"]["packet_max_chars"] if consumer["purpose"] == "foreground"
                  else consumer["policy"]["background_max_chars"])
        # L04: a source whose every connection is paused or disconnected is
        # stalled; its analysis waits instead of churning. Events that belong to
        # no connection row (engine-derived) are never held.
        stamp = now()
        stalled = {row[0] for row in db.execute(
            "SELECT source FROM source_connections GROUP BY source"
            " HAVING SUM(CASE WHEN state='active' THEN 1 ELSE 0 END)=0")}
        hold = consumer["policy"]["on_source_pause"] == "hold"
        if hold and stalled:
            db.execute("UPDATE awareness_batches SET state='held',lease_owner=NULL,lease_until=NULL,"
                       "updated_at=? WHERE consumer_id=? AND state='pending' AND source IN ({})"
                       .format(",".join("?" * len(stalled))),
                       (stamp, consumer_id, *sorted(stalled)))
        resume = db.execute("SELECT id,source FROM awareness_batches WHERE consumer_id=?"
                            " AND state='held'", (consumer_id,)).fetchall()
        if resume:
            db.executemany("UPDATE awareness_batches SET state='pending',updated_at=? WHERE id=?",
                           [(stamp, row["id"]) for row in resume if row["source"] not in stalled])
        created = 0
        current, chars = [], 0

        def flush(members):
            if not members:
                return
            sequences = [e["sequence"] for _, events in members for e in events]
            event_ids = [e["event_id"] for _, events in members for e in events]
            decision, source, _ = members[0][0]
            group_key = "+".join(sorted({key[2] for key, _ in members}))
            size = sum(_event_chars(e) for _, events in members for e in events)
            batch_id = "abatch_" + digest([consumer_id, decision, source, sorted(event_ids)])[:24]
            state = "held" if hold and source in stalled else "pending"
            db.execute("INSERT OR IGNORE INTO awareness_batches(id,consumer_id,decision,source,group_key,"
                       "seq_from,seq_to,membership_digest,event_count,payload_chars,priority,state,attempts,"
                       "lease_owner,lease_fence,lease_until,next_eligible_at,memory_epoch,reason,"
                       "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,NULL,0,NULL,NULL,?,NULL,?,?)",
                       (batch_id, consumer_id, decision, source, group_key, min(sequences), max(sequences),
                        digest(sorted(event_ids)), len(event_ids), size, _PRIORITY[decision],
                        state, epoch, stamp, stamp))
            db.executemany("INSERT OR IGNORE INTO awareness_batch_events(batch_id,sequence,event_id) "
                           "VALUES(?,?,?)",
                           [(batch_id, e["sequence"], e["event_id"])
                            for _, events in members for e in events])

        for key, events in groups.items():
            part, part_chars = [], 0
            for event in events:
                size = _event_chars(event)
                if part and part_chars + size > budget:
                    if current and (key[:2] != current[0][0][:2]
                                    or len(current) + 1 > cap or chars + part_chars > budget):
                        flush(current); created += 1
                        current, chars = [], 0
                    current.append((key, part)); chars += part_chars
                    part, part_chars = [], 0
                part.append(event); part_chars += size
            if part:
                if current and (key[:2] != current[0][0][:2]
                                or len(current) + 1 > cap or chars + part_chars > budget):
                    flush(current); created += 1
                    current, chars = [], 0
                current.append((key, part)); chars += part_chars
        if current:
            flush(current)
            created += 1
        cursor = rows[-1]["sequence"] if rows else start
        if suppressed:
            db.execute("INSERT INTO awareness_receipts(id,consumer_id,kind,subject,state,detail,"
                       "memory_epoch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       ("arcpt_" + digest(["suppress", consumer_id, start, cursor])[:24], consumer_id,
                        "suppressed", f"{start + 1}-{cursor}", "recorded",
                        json.dumps({"suppressed": suppressed}), epoch, stamp, stamp))
        db.execute("UPDATE awareness_consumers SET cursor_seq=?,resync_required=?,updated_at=? WHERE id=?",
                   (cursor, int(resync), stamp, consumer_id))
        return {"enabled": True, "scanned": len(rows), "batches": created,
                "suppressed": suppressed, "cursor_seq": cursor, "resync_required": bool(resync)}


def claim(store, consumer_id, *, owner, ttl=300):
    """Lease the next batch. Expired leases are reclaimed with a strictly
    higher fence; a stale worker's completion becomes impossible."""
    required_text(owner, "owner", 200)
    if type(ttl) is not int or ttl < 0:
        raise ValueError("ttl must be a non-negative integer")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        consumer = _consumer_row(db, consumer_id)
        stamp = now()
        db.execute("UPDATE awareness_batches SET state='pending',lease_owner=NULL,lease_until=NULL,"
                   "updated_at=? WHERE consumer_id=? AND state='leased' AND (lease_until IS NULL"
                   " OR lease_until<=?)", (stamp, consumer_id, stamp))
        candidates = db.execute(
            "SELECT * FROM awareness_batches WHERE consumer_id=?"
            " AND (state='pending' OR (state='retry_wait' AND next_eligible_at<=?))"
            " AND memory_epoch=? ORDER BY priority,seq_from",
            (consumer_id, stamp, _epoch(db))).fetchall()
        if not candidates or not consumer["enabled"]:
            return None
        preferred = [row for row in candidates if row["source"] != consumer["last_source"]]
        batch = (preferred or candidates)[0]
        fence = batch["lease_fence"] + 1
        until = (_moment() + timedelta(seconds=ttl)).isoformat()
        db.execute("UPDATE awareness_batches SET state='leased',lease_owner=?,lease_fence=?,"
                   "lease_until=?,next_eligible_at=NULL,attempts=attempts+1,updated_at=? WHERE id=?",
                   (owner, fence, until, stamp, batch["id"]))
        db.execute("UPDATE awareness_consumers SET last_source=?,updated_at=? WHERE id=?",
                   (batch["source"], stamp, consumer_id))
        events = [dict(row) for row in db.execute(
            "SELECT m.sequence,m.event_id,c.* FROM awareness_batch_events m"
            " JOIN memory_changes c ON c.event_id=m.event_id WHERE m.batch_id=? ORDER BY m.sequence",
            (batch["id"],))]
        lease = dict(batch)
        lease.update({"batch_id": batch["id"], "owner": owner, "fence": fence, "lease_until": until,
                      "epoch": batch["memory_epoch"], "events": events, "state": "leased"})
        lease.pop("id", None)
        return lease


def _validated_lease(db, lease):
    for field in ("batch_id", "owner", "fence"):
        if field not in lease:
            raise ValueError(f"lease is missing {field}")
    row = db.execute("SELECT * FROM awareness_batches WHERE id=?", (lease["batch_id"],)).fetchone()
    if row is None:
        raise ValueError("Unknown awareness batch")
    if (row["state"] != "leased" or row["lease_owner"] != lease["owner"]
            or row["lease_fence"] != lease["fence"] or not row["lease_until"]
            or row["lease_until"] <= now()):
        raise ValueError("Lease is not held (expired or superseded by a newer fence)")
    if row["memory_epoch"] != _epoch(db):
        raise ValueError("Memory epoch changed; this worker is stale")
    return row


def _advance_done(db, consumer_id):
    """The processed watermark never passes an unfinished earlier batch, no
    matter what order completions arrive in (C05)."""
    minimum = db.execute("SELECT MIN(seq_from) FROM awareness_batches WHERE consumer_id=?"
                         " AND state NOT IN ('complete','cancelled','suppressed')",
                         (consumer_id,)).fetchone()[0]
    if minimum is not None:
        return minimum - 1
    maximum = db.execute("SELECT MAX(seq_to) FROM awareness_batches WHERE consumer_id=?"
                         " AND state='complete'", (consumer_id,)).fetchone()[0]
    current = db.execute("SELECT done_through FROM awareness_consumers WHERE id=?",
                         (consumer_id,)).fetchone()[0]
    return max(current, maximum if maximum is not None else current)


def _validate_result(db, batch_id, text, citations, proposals):
    """References, size and kinds are checked before anything commits: an invalid
    model output can never complete work or seed a notification (B04/B06)."""
    if text is not None and len(text) > _SUMMARY_LIMIT:
        raise ValueError(f"summary exceeds the {_SUMMARY_LIMIT}-character result limit")
    refs = {row[0] for row in db.execute(
        "SELECT DISTINCT ref.record_id FROM awareness_batch_events m"
        " JOIN memory_change_refs ref ON ref.event_id=m.event_id WHERE m.batch_id=?", (batch_id,))}
    visible = {row[0] for row in db.execute(
        "SELECT DISTINCT ref.record_id FROM awareness_batch_events m"
        " JOIN memory_change_refs ref ON ref.event_id=m.event_id"
        " JOIN records rec ON rec.id=ref.record_id AND rec.deleted=0"
        " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
        " WHERE m.batch_id=? AND COALESCE(vis.hidden,0)=0", (batch_id,))}
    cites = []
    for item in citations or ():
        record_id = item.get("record_id") if isinstance(item, dict) else item
        if not isinstance(record_id, str) or record_id not in refs or record_id not in visible:
            raise ValueError("Citation does not reference visible evidence in this batch")
        cites.append(record_id)
    props = []
    for item in proposals or ():
        if not isinstance(item, dict) or item.get("kind") not in _PROPOSAL_KINDS:
            raise ValueError(f"proposal kind must be one of {_PROPOSAL_KINDS}")
        props.append(item)
    if len(json.dumps({"text": text, "citations": cites, "proposals": props},
                      default=str)) > _RESULT_BUDGET:
        raise ValueError("result exceeds the durable payload budget")
    return cites, props


def complete(store, lease, *, summary=None, citations=(), proposals=()):
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = _validated_lease(db, lease)
        stamp = now()
        text = None
        if summary is not None:
            text = str(summary.get("text", summary) if isinstance(summary, dict) else summary)
            citations, proposals = _validate_result(db, row["id"], text, citations, proposals)
        db.execute("UPDATE awareness_batches SET state='complete',lease_owner=NULL,lease_until=NULL,"
                   "reason=NULL,updated_at=? WHERE id=?", (stamp, row["id"]))
        if text is not None:
            db.execute("INSERT OR REPLACE INTO awareness_results(id,batch_id,consumer_id,summary,"
                       "citations,proposals,memory_epoch,created_at) VALUES(?,?,?,?,?,?,?,?)",
                       ("ares_" + digest([row["id"]])[:24], row["id"], row["consumer_id"], text,
                        json.dumps(list(citations)), json.dumps(list(proposals)),
                        row["memory_epoch"], stamp))
        db.execute("INSERT OR REPLACE INTO awareness_receipts(id,consumer_id,kind,subject,state,detail,"
                   "memory_epoch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                   ("arcpt_" + digest(["processed", row["id"]])[:24], row["consumer_id"], "processed",
                    row["id"], "complete", json.dumps({"decision": row["decision"]}),
                    row["memory_epoch"], stamp, stamp))
        done = _advance_done(db, row["consumer_id"])
        db.execute("UPDATE awareness_consumers SET done_through=?,updated_at=? WHERE id=?",
                   (max(done, 0), stamp, row["consumer_id"]))
        return {"batch_id": row["id"], "state": "complete", "done_through": max(done, 0)}


def defer(store, lease, *, reason, retry_in=60):
    """Bounded retries with an explicit reason; exhaustion quarantines rather
    than silently dropping the work."""
    if type(retry_in) is not int or retry_in < 0:
        raise ValueError("retry_in must be a non-negative integer")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = _validated_lease(db, lease)
        consumer = _consumer_row(db, row["consumer_id"])
        stamp = now()
        exhausted = row["attempts"] >= consumer["policy"]["max_attempts"]
        state = "quarantined" if exhausted else "retry_wait"
        db.execute("UPDATE awareness_batches SET state=?,lease_owner=NULL,lease_until=NULL,reason=?,"
                   "next_eligible_at=?,updated_at=? WHERE id=?",
                   (state, str(reason)[:2000],
                   None if exhausted else (_moment() + timedelta(seconds=retry_in)).isoformat(),
                   stamp, row["id"]))
        db.execute("INSERT OR REPLACE INTO awareness_receipts(id,consumer_id,kind,subject,state,detail,"
                   "memory_epoch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                   ("arcpt_" + digest(["deferred", row["id"], state])[:24], row["consumer_id"],
                    "deferred", row["id"], state, json.dumps({"reason": str(reason)[:200]}),
                    row["memory_epoch"], stamp, stamp))
        return {"batch_id": row["id"], "state": state}


def prepare(store, *, consumer_id, session_id, turn_id=None):
    """Build the bounded next-turn packet for one trusted session. Preparation
    supplies context; it never acknowledges exposure or processing."""
    required_text(session_id, "session_id", 200)
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        consumer = _consumer_row(db, consumer_id)
        policy = consumer["policy"]
        epoch = _epoch(db)
        batches = db.execute("SELECT * FROM awareness_batches WHERE consumer_id=?"
                             " AND decision='next_turn' AND state IN ('pending','leased','retry_wait')"
                             " AND memory_epoch=? ORDER BY seq_from", (consumer_id, epoch)).fetchall()
        pending_groups = []
        for batch in batches:
            receipt = db.execute("SELECT 1 FROM awareness_receipts WHERE kind='supplied'"
                                 " AND subject=? AND state='exposed' AND memory_epoch=?",
                                 (f"{batch['id']}|{session_id}", epoch)).fetchone()
            if receipt:
                continue
            events = [dict(row) for row in db.execute(
                "SELECT m.sequence,m.event_id,c.* FROM awareness_batch_events m"
                " JOIN memory_changes c ON c.event_id=m.event_id WHERE m.batch_id=? ORDER BY m.sequence",
                (batch["id"],))]
            grouped = OrderedDict()
            for event in events:
                # Recheck visibility at supply time: forgotten or hidden evidence
                # never rides into a packet, and no content is copied here.
                visible = [row[0] for row in db.execute(
                    "SELECT ref.record_id FROM memory_change_refs ref JOIN records rec ON rec.id=ref.record_id"
                    " LEFT JOIN record_visibility vis ON vis.record_id=rec.id"
                    " WHERE ref.event_id=? AND ref.role='current' AND rec.deleted=0"
                    " AND COALESCE(vis.hidden,0)=0 ORDER BY ref.record_id", (event["event_id"],))]
                if event["record_count"] and not visible:
                    continue
                event = {key: event[key] for key in
                         ("sequence", "event_id", "kind", "source", "stream",
                          "source_item_id", "novelty", "origin_mode", "occurred_at",
                          "conversation_key", "classification_basis")}
                event["visible_record_ids"] = visible[:8]
                if len(visible) > 8:
                    event["more_visible_refs"] = len(visible) - 8
                # §8 step 3: readiness travels with the packet so the model can
                # tell analyzed evidence from still-processing evidence.
                event["readiness"] = {rid: _readiness_row(db, rid) for rid in visible[:8]}
                grouped.setdefault(group_of(event), []).append(event)
            for group_key, rows in grouped.items():
                # A batch can contain several groups, while a packet can fit
                # only some of them. Exposure belongs to the exact group of
                # events sent to the model, never to the whole batch.
                group_id = digest([batch["id"], [event["event_id"] for event in rows]])[:24]
                subject = f"{batch['id']}|{session_id}|{group_id}"
                if db.execute("SELECT 1 FROM awareness_receipts WHERE kind='supplied'"
                              " AND subject=? AND state='exposed' AND memory_epoch=?",
                              (subject, epoch)).fetchone():
                    continue
                analysis = None
                for event in rows:
                    # A completed background analysis of the same evidence is reused
                    # instead of paying for another model run (B05).
                    found = db.execute(
                        "SELECT r.summary,r.citations,r.batch_id FROM awareness_results r"
                        " JOIN awareness_batch_events m ON m.batch_id=r.batch_id"
                        " WHERE m.event_id=? AND r.memory_epoch=?"
                        " ORDER BY r.created_at DESC, r.batch_id LIMIT 1",
                        (event["event_id"], epoch)).fetchone()
                    if found is not None and not _invalid_batch_evidence(db, found["batch_id"]):
                        analysis = {"summary": found["summary"], "citations": json.loads(found["citations"])}
                        break
                group = {"group_key": group_key, "batch_id": batch["id"], "events": rows}
                if analysis:
                    group["analysis"] = analysis
                    group["reused_analysis"] = True
                pending_groups.append(group)
        selected, chars = [], 0
        for group in pending_groups:
            if len(selected) >= policy["packet_groups"]:
                break
            size = len(json.dumps(group, default=str, ensure_ascii=False))
            if size > policy["packet_max_chars"] and "analysis" in group:
                group = {key: value for key, value in group.items()
                         if key not in ("analysis", "reused_analysis")}
                size = len(json.dumps(group, default=str, ensure_ascii=False))
            if chars + size > policy["packet_max_chars"]:
                continue
            selected.append(group)
            chars += size
        stamp = now()
        packet_id = "apkt_" + digest([consumer_id, session_id,
                                      epoch, [(g["batch_id"], [e["event_id"] for e in g["events"]])
                                              for g in selected]])[:24]
        for group in selected:
            group_id = digest([group["batch_id"],
                               [event["event_id"] for event in group["events"]]])[:24]
            subject = f"{group['batch_id']}|{session_id}|{group_id}"
            db.execute("INSERT OR REPLACE INTO awareness_receipts(id,consumer_id,kind,subject,"
                       "state,detail,memory_epoch,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       ("arcpt_" + digest(["supplied", subject])[:24], consumer_id,
                        "supplied", subject, "supplied",
                        json.dumps({"packet_id": packet_id, "session": session_id}),
                        epoch, stamp, stamp))
        return {"packet_id": packet_id, "consumer_id": consumer_id, "session_id": session_id,
                "groups": selected, "omitted_groups": len(pending_groups) - len(selected),
                "estimated_tokens": chars // _CHARS_PER_TOKEN, "token_estimate": True,
                "memory_epoch": epoch, "untrusted": True}


def expose(store, *, packet_id, session_id, turn_id):
    """The host confirms the packet actually went into a model request. This
    touches only session receipts: never batch or delivery state."""
    required_text(packet_id, "packet_id", 200)
    required_text(turn_id, "turn_id", 200)
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute("SELECT * FROM awareness_receipts WHERE kind='supplied'"
                          " AND json_extract(detail,'$.packet_id')=?"
                          " AND json_extract(detail,'$.session')=? AND memory_epoch=?",
                          (packet_id, session_id, _epoch(db))).fetchall()
        if not rows:
            raise ValueError("Packet was never supplied to this session")
        already = all(row["state"] == "exposed" for row in rows)
        stamp = now()
        for row in rows:
            db.execute("UPDATE awareness_receipts SET state='exposed',updated_at=?,"
                       "detail=json_set(detail,'$.turn_id',?) WHERE id=?",
                       (stamp, str(turn_id)[:200], row["id"]))
        return {"packet_id": packet_id, "session_id": session_id, "turn_id": turn_id,
                "recorded": not already, "already": already}


def mark_readiness(store, record_id, stage, state, detail=None):
    """Small authoritative projection updated with each engine's completion record.
    Routine completions change status only; they never journal an arrival (R03)."""
    required_text(record_id, "record_id", 100)
    if stage not in READINESS_STAGES:
        raise ValueError(f"stage must be one of {READINESS_STAGES}")
    if state not in READINESS_STATES:
        raise ValueError(f"state must be one of {READINESS_STATES}")
    if detail is not None and not isinstance(detail, dict):
        raise ValueError("detail must be an object")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT 1 FROM records WHERE id=?", (record_id,)).fetchone() is None:
            raise ValueError("Readiness requires a stored record revision")
        db.execute("INSERT OR REPLACE INTO processing_readiness VALUES(?,?,?,?,?)",
                   (record_id, stage, state, json.dumps(detail) if detail is not None else None,
                    now()))
    return {"record_id": record_id, "stage": stage, "state": state}


def _readiness_row(db, record_id):
    """One record's stage states derived inside the caller's transaction."""
    row = db.execute(
        "SELECT rec.deleted, COALESCE(vis.hidden,0) AS hidden FROM records rec"
        " LEFT JOIN record_visibility vis ON vis.record_id=rec.id WHERE rec.id=?",
        (record_id,)).fetchone()
    state = {"canonical": ("missing" if row is None else "forgotten" if row["deleted"]
                           else "hidden" if row["hidden"] else "ready")}
    for stage in READINESS_STAGES:
        done = db.execute("SELECT state FROM processing_readiness WHERE record_id=? AND stage=?",
                          (record_id, stage)).fetchone()
        if done:
            state[stage] = done[0]
        elif stage in ("semantic", "hindsight"):
            table = "vector_done" if stage == "semantic" else "hindsight_done"
            installed = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                                   (table,)).fetchone()
            if not installed:
                state[stage] = "disabled"
            else:
                complete = db.execute(f"SELECT 1 FROM {table} WHERE record_id=? LIMIT 1",
                                      (record_id,)).fetchone()
                state[stage] = "ready" if complete else "pending"
        else:
            state[stage] = "pending" if stage == "consolidation" else "disabled"
    return state


def readiness(store, record_ids):
    """Canonical truth is derived live; optional engines default to pending and
    never block awareness (R01)."""
    with store.connect() as db:
        result = {}
        for record_id in dict.fromkeys(str(item) for item in record_ids):
            result[record_id] = _readiness_row(db, record_id)
        return result


def material_enrichment(store, *, source, stream, source_item_id, record_ids, cause_event_id,
                        conversation_key=None):
    """Journal a genuinely new source-backed connection linked to its cause event.
    It schedules as a catch-up digest with citations, never a live arrival (R04)."""
    from . import changes
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute("SELECT 1 FROM memory_changes WHERE event_id=?",
                      (cause_event_id,)).fetchone() is None:
            raise ValueError("cause_event_id does not reference a journaled event")
        for record_id in record_ids:
            if db.execute("SELECT 1 FROM records WHERE id=? AND deleted=0",
                          (record_id,)).fetchone() is None:
                raise ValueError("Enrichment cites non-live evidence")
        event = changes.append(db, connection_id="engine:readiness", source=source, stream=stream,
                               partition="", generation=1, source_item_id=source_item_id,
                               kind="enrichment", origin_mode="derived", record_ids=list(record_ids),
                               cause_event_id=cause_event_id, novelty="recovered",
                               classification_basis="material-connection",
                               conversation_key=conversation_key)
    if event is None:
        raise ValueError("The change journal is disabled; enrichment records nothing")
    return {**event, "kind": "enrichment"}


def _in_quiet(window, moment):
    if not isinstance(window, dict) or "start" not in window or "end" not in window:
        return False

    def minutes(text):
        hour, minute = text.split(":")
        return int(hour) * 60 + int(minute)

    start, end = minutes(window["start"]), minutes(window["end"])
    if start == end:
        return True  # an explicitly whole-day quiet window
    now_minutes = moment.hour * 60 + moment.minute
    if start < end:
        return start <= now_minutes < end
    return now_minutes >= start or now_minutes < end  # overnight window


def queue_delivery(store, *, consumer_id, batch_id, group_key, group_version=None, urgent=False):
    """Durable notification intent keyed by profile, destination, decision and analysed
    group version. Coalesced at the profile level; disabled until configured (D01/D02)."""
    required_text(group_key, "group_key", 500)
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        consumer = _consumer_row(db, consumer_id)
        batch = db.execute("SELECT * FROM awareness_batches WHERE id=?",
                           (batch_id,)).fetchone()
        if batch is None or batch["consumer_id"] != consumer_id:
            raise ValueError("Unknown awareness batch for this consumer")
        if batch["state"] != "complete":
            raise ValueError("Only a completed analysis may intend a notification")
        if batch["memory_epoch"] != _epoch(db):
            raise ValueError("Memory epoch changed after this batch was analysed")
        delivery = consumer["delivery"]
        if not delivery.get("enabled") or not delivery.get("destination"):
            return {"state": "disabled", "delivery_id": None, "already": False}
        key = digest(["deliver", consumer["profile"], delivery["destination"], batch["decision"],
                      group_key, group_version or batch["membership_digest"]])
        existing = db.execute("SELECT * FROM awareness_deliveries WHERE idempotency_key=?",
                              (key,)).fetchone()
        if existing is not None:
            return {"state": existing["state"], "delivery_id": existing["id"], "already": True}
        held = _in_quiet(delivery.get("quiet_hours"), _moment()) \
            and not (urgent and delivery.get("urgent_bypass"))
        state = "quiet_hold" if held else "queued"
        delivery_id = "adel_" + key[:24]
        stamp = now()
        db.execute("INSERT INTO awareness_deliveries(id,idempotency_key,consumer_id,profile,destination,"
                   "batch_id,decision,state,attempts,receipt,uncertainty,created_at,updated_at)"
                   " VALUES(?,?,?,?,?,?,?,?,0,NULL,NULL,?,?)",
                   (delivery_id, key, consumer_id, consumer["profile"], delivery["destination"],
                    batch_id, batch["decision"], state, stamp, stamp))
    return {"state": state, "delivery_id": delivery_id, "already": False}


def next_delivery(store):
    """Claim the oldest intent for a Hermes run. Claiming is not sending: the intent
    stays durable and safely retryable until a receipt lands (D03)."""
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM awareness_deliveries WHERE state='queued'"
                         " ORDER BY created_at,id LIMIT 1").fetchone()
        if row is None:
            return None
        db.execute("UPDATE awareness_deliveries SET state='attempted',attempts=attempts+1,"
                   "updated_at=? WHERE id=?", (now(), row["id"]))
        claimed = dict(row)
        claimed.update({"state": "attempted", "attempts": row["attempts"] + 1})
        return claimed


def confirm_delivery(store, delivery_id, *, receipt):
    required_text(receipt, "receipt", 500)
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM awareness_deliveries WHERE id=?",
                         (delivery_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown delivery intent")
        if row["state"] == "confirmed":
            return {"state": "confirmed", "already": True}
        if row["state"] != "attempted":
            raise ValueError("Only an attempted intent may receive a delivery receipt")
        db.execute("UPDATE awareness_deliveries SET state='confirmed',receipt=?,uncertainty=NULL,"
                   "updated_at=? WHERE id=?", (str(receipt)[:500], now(), delivery_id))
    return {"state": "confirmed", "already": False}


def reconcile_deliveries(store, *, stale_seconds=3600):
    """Attempted-without-receipt becomes explicitly uncertain: exactly-once cannot
    be promised on a channel without idempotency, so no blind resend (D04).
    Expired quiet holds are released here as well."""
    if type(stale_seconds) is not int or stale_seconds < 0:
        raise ValueError("stale_seconds must be a non-negative integer")
    cutoff = (_moment() - timedelta(seconds=stale_seconds)).isoformat()
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        stale = [row["id"] for row in db.execute(
            "SELECT id FROM awareness_deliveries WHERE state='attempted' AND updated_at<=?",
            (cutoff,))]
        for delivery_id in stale:
            db.execute("UPDATE awareness_deliveries SET state='uncertain',uncertainty=?,updated_at=?"
                       " WHERE id=?",
                       ("Attempted without a recorded receipt; resending requires an explicit "
                        "idempotency check against the channel", now(), delivery_id))
        released = 0
        for row in db.execute("SELECT id,consumer_id FROM awareness_deliveries"
                              " WHERE state='quiet_hold'"):
            consumer = _consumer_row(db, row["consumer_id"])
            if not _in_quiet(consumer["delivery"].get("quiet_hours"), _moment()):
                db.execute("UPDATE awareness_deliveries SET state='queued',updated_at=? WHERE id=?",
                           (now(), row["id"]))
                released += 1
    return {"uncertain": len(stale), "released": released}


def revalidate_delivery(store, delivery_id):
    """Recheck current evidence visibility before sending: a source deleted or
    corrected while queued cancels rather than resurfaces stale content (D05)."""
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM awareness_deliveries WHERE id=?",
                         (delivery_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown delivery intent")
        if row["state"] not in ("queued", "quiet_hold", "attempted"):
            return {"state": row["state"], "changed": False}
        batch = db.execute("SELECT memory_epoch FROM awareness_batches WHERE id=?",
                           (row["batch_id"],)).fetchone()
        if (batch is None or batch["memory_epoch"] != _epoch(db)
                or _invalid_batch_evidence(db, row["batch_id"])):
            db.execute("UPDATE awareness_deliveries SET state='cancelled',uncertainty=?,updated_at=?"
                       " WHERE id=?",
                       ("Source evidence was deleted or hidden after the intent was queued",
                        now(), delivery_id))
            return {"state": "cancelled", "changed": True}
    return {"state": row["state"], "changed": False}


def compact(store, *, through, force=False):
    """Retention never silently erases a consumer's unread range: crossing one
    requires an explicit force and leaves a visible, resumable gap."""
    if type(through) is not int or through < 0:
        raise ValueError("through must be a non-negative integer")
    with store.lock, store.connect() as db:
        db.execute("BEGIN IMMEDIATE")
        least = db.execute("SELECT MIN(cursor_seq) FROM awareness_consumers").fetchone()[0]
        unfinished = [dict(row) for row in db.execute(
            "SELECT id,consumer_id FROM awareness_batches WHERE seq_from<=?"
            " AND state NOT IN ('complete','suppressed','cancelled')", (through,))]
        if not force and ((least is not None and through > least) or unfinished):
            raise ValueError("Compaction would cross unread or unfinished awareness work; pass force explicitly")
        event_ids = [row[0] for row in db.execute(
            "SELECT event_id FROM memory_changes WHERE sequence<=?", (through,))]
        touched = [row[0] for row in db.execute(
            "SELECT DISTINCT batch_id FROM awareness_batch_events WHERE sequence<=?", (through,))]
        if touched:
            db.executemany("DELETE FROM awareness_results WHERE batch_id=?", [(i,) for i in touched])
            db.executemany("UPDATE awareness_deliveries SET state='cancelled',updated_at=?"
                           " WHERE batch_id=? AND state IN ('queued','quiet_hold')",
                           [(now(), i) for i in touched])
        if force and unfinished:
            ids = [row["id"] for row in unfinished]
            db.executemany("UPDATE awareness_batches SET state='cancelled',lease_owner=NULL,"
                           "lease_until=NULL,reason='Journal compacted by administrator',updated_at=?"
                           " WHERE id=?", [(now(), batch_id) for batch_id in ids])
        db.executemany("DELETE FROM memory_change_refs WHERE event_id=?", [(e,) for e in event_ids])
        db.execute("DELETE FROM memory_changes WHERE sequence<=?", (through,))
        affected = []
        if force:
            rows = db.execute("SELECT id FROM awareness_consumers WHERE cursor_seq<?"
                              " OR done_through<?", (through, through)).fetchall()
            affected = sorted({row["id"] for row in rows} | {row["consumer_id"] for row in unfinished})
            for consumer_id in affected:
                db.execute("UPDATE awareness_consumers SET resync_required=1,updated_at=? WHERE id=?",
                           (now(), consumer_id))
        db.execute("INSERT INTO audit(action,object_id,created_at,metadata) VALUES(?,?,?,?)",
                   ("changes_compact", str(through), now(),
                    json.dumps({"deleted": len(event_ids), "resync": affected})))
        return {"deleted": len(event_ids), "resync_consumers": affected}


def replay(store, *, consumer_id, from_sequence=0, dry_run=True):
    """Bounded administrator journal replay into fresh analysis work, dry-run
    by default. Replay never re-sends notifications: intents already attempted,
    confirmed or uncertain are reported and left untouched."""
    if type(from_sequence) is not int or from_sequence < 0:
        raise ValueError("from_sequence must be a non-negative integer")
    with store.lock, store.connect() as db:
        consumer = _consumer_row(db, consumer_id)
        cursor = consumer["cursor_seq"]
        if from_sequence > cursor:
            raise ValueError("Replay start is ahead of the consumer cursor")
        first = max(from_sequence, 1)  # sequence 0 is not an event: whole-journal means from 1
        protected = db.execute("SELECT COUNT(*) FROM awareness_deliveries WHERE consumer_id=?"
                               " AND state IN ('attempted','confirmed','uncertain')",
                               (consumer_id,)).fetchone()[0]
        result = {"consumer_id": consumer_id, "from_sequence": from_sequence,
                  "through_sequence": cursor, "events": max(0, cursor - first + 1),
                  "notifications_protected": protected, "dry_run": bool(dry_run)}
        if dry_run:
            return result
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE awareness_consumers SET cursor_seq=?,resync_required=0,updated_at=?"
                   " WHERE id=?", (max(from_sequence - 1, 0), now(), consumer_id))
        # Batch identity is the frozen event set: re-queue finished work in range
        # instead of letting the sweep's INSERT OR IGNORE drop it (replay means
        # reprocessing the analysis, never re-notifying).
        db.execute("UPDATE awareness_batches SET state='pending',lease_owner=NULL,lease_until=NULL,"
                   "next_eligible_at=NULL,reason=NULL,updated_at=? WHERE consumer_id=?"
                   " AND state IN ('complete','suppressed') AND seq_to>=? AND seq_from<=?",
                   (now(), consumer_id, first, cursor))
        db.execute("INSERT INTO audit(action,object_id,created_at,metadata) VALUES(?,?,?,?)",
                   ("awareness_replay", consumer_id, now(),
                    json.dumps({"from_sequence": from_sequence, "events": result["events"]})))
        result["applied"] = True
        return result


def diagnose(store):
    """Doctor-facing operational health checks: state only, never content."""
    from . import changes
    settings = changes.config(store)
    checks = []

    def add(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    initialize(store)
    with store.connect() as db:
        consumers = [dict(row) for row in db.execute("SELECT * FROM awareness_consumers")]
        add("awareness_journal", settings["enabled"] or not consumers,
            "enabled" if settings["enabled"] else "disabled; no consumers registered" if not consumers
            else "disabled while consumers are registered")
        missing = [row["id"] for row in consumers
                   if (json.loads(row["delivery"] or "{}")).get("enabled")
                   and not (json.loads(row["delivery"] or "{}")).get("destination")]
        add("awareness_destinations", not missing,
            "all delivery consumers have destinations" if not missing
            else "delivery enabled without destination: " + ", ".join(sorted(missing)))
        quarantined = db.execute("SELECT COUNT(*) FROM awareness_batches WHERE state='quarantined'"
                                 ).fetchone()[0]
        resync = sum(1 for row in consumers if row["resync_required"])
        add("awareness_consumers", quarantined == 0,
            f"{len(consumers)} consumers healthy" if quarantined == 0
            else f"{quarantined} quarantined batches need operator review"
                 + (f"; {resync} consumers awaiting explicit resync" if resync else ""))
        stale = db.execute("SELECT COUNT(*) FROM awareness_batches WHERE state='leased'"
                           " AND lease_until<?", (_moment().isoformat(),)).fetchone()[0]
        add("awareness_leases", stale == 0,
            "no expired leases in flight" if stale == 0
            else f"{stale} expired leases await reclaim by the next claim")
        failing = db.execute("SELECT stage,COUNT(*) FROM processing_readiness"
                             " WHERE state='failed' GROUP BY stage ORDER BY stage").fetchall()
        add("awareness_stages", not failing,
            "no failing processing stages" if not failing else "failing stages: "
            + ", ".join(f"{row[0]} ({row[1]})" for row in failing))
    return checks


def status(store):
    from . import changes
    with store.connect() as db:
        consumers = []
        for row in db.execute("SELECT * FROM awareness_consumers ORDER BY id"):
            consumers.append({"id": row["id"], "profile": row["profile"], "purpose": row["purpose"],
                              "enabled": bool(row["enabled"]), "cursor_seq": row["cursor_seq"],
                              "done_through": row["done_through"],
                              "cursor_lag": row["cursor_seq"] - row["done_through"],
                              "resync_required": bool(row["resync_required"]),
                              "pending_batches": db.execute(
                                  "SELECT COUNT(*) FROM awareness_batches WHERE consumer_id=?"
                                  " AND state IN ('pending','leased','retry_wait')",
                                  (row["id"],)).fetchone()[0]})
        batches = {row[0]: row[1] for row in db.execute(
            "SELECT state,COUNT(*) FROM awareness_batches GROUP BY state")}
        counts = {(row[0], row[1]): row[2] for row in db.execute(
            "SELECT kind,state,COUNT(*) FROM awareness_receipts GROUP BY kind,state")}
        receipts = {"supplied_unexposed": counts.get(("supplied", "supplied"), 0),
                    "exposed": counts.get(("supplied", "exposed"), 0),
                    "processed": counts.get(("processed", "complete"), 0),
                    "deferred": sum(v for (kind, _), v in counts.items() if kind == "deferred"),
                    "suppressed": counts.get(("suppressed", "recorded"), 0)}
        deliveries = {row[0]: row[1] for row in db.execute(
            "SELECT state,COUNT(*) FROM awareness_deliveries GROUP BY state")}
        moment = _moment()
        oldest = db.execute("SELECT MIN(created_at) FROM awareness_batches"
                            " WHERE state IN ('pending','retry_wait')").fetchone()[0]
        counters = {"events_total": db.execute("SELECT COUNT(*) FROM memory_changes").fetchone()[0],
                    "batches_created": db.execute("SELECT COUNT(*) FROM awareness_batches").fetchone()[0],
                    "claims": db.execute("SELECT COALESCE(SUM(attempts),0) FROM awareness_batches").fetchone()[0],
                    "completions": batches.get("complete", 0),
                    "deferrals": receipts["deferred"],
                    "quarantined": batches.get("quarantined", 0),
                    "expired_leases": db.execute("SELECT COUNT(*) FROM awareness_batches"
                                                 " WHERE state='leased' AND lease_until<?",
                                                 (moment.isoformat(),)).fetchone()[0],
                    "oldest_pending_seconds": None if oldest is None else max(
                        0, int((moment - datetime.fromisoformat(oldest)).total_seconds())),
                    "packets_supplied": db.execute(
                        "SELECT COUNT(DISTINCT json_extract(detail,'$.packet_id'))"
                        " FROM awareness_receipts WHERE kind='supplied'").fetchone()[0]}
    return {"journal": changes.status(store), "consumers": consumers, "batches": batches,
            "receipts": receipts, "deliveries": deliveries, "counters": counters,
            "meaning": "Queue state only; model execution and delivery are tracked separately."}
