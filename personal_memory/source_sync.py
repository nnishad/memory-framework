"""Durable source-sync state: connections, streams, leases, receipts, heads, coverage, jobs.

One authoritative database: progress and canonical records commit in the same
SQLite transaction, so an acknowledged page is never half-written and a crash
replays safely. Fencing tokens and generations make stale workers structurally
unable to commit; empty and removal-only pages advance cursors legitimately.
"""
import json
import uuid
from datetime import datetime, timedelta, timezone

from . import changes
from .common import digest, now, required_text, timestamp
from .source_sdk import (AdapterError, page_digest, read_state, validate_page, connection_context)


SCHEMA = '''
CREATE TABLE IF NOT EXISTS source_connections(
  id TEXT PRIMARY KEY, adapter_id TEXT NOT NULL, source TEXT NOT NULL,
  scope TEXT NOT NULL, scope_hash TEXT NOT NULL, retention TEXT NOT NULL,
  secret_ref TEXT, state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
  formation_policy TEXT NOT NULL DEFAULT 'deterministic',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  UNIQUE(adapter_id, source));
CREATE TABLE IF NOT EXISTS source_streams(
  connection_id TEXT NOT NULL REFERENCES source_connections(id),
  stream TEXT NOT NULL, partition TEXT NOT NULL, role TEXT NOT NULL,
  state_version INTEGER NOT NULL DEFAULT 1, cursor TEXT,
  scan INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT, lease_fence INTEGER NOT NULL DEFAULT 0, lease_until TEXT,
  last_delta_at TEXT, updated_at TEXT,
  PRIMARY KEY(connection_id, stream, partition, role));
CREATE TABLE IF NOT EXISTS source_page_receipts(
  op_id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, stream TEXT NOT NULL,
  partition TEXT NOT NULL, role TEXT NOT NULL, page_digest TEXT NOT NULL,
  outcome TEXT NOT NULL, committed_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS source_receipts_connection ON source_page_receipts(connection_id,committed_at);
CREATE TABLE IF NOT EXISTS source_heads(
  source TEXT NOT NULL, source_id TEXT NOT NULL, connection_id TEXT NOT NULL,
  record_id TEXT NOT NULL, revision TEXT NOT NULL, source_version TEXT,
  origin_role TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'live',
  changed_at TEXT NOT NULL, PRIMARY KEY(source, source_id));
CREATE TABLE IF NOT EXISTS source_coverage(
  id INTEGER PRIMARY KEY AUTOINCREMENT, connection_id TEXT NOT NULL,
  stream TEXT NOT NULL, partition TEXT NOT NULL, generation INTEGER NOT NULL,
  start TEXT NOT NULL, end TEXT NOT NULL, state TEXT NOT NULL, note TEXT NOT NULL,
  created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS source_coverage_scope ON source_coverage(connection_id,stream,generation);
CREATE TABLE IF NOT EXISTS source_jobs(
  id TEXT PRIMARY KEY, connection_id TEXT NOT NULL, kind TEXT NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
  available_at TEXT NOT NULL, lease_owner TEXT, lease_until TEXT,
  fence INTEGER NOT NULL DEFAULT 0, result TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS source_jobs_due ON source_jobs(state,available_at,kind);
CREATE TABLE IF NOT EXISTS source_inbox(
  id INTEGER PRIMARY KEY AUTOINCREMENT, connection_id TEXT NOT NULL,
  event_id TEXT, payload TEXT, received_at TEXT NOT NULL,
  served INTEGER NOT NULL DEFAULT 0,
  UNIQUE(connection_id, event_id));
CREATE INDEX IF NOT EXISTS source_inbox_pending ON source_inbox(connection_id,served,id);
CREATE TABLE IF NOT EXISTS source_current_records(
  source TEXT, source_id TEXT, record_id TEXT, PRIMARY KEY(source,source_id,record_id));
CREATE TABLE IF NOT EXISTS source_item_metadata(
  source TEXT, source_id TEXT, metadata TEXT, PRIMARY KEY(source,source_id));
CREATE TABLE IF NOT EXISTS source_schedule(
  connection_id TEXT, role TEXT, next_at REAL NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0, error TEXT, config_key TEXT,
  PRIMARY KEY(connection_id,role));
'''

CONNECTION_STATES = ("configured", "needs_auth", "active", "paused", "error", "disconnected")
STREAM_ROLES = ("backfill", "incremental", "reconcile")
JOB_STATES = ("pending", "leased", "retry_wait", "succeeded", "quarantined", "cancelled")
# Live roles own the current head against older backfill evidence; equality of
# content hashes never decides chronology.
_ROLE_RANK = {"backfill": 1, "reconcile": 2, "incremental": 2}


def _iso_or_none(value):
    return None if value is None else timestamp(value)


class SourceSync:
    """Generic correctness machinery over adapter-supplied pages. Adapters never
    commit directly; every authoritative write goes through commit_page."""

    def __init__(self, store, registry=None, secrets=None):
        self.store = store
        self.registry = dict(registry or {})
        self.secrets = secrets  # optional SecretStore; connections hold references only

    def register(self, adapter):
        spec = adapter.spec()
        self.registry[spec["adapter_id"]] = adapter
        return spec

    def _adapter(self, adapter_id):
        adapter = self.registry.get(adapter_id)
        if adapter is None:
            raise ValueError(f"Adapter {adapter_id!r} is not installed")
        return adapter

    def _connection(self, db, connection_id):
        row = db.execute("SELECT * FROM source_connections WHERE id=?", (connection_id,)).fetchone()
        if not row:
            raise ValueError("Connection not found")
        return row

    def configure(self, *, adapter_id, source, scope, retention="mirror", secret_ref=None,
                  formation_policy="deterministic"):
        adapter = self._adapter(adapter_id)
        if not isinstance(scope, dict):
            raise ValueError("Source scope must be an object")
        interval = scope.get("reconcile_seconds")
        if interval is not None and (type(interval) is not int or not 60 <= interval <= 86400 * 30):
            raise ValueError("reconcile_seconds must be 60..2592000")
        if retention not in ("mirror", "archive"):
            raise ValueError("retention must be mirror or archive")
        if formation_policy not in ("off", "deterministic", "inferred"):
            raise ValueError("formation_policy must be off, deterministic or inferred")
        scope_json = json.dumps(scope, sort_keys=True, allow_nan=False)
        required_text(source, 'source', 200)
        connection_id = "sconn_" + digest([adapter_id, source])[:24]
        with self.store.lock, self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute("SELECT * FROM source_connections WHERE adapter_id=? AND source=?", (adapter_id,source)).fetchone()
            if existing:
                connection_id = existing['id']
                changed = existing['scope'] != scope_json
                db.execute("UPDATE source_connections SET state='active',secret_ref=?,scope=?,scope_hash=?,"
                           "retention=?,formation_policy=?,generation=generation+1,updated_at=? WHERE id=?",
                           (secret_ref,scope_json,digest(scope_json),retention,formation_policy,now(),connection_id))
                db.execute('UPDATE source_streams SET lease_owner=NULL,lease_until=NULL WHERE connection_id=?',(connection_id,))
                if changed:
                    db.execute('UPDATE source_streams SET cursor=NULL,scan=scan+1,state_version=state_version+1 WHERE connection_id=?',(connection_id,))
                db.execute('DELETE FROM source_schedule WHERE connection_id=?',(connection_id,))
            else:
                db.execute("INSERT INTO source_connections"
                           "(id,adapter_id,source,scope,scope_hash,retention,secret_ref,state,generation,"
                           "formation_policy,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'active',1,?,?,?)",
                           (connection_id, adapter_id, source, scope_json,
                            digest(json.dumps(scope, sort_keys=True)), retention, secret_ref,
                            formation_policy, now(), now()))
                self.store.audit(db, "source_configure", connection_id, {"adapter": adapter_id})
        return {"connection_id": connection_id, "source": source, "adapter_id": adapter_id,
                "state": "active"}

    def connection(self, connection_id):
        with self.store.connect() as db:
            row = dict(self._connection(db, connection_id))
        row["scope"] = json.loads(row["scope"])
        return row

    def verify(self, connection_id):
        """Run the adapter's auth/account check; revoked or mismatched accounts park
        the connection instead of ingesting under the wrong identity (SYNC-05)."""
        with self.store.connect() as db:
            row = self._connection(db, connection_id)
        context = self.context(dict(row))
        try:
            result = self._adapter(row["adapter_id"]).check(context)
            expected = json.loads(row['scope']).get('account_id')
            if expected and result.get('account_id') != expected:
                raise AdapterError('auth', 'Authenticated account does not match the connection')
        except AdapterError as error:
            if error.kind == "auth":
                self._set_state(connection_id, "needs_auth")
            raise
        return result

    def context(self, connection, *, stream=None, partition=None):
        scope = connection['scope']
        if isinstance(scope, str): scope = json.loads(scope)
        reference = connection.get('secret_ref')
        def resolve(name):
            if not reference or name != reference or self.secrets is None:
                raise AdapterError('auth', 'Secret is outside this connection')
            try:return self.secrets.resolve(reference)
            except (KeyError,FileNotFoundError):
                raise AdapterError('auth','Connection credentials are unavailable; reauthorize') from None
        context = connection_context(connection_id=connection['id'],source=connection['source'],
                                     scope=scope,secrets=resolve,stream=stream,partition=partition)
        context['secret_ref'] = reference
        if hasattr(self,'cancel_event'):context['cancelled']=self.cancel_event.is_set
        return context

    def _set_state(self, connection_id, state):
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._connection(db, connection_id)
            db.execute("UPDATE source_connections SET state=?,updated_at=? WHERE id=?",
                       (state, now(), connection_id))

    def pause(self, connection_id):
        with self.store.lock, self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._connection(db, connection_id)
            db.execute("UPDATE source_connections SET state='paused',generation=generation+1,updated_at=? WHERE id=?",
                       (now(),connection_id))
            db.execute('UPDATE source_streams SET lease_owner=NULL,lease_until=NULL WHERE connection_id=?',(connection_id,))
            db.execute("UPDATE source_jobs SET state='retry_wait',lease_owner=NULL,lease_until=NULL,"
                       "fence=fence+1,available_at=?,updated_at=? WHERE connection_id=? AND state='leased'",
                       (now(),now(),connection_id))
        return {"connection_id": connection_id, "state": "paused"}

    def resume(self, connection_id):
        self._set_state(connection_id, "active")
        return {"connection_id": connection_id, "state": "active"}

    def disconnect(self, connection_id):
        """Fences all in-flight work: the generation bump makes stale commits
        impossible; deleting stored memories remains a separate operation."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._connection(db, connection_id)
            db.execute("UPDATE source_connections SET state='disconnected',generation=generation+1,updated_at=? WHERE id=?",
                       (now(), connection_id))
            db.execute("UPDATE source_streams SET lease_owner=NULL,lease_until=NULL WHERE connection_id=?",
                       (connection_id,))
            db.execute("UPDATE source_jobs SET state='cancelled',updated_at=? WHERE connection_id=? AND state IN ('pending','leased','retry_wait')",
                       (now(), connection_id))
            self.store.audit(db, "source_disconnect", connection_id, {"generation": row["generation"] + 1})
        return {"connection_id": connection_id, "state": "disconnected"}

    def register_trigger(self, connection_id, *, kind):
        """Events/subscriptions scheduling is gated on the adapter's declared
        capabilities; undeclared work is rejected before it can be stored (SDK-03)."""
        required_text(kind, "kind", 50)
        with self.store.connect() as db:
            row = self._connection(db, connection_id)
        spec = self._adapter(row["adapter_id"]).spec()
        capability = {"events": "events", "subscriptions": "subscriptions"}.get(kind)
        if capability is None or not spec["capabilities"].get(capability):
            raise ValueError(f"Adapter {row['adapter_id']} does not declare {kind} support")
        with self.store.connect() as db:
            self.store.audit(db, "source_trigger", connection_id, {"kind": kind})
        return {"connection_id": connection_id, "kind": kind}

    # ---- leases -------------------------------------------------------------

    def claim(self, connection_id, *, stream, partition="", role="backfill", owner, ttl=300):
        """One active lease per (connection, stream, partition, role). Re-claim by the
        same owner renews; expiry hands a strictly higher fence to the next worker."""
        if role not in STREAM_ROLES:
            raise ValueError(f"role must be one of {STREAM_ROLES}")
        required_text(owner, "owner", 200)
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            connection = self._connection(db, connection_id)
            if connection["state"] != "active":
                raise ValueError(f"Connection is {connection['state']}; only active connections lease work")
            db.execute("INSERT OR IGNORE INTO source_streams"
                       "(connection_id,stream,partition,role,state_version,cursor,lease_owner,lease_fence,"
                       "lease_until,last_delta_at,updated_at) VALUES(?,?,?,?,1,NULL,NULL,0,NULL,NULL,?)",
                       (connection_id, stream, partition, role, now()))
            row = db.execute("SELECT * FROM source_streams WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                             (connection_id, stream, partition, role)).fetchone()
            live = row["lease_until"] and row["lease_until"] > now()
            if live and row["lease_owner"] != owner:
                raise ValueError(f"Lease held by {row['lease_owner']} until {row['lease_until']}")
            fence = row["lease_fence"]
            if not live or row["lease_owner"] is None:
                fence += 1
            until = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()
            db.execute("UPDATE source_streams SET lease_owner=?,lease_fence=?,lease_until=?,updated_at=? "
                       "WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                       (owner, fence, until, now(), connection_id, stream, partition, role))
            from . import reset
            return {"connection_id": connection_id, "stream": stream, "partition": partition,
                    "role": role, "owner": owner, "fence": fence, "lease_until": until,
                    "state_version": row["state_version"],
                    "cursor": json.loads(row["cursor"]) if row["cursor"] else None,
                    "scan": row["scan"],
                    "generation": connection["generation"],
                    "epoch": reset.epoch(self.store)}

    def release(self, lease):
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute("UPDATE source_streams SET lease_owner=NULL,lease_until=NULL,updated_at=?"
                                " WHERE connection_id=? AND stream=? AND partition=? AND role=?"
                                " AND lease_owner=? AND lease_fence=?",
                                (now(), lease["connection_id"], lease["stream"], lease["partition"],
                                 lease["role"], lease["owner"], lease["fence"]))
            if not cursor.rowcount:
                raise ValueError("Lease already lost or superseded")
        return True

    def stream_state(self, connection_id, stream, partition="", role="backfill"):
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM source_streams WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                             (connection_id, stream, partition, role)).fetchone()
        if not row:
            return {"cursor": None, "state_version": 0, "role": role, "stream": stream}
        return {"cursor": json.loads(row["cursor"]) if row["cursor"] else None,
                "state_version": row["state_version"], "role": role, "stream": stream,
                "partition": row["partition"], "lease_owner": row["lease_owner"],
                "lease_fence": row["lease_fence"], "lease_until": row["lease_until"],
                "last_delta_at": row["last_delta_at"]}

    def restart_stream(self, connection_id, *, stream, partition="", role="backfill"):
        """Explicit rescan of one stream: progress resets, records dedupe by stable
        identity on re-delivery. Never triggered automatically; absence under a
        changed filter is handled by a new scan generation, not deletions."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            connection = self._connection(db, connection_id)
            if connection["state"] == "disconnected":
                raise ValueError("Cannot rescan a disconnected connection")
            db.execute("UPDATE source_streams SET cursor=NULL,state_version=state_version+1,scan=scan+1,lease_owner=NULL,"
                       "lease_until=NULL,updated_at=? WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                       (now(), connection_id, stream, partition, role))
            self.store.audit(db, "source_rescan", connection_id,
                             {"stream": stream, "partition": partition, "role": role})
        return {"connection_id": connection_id, "stream": stream, "role": role, "restarted": True}

    # ---- atomic page commit -------------------------------------------------

    def commit_page(self, lease, *, op_id, page, items=None, quarantined=(), declarations=None):
        """The one bounded, transaction-aware sync commit: records, source heads,
        jobs, coverage, receipt and cursor share a single SQLite transaction.
        Replay of an identical operation returns the committed outcome; reusing an
        operation ID for different content is a conflict, never a mutation."""
        required_text(op_id, "op_id", 200)
        validate_page(page)
        quarantined = list(quarantined)
        digest_value = digest([page_digest(page), items, quarantined,lease['connection_id'],
                               lease['stream'],lease['partition'],lease['role']])
        connection_info = self.connection(lease['connection_id'])
        version_order = self._version_order(connection_info, lease['stream'], declarations)
        results = {"applied": 0, "suppressed": 0, "history_only": 0, "skipped": 0,
                   "records": [], "page_complete": bool(page["complete"]), "replayed": False}
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            connection = self._connection(db, lease["connection_id"])
            stream = db.execute("SELECT * FROM source_streams WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                                (lease["connection_id"], lease["stream"], lease["partition"], lease["role"])).fetchone()
            if stream is None:
                raise ValueError("Stream state is missing for this lease")
            # Fencing: a worker whose lease expired, whose connection was
            # reconfigured (generation) or whose memory was reset (epoch) cannot commit.
            if (stream["lease_owner"] != lease["owner"] or stream["lease_fence"] != lease["fence"]
                    or not stream["lease_until"] or stream["lease_until"] <= now()):
                raise ValueError("Lease is not held (expired or superseded by a newer fence)")
            if connection["state"] != "active":
                raise ValueError(f"Connection is {connection['state']}; commit is fenced")
            if connection["generation"] != lease["generation"]:
                raise ValueError("Connection generation changed; this worker is stale")
            from . import reset
            if reset.epoch(self.store) != lease["epoch"]:
                raise ValueError("Memory epoch changed; this worker is stale")
            prior = db.execute("SELECT page_digest,outcome FROM source_page_receipts WHERE op_id=?", (op_id,)).fetchone()
            if prior:
                if prior["page_digest"] != digest_value:
                    raise ValueError("Operation ID conflict: a committed operation cannot be reused for a different page")
                outcome = json.loads(prior["outcome"])
                outcome["replayed"] = True
                return outcome
            if stream['state_version'] != lease['state_version']:
                raise ValueError('Checkpoint version changed; reread the stream')
            previous_cursor = json.loads(stream["cursor"]) if stream["cursor"] else None
            for operation in page["operations"]:
                self._apply_operation(db, connection, lease, operation, results, version_order)
            if lease["role"] == "backfill" and previous_cursor is None:
                changes.journal_stream_lifecycle(db, connection, lease, "backfill_progress")
            for entry in page.get("coverage", []):
                db.execute("INSERT INTO source_coverage(connection_id,stream,partition,generation,start,end,state,note,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                           (lease["connection_id"], lease["stream"], lease["partition"],
                            connection["generation"], entry["start"], entry["end"], entry["state"],
                            entry["note"], now()))
                if entry["state"] == "gap":
                    changes.journal_gap(db, connection, lease, entry.get("note"))
            for bad in quarantined:
                source_id = required_text(bad.get("source_id"), "quarantined.source_id", 1000)
                reason = str(bad.get("reason", ""))[:2000]
                db.execute("INSERT INTO source_coverage"
                           "(connection_id,stream,partition,generation,start,end,state,note,created_at)"
                           " VALUES(?,?,?,?,?,?,'gap',?,?)",
                           (lease["connection_id"], lease["stream"], lease["partition"],
                            connection["generation"], source_id, source_id, reason, now()))
                self._enqueue_job(db, lease["connection_id"], "quarantine",
                                  f"quarantine:{lease['connection_id']}:{source_id}",
                                  {"source_id": source_id, "reason": reason})
                changes.journal_gap(db, connection, lease, reason)
            self._enqueue_item_obligations(db, lease["connection_id"], items or page["operations"], results)
            if page["complete"]:
                db.execute("UPDATE source_streams SET cursor=?,state_version=state_version+1,last_delta_at=?,updated_at=? "
                           "WHERE connection_id=? AND stream=? AND partition=? AND role=?",
                           (json.dumps(page["next_state"]["cursor"], sort_keys=True, allow_nan=False),
                            now(), now(), lease["connection_id"], lease["stream"],
                            lease["partition"], lease["role"]))
                next_cursor = page["next_state"]["cursor"]
                if lease["role"] == "backfill" and isinstance(next_cursor, dict) and next_cursor.get("done"):
                    changes.journal_stream_lifecycle(db, connection, lease, "backfill_complete")
            outcome = dict(results)
            db.execute("INSERT INTO source_page_receipts VALUES(?,?,?,?,?,?,?,?)",
                       (op_id, lease["connection_id"], lease["stream"], lease["partition"],
                        lease["role"], digest_value, json.dumps(outcome, sort_keys=True), now()))
        if page['complete']:
            lease['state_version'] += 1
            lease['cursor'] = page['next_state']['cursor']
        return outcome

    def _version_order(self, connection, stream, declarations=None):
        # The validated declarations resolved once by the supervisor are reused here; a
        # direct/manual commit re-resolves them for itself so no page rediscovers twice.
        if declarations is None:
            declarations = self._adapter(connection["adapter_id"]).discover(self.context(connection))
        try:
            for declared in declarations:
                if declared["stream_id"] == stream:
                    return declared.get("version_order", "opaque")
        except AdapterError:
            raise
        raise ValueError('Stream is not declared by the adapter')

    def _newer(self, candidate_version, candidate_role, head, version_order):
        """May a candidate move the current head? Provider tokens order only when the
        adapter declared how; otherwise live roles win over backfill, never timestamps."""
        head_version, head_role = head["source_version"], head["origin_role"]
        if candidate_version is None or head_version is None:
            return _ROLE_RANK.get(candidate_role, 0) >= _ROLE_RANK.get(head_role, 0)
        if version_order == "integer":
            try:
                return int(candidate_version) >= int(head_version)
            except (TypeError, ValueError):
                return False
        if version_order == "timestamp":
            try:
                return timestamp(candidate_version) >= timestamp(head_version)
            except (TypeError, ValueError):
                return False
        if candidate_version == head_version:
            return True
        return _ROLE_RANK.get(candidate_role, 0) >= _ROLE_RANK.get(head_role, 0)

    def _apply_operation(self, db, connection, lease, operation, results, version_order):
        source = connection["source"]
        source_id = operation["source_id"]
        action = operation["action"]
        head = db.execute("SELECT * FROM source_heads WHERE source=? AND source_id=?",
                          (source, source_id)).fetchone()
        if action == "skip":
            results["skipped"] += 1
            return
        if action in ("remove", "restore"):
            if head is not None and head['state'] == ('removed' if action == 'remove' else 'live'):
                # A later page may carry a newer provider version for an unchanged
                # state. Advance the fence without inventing another transition.
                if self._newer(operation.get('source_version'), lease['role'], head, version_order):
                    db.execute("UPDATE source_heads SET source_version=?,origin_role=?,changed_at=?"
                               " WHERE source=? AND source_id=?",
                               (operation.get('source_version') or head['source_version'],
                                lease['role'], now(), source, source_id))
                results['skipped'] += 1
                return
            if head is None and action == "remove":
                db.execute('INSERT INTO source_heads VALUES(?,?,?,?,?,?,?,?,?)',
                           (source,source_id,connection['id'],'','',operation.get('source_version'),lease['role'],'removed',now()))
                self.store.audit(db,'source_remove',source_id)
                changes.journal_state_change(db, connection, lease, source_id, "removed")
                results['applied'] += 1
                return
            state = "removed" if action == "remove" else "live"
            if head:
                if not self._newer(operation.get('source_version'),lease['role'],head,version_order):
                    results['history_only'] += 1
                    return
                if action == 'restore' and not head['record_id']:
                    raise ValueError('Restoration requires source content')
                db.execute("UPDATE source_heads SET state=?,changed_at=?,source_version=?,"
                           "origin_role=?,record_id=? WHERE source=? AND source_id=?",
                           (state, now(), operation.get("source_version") or head["source_version"],
                            lease["role"], head["record_id"], source, source_id))
                affected = sorted(self._current(db, source, source_id, head))
                if connection["retention"] == "mirror":
                    if action == "remove":
                        for rid in affected: self._retire(db,rid)
                    else:
                        db.executemany('DELETE FROM record_visibility WHERE record_id=?',[(r,) for r in affected])
                        # Restored evidence is invisible to the index until re-embedded;
                        # enqueue the work in this same transaction.
                        from . import semantic
                        semantic.enqueue(db, "index", sorted(affected))
                self.store.audit(db,'source_'+action,source_id)
                changes.journal_state_change(db, connection, lease, source_id,
                                             "removed" if action == "remove" else "restored", affected)
            results["applied"] += 1
            return
        if action in ("upsert", "metadata_update"):
            for record in operation["records"]:
                if record["source"] != source:
                    # One connection writes only into its own evidence namespace (SEC-01).
                    raise ValueError("Record source is outside the connection namespace")
        # upsert / metadata_update: content commits first, visibility follows the head rule.
        record_ids = []
        applied_rows = []
        for record in operation["records"]:
            if self._is_tombstoned(record):
                # Forgotten items and revisions are durable suppressions, not page
                # failures (SEC-06): they never become heads, jobs, projections or evidence.
                results["suppressed"] += 1
                continue
            item = {"source": record["source"], "source_id": record["source_id"],
                    "revision": record["revision"], "occurred_at": record["occurred_at"],
                    "kind": record["kind"], "text": record["text"],
                    "metadata": {"participants": record["participants"], "extensions": record["extensions"],
                                 "origin": record["provenance"]["origin"],
                                 "parent_record_ids": record["provenance"]["parent_record_ids"]},
                    "_contract": record}
            applied = self.store._apply_ingest_item(db, item)
            applied_rows.append(applied)
            record_ids.append(applied["id"])
        if not record_ids:
            return
        primary = record_ids[-1]
        results["records"].extend(record_ids)
        results["applied"] += 1
        current = self._current(db,source,source_id,head)
        accepted = head is None or self._newer(operation.get('source_version'),lease['role'],head,version_order)
        if head and head['state']=='removed' and lease['role']=='backfill': accepted=False
        if accepted:
            stored = db.execute("SELECT metadata FROM source_item_metadata WHERE source=? AND source_id=?",
                                (source, source_id)).fetchone()
            stored_metadata = None if stored is None else stored["metadata"]
            for rid in current - set(record_ids): self._retire(db,rid,primary)
            db.executemany('DELETE FROM record_visibility WHERE record_id=?',[(r,) for r in record_ids])
            from . import semantic
            semantic.enqueue(db, "index", record_ids)
            db.execute('DELETE FROM source_current_records WHERE source=? AND source_id=?',(source,source_id))
            db.executemany('INSERT INTO source_current_records VALUES(?,?,?)',[(source,source_id,r) for r in record_ids])
            db.execute('INSERT OR REPLACE INTO source_heads VALUES(?,?,?,?,?,?,?,?,?)',
                       (source,source_id,connection['id'],primary,operation['records'][-1]['revision'],
                        operation.get('source_version'),lease['role'],'live',now()))
            db.execute('INSERT OR REPLACE INTO source_item_metadata VALUES(?,?,?)',
                       (source,source_id,json.dumps(operation.get('metadata') or {},sort_keys=True)))
            if current != set(record_ids) or (head and head['state']!='live'):
                self.store.audit(db,'source_head',primary)
            changes.journal_operation_transition(db, connection, lease, operation, head,
                                                 applied_rows, current, stored_metadata)
        else:
            for rid in set(record_ids)-current: self._retire(db,rid,head['record_id'] or None)
            results["applied"] -= 1
            results["history_only"] += 1

    @staticmethod
    def _record_id(source, source_id, revision):
        return "rec_" + digest([source, source_id, revision])[:32]

    def _is_tombstoned(self, record):
        """True for a whole-item forget or an individually-forgotten revision."""
        if self.store.deletions.contains(
                self.store.source_key(record["source"], record["source_id"])):
            return True
        return self.store.deletions.contains(self._record_id(
            record["source"], record["source_id"], record["revision"]))

    @staticmethod
    def _current(db, source, source_id, head):
        ids={r[0] for r in db.execute('SELECT record_id FROM source_current_records WHERE source=? AND source_id=?',(source,source_id))}
        return ids or ({head['record_id']} if head and head['record_id'] else set())

    def _retire(self, db, record_id, replacement=None):
        from . import lifecycle
        lifecycle.retire(db, self.store, record_id, replacement=replacement)

    def _enqueue_item_obligations(self, db, connection_id, sources_for_jobs, results):
        """Each attachment belongs to a specific live evidence revision."""
        jobs_before = results.get("jobs", 0)
        for entry in sources_for_jobs:
            records = entry.get("records", []) if isinstance(entry, dict) else []
            if records and not any(self._record_id(r["source"], r["source_id"], r["revision"])
                                   in results["records"] for r in records):
                # Every record here was suppressed as forgotten: derive no attachment or
                # projection obligations from evidence that never became current.
                continue
            descriptors = entry.get("attachments") if isinstance(entry, dict) else None
            for descriptor in descriptors or []:
                records=entry.get('records',[])
                if not records:
                    continue
                record=records[0]
                rid=self._record_id(record['source'],record['source_id'],record['revision'])
                if rid not in results['records'] or db.execute('SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1',(rid,)).fetchone(): continue
                identity=descriptor.get('part_id') or descriptor.get('sha256')
                if not identity: raise ValueError('Attachment needs a stable part identity')
                self._enqueue_job(db, connection_id, "attachment",
                                  'attachment:'+digest([connection_id,rid,identity]),
                                  {**descriptor,'record_id':rid,'source_id':record['source_id']})
            for projection in entry.get("projections") or []:
                key = "projection:" + digest([connection_id, projection.get("kind"),
                                              projection.get("source_id"), projection.get("transform"),entry.get('records')])
                self._enqueue_job(db, connection_id, "projection", key, dict(projection))
        results["jobs"] = jobs_before

    def _enqueue_job(self, db, connection_id, kind, dedupe_key, payload):
        db.execute("INSERT OR IGNORE INTO source_jobs"
                   "(id,connection_id,kind,dedupe_key,payload,state,attempts,available_at,lease_owner,"
                   "lease_until,fence,result,created_at,updated_at) "
                   "VALUES(?,?,?,?,?,'pending',0,?,NULL,NULL,0,NULL,?,?)",
                   ("sjob_" + digest([dedupe_key])[:24], connection_id, kind, dedupe_key,
                    json.dumps(payload, sort_keys=True), now(), now(), now()))

    # ---- durable event inbox -------------------------------------------------

    def signal(self, connection_id, *, event_id=None, payload=None):
        """An event is acknowledged only once it is durable here. Provider delivery
        IDs deduplicate; signals without one are queued as-is."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._connection(db, connection_id)
            if event_id is not None:
                required_text(event_id, "event_id", 500)
                if db.execute("SELECT 1 FROM source_inbox WHERE connection_id=? AND event_id=?",
                              (connection_id, event_id)).fetchone():
                    return {"accepted": True, "duplicate": True}
            db.execute("INSERT INTO source_inbox(connection_id,event_id,payload,received_at,served) VALUES(?,?,?,?,0)",
                       (connection_id, event_id,
                        None if payload is None else json.dumps(payload, sort_keys=True, allow_nan=False),
                        now()))
        return {"accepted": True, "duplicate": False}

    def pending_signals(self, connection_id):
        with self.store.connect() as db:
            return db.execute("SELECT COUNT(*) FROM source_inbox WHERE connection_id=? AND served=0",
                              (connection_id,)).fetchone()[0]

    def take_signals(self, connection_id):
        """Read a stable high-water mark; only acknowledgment consumes events."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT COUNT(*) AS n, MAX(id) AS top FROM source_inbox "
                             "WHERE connection_id=? AND served=0", (connection_id,)).fetchone()
        return {"count": row["n"], "up_to": row["top"] or 0}

    def ack_signals(self, connection_id, *, up_to):
        """Bounded inbox retention: purge served rows a completed pass has consumed."""
        with self.store.connect() as db:
            cursor = db.execute("DELETE FROM source_inbox WHERE connection_id=? AND id<=?",
                                (connection_id, int(up_to)))
        return {"purged": cursor.rowcount}

    # ---- obligation and processing jobs --------------------------------------

    def claim_job(self, owner, *, kinds=(), ttl=300, connection_id=None):
        """Queue-position leases with per-job fencing. No due job returns None; a
        job actively leased by another worker raises rather than double-running."""
        required_text(owner, "owner", 200)
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            moment = now()
            arguments = [] if connection_id is None else [connection_id]
            scope_sql = "" if connection_id is None else " AND connection_id=?"
            kind_sql = ""
            if kinds:
                kind_sql = " AND kind IN (" + ",".join("?" * len(kinds)) + ")"
                arguments.extend(kinds)
            due = db.execute("SELECT * FROM source_jobs WHERE connection_id IN (SELECT id FROM source_connections WHERE state='active') AND ((state IN ('pending','retry_wait')"
                             " AND available_at<=?) OR (state='leased' AND lease_until<=?))"
                             + scope_sql + kind_sql +
                             " ORDER BY created_at LIMIT 1", [moment, moment] + arguments).fetchone()
            if due is None:
                held = db.execute("SELECT 1 FROM source_jobs WHERE state='leased' AND lease_until>?"
                                  + scope_sql + kind_sql, [moment] + arguments).fetchone()
                if held:
                    raise ValueError("A matching job is actively leased by another worker")
                return None
            fence = due["fence"] + 1
            until = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()
            db.execute("UPDATE source_jobs SET state='leased',lease_owner=?,lease_until=?,fence=?,updated_at=? WHERE id=?",
                       (owner, until, fence, now(), due["id"]))
            job = dict(due)
            job.update({"state": "leased", "lease_owner": owner, "lease_until": until, "fence": fence})
            job["payload"] = json.loads(job["payload"])
            return job

    def _job_guard(self, db, job, state):
        owner = job.get("owner", job.get("lease_owner"))
        cursor = db.execute("UPDATE source_jobs SET state=?,updated_at=? WHERE id=? AND lease_owner=?"
                            " AND fence=? AND state=? AND lease_until>? AND connection_id IN (SELECT id FROM source_connections WHERE state='active')",
                            (state, now(), job["id"], owner, job["fence"], "leased",now()))
        if not cursor.rowcount:
            raise ValueError("Job lease is stale or superseded")

    def complete_job(self, job, result=None):
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._job_guard(db, job, "succeeded")
            db.execute("UPDATE source_jobs SET result=? WHERE id=?",
                       (None if result is None else json.dumps(result, sort_keys=True), job["id"]))
        return {"id": job["id"], "state": "succeeded"}

    def fail_job(self, job, *, reason, retry_after=60, quarantine=False):
        state = "quarantined" if quarantine else "retry_wait"
        available = (datetime.now(timezone.utc) + timedelta(seconds=0 if quarantine else max(0, retry_after or 0))).isoformat()
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._job_guard(db, job, state)
            db.execute("UPDATE source_jobs SET attempts=attempts+1,available_at=?,lease_owner=NULL,"
                       "lease_until=NULL,result=? WHERE id=?",
                       (available, json.dumps({"reason": str(reason)[:2000]}, sort_keys=True), job["id"]))
        return {"id": job["id"], "state": state}

    # ---- reads --------------------------------------------------------------

    def head(self, source, source_id):
        with self.store.connect() as db:
            row = db.execute("SELECT * FROM source_heads WHERE source=? AND source_id=?",
                             (source, source_id)).fetchone()
        return dict(row) if row else None

    def coverage(self, connection_id, stream, partition=""):
        with self.store.connect() as db:
            rows = db.execute("SELECT start,end,state,note,generation FROM source_coverage "
                              "WHERE connection_id=? AND stream=? AND partition=? ORDER BY id",
                              (connection_id, stream, partition)).fetchall()
        return [dict(row) for row in rows]

    def status(self, connection_id):
        with self.store.connect() as db:
            connection = dict(self._connection(db, connection_id))
            streams = [dict(row) for row in db.execute(
                "SELECT stream,partition,role,state_version,cursor,last_delta_at,lease_owner,lease_until "
                "FROM source_streams WHERE connection_id=?", (connection_id,))]
        for stream in streams:
            stream["cursor"] = json.loads(stream["cursor"]) if stream["cursor"] else None
        connection["scope"] = json.loads(connection["scope"])
        connection["streams"] = streams
        connection["last_delta_at"] = max((s["last_delta_at"] or "" for s in streams), default=None) or None
        return connection

    def pending_jobs(self, connection_id, kinds=(), limit=100):
        query = "SELECT * FROM source_jobs WHERE connection_id=? AND state IN ('pending','retry_wait','leased')"
        arguments = [connection_id]
        if kinds:
            query += " AND kind IN (" + ",".join("?" * len(kinds)) + ")"
            arguments.extend(kinds)
        query += " ORDER BY created_at LIMIT ?"
        arguments.append(limit)
        with self.store.connect() as db:
            rows = db.execute(query, arguments).fetchall()
        jobs = [dict(row) for row in rows]
        for job in jobs:
            job["payload"] = json.loads(job["payload"])
        return jobs


class SyncWorker:
    """A bounded, stateless pass over one stream lease. Safe to run from Hermes
    no_agent cron, a supervisor loop or a manual command: every invocation either
    commits a page or reports the classified reason it could not."""

    def __init__(self, sync, *, owner=None):
        self.sync = sync
        self.owner = owner or 'source-worker-'+uuid.uuid4().hex

    def _context(self, connection, *, stream=None, partition=None):
        return self.sync.context(connection, stream=stream, partition=partition)

    def run_once(self, connection_id, *, stream, partition="", role="backfill", owner=None, ttl=60,
                 declarations=None):
        lease = None
        try:
            lease = self.sync.claim(connection_id, stream=stream, partition=partition,
                                    role=role, owner=owner or self.owner, ttl=ttl)
            connection = self.sync.connection(connection_id)
            adapter = self.sync._adapter(connection["adapter_id"])
            # The supervisor resolves declarations once per refresh interval and passes
            # them here; a manual or cron pass discovers once for itself.
            if declarations is None:
                declarations = adapter.discover(self._context(connection))
            selected=next((d for d in declarations if d['stream_id']==stream), None)
            if selected is None or role not in selected['modes']:
                raise AdapterError('unsupported','Stream does not support this read mode')
            # Validate the selectors before reading so a typo cannot silently read a
            # neighbouring partition or an undeclared stream.
            declared_partitions=[p['id'] for p in selected.get('partitions',[])]
            if declared_partitions:
                if partition not in declared_partitions:
                    raise AdapterError('unsupported','Partition is not declared by this stream')
            elif partition:
                raise AdapterError('unsupported','This stream declares no partitions')
            state = read_state(state_version=1, cursor=lease["cursor"], mode=role,scope_hash=connection['scope_hash'])
            page = adapter.read_page(self._context(connection, stream=stream, partition=partition), state)
            op_id = "sop_" + digest([connection_id, stream, partition, role,
                                     lease["cursor"], lease["scan"],lease['epoch'],lease['generation'], page["page_id"]])[:32]
            result = self.sync.commit_page(lease, op_id=op_id, page=page, declarations=declarations)
            terminal = bool(page['complete']) and not page['operations']
            return {"status": "complete" if terminal else "committed",
                    "applied": result["applied"], "suppressed": result["suppressed"],
                    "history_only": result["history_only"], "skipped": result["skipped"],
                    "replayed": result["replayed"], "page_complete": result["page_complete"]}
        except AdapterError as error:
            if error.kind == "auth":
                self.sync._set_state(connection_id, "needs_auth")
                return {"status": "needs_auth", "reason": error.message}
            if error.kind in ("temporary", "rate_limit"):
                return {"status": "retry", "retry_after": error.retry_after, "reason": error.message}
            if error.kind == "cursor":
                return {"status": "resync_required", "reason": error.message}
            return {"status": "failed", "reason": error.message}
        finally:
            if lease is not None:
                try:
                    self.sync.release(lease)
                except ValueError:
                    pass  # the fence already moved on; the commit guard did its job
