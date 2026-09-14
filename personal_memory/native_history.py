"""Read-only Hermes history adapter and resumable administrative reconciliation.

Explicit sessions are required. Unknown generated provenance is withheld. This
module never edits state.db or treats native authority files as instructions.
"""
import json
import sqlite3
import tempfile
import time
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .common import digest, now, required_text
from .ingestion import ConnectorSpec, IngestionConnector


class HermesHistoryConnector(IngestionConnector):
    spec = ConnectorSpec('hermes.history', __version__, 'hermes-history')

    def __init__(self, database, archive_id, session_ids, parents_by_message=None):
        self.database = Path(database).expanduser().resolve(strict=True)
        self.archive_id = required_text(archive_id, 'archive_id', 200)
        if not isinstance(session_ids, list) or not session_ids:
            raise ValueError('Select explicit native session IDs')
        self.session_ids = sorted({required_text(s, 'session_id', 200) for s in session_ids})
        self.parents_by_message = parents_by_message or {}
        if not isinstance(self.parents_by_message, dict):
            raise ValueError('Lineage must map native message IDs to canonical evidence ID lists')
        for key, parents in self.parents_by_message.items():
            if not isinstance(key, str) or not key.isdecimal() or not isinstance(parents, list) or not 1 <= len(parents) <= 100:
                raise ValueError('Lineage keys must be decimal message IDs with 1..100 parent IDs')
            for parent in parents: required_text(parent, 'parent_record_id', 100)
        self.report = {}
        self.on_present = lambda source_id: None

    def read(self, checkpoint=None):
        if checkpoint is not None:
            raise ValueError('Use sync_history for checkpoint/reconciliation; an append cursor misses native edits/deletions')
        self.report = {'read': 0, 'withheld_generated': 0, 'excluded_inactive_or_summary': 0, 'empty': 0}
        # Online SQLite backup takes a coherent snapshot without holding a read
        # transaction open on the native writer during network ingestion.
        with tempfile.TemporaryDirectory(prefix='hermes-history-') as temp:
            snapshot = Path(temp) / 'snapshot.db'
            source = sqlite3.connect(self.database.as_uri() + '?mode=ro', uri=True, timeout=10)
            destination = sqlite3.connect(snapshot)
            try:
                deadline = time.monotonic() + 30
                def progress(status, remaining, total):
                    if time.monotonic() > deadline:
                        raise TimeoutError('Native snapshot exceeded 30 seconds; no reconciliation performed')
                source.backup(destination, pages=256, progress=progress)
            finally:
                source.close()
                destination.close()
            snapshot.chmod(0o600)
            db = sqlite3.connect(snapshot.as_uri() + '?mode=ro', uri=True)
            db.row_factory = sqlite3.Row
            try:
                columns = {row[1] for row in db.execute('PRAGMA table_info(messages)')}
                required = {'id', 'session_id', 'role', 'content', 'timestamp', 'active', 'compacted', '_compressed_summary'}
                if not required <= columns:
                    raise ValueError('Unsupported Hermes message schema; no records reconciled')
                for sid in self.session_ids:
                    cursor = 0
                    while True:
                        rows = db.execute('SELECT id,session_id,role,content,timestamp,active,compacted,_compressed_summary '
                                          'FROM messages WHERE session_id=? AND id>? ORDER BY id LIMIT 200', (sid, cursor)).fetchall()
                        if not rows: break
                        for row in rows:
                            self.report['read'] += 1
                            if row['_compressed_summary'] or not (row['active'] or row['compacted']):
                                self.report['excluded_inactive_or_summary'] += 1
                                continue
                            source_id = f"{digest([self.archive_id, sid])}/message/{row['id']}"
                            self.on_present(source_id)
                            parents = self.parents_by_message.get(str(row['id']), [])
                            if row['role'] not in {'user', 'assistant', 'tool'} or (row['role'] != 'user' and not parents):
                                self.report['withheld_generated'] += 1
                                continue
                            content = row['content']
                            if content is None or not content.strip():
                                self.report['empty'] += 1
                                continue
                            when = datetime.fromtimestamp(float(row['timestamp']), timezone.utc).isoformat()
                            native_id = str(row['id'])
                            # Immutable content revisions. Native compaction copies
                            # remain distinct rows; no speculative identity merging.
                            revision = digest([content, when, row['role'], sorted(set(parents))])
                            yield {
                                'schema_version': '1.0', 'source': self.spec.source, 'source_id': source_id,
                                'revision': revision, 'kind': 'native_' + row['role'], 'occurred_at': when,
                                'observed_at': now(), 'text': content, 'participants': [],
                                'provenance': {'connector_id': self.spec.connector_id,
                                    'connector_version': self.spec.connector_version,
                                    'source_locator': 'hermes-history://' + source_id,
                                    'origin': 'derived' if parents else 'source', 'parent_record_ids': sorted(set(parents))},
                                'extensions': {'hermes.history': {'version': '1.0', 'data': {
                                    'archive_id': self.archive_id, 'session_id': sid,
                                    'native_message_id': native_id, 'role': row['role'],
                                    'timestamp_basis': 'native_message_timestamp',
                                    'authority': 'observation', 'attachment_blobs_imported': False}}}}
                        cursor = rows[-1]['id']
            finally:
                db.close()


@contextmanager
def sync_state(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(path, timeout=10)
    path.chmod(0o600)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA synchronous=FULL')
    try:
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS native_history_map('
                       'source_id TEXT PRIMARY KEY,archive_id TEXT,session_id TEXT,revision TEXT,record_id TEXT,seen_run TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS native_history_retirements(record_id TEXT PRIMARY KEY)')
            db.execute('CREATE TABLE IF NOT EXISTS native_history_archives(archive_id TEXT PRIMARY KEY,database_path TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS native_history_fingerprints(fingerprint TEXT PRIMARY KEY,record_id TEXT,source_id TEXT)')
            yield db
    finally:
        db.close()
    path.chmod(0o600)


def sync_history(client, connector, state_path):
    """Use an admin client; replay-safe writes plus deletion propagation.

    Full selected-session scans are intentional: append-only cursors cannot
    detect native updates, rewind, or deletion. Partial scans never sweep.
    """
    import uuid
    run_id = uuid.uuid4().hex
    report = {'imported': 0, 'duplicates': 0, 'suppressed_forgotten': 0, 'retired_revisions': 0,
              'removed_native_items': 0, 'native_database_modified': False}

    def retire_pending():
        while True:
            with sync_state(state_path) as db:
                rows = db.execute('SELECT record_id FROM native_history_retirements LIMIT 100').fetchall()
            if not rows: return
            for row in rows:
                rid = row['record_id']
                state = client.call('/v1/lineage/status', {'record_ids': [rid]})['states'][rid]
                if state == 'live': client.call('/v1/forget', {'record_id': rid})
                if state == 'unknown': raise ValueError('Retirement record missing; reconcile restored state before syncing')
                with sync_state(state_path) as db:
                    db.execute('DELETE FROM native_history_retirements WHERE record_id=?', (rid,))
                report['retired_revisions'] += 1

    # One OS-process lock prevents two scans from treating each other's run IDs
    # as native deletions. Use the same POSIX process lease as the service.
    from .asgi import ProcessLease
    with closing(ProcessLease(Path(state_path).with_name('native-history-sync.lock'))):
        with sync_state(state_path) as db:
            binding = db.execute('SELECT database_path FROM native_history_archives WHERE archive_id=?', (connector.archive_id,)).fetchone()
            if binding and binding[0] != str(connector.database):
                raise ValueError('Archive identity is bound to a different native database path')
            db.execute('INSERT OR IGNORE INTO native_history_archives VALUES(?,?)', (connector.archive_id, str(connector.database)))
        retire_pending()
        seen = []
        def flush_seen():
            if not seen: return
            with sync_state(state_path) as db:
                db.executemany('UPDATE native_history_map SET seen_run=? WHERE source_id=?', [(run_id, sid) for sid in seen])
            seen.clear()
        def mark_present(source_id):
            seen.append(source_id)
            if len(seen) >= 100: flush_seen()
        connector.on_present = mark_present

        def bounded_batches(records):
            batch, size = [], 0
            for item in records:
                item_size = len(json.dumps(item, ensure_ascii=False).encode('utf-8')) + 2
                if batch and (len(batch) >= 100 or size + item_size > 1500000):
                    yield batch
                    batch, size = [], 0
                batch.append(item)
                size += item_size
            if batch: yield batch

        def record_states(ids):
            ids, states = sorted(set(ids)), {}
            for start in range(0, len(ids), 100):
                states.update(client.call('/v1/lineage/status', {'record_ids': ids[start:start+100]})['states'])
            return states

        for batch in bounded_batches(connector.records()):
            fingerprints = {item['source_id']: digest([connector.archive_id,
                item['extensions']['hermes.history']['data']['session_id'],
                item['extensions']['hermes.history']['data']['role'], item['occurred_at'], item['text']]) for item in batch}
            aliases = {}
            for item in batch:
                fingerprint = fingerprints[item['source_id']]
                if fingerprint not in aliases:
                    with sync_state(state_path) as db:
                        row = db.execute('SELECT record_id,source_id FROM native_history_fingerprints WHERE fingerprint=?', (fingerprint,)).fetchone()
                    if row: aliases[fingerprint] = dict(row)
                alias = aliases.get(fingerprint)
                if alias and alias['source_id'] != item['source_id']:
                    # Match the host display-history equivalence: identical
                    # session, role, timestamp and content. Compaction copies
                    # depend on the first canonical observation, not a new fact.
                    parents = sorted(set(item['provenance']['parent_record_ids']) | {alias['record_id']})
                    if len(parents) > 100: raise ValueError('Native copy lineage exceeds the ingestion contract')
                    item['provenance'].update(origin='derived', parent_record_ids=parents)
                    data = item['extensions']['hermes.history']['data']
                    item['revision'] = digest([item['text'], item['occurred_at'], data['role'], parents])
                    mark_present(alias['source_id'])
                if not alias:
                    aliases[fingerprint] = {'source_id': item['source_id'],
                        'record_id': 'rec_' + digest([item['source'], item['source_id'], item['revision']])[:32]}
            source_ids = [item['source_id'] for item in batch]
            with sync_state(state_path) as db:
                previous = {row['source_id']: row['record_id'] for row in db.execute(
                    'SELECT source_id,record_id FROM native_history_map WHERE source_id IN (' + ','.join('?' for _ in source_ids) + ')', source_ids)}
            ids = {item['source_id']: 'rec_' + digest([item['source'], item['source_id'], item['revision']])[:32] for item in batch}
            families = {}
            family_ids = sorted(set(source_ids) | {a['source_id'] for a in aliases.values()})
            for start in range(0, len(family_ids), 100):
                families.update(client.call('/v1/source/status', {'source': connector.spec.source, 'source_ids': family_ids[start:start+100]})['states'])
            states = record_states(list(ids.values()) + list(previous.values()) +
                                   [p for item in batch for p in item['provenance']['parent_record_ids']])
            pending, unchanged, accepted_ids = [], [], set()
            for item in batch:
                sid = item['source_id']
                candidates = [ids[sid]] + ([previous[sid]] if sid in previous else [])
                parents = item['provenance']['parent_record_ids']
                if any(states[p] == 'unknown' and p not in accepted_ids for p in parents):
                    raise ValueError('Historical lineage contains unknown evidence; reconciliation stopped')
                alias = aliases[fingerprints[sid]]
                if families[sid] == 'forgotten' or families[alias['source_id']] == 'forgotten' or any(states[rid] == 'forgotten' for rid in candidates + parents):
                    client.call('/v1/forget-source', {'source': item['source'], 'source_id': sid})
                    report['suppressed_forgotten'] += 1
                    continue
                if previous.get(sid) == ids[sid] and states[ids[sid]] == 'live':
                    unchanged.append(item)
                    report['duplicates'] += 1
                else:
                    pending.append(item)
                accepted_ids.add(ids[sid])
            receipts = client.call('/v1/ingest', {'items': pending})['records'] if pending else []
            if len(receipts) != len(pending): raise RuntimeError('Missing ingestion receipts')
            for item, receipt in zip(pending, receipts):
                if receipt['id'] != ids[item['source_id']]: raise RuntimeError('Canonical source identity mismatch')
                report['duplicates'] += int(receipt['duplicate'])
            with sync_state(state_path) as db:
                for item in pending + unchanged:
                    sid = item['source_id']
                    data = item['extensions']['hermes.history']['data']
                    alias = aliases[fingerprints[sid]]
                    db.execute('INSERT OR IGNORE INTO native_history_fingerprints VALUES(?,?,?)', (fingerprints[sid], alias['record_id'], alias['source_id']))
                    if sid in previous and previous[sid] != ids[sid]:
                        db.execute('INSERT OR IGNORE INTO native_history_retirements VALUES(?)', (previous[sid],))
                    db.execute('INSERT OR REPLACE INTO native_history_map VALUES(?,?,?,?,?,?)',
                               (sid, connector.archive_id, data['session_id'], item['revision'], ids[sid], run_id))
            report['imported'] += len(pending) + len(unchanged)
            retire_pending()
        flush_seen()
        # Complete snapshot only. Missing/rewound native items
        # become source-family tombstones; restore/reimport cannot revive them.
        for sid in connector.session_ids:
            while True:
                with sync_state(state_path) as db:
                    rows = db.execute('SELECT source_id FROM native_history_map WHERE archive_id=? AND session_id=? AND seen_run!=? LIMIT 100',
                                      (connector.archive_id, sid, run_id)).fetchall()
                if not rows: break
                for row in rows:
                    client.call('/v1/forget-source', {'source': connector.spec.source, 'source_id': row['source_id']})
                    with sync_state(state_path) as db:
                        db.execute('UPDATE native_history_map SET seen_run=? WHERE source_id=?', (run_id, row['source_id']))
                    report['removed_native_items'] += 1
    report.update(connector.report, coverage='partial; only explicitly selected sessions and eligible messages',
                  source='hermes-history', archive_id=connector.archive_id)
    return report
