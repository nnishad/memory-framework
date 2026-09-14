import json
import sqlite3
from pathlib import Path

import test_memory as fixtures
from personal_memory.native_history import HermesHistoryConnector, sync_history
from personal_memory.store import Store


class NativeHistoryTests(fixtures.HTTPFixture):
    def setUp(self):
        super().setUp()
        self.native = Path(self.tmp.name) / 'native.db'
        self.state = Path(self.tmp.name) / 'outbox.db'
        with sqlite3.connect(self.native) as db:
            db.execute('CREATE TABLE messages(id INTEGER PRIMARY KEY,session_id TEXT,role TEXT,content TEXT,timestamp REAL,active INTEGER,compacted INTEGER,_compressed_summary INTEGER)')
            db.executemany('INSERT INTO messages VALUES(?,?,?,?,?,1,0,0)', [
                (1, 'selected', 'user', 'Fictional bicycle stored in cedar-shed-527.', 1700000000),
                (2, 'selected', 'assistant', 'Untracked assistant copy coral-bridge-419.', 1700000001),
                (3, 'other', 'user', 'Private other session maple-north-824.', 1700000002)])

    def connector(self, lineage=None):
        return HermesHistoryConnector(self.native, 'fictional-archive', ['selected'], lineage)

    def sync(self, lineage=None):
        return sync_history(self.client, self.connector(lineage), self.state)

    def search(self, text):
        return self.client.call('/v1/search', {'query': text})['episodes']

    def test_import_is_scoped_idempotent_and_preserves_native_database(self):
        before = self.native.read_bytes()
        report = self.sync()
        self.assertEqual(report['imported'], 1)
        self.assertEqual(report['withheld_generated'], 1)
        self.assertTrue(self.search('cedar-shed-527'))
        self.assertFalse(self.search('coral-bridge-419'))
        self.assertFalse(self.search('maple-north-824'))
        self.assertEqual(self.sync()['duplicates'], 1)
        self.assertEqual(before, self.native.read_bytes())

    def test_native_edit_retires_old_revision_without_losing_new(self):
        self.sync()
        with sqlite3.connect(self.native) as db:
            db.execute('UPDATE messages SET content=? WHERE id=1', ('Fictional bicycle now in amber-garage-628.',))
        report = self.sync()
        self.assertEqual(report['retired_revisions'], 1)
        self.assertFalse(self.search('cedar-shed-527'))
        self.assertTrue(self.search('amber-garage-628'))
        self.assertEqual(self.sync()['duplicates'], 1)

    def test_record_forget_blocks_edited_native_reimport(self):
        self.sync()
        rid = self.search('cedar-shed-527')[0]['id']
        self.client.call('/v1/forget', {'record_id': rid})
        with sqlite3.connect(self.native) as db:
            db.execute('UPDATE messages SET content=? WHERE id=1', ('Fictional revised cedar-shed-527 record.',))
        self.assertEqual(self.sync()['suppressed_forgotten'], 1)
        self.assertFalse(self.search('cedar-shed-527'))

    def test_native_delete_and_restore_cannot_revive_import(self):
        self.sync()
        with sqlite3.connect(self.native) as db:
            row = db.execute('SELECT * FROM messages WHERE id=1').fetchone()
            db.execute('DELETE FROM messages WHERE id=1')
        self.assertEqual(self.sync()['removed_native_items'], 1)
        self.assertFalse(self.search('cedar-shed-527'))
        with sqlite3.connect(self.native) as db:
            db.execute('INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)', row)
        self.assertEqual(self.sync()['suppressed_forgotten'], 1)
        self.assertFalse(self.search('cedar-shed-527'))

    def test_lineage_backed_assistant_copy_follows_source_deletion(self):
        self.sync()
        rid = self.search('cedar-shed-527')[0]['id']
        self.sync({'2': [rid]})
        self.assertTrue(self.search('coral-bridge-419'))
        # Omitting a lineage file withholds new generated imports; it must not
        # mistake an existing native message for a deletion.
        self.sync()
        self.assertTrue(self.search('coral-bridge-419'))
        self.client.call('/v1/forget', {'record_id': rid})
        self.assertFalse(self.search('coral-bridge-419'))

    def test_partial_scan_never_sweeps_unseen_native_records(self):
        self.sync()
        connector = self.connector()
        def fail(checkpoint=None):
            raise RuntimeError('Fictional read failure')
            yield
        connector.read = fail
        with self.assertRaisesRegex(RuntimeError, 'read failure'):
            sync_history(self.client, connector, self.state)
        self.assertTrue(self.search('cedar-shed-527'))

    def test_source_tombstone_survives_restore_and_blocks_new_revisions(self):
        # Isolated canonical backup before the source-wide deletion.
        root = Path(self.tmp.name) / 'isolated'
        root.mkdir()
        original = Store(root / 'memory.db')
        item = fixtures.wire_record()
        rid = original.ingest_contract([item])['records'][0]['id']
        backup = root / 'prior.db'
        with original.connect() as source, sqlite3.connect(backup) as dest:
            source.backup(dest)
        original.forget_source(item['source'], item['source_id'])
        # Restore old content alongside the latest independent deletion ledger.
        with sqlite3.connect(backup) as source, sqlite3.connect(original.path) as dest:
            source.backup(dest)
        restored = Store(original.path)
        with self.assertRaises(ValueError): restored.evidence(rid)
        item['revision'] = 'new-revision'
        with self.assertRaisesRegex(ValueError, 'all revisions'):
            restored.ingest_contract([item])

    def test_interrupted_revision_retirement_recovers_on_retry(self):
        from unittest.mock import patch
        from personal_memory.client import ServiceError
        self.sync()
        with sqlite3.connect(self.native) as db:
            db.execute('UPDATE messages SET content=? WHERE id=1', ('New location silver-attic-652.',))
        real_call = self.client.call
        def fail_forget(path, *args, **kwargs):
            if path == '/v1/forget': raise ServiceError(503, 'Fictional outage')
            return real_call(path, *args, **kwargs)
        with patch.object(self.client, 'call', side_effect=fail_forget):
            with self.assertRaises(ServiceError): self.sync()
        self.sync()
        self.assertFalse(self.search('cedar-shed-527'))
        self.assertTrue(self.search('silver-attic-652'))

    def test_ingestion_batches_and_unchanged_scan_avoid_per_record_requests(self):
        from unittest.mock import patch
        with sqlite3.connect(self.native) as db:
            db.executemany('INSERT INTO messages VALUES(?,?,?,?,?,1,0,0)', [
                (i, 'selected', 'user', f'Fictional native archive entry number {i}.', 1700000000 + i)
                for i in range(10, 210)])
        with patch.object(self.client, 'call', wraps=self.client.call) as calls:
            self.sync()
            ingest = [c for c in calls.call_args_list if c.args[0] == '/v1/ingest']
            self.assertEqual(len(ingest), 3)
        with patch.object(self.client, 'call', wraps=self.client.call) as calls:
            report = self.sync()
            self.assertEqual(report['duplicates'], 201)
            self.assertFalse([c for c in calls.call_args_list if c.args[0] == '/v1/ingest'])
            self.assertLess(len(calls.call_args_list), 12)

    def test_compaction_copy_retains_lineage_when_original_native_row_disappears(self):
        self.sync()
        original = self.search('cedar-shed-527')[0]['id']
        with sqlite3.connect(self.native) as db:
            db.execute('INSERT INTO messages SELECT 4,session_id,role,content,timestamp,1,0,0 FROM messages WHERE id=1')
            db.execute('DELETE FROM messages WHERE id=1')
        self.sync()
        copies = self.search('cedar-shed-527')
        self.assertTrue(copies)
        self.client.call('/v1/forget', {'record_id': original})
        self.assertFalse(self.search('cedar-shed-527'))
        self.sync()
        self.assertFalse(self.search('cedar-shed-527'))

    def test_same_snapshot_compaction_copy_links_to_first_canonical_record(self):
        with sqlite3.connect(self.native) as db:
            db.execute('INSERT INTO messages SELECT 4,session_id,role,content,timestamp,1,0,0 FROM messages WHERE id=1')
        self.sync()
        hits = self.search('cedar-shed-527')
        self.assertEqual(len(hits), 2)
        originals = [r for r in hits if not self.client.call('/v1/evidence', {'record_id': r['id']})['metadata']['parent_record_ids']]
        self.assertEqual(len(originals), 1)
        self.client.call('/v1/forget', {'record_id': originals[0]['id']})
        self.assertFalse(self.search('cedar-shed-527'))
