"""Bundled email-export adapter and large-volume ingest qualification.

Proves the second-source requirement (OPS-05): a file/export source uses the
identical runtime with no provider branch in the core, and the commit path
carries high-volume backfill in few, bounded transactions.
"""
import tempfile
import time
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_sdk import wrap_ingestion_connector
from personal_memory.source_sync import SourceSync, SyncWorker
from personal_memory.source_adapters import EmailExportAdapter
from tests.test_source_sync import FixtureAdapter, note_record, source_page, source_operation, read_state

MBOX = """From mbox@local Tue Sep 01 10:00:00 2026
Date: Tue, 01 Sep 2026 10:00:00 +0000
Message-ID: <m1@export.test>
From: ada@example.test
To: ben@example.test
Subject: Violet folder

The violet folder is in the bedroom cupboard.

From mbox@local Wed Sep 02 11:00:00 2026
Date: Wed, 02 Sep 2026 11:00:00 +0000
Message-ID: <m2@export.test>
From: ben@example.test
To: ada@example.test
Subject: Re: Violet folder

Found it, thanks.

From mbox@local Thu Sep 03 12:00:00 2026
Date: Thu, 03 Sep 2026 12:00:00 +0000
Message-ID: <m3@export.test>
From: cy@example.test
To: ada@example.test
Subject: Router PIN

The spare router PIN is 4417.
"""


class EmailExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.mbox = Path(self.tmp.name) / "archive.mbox"
        self.mbox.write_text(MBOX, encoding="utf-8")
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.adapter = EmailExportAdapter(path=self.mbox, source="email", page_size=2)
        self.sync = SourceSync(self.store, {self.adapter.spec()["adapter_id"]: self.adapter})
        self.conn = self.sync.configure(adapter_id=self.adapter.spec()["adapter_id"],
                                        source="email", scope={}, retention="mirror")

    def run_to_completion(self, worker):
        statuses = []
        for _ in range(10):
            result = worker.run_once(self.conn["connection_id"], stream="export", role="backfill")
            statuses.append(result["status"])
            if result["status"] == "complete":
                return statuses
        self.fail(f"export never completed: {statuses}")

    def test_export_imports_in_bounded_pages_and_is_recallable(self):
        worker = SyncWorker(self.sync)
        statuses = self.run_to_completion(worker)
        self.assertEqual(statuses, ["committed", "committed", "complete"])
        with self.store.connect() as db:
            ids = [row["id"] for row in db.execute(
                "SELECT id FROM record_fts WHERE record_fts MATCH 'violet folder'")]
            self.assertTrue(ids)
            total = db.execute("SELECT COUNT(*) FROM records WHERE source='email'").fetchone()[0]
        self.assertEqual(total, 3)
        head = self.sync.head("email", "<m1@export.test>")
        self.assertEqual(head["state"], "live")

    def test_rerunning_the_export_changes_nothing(self):
        worker = SyncWorker(self.sync)
        self.run_to_completion(worker)
        with self.store.connect() as db:
            before = db.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        self.run_to_completion(worker)  # resume from done cursor: terminal replay only
        with self.store.connect() as db:
            after = db.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        self.assertEqual(before, after)

    def test_appended_message_is_picked_up_by_an_explicit_rescan(self):
        worker = SyncWorker(self.sync)
        self.run_to_completion(worker)
        self.mbox.write_text(MBOX + """
From mbox@local Fri Sep 04 09:00:00 2026
Date: Thu, 03 Sep 2026 12:00:00 +0000
Message-ID: <m4@export.test>
From: dee@example.test
To: ada@example.test
Subject: Gate code

The side gate code is 9W2Q.

""", encoding="utf-8")
        # An export is a snapshot stream: resuming a finished cursor replays empty;
        # restarting the scan generation ingests the new file state.
        self.sync.restart_stream(self.conn["connection_id"], stream="export", role="backfill")
        self.run_to_completion(worker)
        head = self.sync.head("email", "<m4@export.test>")
        self.assertIsNotNone(head)

    def test_legacy_connector_wrapper_runs_on_the_same_runtime(self):
        # OPS-05: the same runtime drives a wrapped legacy connector, unchanged core.
        from tests.test_source_sdk import LegacyNotes
        adapter = wrap_ingestion_connector(LegacyNotes())
        sync = SourceSync(self.store, {"example.notes": adapter})
        conn = sync.configure(adapter_id="example.notes", source="custom-notes", scope={},
                              retention="mirror")
        worker = SyncWorker(sync)
        first = worker.run_once(conn["connection_id"], stream="export", role="backfill")
        self.assertEqual(first["applied"], 2)
        second = worker.run_once(conn["connection_id"], stream="export", role="backfill")
        self.assertEqual(second["status"], "complete")
        with self.store.connect() as db:
            total = db.execute("SELECT COUNT(*) FROM records WHERE source='custom-notes'").fetchone()[0]
        self.assertEqual(total, 2)


def big_page(index, count):
    operations = [source_operation("upsert", f"msg_{index}_{i}",
                                   records=[note_record(f"msg_{index}_{i}",
                                                        text=f"message body {index}-{i}")],
                                   source_version=index * count + i)
                  for i in range(count)]
    return source_page(page_id=f"big_{index}", operations=operations,
                       next_state=read_state(cursor={"through": index * count + count}))


class VolumeTests(unittest.TestCase):
    def test_two_thousand_records_commit_idempotently_and_quickly(self):
        pages = [big_page(index, 250) for index in range(8)]
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / "memory.db")
            sync = SourceSync(store, {"fixture.gmail": FixtureAdapter()})
            conn = sync.configure(adapter_id="fixture.gmail", source="gmail-acct1",
                                  scope={"labels": ["ALL"]}, retention="mirror")
            lease = sync.claim(conn["connection_id"], stream="messages", role="backfill",
                               owner="bulk", ttl=3600)
            started = time.monotonic()
            for page in pages:
                sync.commit_page(lease, op_id="bulk_" + page["page_id"], page=page)
            elapsed = time.monotonic() - started
            with store.connect() as db:
                total = db.execute("SELECT COUNT(*) FROM records WHERE source='gmail-acct1'").fetchone()[0]
                heads = db.execute("SELECT COUNT(*) FROM source_heads").fetchone()[0]
                cursors = db.execute("SELECT cursor FROM source_streams WHERE role='backfill'").fetchone()[0]
            self.assertEqual(total, 2000)
            self.assertEqual(heads, 2000)
            self.assertIn("through", cursors)
            # Generous CI ceiling, not a marketing claim: 2k end-to-end commits < 15s.
            self.assertLess(elapsed, 15.0)
            print(f"\nvolume: 2000 records in {elapsed:.2f}s "
                  f"({2000 / elapsed:.0f} rec/s, {len(pages)} transactions)")
            # Full replay of every page changes nothing (receipts + fingerprint dedupe).
            started = time.monotonic()
            for page in pages:
                result = sync.commit_page(lease, op_id="bulk_" + page["page_id"], page=page)
                self.assertTrue(result["replayed"])
            self.assertLess(time.monotonic() - started, 2.0)


if __name__ == "__main__":
    unittest.main()
