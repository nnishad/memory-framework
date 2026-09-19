"""Bundled email-export adapter and large-volume ingest qualification.

Proves the second-source requirement (OPS-05): a file/export source uses the
identical runtime with no provider branch in the core, and the commit path
carries high-volume backfill in few, bounded transactions.
"""
import base64
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from personal_memory import extraction
from personal_memory.importers import emails as parse_emails
from personal_memory.source_adapters import EmailExportAdapter
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import AdapterError, wrap_ingestion_connector
from personal_memory.source_sync import SourceSync, SyncWorker
from personal_memory.store import Store
from tests.test_extraction_runtime import CANNED, png_bytes
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


def attachment_eml_bytes(png=b"\x89png-bytes"):
    """A minimal multipart EML with one PNG attachment and a text body.

    Bytes-mode headers keep a trailing-LF payload from being newline-mangled
    on text round-trips, so the decoded part matches the fixture exactly.
    """
    boundary = "=sec_part_boundary="
    body = ("This is a multi-part message in MIME format.\n\n"
            f"--{boundary}\n"
            "Content-Type: text/plain; charset=\"utf-8\"\n"
            "Content-Transfer-Encoding: 7bit\n\n"
            "see attached\n\n"
            f"--{boundary}\n"
            "Content-Type: image/png; name=\"s.png\"\n"
            "Content-Disposition: attachment; filename=\"s.png\"\n"
            "Content-Transfer-Encoding: base64\n\n"
            + base64.b64encode(png).decode() + "\n\n"
            f"--{boundary}--\n")
    headers = ("Date: Tue, 01 Sep 2026 10:00:00 +0000\r\n"
               "Message-ID: <att1@export.test>\r\n"
               "From: ada@example.test\r\n"
               "To: ben@example.test\r\n"
               "Subject: Receipt\r\n"
               "MIME-Version: 1.0\r\n"
               f"Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n"
               "\r\n").encode()
    return headers + body.encode()


class AttachmentDescriptorTests(unittest.TestCase):
    """A2: the offline importer path must surface re-fetchable attachment descriptors."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.raw = png_bytes()
        self.eml = Path(self.tmp.name) / "receipt.eml"
        self.eml.write_bytes(attachment_eml_bytes(self.raw))
        self.adapter = EmailExportAdapter(path=self.eml, source="email")

    def test_emails_yields_stable_descriptors(self):
        rows = list(parse_emails(self.eml))
        meta = rows[0]["metadata"]
        self.assertTrue(meta["attachment_contents_imported"])
        descriptor = meta["attachment_descriptors"][0]
        self.assertEqual(descriptor["source_id"], "<att1@export.test>")
        self.assertEqual(descriptor["part_id"], "0")
        self.assertEqual(descriptor["filename"], "s.png")
        self.assertEqual(descriptor["mime"], "image/png")
        self.assertEqual(descriptor["size"], len(self.raw))

    def test_normalize_passes_descriptors_through(self):
        legacy = next(iter(parse_emails(self.eml)))
        item = self.adapter.normalize(legacy)
        self.assertEqual(item["attachments"],
                         legacy["metadata"]["attachment_descriptors"])

    def test_attachment_refetches_exact_bytes(self):
        descriptor = {"source_id": "<att1@export.test>", "part_id": "0",
                      "filename": "s.png", "mime": "image/png", "size": len(self.raw)}
        self.assertEqual(self.adapter.attachment(None, descriptor), self.raw)

    def test_mbox_attachment_refetches_exact_bytes(self):
        mbox_path = Path(self.tmp.name) / "archive2.mbox"
        eml = attachment_eml_bytes(self.raw).replace(b"\r\n", b"\n")
        mbox_path.write_bytes(b"From mbox@local Tue Sep 01 10:00:00 2026\n" + eml)
        adapter = EmailExportAdapter(path=mbox_path, source="email")
        rows = list(parse_emails(mbox_path))
        descriptor = rows[0]["metadata"]["attachment_descriptors"][0]
        self.assertEqual(adapter.attachment(None, descriptor), self.raw)

    def test_unknown_part_is_a_permanent_error(self):
        with self.assertRaises(AdapterError):
            self.adapter.attachment(None, {"source_id": "<att1@export.test>",
                                           "part_id": "9", "filename": "s.png",
                                           "mime": "image/png", "size": 1})


class AttachmentPipelineTests(unittest.TestCase):
    """An imported email attachment flows through the same blob + extraction jobs."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.data_dir = Path(self.tmp.name)
        self.store = Store(self.data_dir / "memory.db")
        self.raw = png_bytes()
        self.eml = self.data_dir / "receipt.eml"
        self.eml.write_bytes(attachment_eml_bytes(self.raw))
        self.adapter = EmailExportAdapter(path=self.eml, source="email")
        self.runtime = SourceRuntime(self.store, self.data_dir, adapters=(self.adapter,),
                                     extraction_config={"base_url": "http://h/v1", "model": "vis"})
        self.sync = self.runtime.sync
        self.cid = self.sync.configure(adapter_id="email.export", source="email", scope={},
                                       retention="mirror")["connection_id"]

    def test_imported_attachment_becomes_searchable_derived_text(self):
        worker = SyncWorker(self.sync)
        for _ in range(10):
            result = worker.run_once(self.cid, stream="export", role="backfill")
            if result["status"] == "complete":
                break
        else:
            self.fail("export never completed")
        with self.store.connect() as db:
            job = db.execute("SELECT state FROM source_jobs WHERE kind='attachment'").fetchone()
        self.assertIsNotNone(job)  # descriptor identity enqueued the attachment obligation
        self.runtime._attachment(self.cid)
        with mock.patch.object(extraction, "extract_text", lambda raw, mime, filename, cfg: dict(CANNED)):
            self.runtime._extraction(self.cid)
        hits = [e for e in self.store.search("Invoice")["episodes"] if e["kind"] == "attachment_text"]
        self.assertTrue(hits)



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
