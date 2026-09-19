"""Runtime wiring tests for attachment text extraction (job enqueue + _extraction worker)."""
import base64
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from personal_memory import blobs, extraction
from personal_memory.common import digest
from personal_memory.store import Store
from personal_memory.source_sync import SourceSync
from personal_memory.source_runtime import SourceRuntime
from personal_memory.ingestion import validate_record


def rec_id(source, source_id, revision="1"):
    return "rec_" + digest([source, source_id, revision])[:32]


def parent_record(source_id="m1", source="gmail-acct1"):
    return {"schema_version": "1.0", "source": source, "source_id": source_id, "revision": "1",
            "kind": "email", "occurred_at": None, "observed_at": "2026-09-06T12:00:00Z",
            "text": "body of " + source_id, "participants": [],
            "provenance": {"connector_id": "fixture.gmail", "connector_version": "1.0",
                           "source_locator": "gmail://" + source_id, "origin": "source",
                           "parent_record_ids": []}, "extensions": {}}


class FakeGmail:
    """Attachment-capable google.gmail adapter for runtime tests."""

    def __init__(self, attachment_bytes=b""):
        self._bytes = attachment_bytes

    def spec(self):
        return {"adapter_id": "google.gmail", "adapter_version": "1.0", "protocol_versions": ["1.0"],
                "capabilities": {"history": True, "incremental": True, "reconciliation": False,
                                 "deletions": False, "attachments": True, "events": False, "subscriptions": False},
                "config_schema": {}, "secret_refs": []}

    def check(self, context):
        return {"account_id": "acct1", "messages_total": 0, "history_id": "1"}

    def discover(self, context):
        return []

    def read_page(self, context, state):
        raise NotImplementedError

    def normalize(self, payload):
        raise NotImplementedError

    def attachment(self, context, descriptor):
        return self._bytes


def png_bytes(size=(8, 8)):
    from PIL import Image
    img = Image.new("RGB", size, (255, 255, 255))
    buf = io.BytesIO(); img.save(buf, format="PNG"); return buf.getvalue()


class EnqueueJobTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.sync = SourceSync(self.store, {"google.gmail": FakeGmail()})
        self.conn = self.sync.configure(adapter_id="google.gmail", source="gmail-acct1",
                                        scope={"labels": ["INBOX"]}, retention="mirror")
        self.cid = self.conn["connection_id"]

    def test_enqueue_is_idempotent_by_dedupe_key(self):
        self.sync.enqueue_job(self.cid, "extraction", "extraction:blob1", {"blob_id": "blob1"})
        self.sync.enqueue_job(self.cid, "extraction", "extraction:blob1", {"blob_id": "blob1"})
        with self.store.connect() as db:
            n = db.execute("SELECT COUNT(*) FROM source_jobs WHERE kind='extraction'").fetchone()[0]
        self.assertEqual(n, 1)

    def test_claim_returns_payload(self):
        self.sync.enqueue_job(self.cid, "extraction", "extraction:b2", {"blob_id": "b2", "record_id": "r"})
        job = self.sync.claim_job("w1", kinds=("extraction",), connection_id=self.cid)
        self.assertEqual(job["payload"]["blob_id"], "b2")

    def test_unknown_connection_rejected(self):
        with self.assertRaises(ValueError):
            self.sync.enqueue_job("nope", "extraction", "k", {})


class AttachmentEnqueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.data_dir = Path(self.tmp.name)
        self.store = Store(self.data_dir / "memory.db")
        self.adapter = FakeGmail(attachment_bytes=png_bytes())
        self.runtime = SourceRuntime(self.store, self.data_dir, adapter=self.adapter,
                                     extraction_config={"base_url": "http://h/v1", "model": "vis"})
        self.sync = self.runtime.sync
        self.cid = self.sync.configure(adapter_id="google.gmail", source="gmail-acct1",
                                       scope={"labels": ["INBOX"]}, retention="mirror")["connection_id"]

    def test_extraction_config_is_normalized(self):
        self.assertTrue(self.runtime.extraction_config["enabled"])
        self.assertEqual(self.runtime.extraction_config["max_pdf_pages"], 20)
        self.assertEqual(self.runtime.extraction_config["model"], "vis")

    def test_absent_config_still_normalizes_to_defaults(self):
        runtime = SourceRuntime(self.store, self.data_dir, adapter=FakeGmail(png_bytes()))
        self.assertTrue(runtime.extraction_config["enabled"])
        self.assertIsNone(runtime.extraction_config["base_url"])

    def test_attachment_step_enqueues_extraction_job(self):
        rid = self.store.ingest_contract([parent_record("m1")])["records"][0]["id"]
        raw = png_bytes()
        self.sync.enqueue_job(self.cid, "attachment", "attachment:part1",
                              {"record_id": rid, "part_id": "part1", "filename": "s.png",
                               "mime": "image/png", "size": len(raw)})
        self.runtime._attachment(self.cid)
        with self.store.connect() as db:
            row = db.execute("SELECT payload FROM source_jobs WHERE kind='extraction'").fetchone()
        self.assertIsNotNone(row)
        payload = json.loads(row[0])
        self.assertEqual(payload["record_id"], rid)
        self.assertTrue(payload["blob_id"].startswith("blob_"))
        self.assertEqual(payload["mime"], "image/png")

    def test_no_extraction_job_when_disabled(self):
        self.runtime.extraction_config = extraction.normalize_config(
            {"enabled": False, "base_url": "http://h/v1", "model": "vis"})
        rid = self.store.ingest_contract([parent_record("m2")])["records"][0]["id"]
        raw = png_bytes()
        self.sync.enqueue_job(self.cid, "attachment", "attachment:part2",
                              {"record_id": rid, "part_id": "part2", "filename": "s.png",
                               "mime": "image/png", "size": len(raw)})
        self.runtime._attachment(self.cid)
        with self.store.connect() as db:
            n = db.execute("SELECT COUNT(*) FROM source_jobs WHERE kind='extraction'").fetchone()[0]
        self.assertEqual(n, 0)


CANNED = {"text": "Invoice 12345", "method": "vision-llm", "model": "vis",
          "truncated": False, "chars": 13, "pages": None}


class ExtractionWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.data_dir = Path(self.tmp.name)
        self.store = Store(self.data_dir / "memory.db")
        self.runtime = SourceRuntime(self.store, self.data_dir, adapter=FakeGmail(png_bytes()),
                                     extraction_config={"base_url": "http://h/v1", "model": "vis"})
        self.sync = self.runtime.sync
        self.cid = self.sync.configure(adapter_id="google.gmail", source="gmail-acct1",
                                       scope={"labels": ["INBOX"]}, retention="mirror")["connection_id"]

    def blob_id(self, rid):
        with self.store.connect() as db:
            return db.execute("SELECT id FROM memory_blobs WHERE record_id=?", (rid,)).fetchone()[0]

    def store_attachment(self, source_id="m1"):
        rid = self.store.ingest_contract([parent_record(source_id)])["records"][0]["id"]
        raw = png_bytes()
        self.sync.enqueue_job(self.cid, "attachment", "attachment:" + source_id,
                              {"record_id": rid, "part_id": source_id, "filename": "s.png",
                               "mime": "image/png", "size": len(raw)})
        self.runtime._attachment(self.cid)
        return rid

    def job(self, dedupe_like="extraction:%"):
        with self.store.connect() as db:
            return db.execute("SELECT state,attempts,result FROM source_jobs WHERE kind='extraction' AND dedupe_key LIKE ?",
                              (dedupe_like,)).fetchone()

    def test_writes_searchable_derived_record(self):
        rid = self.store_attachment()
        with mock.patch.object(extraction, "extract_text", lambda raw, mime, filename, cfg: dict(CANNED)):
            self.runtime._extraction(self.cid)
        derived = [e for e in self.store.search("Invoice")["episodes"] if e["kind"] == "attachment_text"]
        self.assertTrue(derived)
        self.assertEqual(derived[0]["source_id"], "attachment-text:" + self.blob_id(rid))
        self.assertEqual(self.job()["state"], "succeeded")

    def test_skips_when_endpoint_unconfigured(self):
        self.store_attachment()
        self.runtime.extraction_config = extraction.normalize_config({})
        self.runtime._extraction(self.cid)
        self.assertEqual(json.loads(self.job()["result"])["skipped"], "extraction endpoint not configured")

    def test_skips_when_disabled(self):
        self.store_attachment()
        self.runtime.extraction_config = extraction.normalize_config(
            {"enabled": False, "base_url": "http://h/v1", "model": "vis"})
        self.runtime._extraction(self.cid)
        self.assertEqual(json.loads(self.job()["result"])["skipped"], "extraction disabled")

    def test_second_run_is_duplicate_noop(self):
        rid = self.store_attachment()
        blob_id = self.blob_id(rid)
        with mock.patch.object(extraction, "extract_text", lambda raw, mime, filename, cfg: dict(CANNED)):
            self.runtime._extraction(self.cid)
            self.sync.enqueue_job(self.cid, "extraction", "extraction:again:" + blob_id,
                                  {"record_id": rid, "blob_id": blob_id, "filename": "s.png",
                                   "mime": "image/png", "size": 1})
            self.runtime._extraction(self.cid)
        with self.store.connect() as db:
            n = db.execute("SELECT COUNT(*) FROM records WHERE kind='attachment_text' AND deleted=0").fetchone()[0]
            again = db.execute("SELECT result FROM source_jobs WHERE dedupe_key=?",
                               ("extraction:again:" + blob_id,)).fetchone()
        self.assertEqual(n, 1)
        self.assertTrue(json.loads(again["result"])["duplicate"])

    def test_retryable_error_sets_retry_wait(self):
        self.store_attachment()
        def boom(raw, mime, filename, cfg):
            raise extraction.ExtractionError("network", "refused")
        with mock.patch.object(extraction, "extract_text", boom):
            self.runtime._extraction(self.cid)
        row = self.job()
        self.assertEqual(row["state"], "retry_wait")
        self.assertEqual(row["attempts"], 1)

    def test_skip_error_completes_job(self):
        self.store_attachment()
        def boom(raw, mime, filename, cfg):
            raise extraction.ExtractionError("corrupt", "bad bytes")
        with mock.patch.object(extraction, "extract_text", boom):
            self.runtime._extraction(self.cid)
        row = self.job()
        self.assertEqual(row["state"], "succeeded")
        self.assertEqual(json.loads(row["result"])["skipped"], "corrupt")

    def test_retired_parent_is_skipped(self):
        # A hidden-but-not-forgotten parent leaves the job queued (forget would delete
        # it at store._forget line 517), so this exercises the live-evidence race guard.
        rid = self.store_attachment()
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
        self.runtime._extraction(self.cid)
        self.assertEqual(json.loads(self.job()["result"])["skipped"], "evidence retired")


class _HttpStub:
    """Minimal urlopen stand-in returning an OpenAI-shaped chat completion."""

    def __init__(self, text):
        self._body = json.dumps({"choices": [{"message": {"content": text}}]}).encode()

    def __call__(self, request, timeout=None):
        outer = self

        class _Resp:
            status = 200
            def read(self_inner):
                return outer._body
            def __enter__(self_inner):
                return self_inner
            def __exit__(self_inner, *a):
                return False
        return _Resp()


class IntegrationAndCascadeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.data_dir = Path(self.tmp.name)
        self.store = Store(self.data_dir / "memory.db")
        self.runtime = SourceRuntime(self.store, self.data_dir, adapter=FakeGmail(png_bytes()),
                                     extraction_config={"base_url": "http://h/v1", "model": "vis"})
        self.sync = self.runtime.sync
        self.cid = self.sync.configure(adapter_id="google.gmail", source="gmail-acct1",
                                       scope={"labels": ["INBOX"]}, retention="mirror")["connection_id"]

    def store_attachment(self, source_id):
        rid = self.store.ingest_contract([parent_record(source_id)])["records"][0]["id"]
        raw = png_bytes()
        self.sync.enqueue_job(self.cid, "attachment", "attachment:" + source_id,
                              {"record_id": rid, "part_id": source_id, "filename": "s.png",
                               "mime": "image/png", "size": len(raw)})
        self.runtime._attachment(self.cid)
        return rid

    def derived_id(self):
        with self.store.connect() as db:
            return db.execute("SELECT id FROM records WHERE kind='attachment_text' AND deleted=0").fetchone()["id"]

    def test_full_chain_writes_parented_derived_record(self):
        rid = self.store_attachment("m1")
        with mock.patch.object(extraction.urllib.request, "urlopen",
                               _HttpStub("Invoice 12345 total due")):
            self.runtime._extraction(self.cid)
        derived = self.derived_id()
        with self.store.connect() as db:
            edge = db.execute("SELECT parent_id FROM record_dependencies WHERE child_id=?",
                              (derived,)).fetchone()
        self.assertEqual(edge["parent_id"], rid)
        hits = [e for e in self.store.search("Invoice")["episodes"] if e["kind"] == "attachment_text"]
        self.assertTrue(hits)

    def test_forgetting_parent_cascades_to_derived(self):
        rid = self.store_attachment("m2")
        with mock.patch.object(extraction.urllib.request, "urlopen", _HttpStub("Quarterly report")):
            self.runtime._extraction(self.cid)
        derived = self.derived_id()
        self.store.forget(rid)
        with self.store.connect() as db:
            row = db.execute("SELECT deleted FROM records WHERE id=?", (derived,)).fetchone()
        self.assertEqual(row["deleted"], 1)
        self.assertFalse([e for e in self.store.search("Quarterly")["episodes"] if e["id"] == derived])


if __name__ == "__main__":
    unittest.main()
