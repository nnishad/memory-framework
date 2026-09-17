"""Change 1 regression: retired evidence must never wedge the auto-consolidator.

Workflows.scan() selects only live-and-visible evidence, advances its cursor
past records retired between selection and snapshot/job creation, keeps
retrying genuine processing failures, and a scan failure must not stop the
worker from executing unrelated queued jobs.
"""
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from personal_memory.intelligence import Intelligence
from personal_memory.learning import Learning
from personal_memory.store import Store
from personal_memory.workflows import Workflows
from test_ingestion import item


class WorkflowRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.rids = [self.store.ingest_contract([item(name)])["records"][0]["id"]
                     for name in ("first", "second", "third")]

    def _watermark(self):
        with self.store.connect() as db:
            row = db.execute("SELECT watermark FROM workflow_cursor WHERE name='ingest'").fetchone()
            return row[0] if row else 0

    def _audit_id(self, rid):
        with self.store.connect() as db:
            return db.execute("SELECT id FROM audit WHERE action='ingest' AND object_id=?",
                              (rid,)).fetchone()[0]

    def _job_keys(self):
        with self.store.connect() as db:
            return [r[0] for r in db.execute(
                "SELECT logical_key FROM learning_objects WHERE kind='job' ORDER BY logical_key")]

    def _prefix(self, rid):
        return [key for key in self._job_keys() if key.startswith("auto/" + rid)]

    def test_retired_record_does_not_block_valid_records(self):
        self.store.supersede(self.rids[0])
        worker = Workflows(self.store, {"auto_consolidate": True, "timeout": 5})
        self.addCleanup(worker.close)
        # A retired head followed by valid records must not raise; valid records are scanned.
        self.assertEqual(worker.scan(), 2)
        self.assertEqual(self._prefix(self.rids[0]), [])
        for rid in self.rids[1:]:
            self.assertTrue(self._prefix(rid), f"no consolidation job queued for {rid}")
        # The cursor advanced past the retired record's audit row.
        self.assertGreaterEqual(self._watermark(), self._audit_id(self.rids[0]))
        for _ in range(2):
            self.assertTrue(worker.tick())
        with self.store.connect() as db:
            states = {r[0] for r in db.execute(
                "SELECT j.state FROM workflow_jobs j JOIN learning_objects o ON o.id=j.id"
                " WHERE o.kind='job'")}
        self.assertEqual(states, {"completed"})

    def test_retirement_between_selection_and_snapshot_skips_safely(self):
        worker = Workflows(self.store, {"auto_consolidate": True, "timeout": 5})
        self.addCleanup(worker.close)
        original = Intelligence.snapshot

        def racing(self_i, *, key, record_ids, actor):
            # Evidence is retired after scan() selected the rows but before the
            # snapshot and jobs are created.
            if record_ids == [self.rids[0]]:
                self.store.supersede(self.rids[0])
            return original(self_i, key=key, record_ids=record_ids, actor=actor)

        with patch.object(Intelligence, "snapshot", racing):
            self.assertEqual(worker.scan(), 3)
        self.assertEqual(self._prefix(self.rids[0]), [])
        for rid in self.rids[1:]:
            self.assertTrue(self._prefix(rid))
        self.assertGreaterEqual(self._watermark(), self._audit_id(self.rids[-1]))
        # The skipped record never resurfaces: no repeated failure on the next scan.
        self.assertEqual(worker.scan(), 0)

    def test_previously_invalidated_snapshot_does_not_repeat_failure(self):
        worker = Workflows(self.store, {"auto_consolidate": True, "timeout": 5})
        self.addCleanup(worker.close)
        rid = self.rids[0]
        worker.scan()
        first_jobs = self._prefix(rid)
        self.assertTrue(first_jobs)
        self.store.supersede(rid)  # cascades: the auto snapshot is invalidated
        # Legacy restore path: visibility comes back but the invalidated snapshot
        # is never reactivated, so the scan must skip it instead of failing forever.
        with self.store.connect() as db:
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (rid,))
            db.execute("UPDATE workflow_cursor SET watermark=0 WHERE name='ingest'")
        self.assertEqual(worker.scan(), 3)  # all rows are re-selected; must not raise
        self.assertEqual(self._prefix(rid), first_jobs)  # no second job chain
        self.assertGreaterEqual(self._watermark(), self._audit_id(rid))
        self.assertEqual(worker.scan(), 0)

    def test_genuine_processing_failure_is_retried_not_skipped(self):
        worker = Workflows(self.store, {"auto_consolidate": True, "timeout": 5})
        self.addCleanup(worker.close)
        before = self._watermark()
        with patch.object(Intelligence, "snapshot", side_effect=RuntimeError("transient")):
            with self.assertRaises(RuntimeError):
                worker.scan()
            # A genuine failure keeps the cursor in place so the next pass retries.
            self.assertEqual(self._watermark(), before)
            with self.assertRaises(RuntimeError):
                worker.scan()

    def test_scan_failure_does_not_stop_queued_jobs(self):
        worker = Workflows(self.store, {"auto_consolidate": True, "timeout": 5})
        self.addCleanup(worker.close)
        snapshot = Intelligence(self.store).snapshot(key="manual", record_ids=[self.rids[0]], actor="admin")
        job = worker.enqueue(key="manual", type="consolidate", snapshot_id=snapshot["id"], actor="admin")
        state = "pending"
        with patch.object(Workflows, "scan", side_effect=RuntimeError("boom")):
            worker.start()
            deadline = time.time() + 60
            while time.time() < deadline:
                state = worker.get(job_id=job["id"])["state"]
                if state == "completed":
                    break
                time.sleep(0.05)
            self.assertEqual(state, "completed", "queued work must run even while scan keeps failing")
            deadline = time.monotonic() + 5
            while worker.status()["last_error"] is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(worker.status()["last_error"], "RuntimeError")
        deadline = time.monotonic() + 5
        while worker.status()["last_error"] and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertIsNone(worker.status()["last_error"])


class ConsolidationAcceptTests(unittest.TestCase):
    """Acceptance keeps targeted evidence validation; the archive-wide repair moved to init."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.rid = self.store.ingest_contract([item("live")])["records"][0]["id"]
        self.worker = Workflows(self.store, {"timeout": 5})
        self.addCleanup(self.worker.close)

    def _pending_result(self, rid):
        snapshot = Intelligence(self.store).snapshot(key="s", record_ids=[rid], actor="agent")
        job = self.worker.enqueue(key="c", type="consolidate", snapshot_id=snapshot["id"], actor="agent")
        adapter_result = {"summary": "Repair note",
                          "quotes": [{"record_id": rid, "quote": "bicycle repair"}], "proposals": []}
        with patch("personal_memory.workflows.run_adapter", return_value=adapter_result):
            self.assertTrue(self.worker.tick())
        completed = self.worker.get(job_id=job["id"])
        self.assertEqual(completed["state"], "completed", completed)
        return completed["result_id"]

    def test_accept_no_longer_repairs_unrelated_legacy_artifacts(self):
        legacy_rid = self.store.ingest_contract([item("legacy")])["records"][0]["id"]
        outcome = Learning(self.store).outcome(key="legacy-outcome", goal="Repair bicycle",
                                               action="Check", result="Done", outcome="success",
                                               evidence_ids=[legacy_rid], actor="agent")
        # Legacy state the startup migration would repair: hidden evidence, active artifact.
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (legacy_rid,))
        result_id = self._pending_result(self.rid)
        self.worker.accept_consolidation(result_id=result_id, actor="admin")
        self.assertEqual([i["id"] for i in Learning(self.store).browse(kind="outcome", state="recorded")["items"]],
                         [outcome["id"]], "acceptance must not run a full-archive repair")

    def test_accept_rejects_evidence_retired_after_the_job_ran(self):
        result_id = self._pending_result(self.rid)
        # Hidden without the cascade (legacy path): the result itself is still present,
        # so only the targeted evidence validation can block publication.
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (self.rid,))
        with self.assertRaises(ValueError):
            self.worker.accept_consolidation(result_id=result_id, actor="admin")


if __name__ == "__main__":
    unittest.main()
