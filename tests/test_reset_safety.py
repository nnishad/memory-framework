"""Reset safety against a concurrent external indexing engine.

A canonical reset must not race the Hindsight retain/delete cascade: an in-flight
retain may never re-publish evidence into a bank that reset already cleared, a
failed bulk clear must persist a durable cleanup obligation, and the retry loop
must resume that obligation after a failure or a restart. Ordinary per-document
deletion and batch retries stay idempotent.
"""
import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from personal_memory import reset
from personal_memory.hindsight import Hindsight
from personal_memory.retrieval import Hybrid
from personal_memory.service import MemoryService
from personal_memory.store import Store


def record(i, text="vehicle repair", source="whatsapp", when="2024-01-01T12:00:00Z"):
    return {"source": source, "source_id": str(i), "text": text, "occurred_at": when,
            "metadata": {"participants": []}}


def contract_item(source_id):
    return {"schema_version": "1.0", "source": "whatsapp", "source_id": str(source_id),
            "revision": "1", "kind": "episode", "occurred_at": "2024-01-01T12:00:00Z",
            "observed_at": "2026-09-06T12:00:00Z", "text": "vehicle repair",
            "participants": [],
            "provenance": {"connector_id": "wa.test", "connector_version": "1.0.0",
                           "source_locator": "wa://" + str(source_id), "origin": "source",
                           "parent_record_ids": []}, "extensions": {}}


class ControllableBank(unittest.TestCase):
    """Fake Hindsight bank with gates for retain latency and forced bulk-clear failure."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)
        self.remote = {}
        self.bulk_clears = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def _send(self, data, code=200):
                raw = json.dumps(data).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(raw))); self.end_headers()
                self.wfile.write(raw)
            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(n)) if n else {}
                server = self.server
                if self.path.endswith("/memories/recall"):
                    self._send({"results": [{"document_id": d} for d in server.remote]})
                    return
                server.retain_started.set()
                if not server.retain_gate.wait(15):
                    self._send({"error": "gate timeout"}, 500); return
                if server.fail_retain:
                    self._send({"error": "forced retain failure"}, 500); return
                for item in data.get("items", []):
                    server.remote[item["document_id"]] = item
                self._send({"success": True, "async": False, "items_count": len(data.get("items", []))})
            def do_DELETE(self):
                server = self.server
                if self.path.rstrip("/").endswith("/memories"):
                    if server.fail_bulk_clear:
                        self._send({"error": "forced clear failure"}, 500); return
                    self.server.bulk_clears.append(len(server.remote)); server.remote.clear()
                    self._send({"success": True})
                else:
                    server.remote.pop(self.path.rsplit("/", 1)[1], None)
                    self._send({"success": True})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.remote, server.bulk_clears = self.remote, self.bulk_clears
        server.retain_started = threading.Event()
        server.retain_gate = threading.Event(); server.retain_gate.set()
        server.fail_retain = False
        server.fail_bulk_clear = False
        self.server = server
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        self.cfg = {"url": f"http://127.0.0.1:{server.server_port}", "bank_id": "fixture", "sources": ["*"]}

    def put(self, *records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]

    def backend(self, adapter):
        hybrid = Hybrid(self.store, {"rerank": {"enabled": False}, "semantic": {"enabled": False}},
                        hindsight=adapter, start=False)
        self.addCleanup(hybrid.close)
        return hybrid

    def journal(self):
        with self.store.connect() as db:
            done = db.execute("SELECT count(*) FROM hindsight_done").fetchone()[0]
            pending = db.execute("SELECT count(*) FROM hindsight_pending").fetchone()[0]
        return done, pending

    def wait_until(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate(): return True
            time.sleep(0.05)
        return predicate()


class InFlightRetainTests(ControllableBank):
    def test_reset_during_in_flight_retain_leaves_no_forgotten_remote_documents(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1), record(2))
        backend = self.backend(adapter)
        self.server.retain_gate.clear()  # hold the retain open
        worker = threading.Thread(target=adapter.sync, daemon=True)
        worker.start()
        self.assertTrue(self.server.retain_started.wait(10))
        outcome = {}
        resetter = threading.Thread(
            target=lambda: outcome.update(reset.reset(self.store, "canonical", backend=backend)),
            daemon=True)
        resetter.start()
        time.sleep(0.3)
        self.server.retain_gate.set()  # the in-flight retain finishes after reset is waiting
        worker.join(30); resetter.join(30)
        self.assertFalse(worker.is_alive()); self.assertFalse(resetter.is_alive())
        self.assertTrue(outcome["external_engine"]["cleared"])
        self.assertEqual(self.remote, {})  # no forgotten document survives the reset
        self.assertEqual(self.journal(), (0, 0))


class BankClearObligationTests(ControllableBank):
    def test_remote_clear_timeout_preserves_cleanup_obligation(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1))
        adapter.sync()
        self.assertEqual(len(self.remote), 1)
        self.server.fail_bulk_clear = True
        result = adapter.clear_bank()
        self.assertFalse(result["cleared"])
        self.assertTrue(adapter.status()["pending_bank_clear"])  # surfaced for readiness
        self.assertEqual(len(self.remote), 1)                     # nothing was confirmed
        self.assertEqual(self.journal()[0], 1)                    # tracking kept until cleanup

    def test_restart_resumes_interrupted_cleanup(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1))
        adapter.sync()
        self.server.fail_bulk_clear = True
        self.assertFalse(adapter.clear_bank()["cleared"])
        self.server.fail_bulk_clear = False
        restarted = Hindsight(self.store, self.cfg)  # fresh process: same durable state
        restarted.sync()
        self.assertTrue(self.wait_until(lambda: not self.remote))
        self.assertEqual(self.journal(), (0, 0))
        self.assertFalse(restarted.status()["pending_bank_clear"])

    def test_obligation_blocks_new_retains_until_cleanup_completes(self):
        adapter = Hindsight(self.store, self.cfg)
        first = self.put(record(1))
        adapter.sync()
        self.server.fail_bulk_clear = True
        self.assertFalse(adapter.clear_bank()["cleared"])  # obligation now outstanding
        second = self.put(record(2))
        adapter.sync()  # the failing cleanup has priority; nothing may be retained
        self.assertEqual(len(self.remote), 1)              # record 2 was not indexed around it
        self.server.fail_bulk_clear = False
        adapter.sync()                                     # cleanup completes, bank is empty
        self.assertTrue(self.wait_until(lambda: not self.remote))
        adapter.sync()                                     # live evidence is re-indexed afterwards
        self.assertTrue(self.wait_until(lambda: set(self.remote) == {first[0], second[0]}))

    def test_readiness_surfaces_unfinished_external_cleanup(self):
        service = MemoryService(Path(self.tmp.name) / "svc", "a" * 40,
                                retrieval_config={"semantic": {"enabled": False},
                                                  "rerank": {"enabled": False},
                                                  "hindsight": {"enabled": True, "url": self.cfg["url"],
                                                                "bank_id": "fixture", "sources": ["*"]}},
                                source_config={"enabled": False})
        self.addCleanup(service.close)
        admin = service.authenticate("Bearer " + "a" * 40)
        service.dispatch("/v1/ingest", {"items": [contract_item(1)]}, admin)
        self.assertTrue(self.wait_until(lambda: len(self.remote) == 1))  # worker retains it
        self.server.fail_bulk_clear = True
        result = service.dispatch("/v1/reset", {"scope": "canonical"}, admin)
        self.assertFalse(result["external_engine"]["cleared"])
        ready = service.ready()
        self.assertFalse(ready["ready"])
        self.assertTrue(any("external cleanup" in problem for problem in ready["problems"]),
                        ready["problems"])
        self.server.fail_bulk_clear = False
        self.assertTrue(self.wait_until(lambda: not self.remote))       # worker finishes the job
        self.assertTrue(self.wait_until(lambda: service.ready()["ready"]))


class RegressionGuardTests(ControllableBank):
    def test_new_post_reset_evidence_is_eventually_indexed(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1))
        adapter.sync()
        backend = self.backend(adapter)
        reset.reset(self.store, "canonical", backend=backend)
        new_ids = self.put(record(2, text="new fact"))
        adapter.sync()
        retained = set()
        with self.store.connect() as db:
            for rid in new_ids:
                if rid in self.remote: retained.add(rid)
        self.assertEqual(retained, set(new_ids))

    def test_document_deletion_and_batch_retry_remain_idempotent(self):
        adapter = Hindsight(self.store, self.cfg)
        ids = self.put(record(1), record(2))
        adapter.sync()
        self.assertEqual(len(self.remote), 2)
        self.store.forget(ids[0])
        self.assertEqual(adapter.sync(), 1)          # the pending deletion is processed
        self.assertNotIn(ids[0], self.remote)
        adapter.sync(); adapter.sync()               # repeated passes are no-ops
        self.assertEqual(list(self.remote), [ids[1]])
        self.assertIsNone(adapter.last_error)
        # A whole-batch retain failure backs off, then the retry publishes exactly once.
        self.put(record(3))
        self.server.fail_retain = True
        adapter.sync()
        self.assertIsNotNone(adapter.last_error)
        self.server.fail_retain = False
        with self.store.connect() as db:
            db.execute("UPDATE hindsight_pending SET next_retry=0")
        adapter.sync()
        self.assertEqual(len(self.remote), 2)        # ids[1] + ids[2], no duplicates
        self.assertIsNone(adapter.last_error)


if __name__ == "__main__":
    unittest.main()
