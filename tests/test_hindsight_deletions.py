"""Hindsight deletion obligations survive lost acknowledgments and races.

A record that may exist in the external bank stays tracked until the remote
deletion is confirmed. "Do not retain this record again" is never allowed to
erase "a remote copy may exist and must be erased": a retain whose
acknowledgment was lost still counts as a possible remote copy, so retiring
the record before or during a retry must convert the pending row into a
deletion obligation, not into an orphan. Readiness surfaces the unfinished
cleanup while it lasts.
"""
import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from personal_memory.hindsight import Hindsight
from personal_memory.store import Store


def record(i, text="vehicle repair", source="whatsapp"):
    return {"source": source, "source_id": str(i), "text": text,
            "occurred_at": "2024-01-01T12:00:00Z", "metadata": {"participants": []}}


class FakeBank:
    """Minimal controllable Hindsight bank over loopback HTTP."""
    def __init__(self, store):
        self.store = store
        self.remote = {}
        self.ack_loss = False       # store the batch, then fail the acknowledgment
        self.fail_delete = False    # reject per-document deletion
        self.retain_gate = threading.Event(); self.retain_gate.set()
        self.retain_started = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def _send(self, data, code=200):
                raw = json.dumps(data).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(raw))); self.end_headers()
                self.wfile.write(raw)
            def do_POST(self):
                bank = self.server.bank
                n = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(n)) if n else {}
                if self.path.endswith("/memories/recall"):
                    self._send({"results": [{"document_id": d} for d in bank.remote]})
                    return
                bank.retain_started.set()
                if not bank.retain_gate.wait(15):
                    self._send({"error": "gate timeout"}, 500); return
                for item in data.get("items", []):
                    bank.remote[item["document_id"]] = item
                if bank.ack_loss:
                    # The engine persisted the batch, but the response proves it.
                    self._send({"error": "acknowledgment lost"}, 500); return
                self._send({"success": True, "async": False,
                            "items_count": len(data.get("items", []))})
            def do_DELETE(self):
                bank = self.server.bank
                if self.path.rstrip("/").endswith("/memories"):
                    bank.remote.clear(); self._send({"success": True})
                elif bank.fail_delete:
                    self._send({"error": "forced deletion failure"}, 500)
                else:
                    bank.remote.pop(self.path.rsplit("/", 1)[1], None)
                    self._send({"success": True})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.bank = self
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.cfg = {"url": f"http://127.0.0.1:{self.server.server_port}",
                    "bank_id": "fixture", "sources": ["*"]}

    def close(self):
        self.server.shutdown(); self.server.server_close()


class HindsightDeletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.bank = FakeBank(self.store); self.addCleanup(self.bank.close)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]

    def journal(self, adapter):
        with self.store.connect() as db:
            done = db.execute("SELECT count(*) FROM hindsight_done WHERE backend=?",
                              (adapter.key,)).fetchone()[0]
            pending = db.execute("SELECT count(*) FROM hindsight_pending WHERE backend=?",
                                 (adapter.key,)).fetchone()[0]
        return done, pending

    def untracked_forgotten_content(self, adapter):
        """Remote documents for forgotten records with no tracking at all."""
        with self.store.connect() as db:
            tracked = {r[0] for r in db.execute(
                "SELECT record_id FROM hindsight_done WHERE backend=?"
                " UNION SELECT record_id FROM hindsight_pending WHERE backend=?",
                (adapter.key, adapter.key))}
            forgotten = {r[0] for r in db.execute("SELECT id FROM records WHERE deleted=1")}
        return {rid for rid in self.bank.remote if rid in forgotten} - tracked

    def test_lost_retain_acknowledgment_keeps_the_record_deletable(self):
        adapter = Hindsight(self.store, self.bank.cfg)
        rid = self.put(record(1))[0]
        self.bank.ack_loss = True
        adapter.sync()
        self.assertIn(rid, self.bank.remote)          # content persisted remotely
        status = adapter.status()
        self.assertEqual(status["pending_deletions"], 0)  # record is still live
        self.bank.ack_loss = False
        self.store.forget(rid)
        adapter.sync()                                 # the deletion pass erases it
        self.assertNotIn(rid, self.bank.remote)
        self.assertEqual(self.journal(adapter), (0, 0))
        self.assertEqual(self.untracked_forgotten_content(adapter), set())

    def test_record_retired_during_retry_keeps_the_deletion_obligation(self):
        adapter = Hindsight(self.store, self.bank.cfg)
        rid = self.put(record(1))[0]
        # First pass: the retain persists remotely but the acknowledgment is lost.
        self.bank.ack_loss = True
        adapter.sync()
        self.bank.ack_loss = False
        self.assertIn(rid, self.bank.remote)
        with self.store.connect() as db:
            db.execute("UPDATE hindsight_pending SET next_retry=0 WHERE backend=?", (adapter.key,))
        # Retry pass: the record is forgotten between selection and the pre-retain
        # recheck, exactly when an acknowledgment-loss orphan would be created.
        original = adapter._epoch
        state = {"calls": 0}
        def racy_epoch(db=None):
            result = original(db)
            state["calls"] += 1
            if state["calls"] == 1:
                self.store.forget(rid)
            return result
        adapter._epoch = racy_epoch
        adapter.sync()
        # Whatever that pass decided, the obligation must survive it: a later
        # bounded drain must erase the remote copy.
        self.bank.retain_gate.clear()  # no further retain may reach the bank
        for _ in range(3):
            adapter.sync()
        self.assertNotIn(rid, self.bank.remote)
        self.assertEqual(self.journal(adapter), (0, 0))
        self.assertEqual(self.untracked_forgotten_content(adapter), set())

    def test_failed_deletion_survives_restart_and_completes(self):
        adapter = Hindsight(self.store, self.bank.cfg)
        rid = self.put(record(1))[0]
        adapter.sync()
        self.store.forget(rid)
        self.bank.fail_delete = True
        self.assertEqual(adapter.sync(), 0)            # deletion failed, pass aborts
        self.assertIn(rid, self.bank.remote)
        status = adapter.status()
        self.assertEqual(status["pending_deletions"], 1)  # unfinished cleanup is surfaced
        self.assertIsNotNone(status["error"])
        # A restart keeps the obligation: same durable state, new process.
        restarted = Hindsight(self.store, self.bank.cfg)
        self.assertEqual(restarted.status()["pending_deletions"], 1)
        self.bank.fail_delete = False
        restarted.sync()
        self.assertNotIn(rid, self.bank.remote)
        self.assertEqual(self.journal(restarted), (0, 0))
        self.assertEqual(restarted.status()["pending_deletions"], 0)

    def test_zero_deletion_backlog_never_hides_forgotten_remote_content(self):
        adapter = Hindsight(self.store, self.bank.cfg)
        ids = self.put(record(1), record(2))
        self.bank.ack_loss = True
        adapter.sync()                                  # both persisted, acknowledgment lost
        self.bank.ack_loss = False
        self.store.forget(ids[0])
        for _ in range(5):                              # drain the journal to quiescence
            adapter.sync()
            with self.store.connect() as db:
                db.execute("UPDATE hindsight_pending SET next_retry=0 WHERE backend=?", (adapter.key,))
        status = adapter.status()
        if status["pending_deletions"] == 0:
            # The invariant the readiness answer depends on: no tracked deletion
            # work means no forgotten record may still exist in the remote bank.
            self.assertNotIn(ids[0], self.bank.remote)
        self.assertIn(ids[1], self.bank.remote)         # the live sibling is eventually retained
        self.assertEqual(self.untracked_forgotten_content(adapter), set())


if __name__ == "__main__":
    unittest.main()
