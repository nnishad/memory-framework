"""A fresh start must erase the external engine too, warm up cleanly, and stay reclaimable.

These cover the core fixes for the audit findings: a canonical reset cascades a bulk clear to
the managed Hindsight bank (so no residual memories survive), startup warm-up is safe when the
learned models are absent, stale embedded instances are prunable without touching the active one
or unrelated data, and doctor surfaces embedding-stack major drift.
"""
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from personal_memory import reset
from personal_memory.hindsight import Hindsight
from personal_memory.hindsight_runtime import default_config, prune_stale_instances, stale_instances
from personal_memory.operations import _major
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store


def record(i, text="vehicle repair", source="whatsapp", when="2024-01-01T12:00:00Z"):
    return {"source": source, "source_id": str(i), "text": text, "occurred_at": when,
            "metadata": {"participants": []}}


class BankFixture(unittest.TestCase):
    """A fake managed Hindsight bank that honours the bulk DELETE .../memories clear."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)
        self.remote, self.bulk_clears = {}, []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def _send(self, data):
                raw = json.dumps(data).encode(); self.send_response(200)
                self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(n)) if n else {}
                if self.path.endswith("/memories/recall"):
                    self._send({"results": [{"document_id": d} for d in self.server.remote]})
                else:
                    for item in data.get("items", []): self.server.remote[item["document_id"]] = item
                    self._send({"success": True, "async": False})
            def do_DELETE(self):
                if self.path.endswith("/memories"):
                    self.server.bulk_clears.append(len(self.server.remote)); self.server.remote.clear()
                    self._send({"success": True})
                else:
                    self.server.remote.pop(self.path.rsplit("/", 1)[1], None); self._send({"success": True})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.remote, server.bulk_clears = self.remote, self.bulk_clears
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        self.cfg = {"url": f"http://127.0.0.1:{server.server_port}", "bank_id": "fixture", "sources": ["*"]}

    def put(self, *records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]


class ClearBankTests(BankFixture):
    def test_clear_bank_erases_documents_and_local_journal(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1), record(2))
        self.assertEqual(adapter.sync(), 2)
        self.assertEqual(len(self.remote), 2)
        result = adapter.clear_bank()
        self.assertTrue(result["cleared"])
        self.assertEqual(self.remote, {})
        self.assertEqual(self.bulk_clears, [2])  # one bulk DELETE cleared both retained documents
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM hindsight_done").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM hindsight_pending").fetchone()[0], 0)

    def test_canonical_reset_cascades_to_the_external_bank(self):
        adapter = Hindsight(self.store, self.cfg)
        self.put(record(1), record(2))
        adapter.sync()
        self.assertEqual(len(self.remote), 2)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, hindsight=adapter, start=False)
        self.addCleanup(backend.close)
        result = reset.reset(self.store, "canonical", backend=backend)
        self.assertTrue(result["external_engine"]["cleared"])
        self.assertEqual(self.remote, {})          # residual evidence is gone from the engine
        self.assertGreaterEqual(result["epoch"], 1)
        self.assertEqual(reset.epoch(self.store), result["epoch"])

    def test_reset_without_a_backend_reports_no_external_engine(self):
        self.put(record(1))
        result = reset.reset(self.store, "canonical")
        self.assertFalse(result["external_engine"]["cleared"])
        self.assertEqual(result["external_engine"]["reason"], "no external engine bound")

    def test_clear_external_is_a_noop_without_hindsight(self):
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=False)
        self.addCleanup(backend.close)
        self.assertEqual(backend.clear_external(), {"cleared": False, "reason": "no external engine bound"})


class WarmupTests(BankFixture):
    def test_warmup_is_safe_without_learned_models(self):
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=False)
        self.addCleanup(backend.close)
        self.assertEqual(backend.warmup(), {"semantic": False, "hindsight": False, "rerank_loaded": False})

    def test_warmup_primes_the_external_engine_and_swallows_failure(self):
        adapter = Hindsight(self.store, self.cfg)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, hindsight=adapter, start=False)
        self.addCleanup(backend.close)
        self.assertTrue(backend.warmup()["hindsight"])

        class Boom:
            def candidates(self, *a, **k): raise RuntimeError("down")
        backend.hindsight = Boom()
        self.assertTrue(backend.warmup()["hindsight"])  # best-effort: never raises


class PruneTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.data_dir = root / "data"; self.data_dir.mkdir()
        self.instances = root / "pg0" / "instances"; self.instances.mkdir(parents=True)
        self.profiles = root / "hindsight" / "profiles"; self.profiles.mkdir(parents=True)
        self.active = default_config(self.data_dir)["profile"]

    def _layout(self):
        (self.instances / ("hindsight-embed-" + self.active)).mkdir()
        (self.instances / "hindsight-embed-hermes-personal-memory-deadbeefdead").mkdir()
        (self.instances / "hindsight-embed-someother-project-abc123").mkdir()  # unrelated prefix
        (self.profiles / (self.active + ".env")).write_text("x")
        (self.profiles / "hermes-personal-memory-deadbeefdead.env").write_text("x")
        (self.profiles / "hermes-personal-memory-deadbeefdead.log").write_text("x")

    def test_stale_detection_spares_active_and_unrelated(self):
        self._layout()
        with mock.patch.dict(os.environ, {"PG0_HOME": str(self.tmp.name + "/pg0")}):
            found = stale_instances(self.data_dir, home=str(self.tmp.name + "/hindsight"))
        self.assertEqual(found["active_profile"], self.active)
        self.assertEqual(found["stale_profiles"], ["hermes-personal-memory-deadbeefdead"])
        joined = " ".join(found["stale"])
        self.assertIn("hindsight-embed-hermes-personal-memory-deadbeefdead", joined)
        self.assertIn("hermes-personal-memory-deadbeefdead.env", joined)
        self.assertNotIn(self.active, found["stale_profiles"])
        self.assertNotIn("someother-project", joined)          # unrelated Hindsight data untouched
        self.assertNotIn("hindsight-embed-" + self.active, joined)

    def test_prune_is_dry_run_by_default_and_removes_on_apply(self):
        self._layout()
        env = {"PG0_HOME": str(self.tmp.name + "/pg0")}
        home = str(self.tmp.name + "/hindsight")
        stale_dir = self.instances / "hindsight-embed-hermes-personal-memory-deadbeefdead"
        with mock.patch.dict(os.environ, env):
            dry = prune_stale_instances(self.data_dir, home=home, apply=False)
            self.assertEqual(dry["removed"], []); self.assertTrue(stale_dir.exists())
            applied = prune_stale_instances(self.data_dir, home=home, apply=True)
        self.assertFalse(stale_dir.exists())
        self.assertFalse((self.profiles / "hermes-personal-memory-deadbeefdead.env").exists())
        self.assertTrue((self.instances / ("hindsight-embed-" + self.active)).exists())  # active spared
        self.assertTrue((self.instances / "hindsight-embed-someother-project-abc123").exists())
        self.assertTrue(applied["removed"]); self.assertEqual(applied["errors"], [])


class EmbeddingStackTests(unittest.TestCase):
    def test_major_strips_local_cuda_label_and_parses(self):
        self.assertEqual(_major("2.14.0+cu130"), 2)
        self.assertEqual(_major("6.0.1"), 6)
        self.assertEqual(_major("1.30.0"), 1)
        self.assertIsNone(_major("nonsense"))
        self.assertIsNone(_major(None))


if __name__ == "__main__":
    unittest.main()
