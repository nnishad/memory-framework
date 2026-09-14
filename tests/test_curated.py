import tempfile
import unittest
from pathlib import Path

from personal_memory import curated
from personal_memory.host_store import CanonicalMemoryStore
from personal_memory.store import Store
from personal_memory.service import AccessDenied, MemoryService


class CuratedMemoryTests(unittest.TestCase):
    def test_batch_is_atomic_and_versioned(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root) / "memory.db")
            result = curated.apply(
                store, target="memory", expected_version=0, request_id="first",
                operations=[{"action": "add", "content": "likes concise answers"}],
                evidence_ids=[], epoch=0, actor="test")
            self.assertEqual(result["version"], 1)
            with self.assertRaises(curated.VersionConflict):
                curated.apply(
                    store, target="memory", expected_version=0, request_id="stale",
                    operations=[{"action": "add", "content": "stale write"}],
                    evidence_ids=[], epoch=0, actor="test")
            self.assertEqual(
                [entry["text"] for entry in curated.read(store)["stores"]["memory"]["entries"]],
                ["likes concise answers"])

    def test_idempotency_and_atomic_reset(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(Path(root) / "memory.db")
            args = dict(target="user", expected_version=0, request_id="same",
                        operations=[{"action": "add", "content": "name is Ada"}],
                        evidence_ids=[], epoch=0, actor="test")
            self.assertEqual(curated.apply(store, **args), curated.apply(store, **args))
            result = curated.reset(
                store, scope="curated", expected_versions={"memory": 0, "user": 1},
                request_id="reset", epoch=0, actor="test")
            self.assertEqual(result["results"]["memory"]["version"], 1)
            self.assertEqual(result["results"]["user"]["version"], 2)

    def test_prompt_snapshot_does_not_change_after_live_write(self):
        memory = CanonicalMemoryStore(_Provider()).load_from_disk()
        before = memory.format_for_system_prompt("memory")
        self.assertTrue(memory.add("memory", "new preference")["success"])
        self.assertEqual(memory._entries_for("memory"), ["new preference"])
        self.assertEqual(memory.format_for_system_prompt("memory"), before)
        memory.load_from_disk()  # compression-style refresh is live-only
        self.assertEqual(memory.format_for_system_prompt("memory"), before)

    def test_service_contract_and_roles(self):
        with tempfile.TemporaryDirectory() as root:
            service = MemoryService(
                root, "a" * 40, principals=[{"token": "g" * 40, "role": "agent"},
                                             {"token": "r" * 40, "role": "reader"}])
            self.addCleanup(service.close)
            agent = service.authenticate("Bearer " + "g" * 40)
            reader = service.authenticate("Bearer " + "r" * 40)
            state = service.dispatch("/v1/curated/read", {}, reader)
            result = service.dispatch("/v1/curated/apply", {
                "target": "memory", "expected_version": 0, "request_id": "api-edit",
                "operations": [{"action": "add", "content": "prefers tea"}],
                "evidence_ids": [], "epoch": 0}, agent)
            self.assertEqual(result["version"], 1)
            with self.assertRaises(AccessDenied):
                service.dispatch("/v1/curated/reset", {
                    "scope": "memory", "expected_versions": {"memory": 1},
                    "request_id": "reader-reset", "epoch": 0}, reader)
            self.assertEqual(state["contract_version"], "1.0")


class _Client:
    def __init__(self):
        self.version = 0
        self.entries = []

    def call(self, path, payload):
        if path == "/v1/curated/read":
            names = [payload["target"]] if payload.get("target") else ["memory", "user"]
            return {"stores": {name: {"version": self.version if name == "memory" else 0,
                    "entries": list(self.entries) if name == "memory" else []} for name in names}}
        self.assert_path(path)
        self.version += 1
        self.entries.append({"id": "cur_1", "text": payload["operations"][0]["content"],
                             "evidence_ids": []})
        return {"success": True, "version": self.version, "entries": list(self.entries)}

    @staticmethod
    def assert_path(path):
        if path != "/v1/curated/apply":
            raise AssertionError(path)


class _Provider:
    def __init__(self):
        self.client = _Client()
        self.session_id = "session"
        self.memory_epoch = 0
        self.lineage = None

    def curated_write_evidence(self):
        return []

    def _invalidate(self):
        pass


if __name__ == "__main__":
    unittest.main()
