import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path

from personal_memory.client import Client
from personal_memory.outbox import Outbox
from personal_memory.server import create_server
from personal_memory.setup import install, rollback
from personal_memory.store import Store
from personal_memory.ingestion import adapt_existing


def record(source_id="a", text="Unknown account discussed PostgreSQL", **kw):
    return {"source": "fixture-whatsapp", "source_id": source_id, "text": text,
            "occurred_at": "2024-03-01T09:00:00+00:00", **kw}


def wire_record():
    return adapt_existing(record(),connector_id="tests.fixture",connector_version="1.0",
                          source_locator="fixture://a",observed_at="2024-03-01T09:00:00Z")


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")

    def ingest(self, item=None):
        return self.store.ingest([item or record()])["records"][0]["id"]

    def test_import_idempotent_and_revision_conflicts(self):
        rid = self.ingest()
        self.assertTrue(self.store.ingest([record()])["records"][0]["duplicate"])
        with self.assertRaises(ValueError): self.ingest(record(text="Changed content"))
        self.assertNotEqual(rid, self.ingest(record(text="Changed content", revision="2")))

    def test_batch_rolls_back_on_invalid_record(self):
        with self.assertRaises(ValueError):
            self.store.ingest([record(), record("b", occurred_at="2024-01-01")])
        self.assertEqual(self.store.status()["records"], 0)

    def test_unknown_account_retains_history_after_label_update(self):
        rid = self.ingest()
        entity = self.store.entity("account", "unknown-123", record_id=rid, relation="author")
        eid = entity["id"]
        self.store.entity("account", "Amit's WhatsApp account", False, eid)
        self.assertEqual(self.store.timeline(eid)["episodes"][0]["id"], rid)
        self.assertEqual(self.store.entities("Amit")["entities"][0]["id"], eid)

    def test_equal_names_do_not_merge(self):
        first = self.store.entity("person", "Alex")
        second = self.store.entity("person", "Alex")
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(len(self.store.entities("Alex")["entities"]), 2)

    def test_claim_requires_evidence(self):
        with self.assertRaises(ValueError): self.store.claim("Amit works at Acme", "missing")

    def test_correction_preserves_history_and_is_idempotent(self):
        rid = self.ingest()
        eid = self.store.entity("person", "Amit")["id"]
        old = self.store.claim("Amit uses PostgreSQL", rid, subject_id=eid, predicate="database")
        rid2 = self.ingest(record("b", "Amit now uses SQLite"))
        args = dict(subject_id=eid, predicate="database", supersedes=old["id"])
        updated = self.store.claim("Amit uses SQLite", rid2, **args)
        self.assertEqual(updated["id"], self.store.claim("Amit uses SQLite", rid2, **args)["id"])
        self.assertEqual(self.store.search("PostgreSQL")["claims"], [])
        self.assertEqual(self.store.search("PostgreSQL", include_history=True)["claims"][0]["status"], "superseded")
        self.assertEqual(len(self.store.timeline(eid)["claims"]), 2)

    def test_correction_cannot_change_subject(self):
        rid = self.ingest()
        a = self.store.entity("person", "A")["id"]
        b = self.store.entity("person", "B")["id"]
        c = self.store.claim("A uses PostgreSQL", rid, subject_id=a)
        with self.assertRaises(ValueError): self.store.claim("B uses PostgreSQL", rid, subject_id=b, supersedes=c["id"])

    def test_source_entity_and_time_filters(self):
        rid = self.ingest()
        eid = self.store.entity("account", "unknown", record_id=rid)["id"]
        self.ingest(record("b", source="email", occurred_at="2025-01-01T00:00:00Z"))
        self.assertEqual(len(self.store.search("PostgreSQL", entity_id=eid)["episodes"]), 1)
        self.assertEqual(len(self.store.search("PostgreSQL", after="2025-01-01T00:00:00Z")["episodes"]), 1)
        self.assertEqual(len(self.store.search("PostgreSQL", source="email")["episodes"]), 1)

    def test_literal_fts_and_unicode(self):
        self.ingest(record(text="मुझे PostgreSQL chahiye"))
        self.assertTrue(self.store.search('PostgreSQL OR " --')["episodes"])
        self.assertTrue(self.store.search("chahiye")["episodes"])
        self.assertEqual(self.store.search("absentkeyword")["episodes"], [])

    def test_forget_retracts_claims_and_blocks_reimport(self):
        rid = self.ingest()
        self.store.claim("PostgreSQL is preferred", rid)
        self.store.forget(rid)
        self.assertEqual(self.store.search("PostgreSQL")["episodes"], [])
        self.assertEqual(self.store.search("PostgreSQL")["claims"], [])
        with self.assertRaises(ValueError): self.store.evidence(rid)
        with self.assertRaises(ValueError): self.ingest()

    def test_reopen_and_coverage_remain_explicit(self):
        self.ingest()
        reopened = Store(self.store.path)
        self.assertEqual(reopened.status()["records"], 1)
        self.assertEqual(reopened.status()["sources"][0]["state"], "unknown")
        with self.assertRaises(ValueError): reopened.coverage("fixture-whatsapp", "complete")
        reopened.coverage("fixture-whatsapp", "partial", note="Only one group exported")
        self.assertEqual(reopened.status()["sources"][0]["state"], "partial")


class HTTPFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.token = "t" * 40
        self.server = create_server(Path(self.tmp.name) / "data", self.token, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.client = Client(f"http://127.0.0.1:{self.server.server_port}", self.token)


class ServerTests(HTTPFixture):
    def test_authentication_and_http_roundtrip(self):
        bad = Client(self.client.url, "wrong")
        with self.assertRaises(RuntimeError): bad.call("/v1/status")
        rid = self.client.call("/v1/ingest", {"items": [wire_record()]})["records"][0]["id"]
        self.assertEqual(self.client.call("/v1/evidence", {"record_id": rid})["text"], record()["text"])
        self.assertEqual(self.client.call("/v1/status")["records"], 1)

    def test_invalid_input_does_not_write(self):
        with self.assertRaises(RuntimeError): self.client.call("/v1/ingest", {"items": []})
        self.assertEqual(self.client.call("/v1/status")["records"], 0)

    def test_restart_outbox_delivers_and_deduplicates(self):
        class Offline:
            def call(self, *args): raise ConnectionError("offline")
        path = Path(self.tmp.name) / "outbox.db"
        box = Outbox(path, Offline())
        box.enqueue([record()]); box.flush()
        self.assertEqual(box.pending(), 1)
        box.close()
        box = Outbox(path, self.client)
        self.addCleanup(box.close)
        box.flush()
        self.assertEqual(box.pending(), 0)
        box.enqueue([record(occurred_at="2026-01-01T00:00:00Z")])
        self.assertEqual(box.pending(), 0)
        self.assertEqual(self.client.call("/v1/status")["records"], 1)

    def test_non_loopback_http_rejected(self):
        with self.assertRaises(ValueError): Client("http://example.com", self.token)
        with self.assertRaises(ValueError): create_server(Path(self.tmp.name), self.token, host="0.0.0.0")


class SetupTests(unittest.TestCase):
    def test_switch_and_rollback_preserve_other_settings(self):
        import yaml
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            (home / "config.yaml").write_text("model: example-model\nmemory:\n  provider: hindsight\n  memory_char_limit: 3000\n")
            install(home, exclusive=True)
            cfg = yaml.safe_load((home / "config.yaml").read_text())
            self.assertEqual(cfg["model"], "example-model")
            self.assertEqual(cfg["memory"]["provider"], "personal-memory")
            self.assertEqual(cfg["memory"]["store"], "provider")
            self.assertTrue(cfg["memory"]["memory_enabled"])
            self.assertTrue(cfg["memory"]["user_profile_enabled"])
            self.assertTrue((home / "plugins/personal-memory/personal_memory/provider.py").is_file())
            rollback(home)
            cfg = yaml.safe_load((home / "config.yaml").read_text())
            self.assertEqual(cfg["memory"], {"provider": "hindsight", "memory_char_limit": 3000})

    def test_rollback_refuses_to_clobber_later_memory_changes(self):
        import yaml
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp); install(home)
            cfg = yaml.safe_load((home / "config.yaml").read_text())
            cfg["memory"]["provider"] = "mem0"
            (home / "config.yaml").write_text(yaml.safe_dump(cfg))
            with self.assertRaises(ValueError): rollback(home)


@unittest.skipUnless(os.environ.get("HERMES_PROVIDER_CONTRACT"), "Set HERMES_PROVIDER_CONTRACT to upstream agent/memory_provider.py")
class UpstreamContractTests(HTTPFixture):
    def provider(self):
        module_name = "agent.memory_provider"
        sys.modules.setdefault("agent", types.ModuleType("agent"))
        spec = importlib.util.spec_from_file_location(module_name, os.environ["HERMES_PROVIDER_CONTRACT"])
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        from personal_memory.provider import PersonalMemoryProvider
        home = Path(self.tmp.name) / "hermes"
        install(home)
        path = home / "personal-memory/settings.json"
        cfg = json.loads(path.read_text()); cfg.update(url=self.client.url, token=self.token,agent_token=self.token)
        path.write_text(json.dumps(cfg))
        public=home/"personal-memory/config.json"
        values=json.loads(public.read_text());values.pop("port",None);public.write_text(json.dumps(values))
        provider = PersonalMemoryProvider()
        provider.initialize("session-a", hermes_home=str(home), platform="cli", user_id="owner")
        self.addCleanup(provider.shutdown)
        return provider

    def test_provider_tools_capture_and_session_switch(self):
        provider = self.provider()
        self.assertIn("personal_memory_search", provider.system_prompt_block())
        self.assertEqual(len(provider.get_tool_schemas()), 20)
        provider.sync_turn("I use PostgreSQL", "Understood", messages=[{"role": "user", "content": "I use PostgreSQL"}])
        provider.outbox.flush()
        data = json.loads(provider.handle_tool_call("personal_memory_search", {"query": "PostgreSQL"}))
        self.assertTrue(data["episodes"])
        provider.on_session_switch("session-b")
        provider.sync_turn("I use SQLite", "Understood")
        provider.outbox.flush()
        row = self.client.call("/v1/search", {"query": "SQLite"})["episodes"][0]
        self.assertTrue(row["source_id"].startswith("session-b/"))

    def test_checkpoint_and_non_primary_write_guard(self):
        provider = self.provider()
        result = provider.on_pre_compress([{"role": "user", "content": "Remember the deployment"}], require_checkpoint=True)
        self.assertIn("committed locally", result)
        provider.outbox.flush()
        self.assertTrue(self.client.call("/v1/search", {"query": "deployment"})["episodes"])
        provider.agent_context = "cron"
        result = json.loads(provider.handle_tool_call("personal_memory_capture", record()))
        self.assertIn("error", result)

    def test_external_forget_invalidates_cached_recall(self):
        rid=self.client.call("/v1/ingest", {"items":[wire_record()]})["records"][0]["id"]
        provider=self.provider();provider.prefetch("PostgreSQL")
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            result=provider.prefetch("PostgreSQL")
            if "Untrusted personal" in result:break
            time.sleep(.02)
        self.assertIn("PostgreSQL",result)
        self.client.call("/v1/forget",{"record_id":rid})
        self.assertNotIn("PostgreSQL",provider.prefetch("PostgreSQL"))

    def test_prefetch_eventually_returns_evidence_without_network_block(self):
        self.client.call("/v1/ingest", {"items": [wire_record()]})
        provider = self.provider()
        start = time.monotonic()
        provider.prefetch("PostgreSQL")
        self.assertLess(time.monotonic() - start, .3)
        deadline = time.monotonic() + 3
        result = ""
        while time.monotonic() < deadline:
            result = provider.prefetch("PostgreSQL")
            if "Untrusted personal" in result: break
            time.sleep(.02)
        self.assertIn("PostgreSQL", result)


if __name__ == "__main__":
    unittest.main()

class UpgradeIntegrationTests(unittest.TestCase):
    def test_setup_updates_manifest_and_module_without_rotating_credentials(self):
        import yaml
        from personal_memory import __version__
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);install(home)
            settings=home/'personal-memory/settings.json';before=json.loads(settings.read_text())
            manifest=home/'plugins/personal-memory/plugin.yaml';old=yaml.safe_load(manifest.read_text());old['version']='0.0.0';manifest.write_text(yaml.safe_dump(old))
            copied=home/'plugins/personal-memory/personal_memory/investigate.py';copied.unlink()
            result=install(home)
            self.assertEqual(json.loads(settings.read_text()),before)
            self.assertEqual(yaml.safe_load(manifest.read_text())['version'],__version__)
            self.assertTrue(copied.is_file());self.assertEqual(result['required_restarts'],['memory service','Hermes'])
    def test_doctor_detects_stale_copied_provider(self):
        import yaml
        from personal_memory.operations import doctor
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);install(home)
            cfg=json.loads((home/'personal-memory/settings.json').read_text())
            Store(Path(cfg['data_dir'])/'memory.db')
            manifest=home/'plugins/personal-memory/plugin.yaml';value=yaml.safe_load(manifest.read_text());value['version']='0.0.0';manifest.write_text(yaml.safe_dump(value))
            result=doctor(home,offline=True)
            self.assertFalse(next(c for c in result['checks'] if c['check']=='installed_provider_version')['passed'])
