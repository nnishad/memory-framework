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

    def test_compact_evidence_returns_one_text_copy_and_optional_span(self):
        rid=self.ingest(record(text="alpha beta gamma"))
        full=self.store.evidence(rid)
        self.assertIn("metadata",full)
        compact=self.store.evidence(rid,compact=True,start=6,end=10)
        self.assertEqual(compact["text"],"beta")
        self.assertTrue(compact["truncated"])
        self.assertNotIn("ingestion_record",compact)

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
        # The first search with two or more candidates loads the cross-encoder inside the request, which
        # takes seconds on a cold model cache. That is latency, not behaviour, so the client waits instead
        # of failing an assertion; every claim under test is still checked exactly as written.
        self.client = Client(f"http://127.0.0.1:{self.server.server_port}", self.token, timeout=60)


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

    def test_contract_rejection_is_self_correcting_over_http(self):
        from personal_memory.client import ServiceError
        # An empty hub read used to be a bare 400 the model could not act on; it is now a 422 whose
        # detail names the exact contract, so a small model can correct the call instead of retrying.
        with self.assertRaises(ServiceError) as caught:
            self.client.call("/v1/intelligence/read", {})
        self.assertEqual(caught.exception.status, 422)
        self.assertEqual(caught.exception.path, "$")
        self.assertIn("operation and arguments", str(caught.exception))
        # A read that supplies only the operation succeeds without an explicit empty arguments object.
        self.assertIn("learning_states", self.client.call("/v1/intelligence/read", {"operation": "quality"}))

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

    def test_message_capture_host_ids_are_durable_across_reopen(self):
        class Offline:
            def call(self, *args): raise ConnectionError("offline")
        path = Path(self.tmp.name) / "capture.db"
        box = Outbox(path, Offline())
        box.mark_message_capture("s1", "user", "digestA", "id:m1")
        box.mark_message_capture("s1", "user", "digestB")  # no host id => occurrence-counter fallback
        box.close()
        box = Outbox(path, Offline()); self.addCleanup(box.close)
        self.assertEqual(box.captured_host_ids("s1"), {"id:m1"})  # the host-id ledger survived reopen
        self.assertEqual(box.captured_count("s1", "user", "digestA"), 1)
        self.assertEqual(box.captured_count("s1", "user", "digestB"), 1)

    def test_preexisting_outbox_db_gains_host_id_ledger(self):
        import sqlite3
        class Offline:
            def call(self, *args): raise ConnectionError("offline")
        path = Path(self.tmp.name) / "legacy.db"
        legacy = sqlite3.connect(path)  # a DB created before the host-id ledger existed
        legacy.execute("CREATE TABLE pending(id TEXT PRIMARY KEY,payload TEXT,attempts INTEGER DEFAULT 0,last_error TEXT,created_at TEXT)")
        legacy.execute("CREATE TABLE message_captures(session_id TEXT,role TEXT,content_digest TEXT,occurrences INTEGER,PRIMARY KEY(session_id,role,content_digest))")
        legacy.commit(); legacy.close()
        box = Outbox(path, Offline()); self.addCleanup(box.close)  # opening migrates it additively
        with box.connect() as db:
            self.assertTrue(db.execute("SELECT 1 FROM sqlite_master WHERE name='message_capture_ids'").fetchone())
        box.mark_message_capture("s1", "user", "d", "id:9")
        self.assertEqual(box.captured_host_ids("s1"), {"id:9"})

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

    def test_turn_capture_is_not_duplicated_and_current_input_is_not_recalled(self):
        provider=self.provider()
        provider.on_turn_start(1,"current-query-canary")
        provider.sync_turn("current-query-canary","unrelated-response-token",
                           messages=[{"role":"user","content":"current-query-canary"},
                                     {"role":"assistant","content":"unrelated-response-token"}])
        provider.on_pre_compress([{"role":"user","content":"current-query-canary"},
                                  {"role":"assistant","content":"unrelated-response-token"}],require_checkpoint=True)
        provider.on_session_end([{"role":"user","content":"current-query-canary"},
                                 {"role":"assistant","content":"unrelated-response-token"}])
        provider.outbox.flush()
        self.assertEqual(self.client.call('/v1/status')["records"],2)
        result=json.loads(provider.handle_tool_call('personal_memory_search',{'query':'current-query-canary'}))
        self.assertEqual(result['episodes'],[])

    def test_started_input_is_not_duplicated_when_sync_has_no_transcript(self):
        provider=self.provider()
        provider.on_turn_start(1,"same-input")
        provider.sync_turn("same-input","same-answer")
        provider.on_turn_start(2,"same-input")
        provider.sync_turn("same-input","same-answer")
        provider.outbox.flush()
        self.assertEqual(self.client.call('/v1/status')["records"],4)

    def test_prefetch_does_not_reinject_evidence_already_in_session(self):
        self.client.call('/v1/ingest',{'items':[wire_record()]})
        provider=self.provider()
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+3
        while time.monotonic()<deadline and "Untrusted personal" not in result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertIn("PostgreSQL",result)
        provider._invalidate()
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+3
        while time.monotonic()<deadline and result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertEqual(result,"")

    def test_pre_compress_rehydrates_previously_suppressed_evidence(self):
        self.client.call('/v1/ingest',{'items':[wire_record()]})
        provider=self.provider()
        # Generous recall budget: this asserts evidence rehydrates, not that it does so within a
        # fixed window; local-model inference (model-on pass) makes each background search slower.
        wait=10
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+wait
        while time.monotonic()<deadline and "Untrusted personal" not in result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertIn("PostgreSQL",result)  # first recall injects the evidence
        provider._invalidate()
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+wait
        while time.monotonic()<deadline and result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertEqual(result,"")  # still in context => suppressed, no empty envelope appended
        # Compression evicts the injected evidence from the live transcript; the epoch bump forces
        # the next automatic recall to rehydrate it instead of suppressing it forever.
        provider.on_pre_compress([{"role":"user","content":"recall PostgreSQL"}],require_checkpoint=True)
        provider._invalidate()
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+wait
        while time.monotonic()<deadline and "Untrusted personal" not in result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertIn("PostgreSQL",result)

    def test_claim_fingerprint_change_reinjects_without_compression(self):
        provider=self.provider();sid=provider.session_id
        from personal_memory.provider import _claim_fingerprint
        claim={"id":7,"record_id":"r1","text":"Amit uses PostgreSQL","status":"active",
               "valid_from":None,"valid_to":None}
        provider.mark_injected({"episodes":[],"claims":[claim]},sid)
        retained=provider._retained_rows(sid)
        # Unchanged claim in the same epoch is suppressed (not re-injected).
        self.assertTrue(provider._row_retained(retained,"c:7",_claim_fingerprint(claim)))
        # A correction changes status/validity => a new fingerprint => re-injected without compression.
        corrected=dict(claim,status="superseded",valid_to="2024-06-01T00:00:00Z")
        self.assertFalse(provider._row_retained(retained,"c:7",_claim_fingerprint(corrected)))
        # Compression bumps the epoch: even the identical claim rehydrates.
        provider._bump_injection_epoch(sid)
        self.assertFalse(provider._row_retained(provider._retained_rows(sid),"c:7",_claim_fingerprint(claim)))

    def test_explicit_default_search_does_not_reuse_the_narrow_prefetch(self):
        self.client.call('/v1/ingest',{'items':[wire_record()]})
        provider=self.provider();calls=0;real=provider.client.call
        def counted(path,*args,**kwargs):
            nonlocal calls
            if path=='/v1/search':calls+=1
            return real(path,*args,**kwargs)
        provider.client.call=counted
        provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            with provider.lock:
                if ('session-a','PostgreSQL') not in provider.inflight:break
            time.sleep(.02)
        # A bare search resolves to balanced/8, which the fast/4 prefetch cannot cover, so it must
        # run a real retrieval instead of silently downgrading to the cached narrow one.
        result=json.loads(provider.handle_tool_call('personal_memory_search',{'query':'PostgreSQL'}))
        self.assertTrue(result['episodes'])
        self.assertNotIn('reused_automatic_prefetch',result['diagnostics'])
        self.assertEqual(result['diagnostics']['depth'],'balanced')
        self.assertEqual(calls,2)  # the prefetch search plus a real balanced/8 search

    def test_explicit_fast_search_reuses_completed_automatic_prefetch(self):
        self.client.call('/v1/ingest',{'items':[wire_record()]})
        provider=self.provider();calls=0;real=provider.client.call
        def counted(path,*args,**kwargs):
            nonlocal calls
            if path=='/v1/search':calls+=1
            return real(path,*args,**kwargs)
        provider.client.call=counted
        provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+3
        while time.monotonic()<deadline:
            with provider.lock:
                if ('session-a','PostgreSQL') not in provider.inflight:break
            time.sleep(.02)
        # An explicit search that asks for no more than the prefetch capability (fast/4) reuses it.
        result=json.loads(provider.handle_tool_call('personal_memory_search',
                                                    {'query':'PostgreSQL','depth':'fast','limit':4}))
        self.assertTrue(result['episodes'])
        self.assertTrue(result['diagnostics']['reused_automatic_prefetch'])
        self.assertEqual(calls,1)

    def test_host_message_ids_dedup_across_lifecycle_hooks(self):
        provider=self.provider()
        msgs=[{"role":"user","content":"canary","id":"u1"},
              {"role":"assistant","content":"reply","id":"a1"}]
        provider.sync_turn("canary","reply",messages=msgs)
        provider.on_pre_compress(msgs,require_checkpoint=True)
        provider.on_session_end(msgs)
        provider.outbox.flush()
        # Three hooks see the same two host messages; stable ids keep each captured exactly once.
        self.assertEqual(self.client.call('/v1/status')["records"],2)
        self.assertEqual(provider.outbox.captured_host_ids("session-a"),{"id:u1","id:a1"})

    def test_host_message_id_dedups_when_occurrence_counter_would_not(self):
        provider=self.provider()
        # Two distinct user messages share identical content; sync_turn captures the turn once and
        # records only the last occurrence's host id, leaving occurrences(1) below the count(2).
        provider.sync_turn("ping","",messages=[{"role":"user","content":"ping","id":"p1"},
                                               {"role":"user","content":"ping","id":"p2"}])
        provider.outbox.flush()
        # A later turn re-delivers "ping" (still two occurrences, so the counter alone would capture
        # again) but the last occurrence carries the already-captured host id => recognised as a dup.
        provider.sync_turn("ping","ack",messages=[{"role":"user","content":"ping","id":"p1"},
                                                  {"role":"user","content":"ping","id":"p2"},
                                                  {"role":"assistant","content":"ack","id":"a1"}])
        provider.outbox.flush()
        ping=self.client.call('/v1/search',{"query":"ping"})["episodes"]
        self.assertEqual(len(ping),1)  # "ping" captured exactly once despite the counter under-count
        self.assertIn("id:p2",provider.outbox.captured_host_ids("session-a"))

    def test_host_message_id_helpers_prefer_stable_id_and_fall_back(self):
        provider=self.provider()
        self.assertEqual(provider._host_message_id({"id":"m1","role":"user"}),"id:m1")
        self.assertEqual(provider._host_message_id({"platform_message_id":7}),"platform_message_id:7")
        self.assertEqual(provider._host_message_id({"message_id":"x"}),"message_id:x")
        self.assertIsNone(provider._host_message_id({"role":"user","content":"no id"}))
        self.assertIsNone(provider._host_message_id("not a dict"))
        rows=[{"role":"user","content":"x","id":"1"},{"role":"user","content":"x","id":"2"}]
        self.assertEqual(provider._last_message(rows,"user","x")["id"],"2")  # the current occurrence
        self.assertIsNone(provider._last_message(rows,"assistant","x"))

    def test_tool_result_attributes_lineage_without_suppressing_recall(self):
        self.client.call('/v1/ingest',{'items':[wire_record()]})
        provider=self.provider()
        # A model-requested search result is provenance, not prompt injection.
        search=json.loads(provider.handle_tool_call('personal_memory_search',{'query':'PostgreSQL'}))
        self.assertTrue(search['episodes'])
        self.assertTrue(provider.lineage.exposed('session-a'))  # lineage attributed
        self.assertEqual(provider.exposure.get('session-a',{}).get('rows',{}),{})  # nothing marked injected
        # The next automatic recall still injects the evidence (it was never suppressed).
        result=provider.prefetch('PostgreSQL')
        deadline=time.monotonic()+3
        while time.monotonic()<deadline and "Untrusted personal" not in result:
            time.sleep(.02);result=provider.prefetch('PostgreSQL')
        self.assertIn("PostgreSQL",result)

    def test_session_switch_is_scoped_to_the_switching_sessions(self):
        provider=self.provider()  # session-a
        # Seed state for an unrelated live session that is not part of either switch.
        with provider.lock:
            provider.current_input_record_ids['session-x']=['rec_x']
            provider.started_inputs[('session-x','dx')]=1
            provider.exposure['session-x']={'epoch':0,'rows':{'e:9':{'epoch':0,'fp':'fx'}}}
        provider.on_session_switch('session-b')  # a -> b
        provider.on_session_switch('session-c')  # b -> c
        with provider.lock:
            # The unrelated session survives both switches (the old code called a global clear()).
            self.assertEqual(provider.current_input_record_ids['session-x'],['rec_x'])
            self.assertEqual(provider.started_inputs[('session-x','dx')],1)
            self.assertEqual(provider.exposure['session-x']['rows']['e:9']['fp'],'fx')
            self.assertEqual(provider.session_id,'session-c')
            self.assertNotIn('session-b',provider.current_input_record_ids)

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
