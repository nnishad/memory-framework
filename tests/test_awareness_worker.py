"""The background seam must not initialize Hermes on idle, and must fence failures."""
import sys
import types
from unittest.mock import patch

from personal_memory import awareness
from personal_memory.awareness_worker import process_once, hermes_analyze, deliver_once
from tests.test_awareness import AwarenessFixture


class WorkerTests(AwarenessFixture):
    def test_hermes_adapter_uses_existing_cron_agent_lifecycle(self):
        observed = []
        scheduler = types.ModuleType("cron.scheduler")
        scheduler.run_job = lambda job: (observed.append(job) or True, "",
                                         '{"summary":"reviewed","citations":[],"proposals":[]}', None)
        with patch.dict(sys.modules, {"cron": types.ModuleType("cron"),
                                      "cron.scheduler": scheduler}):
            answer = hermes_analyze({"batch_id": "abatch_one", "events": []})
        self.assertEqual(answer["summary"], "reviewed")
        self.assertTrue(observed[0]["_memory_awareness_read_only"])
        self.assertEqual(observed[0]["deliver"], "local")

    def test_hermes_adapter_binds_an_explicit_profile_for_the_run(self):
        observed = []
        scheduler = types.ModuleType("cron.scheduler")
        scheduler.run_job = lambda job: (True, "", '{"summary":"reviewed","citations":[],"proposals":[]}', None)
        constants = types.ModuleType("hermes_constants")
        constants.set_hermes_home_override = lambda home: observed.append(("set", home)) or "token"
        constants.reset_hermes_home_override = lambda token: observed.append(("reset", token))
        with patch.dict(sys.modules, {"cron": types.ModuleType("cron"),
                                      "cron.scheduler": scheduler,
                                      "hermes_constants": constants}):
            hermes_analyze({"batch_id": "abatch_one", "events": []}, hermes_home="/profiles/personal")
        self.assertEqual(observed, [("set", "/profiles/personal"), ("reset", "token")])

    def test_idle_does_not_call_model(self):
        self.consumer("bg", "background")
        called = []
        self.assertEqual(process_once(self.store, consumer_id="bg",
                                      analyze=lambda packet: called.append(packet),
                                      retrieval=lambda: called.append("index initialized")),
                         {"state": "idle"})
        self.assertEqual(called, [])

    def test_sweeps_analyzes_and_commits_visible_references(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        seen = []

        def analyze(packet):
            seen.append(packet)
            return {"summary": "One new mail", "citations": packet["events"][0]["record_ids"],
                    "proposals": []}

        self.assertEqual(process_once(self.store, consumer_id="bg", analyze=analyze)["state"],
                         "complete")
        self.assertEqual(len(seen), 1)
        self.assertEqual(awareness.pending(self.store, "bg")["pending"], False)

    def test_notification_proposal_queues_then_confirms_only_after_dispatch_receipt(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        awareness.configure_consumer(
            self.store, id="bg", profile="owner", purpose="background",
            delivery={"enabled": True, "destination": "telegram:owner"})
        outcome = process_once(
            self.store, consumer_id="bg",
            analyze=lambda packet: {"summary": "A new important message.",
                                    "citations": packet["events"][0]["record_ids"],
                                    "proposals": [{"kind": "notification"}]})
        self.assertEqual(outcome["delivery"]["state"], "queued")
        calls = []
        sent = deliver_once(self.store, dispatch=lambda intent, content:
                            calls.append((intent, content)) or "hermes:receipt-1")
        self.assertEqual(sent["state"], "confirmed")
        self.assertEqual(len(calls), 1)
        self.assertIn("A new important message.", calls[0][1])
        self.assertEqual(deliver_once(self.store, dispatch=lambda *_: self.fail("must not resend")),
                         {"state": "idle"})

    def test_failed_dispatch_stays_attempted_for_uncertainty_reconciliation(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        awareness.configure_consumer(
            self.store, id="bg", profile="owner", purpose="background",
            delivery={"enabled": True, "destination": "telegram:owner"})
        process_once(self.store, consumer_id="bg",
                     analyze=lambda packet: {"summary": "A new important message.",
                                             "citations": packet["events"][0]["record_ids"],
                                             "proposals": [{"kind": "notification"}]})
        result = deliver_once(self.store, dispatch=lambda *_: (_ for _ in ()).throw(RuntimeError("transport lost")))
        self.assertEqual(result["state"], "attempted")
        self.assertIn("transport lost", result["error"])
        self.assertEqual(awareness.reconcile_deliveries(self.store, stale_seconds=0)["uncertain"], 1)

    def test_bad_agent_result_is_retryable_without_false_completion(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        result = process_once(self.store, consumer_id="bg",
                              analyze=lambda packet: {"summary": "unsupported",
                                                      "citations": ["rec_outside"]})
        self.assertEqual(result["state"], "retry_wait")
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM awareness_results").fetchone()[0], 0)
