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
        sent = deliver_once(self.store, consumer_id="bg", profile="owner",
                            dispatch=lambda intent, content:
                            calls.append((intent, content)) or "hermes:receipt-1")
        self.assertEqual(sent["state"], "confirmed")
        self.assertEqual(len(calls), 1)
        self.assertIn("A new important message.", calls[0][1])
        self.assertEqual(deliver_once(self.store, consumer_id="bg", profile="owner",
                                      dispatch=lambda *_: self.fail("must not resend")),
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
        result = deliver_once(self.store, consumer_id="bg", profile="owner",
                              dispatch=lambda *_: (_ for _ in ()).throw(RuntimeError("transport lost")))
        self.assertEqual(result["state"], "attempted")
        self.assertIn("transport lost", result["error"])
        self.assertEqual(awareness.reconcile_deliveries(self.store, stale_seconds=0)["uncertain"], 1)

    def test_notification_and_analysis_commit_together(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        awareness.configure_consumer(
            self.store, id="bg", profile="owner", purpose="background",
            delivery={"enabled": True, "destination": "telegram:owner"})
        awareness.sweep(self.store, "bg")
        lease = awareness.claim(self.store, "bg", owner="worker")
        with patch.object(awareness, "_queue_delivery_db", side_effect=RuntimeError("intent write failed")):
            with self.assertRaisesRegex(RuntimeError, "intent write failed"):
                awareness.complete(self.store, lease, summary="Message changed",
                                   proposals=[{"kind": "notification"}])
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT state FROM awareness_batches WHERE id=?",
                                        (lease["batch_id"],)).fetchone()[0], "leased")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM awareness_results").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM awareness_deliveries").fetchone()[0], 0)
        completed = awareness.complete(self.store, lease, summary="Message changed",
                                       proposals=[{"kind": "notification"}])
        self.assertEqual(completed["delivery"]["state"], "queued")

    def test_delivery_worker_claims_only_its_consumer_and_profile(self):
        self.arrive("m1", 1)
        for consumer_id, profile in (("owner-bg", "owner"), ("other-bg", "other")):
            awareness.configure_consumer(
                self.store, id=consumer_id, profile=profile, purpose="background",
                delivery={"enabled": True, "destination": "telegram:" + profile})
            process_once(self.store, consumer_id=consumer_id,
                         analyze=lambda packet: {"summary": "One new message",
                                                 "citations": packet["events"][0]["record_ids"],
                                                 "proposals": [{"kind": "notification"}]})
        with self.assertRaises(ValueError):
            deliver_once(self.store, consumer_id="owner-bg", profile="other",
                         dispatch=lambda *_: self.fail("wrong profile must not send"))
        seen = []
        result = deliver_once(self.store, consumer_id="other-bg", profile="other",
                              dispatch=lambda intent, _: seen.append(intent["consumer_id"]) or "other:msg-1")
        self.assertEqual(result["state"], "confirmed")
        self.assertEqual(seen, ["other-bg"])
        with self.store.connect() as db:
            remaining = db.execute("SELECT consumer_id FROM awareness_deliveries"
                                   " WHERE state='queued'").fetchall()
        self.assertEqual([row[0] for row in remaining], ["owner-bg"])

    def test_changed_delivery_policy_cancels_and_quiet_hours_hold(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        original = {"enabled": True, "destination": "telegram:owner"}
        awareness.configure_consumer(self.store, id="bg", profile="owner",
                                     purpose="background", delivery=original)
        process_once(self.store, consumer_id="bg",
                     analyze=lambda packet: {"summary": "One new message",
                                             "citations": packet["events"][0]["record_ids"],
                                             "proposals": [{"kind": "notification"}]})
        awareness.configure_consumer(self.store, id="bg", profile="owner", purpose="background",
                                     delivery={"enabled": True, "destination": "telegram:other"})
        result = deliver_once(self.store, consumer_id="bg", profile="owner",
                              dispatch=lambda *_: self.fail("old recipient must not receive mail"))
        self.assertEqual(result["state"], "cancelled")

        self.arrive("m2", 2)
        awareness.configure_consumer(self.store, id="bg", profile="owner",
                                     purpose="background", delivery=original)
        process_once(self.store, consumer_id="bg",
                     analyze=lambda packet: {"summary": "Another message",
                                             "citations": packet["events"][0]["record_ids"],
                                             "proposals": [{"kind": "notification"}]})
        awareness.configure_consumer(
            self.store, id="bg", profile="owner", purpose="background",
            delivery={**original, "quiet_hours": {"start": "00:00", "end": "00:00"}})
        held = deliver_once(self.store, consumer_id="bg", profile="owner",
                            dispatch=lambda *_: self.fail("quiet hours must hold delivery"))
        self.assertEqual(held["state"], "quiet_hold")
        awareness.configure_consumer(self.store, id="bg", profile="owner",
                                     purpose="background", delivery=original)
        self.assertEqual(awareness.reconcile_deliveries(self.store)["released"], 1)
        sent = deliver_once(self.store, consumer_id="bg", profile="owner",
                            dispatch=lambda *_: "telegram:owner:msg-2")
        self.assertEqual(sent["state"], "confirmed")

        self.arrive("m3", 3)
        process_once(self.store, consumer_id="bg",
                     analyze=lambda packet: {"summary": "Third message",
                                             "citations": packet["events"][0]["record_ids"],
                                             "proposals": [{"kind": "notification"}]})
        awareness.configure_consumer(self.store, id="bg", profile="owner", purpose="background",
                                     delivery={"enabled": False, "destination": "telegram:owner"})
        disabled = deliver_once(self.store, consumer_id="bg", profile="owner",
                                dispatch=lambda *_: self.fail("disabled policy must cancel"))
        self.assertEqual(disabled["state"], "cancelled")

    def test_bad_agent_result_is_retryable_without_false_completion(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        result = process_once(self.store, consumer_id="bg",
                              analyze=lambda packet: {"summary": "unsupported",
                                                      "citations": ["rec_outside"]})
        self.assertEqual(result["state"], "retry_wait")
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM awareness_results").fetchone()[0], 0)

    # --- Fix 3: recovery probe recognizes reclaimable work; idle scheduling sleeps ---

    def test_probe_recovers_an_orphaned_expired_lease_on_restart(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        awareness.sweep(self.store, "bg")
        orphan = awareness.claim(self.store, "bg", owner="dead-worker", ttl=0)
        self.assertIsNotNone(orphan)
        # A fresh process's cheap probe must see the recoverable lease.
        self.assertTrue(awareness.pending(self.store, "bg")["pending"])
        outcome = process_once(self.store, consumer_id="bg",
                               analyze=lambda packet: {"summary": "caught up",
                                       "citations": packet["events"][0]["record_ids"],
                                       "proposals": []})
        self.assertEqual(outcome["state"], "complete")

    def test_probe_releases_held_work_only_once_the_source_resumes(self):
        self.arrive("m1", 1)
        self.sync.pause(self.gmail["connection_id"])
        self.consumer("bg", "background", policy={"on_source_pause": "hold"})
        awareness.sweep(self.store, "bg")
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT state FROM awareness_batches").fetchone()[0], "held")
        # Still stalled: held work is not claimable, so the loop sleeps rather than spins.
        self.assertFalse(awareness.pending(self.store, "bg")["pending"])
        self.sync.resume(self.gmail["connection_id"])
        self.assertTrue(awareness.pending(self.store, "bg")["pending"])
        outcome = process_once(self.store, consumer_id="bg",
                               analyze=lambda packet: {"summary": "resumed",
                                       "citations": packet["events"][0]["record_ids"],
                                       "proposals": []})
        self.assertEqual(outcome["state"], "complete")

    def test_stale_completion_is_rejected_after_lease_reclaim(self):
        self.arrive("m1", 1)
        self.consumer("bg", "background")
        awareness.sweep(self.store, "bg")
        first = awareness.claim(self.store, "bg", owner="w1", ttl=0)     # expires at once
        second = awareness.claim(self.store, "bg", owner="w2", ttl=300)  # reclaims, higher fence
        with self.assertRaises(ValueError):
            awareness.complete(self.store, first, summary="ghost")
        done = awareness.complete(self.store, second, summary="real")
        self.assertEqual(done["state"], "complete")

    def test_made_progress_governs_the_sleep_decision(self):
        from personal_memory.awareness_worker import made_progress
        self.assertFalse(made_progress({"state": "idle"}))                    # no --deliver, idle
        self.assertFalse(made_progress({"state": "idle"}, {"state": "idle"}))  # --deliver, nothing
        self.assertFalse(made_progress({"state": "retry_wait"}))              # backoff scheduled later
        self.assertFalse(made_progress({"state": "idle"}, {"state": "attempted"}))  # held for reconcile
        self.assertTrue(made_progress({"state": "complete"}))                 # analysis advanced
        self.assertTrue(made_progress({"state": "idle"}, {"state": "confirmed"}))  # delivery sent
