"""Delivery-layer tests: durable inbox, obligations queue, secrets, worker recovery.

Covers SYNC-01/02, SEC-02 (dedup), review fixes #1 (jobs live in the sync
obligations queue, never in learning workflows), #3 (a real second connection =
a second process for fencing purposes) and #5 (secret storage backend).
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import (connection_context, normalized_item, read_state,
                                        source_operation, source_page, stream_spec)
from personal_memory.source_secrets import SecretStore
from personal_memory.source_sync import SourceSync, SyncWorker
from tests.test_source_sync import FixtureAdapter, note_record


class DeliveryTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "memory.db"
        self.store = Store(self.path)
        self.sync = SourceSync(self.store, {"fixture.gmail": FixtureAdapter()})
        self.conn = self.sync.configure(adapter_id="fixture.gmail", source="gmail-acct1",
                                        scope={}, retention="mirror")

    def cid(self):
        return self.conn["connection_id"]


class RuntimeRegistryTests(unittest.TestCase):
    def test_discovery_is_scheduled_after_a_successful_pass(self):
        class CountingAdapter(FixtureAdapter):
            def __init__(self):
                super().__init__(); self.discoveries = 0

            def discover(self, context):
                self.discoveries += 1
                return [stream_spec("messages", modes=["backfill", "incremental"],
                                    version_order="integer")]

            def read_page(self, context, state):
                return source_page(page_id="empty", operations=[],
                    next_state=read_state(cursor={"done": True}, mode=state["mode"],
                                          state_version=state["state_version"]), complete=True)

        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        store = Store(Path(tmp.name) / "memory.db")
        adapter = CountingAdapter()
        runtime = SourceRuntime(store, Path(tmp.name), config={"enabled": False}, adapter=adapter)
        self.addCleanup(runtime.close)
        connection = runtime.sync.configure(adapter_id="google.gmail", source="notes-acct1",
                                            scope={"poll_seconds": 300}, retention="archive")
        runtime.tick()
        after_first_tick = adapter.discoveries
        runtime.tick()
        self.assertGreater(after_first_tick, 0)
        self.assertEqual(adapter.discoveries, after_first_tick)
        status = runtime.status(connection["connection_id"])["connections"][0]
        self.assertTrue(any(row["role"] == "discovery" for row in status["schedule"]))

    def test_registered_non_gmail_adapter_runs_through_service_loop(self):
        class NotesAdapter(FixtureAdapter):
            def discover(self, context):
                return [stream_spec("notes", modes=["backfill", "incremental"],
                                    version_order="integer")]

            def read_page(self, context, state):
                record = note_record("note-1", source=context["source"])
                return source_page(page_id="notes-first", operations=[source_operation(
                    "upsert", "note-1", records=[record], source_version=1)],
                    next_state=read_state(cursor={"done": True}, mode=state["mode"],
                                          state_version=state["state_version"]), complete=True)

        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        store = Store(root / "memory.db")
        runtime = SourceRuntime(store, root, config={"enabled": False},
                                adapters=[NotesAdapter()])
        self.addCleanup(runtime.close)
        connection = runtime.sync.configure(adapter_id="fixture.gmail", source="notes-acct1",
                                            scope={}, retention="archive")
        runtime.tick()
        self.assertIsNotNone(runtime.sync.head("notes-acct1", "note-1"))
        self.assertEqual(runtime.sync.status(connection["connection_id"])["state"], "active")


class InboxTests(DeliveryTestCase):
    def test_sync02_events_are_durable_and_deduplicated(self):
        # SYNC-02/SEC-02: acknowledgment means durable inbox insertion; a replayed
        # provider delivery ID is accepted but never queued twice.
        first = self.sync.signal(self.cid(), event_id="deliv-1", payload={"historyId": 7})
        replay = self.sync.signal(self.cid(), event_id="deliv-1", payload={"historyId": 7})
        self.assertTrue(first["accepted"])
        self.assertTrue(replay["duplicate"])
        self.assertEqual(self.sync.pending_signals(self.cid()), 1)
        # A fresh Store proves the signal survived the "process".
        rebuilt = SourceSync(Store(self.path), {"fixture.gmail": FixtureAdapter()})
        self.assertEqual(rebuilt.pending_signals(self.cid()), 1)

    def test_sync01_signal_arriving_mid_run_causes_a_follow_up_pass(self):
        self.sync.signal(self.cid(), payload={"n": 1})
        taken = self.sync.take_signals(self.cid())
        self.assertEqual(taken["count"], 1)
        self.assertEqual(self.sync.pending_signals(self.cid()), 1)
        # Arrives after the run's take but before its final pass: must survive.
        self.sync.signal(self.cid(), payload={"n": 2})
        self.assertEqual(self.sync.pending_signals(self.cid()), 2)
        second = self.sync.take_signals(self.cid())
        self.assertGreater(second["up_to"], taken["up_to"])
        # Acking through a bound purges only delivered rows: bounded retention.
        self.sync.ack_signals(self.cid(), up_to=second["up_to"])
        self.assertEqual(self.sync.pending_signals(self.cid()), 0)


class JobQueueTests(DeliveryTestCase):
    def job(self, kind="attachment", key="k1", payload=None):
        with self.store.connect() as db:
            self.sync._enqueue_job(db, self.cid(), kind, f"{kind}:{self.cid()}:{key}",
                                   payload or {"x": 1})

    def test_leased_jobs_transition_and_stale_completion_is_rejected(self):
        self.job()
        claimed = self.sync.claim_job("w1", kinds=("attachment",))
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["state"], "leased")
        self.assertEqual(claimed["fence"], 1)
        with self.assertRaises(ValueError):
            self.sync.claim_job("w2", kinds=("attachment",), ttl=300)  # held
        stale = dict(claimed, owner="ghost", fence=9)
        with self.assertRaises(ValueError):
            self.sync.complete_job(stale)
        self.sync.complete_job(claimed, {"stored": True})
        self.assertEqual(self.sync.pending_jobs(self.cid()), [])

    def test_failed_job_retries_then_quarantines(self):
        self.job()
        claimed = self.sync.claim_job("w1", kinds=("attachment",))
        self.sync.fail_job(claimed, reason="429", retry_after=0)
        again = self.sync.claim_job("w1", kinds=("attachment",))
        self.assertEqual(again["attempts"], 1)
        self.assertEqual(again["fence"], 2)
        self.sync.fail_job(again, reason="corrupt", quarantine=True)
        self.assertEqual(self.sync.claim_job("w1", kinds=("attachment",)), None)
        with self.store.connect() as db:
            state = db.execute("SELECT state FROM source_jobs").fetchone()[0]
        self.assertEqual(state, "quarantined")

    def test_review1_formation_obligations_are_sync_jobs_not_learning_proposals(self):
        # Review fix #1: formation rides the sync obligations queue with dedupe keys;
        # workflows.py keeps its closed {'consolidate','evaluate'} contract.
        lease = self.sync.claim(self.cid(), stream="messages", role="backfill", owner="w1")
        item = normalized_item("m1", records=[note_record("m1")],
                               attachments=[{"sha256": "a" * 64, "filename": "log.pdf",
                                             "mime": "application/pdf", "size": 10}])
        page = source_page(page_id="pg_1",
                           operations=[source_operation("upsert", "m1", records=[note_record("m1")],
                                                        source_version=1)],
                           next_state=read_state(cursor={"after": 1}))
        self.sync.commit_page(lease, op_id="op_1", page=page, items=[item])
        jobs = self.sync.pending_jobs(self.cid())
        kinds = {job["kind"] for job in jobs}
        self.assertIn("attachment", kinds)
        # Attachment ownership must be preserved for each evidence revision.
        lease2 = self.sync.claim(self.cid(), stream="messages", role="incremental", owner="w1")
        again = normalized_item("m1", records=[note_record("m1", revision="2")],
                                attachments=[{"sha256": "a" * 64, "filename": "log.pdf",
                                              "mime": "application/pdf", "size": 10}])
        self.sync.commit_page(lease2, op_id="op_2", page=source_page(
            page_id="pg_2", operations=[source_operation("upsert", "m1",
                records=[note_record("m1", revision="2")], source_version=2)],
            next_state=read_state(mode="incremental", cursor={"after": 2})), items=[again])
        attachments = [job for job in self.sync.pending_jobs(self.cid()) if job["kind"] == "attachment"]
        self.assertEqual(len(attachments), 2)
        # A new projection transform version schedules a new job; identical ones do not.
        with self.store.connect() as db:
            self.sync._enqueue_job(db, self.cid(), "formation", f"formation:{self.cid()}:m1:v1",
                                   {"source_id": "m1"})
            self.sync._enqueue_job(db, self.cid(), "formation", f"formation:{self.cid()}:m1:v1",
                                   {"source_id": "m1"})
            count = db.execute("SELECT COUNT(*) FROM source_jobs WHERE kind='formation'").fetchone()[0]
        self.assertEqual(count, 1)


class WorkerRecoveryTests(unittest.TestCase):
    """Review fix #3: fencing is enforced in the database, so it works between
    separate Store connections, which is what separate processes get."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "memory.db"
        self.store_a = Store(self.path)
        self.store_b = Store(self.path)
        self.sync_a = SourceSync(self.store_a, {"fixture.gmail": FixtureAdapter()})
        self.sync_b = SourceSync(self.store_b, {"fixture.gmail": FixtureAdapter()})
        self.conn = self.sync_a.configure(adapter_id="fixture.gmail", source="gmail-acct1",
                                          scope={}, retention="mirror")

    def test_abandoned_worker_cannot_commit_after_takeover(self):
        cid = self.conn["connection_id"]
        abandoned = self.sync_a.claim(cid, stream="messages", role="backfill", owner="a", ttl=-1)
        takeover = self.sync_b.claim(cid, stream="messages", role="backfill", owner="b", ttl=60)
        self.assertGreater(takeover["fence"], abandoned["fence"])
        page = source_page(page_id="pg_x", operations=[], next_state=read_state(cursor={"done": 1}))
        with self.assertRaises(ValueError):
            self.sync_a.commit_page(abandoned, op_id="op_stale", page=page)
        self.sync_b.commit_page(takeover, op_id="op_fresh", page=page)
        self.assertEqual(self.sync_a.stream_state(cid, "messages", "", "backfill")["cursor"],
                         {"done": 1})

    def test_run_once_commits_adapter_page_and_finishes_export(self):
        class Paging(FixtureAdapter):
            def __init__(self):
                super().__init__(); self.served = 0

            def read_page(self, context, state):
                seen = (state["cursor"] or {}).get("seen", 0)
                if seen >= 4:
                    return source_page(page_id="pg_done", operations=[], next_state=read_state(
                        state_version=state["state_version"], cursor={"seen": seen, "done": True},
                        mode=state["mode"]))
                operations = [source_operation("upsert", f"m{i}",
                                               records=[note_record(f"m{i}", source="paging-acct")],
                                               source_version=i)
                              for i in range(seen + 1, seen + 3)]
                return source_page(page_id=f"pg_{seen // 2}", operations=operations,
                                   next_state=read_state(state_version=state["state_version"],
                                                         cursor={"seen": seen + 2,
                                                                 "done": seen + 2 >= 4},
                                                         mode=state["mode"]))

        sync = SourceSync(self.store_a, {"fixture.paging": Paging()})
        conn = sync.configure(adapter_id="fixture.paging", source="paging-acct", scope={},
                              retention="mirror")
        worker = SyncWorker(sync)
        first = worker.run_once(conn["connection_id"], stream="messages", role="backfill", owner="w")
        self.assertEqual(first["status"], "committed")
        self.assertEqual(first["applied"], 2)
        second = worker.run_once(conn["connection_id"], stream="messages", role="backfill", owner="w")
        self.assertEqual(second["status"], "committed")
        third = worker.run_once(conn["connection_id"], stream="messages", role="backfill", owner="w")
        self.assertEqual(third["status"], "complete")
        # A fourth pass replays the terminal receipt instead of rescanning.
        fourth = worker.run_once(conn["connection_id"], stream="messages", role="backfill", owner="w")
        self.assertEqual(fourth["status"], "complete")
        self.assertTrue(fourth["replayed"])
        with self.store_a.connect() as db:
            total = db.execute("SELECT COUNT(*) FROM records WHERE source='paging-acct'").fetchone()[0]
        self.assertEqual(total, 4)

    def test_temporary_adapter_failure_reports_retry_without_commit(self):
        class Flaky(FixtureAdapter):
            def read_page(self, context, state):
                from personal_memory.source_sdk import AdapterError
                raise AdapterError("rate_limit", "429", retry_after=17)
        sync = SourceSync(self.store_a, {"fixture.flaky": Flaky()})
        conn = sync.configure(adapter_id="fixture.flaky", source="flaky-acct", scope={},
                              retention="mirror")
        result = SyncWorker(sync).run_once(conn["connection_id"], stream="messages",
                                           role="incremental", owner="w")
        self.assertEqual(result["status"], "retry")
        self.assertEqual(result["retry_after"], 17)
        self.assertIsNone(sync.stream_state(conn["connection_id"], "messages", "", "incremental")["cursor"])
        # The lease is released so the retry can be claimed immediately.
        self.assertIsNotNone(sync.claim(conn["connection_id"], stream="messages",
                                        role="incremental", owner="w2", ttl=1))


class SecretStoreTests(unittest.TestCase):
    def test_review5_secrets_live_in_protected_storage_and_resolve_by_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "secrets.json"
            secrets = SecretStore(path)
            ref = secrets.put("gmail_refresh", "SECRET-VALUE-123")
            self.assertEqual(ref, "secret://gmail_refresh")
            self.assertEqual(secrets.resolve(ref), "SECRET-VALUE-123")
            self.assertEqual(secrets.names(), ["gmail_refresh"])
            # The database never holds the value: only the reference.
            store = Store(Path(tmp) / "memory.db")
            sync = SourceSync(store, {"fixture.gmail": FixtureAdapter()})
            conn = sync.configure(adapter_id="fixture.gmail", source="acct", scope={},
                                  retention="mirror", secret_ref=ref)
            with store.connect() as db:
                blob = db.execute("SELECT * FROM source_connections").fetchone()
            self.assertNotIn("SECRET-VALUE-123", str(tuple(blob)))
            self.assertEqual(blob["secret_ref"], ref)
            with self.assertRaises(KeyError):
                secrets.resolve("secret://absent")
            # Connection context resolves lazily through the reference only.
            context = connection_context(connection_id=conn["connection_id"], source="acct",
                                         secrets=secrets.resolver())
            self.assertEqual(context["secrets"](ref), "SECRET-VALUE-123")
            secrets.delete("gmail_refresh")
            self.assertEqual(secrets.names(), [])

    def test_secret_values_never_appear_in_repr(self):
        with tempfile.TemporaryDirectory() as tmp:
            secrets = SecretStore(Path(tmp) / "s.json")
            secrets.put("token", "super-secret")
            self.assertNotIn("super-secret", repr(secrets))


if __name__ == "__main__":
    unittest.main()
