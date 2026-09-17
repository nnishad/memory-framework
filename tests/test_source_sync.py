"""Sync-core correctness tests (SOURCE_ADAPTER_IMPLEMENTATION.md §4-§5, TX/SYNC/SEC matrices).

Real SQLite transactions throughout; the store boundary is the unit under test.
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory.common import digest
from personal_memory.store import Store
from personal_memory.source_sdk import (connection_context, read_state, source_operation,
                                        source_page, stream_spec, wrap_ingestion_connector)
from personal_memory.source_sync import SourceSync
from personal_memory.ingestion import ConnectorSpec, IngestionConnector


def rec_id(source, source_id, revision):
    return "rec_" + digest([source, source_id, revision])[:32]


def note_record(source_id, text=None, revision="1", source="gmail-acct1"):
    return {"schema_version": "1.0", "source": source, "source_id": source_id,
            "revision": revision, "kind": "email", "occurred_at": None,
            "observed_at": "2026-09-06T12:00:00Z",
            "text": text if text is not None else f"body of {source_id}", "participants": [],
            "provenance": {"connector_id": "fixture.gmail", "connector_version": "1.0",
                           "source_locator": "gmail://" + source_id, "origin": "source",
                           "parent_record_ids": []}, "extensions": {}}


class FixtureAdapter:
    """Minimal declared-capability adapter for engine tests."""

    def __init__(self, capabilities=None):
        self._capabilities = {"history": True, "incremental": True, **(capabilities or {})}

    def spec(self):
        return {"adapter_id": "fixture.gmail", "adapter_version": "1.0",
                "protocol_versions": ["1.0"],
                "capabilities": {k: bool(self._capabilities.get(k, False))
                                 for k in ("history", "incremental", "reconciliation", "deletions",
                                           "attachments", "events", "subscriptions")},
                "config_schema": {}, "secret_refs": []}

    def check(self, context):
        return {"account_id": "acct1"}

    def discover(self, context):
        return [stream_spec("messages", modes=["backfill", "incremental"],
                            version_order="integer")]

    def read_page(self, context, state):
        raise NotImplementedError

    def normalize(self, payload):
        raise NotImplementedError


class SyncTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.sync = SourceSync(self.store, {"fixture.gmail": FixtureAdapter()})
        self.conn = self.sync.configure(adapter_id="fixture.gmail", source="gmail-acct1",
                                        scope={"labels": ["INBOX"]}, retention="mirror")

    def lease(self, owner="w1", role="backfill", ttl=60, stream="messages", partition=""):
        return self.sync.claim(self.conn["connection_id"], stream=stream, partition=partition,
                               role=role, owner=owner, ttl=ttl)

    def page(self, page_id, operations, cursor, complete=True, coverage=()):
        return source_page(page_id=page_id, operations=operations, coverage=coverage,
                           next_state=read_state(cursor=cursor, mode="backfill",
                                                 state_version=self.expected_version))

    expected_version = 1

    def upsert(self, source_id, version, **kwargs):
        return source_operation("upsert", source_id, records=[note_record(source_id, **kwargs)],
                                source_version=version)


class LeaseTests(SyncTestCase):
    def test_tx06_two_claims_one_active_lease(self):
        first = self.lease(owner="w1")
        self.assertEqual(first["fence"], 1)
        with self.assertRaises(ValueError):
            self.lease(owner="w2")  # active lease is held

    def test_expired_lease_reclaims_with_higher_fence(self):
        self.lease(owner="w1", ttl=-1)  # already expired
        second = self.lease(owner="w2")
        self.assertEqual(second["fence"], 2)

    def test_stale_fence_cannot_commit(self):
        self.lease(owner="w1", ttl=-1)
        stale = self.lease(owner="w2")
        # Simulate w1 waking up after its lease expiry with its old fence.
        expired_like_w1 = dict(stale, owner="w1", fence=1)
        with self.assertRaises(ValueError) as error:
            self.sync.commit_page(expired_like_w1, op_id="op_x",
                                  page=self.page("pg", [], {"after": 1}))
        self.assertIn("lease", str(error.exception).lower())

    def test_release_lets_next_worker_claim(self):
        first = self.lease(owner="w1")
        self.sync.release(first)
        second = self.lease(owner="w2")
        self.assertEqual(second["fence"], 2)


class CommitTests(SyncTestCase):
    def test_happy_path_records_head_cursor_and_status(self):
        lease = self.lease()
        result = self.sync.commit_page(lease, op_id="op_1",
                                       page=self.page("pg_1", [self.upsert("m1", 10), self.upsert("m2", 11)],
                                                      {"after": 11}))
        self.assertEqual(result["applied"], 2)
        self.assertFalse(result["replayed"])
        head = self.sync.head("gmail-acct1", "m1")
        self.assertEqual(head["source_version"], "10")
        self.assertEqual(lease["state_version"], self.sync.stream_state(
            self.conn["connection_id"], "messages", "", "backfill")["state_version"])
        self.assertEqual(self.sync.stream_state(
            self.conn["connection_id"], "messages", "", "backfill")["cursor"], {"after": 11})

    def test_tx02_replay_returns_committed_result_without_duplicates(self):
        lease = self.lease()
        page = self.page("pg_1", [self.upsert("m1", 10)], {"after": 10})
        first = self.sync.commit_page(lease, op_id="op_1", page=page)
        replay = self.sync.commit_page(lease, op_id="op_1", page=page)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["records"], first["records"])
        with self.store.connect() as db:
            rows = db.execute("SELECT COUNT(*) FROM records WHERE source='gmail-acct1'").fetchone()[0]
        self.assertEqual(rows, 1)

    def test_tx03_reused_operation_with_altered_payload_conflicts(self):
        lease = self.lease()
        self.sync.commit_page(lease, op_id="op_1",
                              page=self.page("pg_1", [self.upsert("m1", 10)], {"after": 10}))
        with self.assertRaises(ValueError) as error:
            self.sync.commit_page(lease, op_id="op_1",
                                  page=self.page("pg_1", [self.upsert("m1", 10, text="changed")],
                                                 {"after": 10}))
        self.assertIn("conflict", str(error.exception).lower())
        # No mutation occurred: the original content stands.
        head = self.sync.head("gmail-acct1", "m1")
        self.assertEqual(self.store.evidence(head["record_id"])["text"], "body of m1")

    def test_tx04_empty_and_removal_only_pages_advance_cursor(self):
        lease = self.lease()
        self.sync.commit_page(lease, op_id="op_1",
                              page=self.page("pg_1", [self.upsert("m1", 10)], {"after": 10}))
        empty = self.sync.commit_page(lease, op_id="op_2",
                                      page=self.page("pg_2", [], {"after": 11}))
        self.assertEqual(empty["applied"], 0)
        removal = source_operation("remove", "m1", source_version=12)
        third = self.sync.commit_page(lease, op_id="op_3",
                                      page=source_page(page_id="pg_3", operations=[removal],
                                                       next_state=read_state(cursor={"after": 12}),
                                                       complete=True))
        self.assertEqual(third["applied"], 1)
        self.assertEqual(self.sync.head("gmail-acct1", "m1")["state"], "removed")
        self.assertEqual(self.sync.stream_state(
            self.conn["connection_id"], "messages", "", "backfill")["cursor"], {"after": 12})

    def test_tx07_old_backfill_cannot_move_a_live_head(self):
        live = self.lease(role="incremental")
        self.sync.commit_page(live, op_id="op_live", page=source_page(
            page_id="pg_l", operations=[self.upsert("m1", 50)],
            next_state=read_state(mode="incremental", cursor={"h": 50})))
        backfill = self.lease(role="backfill")
        result = self.sync.commit_page(backfill, op_id="op_bf", page=source_page(
            page_id="pg_b", operations=[self.upsert("m1", 40)],
            next_state=read_state(mode="backfill", cursor={"h": 40})))
        self.assertEqual(result["history_only"], 1)  # evidence retained, head unmoved
        self.assertEqual(self.sync.head("gmail-acct1", "m1")["source_version"], "50")

    def test_tx08_stale_epoch_and_generation_are_rejected(self):
        lease = self.lease()
        self.sync.pause(self.conn["connection_id"])
        with self.assertRaises(ValueError):
            self.sync.commit_page(lease, op_id="op_1",
                                  page=self.page("pg_1", [self.upsert("m1", 1)], {"a": 1}))
        self.sync.resume(self.conn["connection_id"])
        # Pausing fences an old lease even after the connection becomes active again.
        with self.assertRaises(ValueError):
            self.sync.commit_page(lease, op_id="op_after_resume",
                                  page=self.page("pg_1", [self.upsert("m1", 1)], {"a": 1}))
        # Disconnect bumps the generation: the old lease can never commit again.
        self.sync.disconnect(self.conn["connection_id"])
        with self.assertRaises(ValueError):
            self.sync.commit_page(lease, op_id="op_2",
                                  page=self.page("pg_1", [self.upsert("m1", 1)], {"a": 1}))

    def test_tx05_split_pages_advance_completion_only_at_the_tail(self):
        lease = self.lease()
        head_batch = self.page("pg_big", [self.upsert(f"m{i}", i) for i in range(1, 4)],
                               {"after": 3})
        head_batch["complete"] = False
        partial = self.sync.commit_page(lease, op_id="op_a", page=head_batch)
        self.assertFalse(partial["page_complete"])
        state = self.sync.stream_state(self.conn["connection_id"], "messages", "", "backfill")
        self.assertIsNone(state["cursor"])  # upstream cursor never skips the tail
        self.assertEqual(self.sync.head("gmail-acct1", "m2")["source_version"], "2")
        tail_batch = self.page("pg_big", [self.upsert("m4", 4)], {"after": 4})
        done = self.sync.commit_page(lease, op_id="op_b", page=tail_batch)
        self.assertTrue(done["page_complete"])
        state = self.sync.stream_state(self.conn["connection_id"], "messages", "", "backfill")
        self.assertEqual(state["cursor"], {"after": 4})

    def test_tx01_exception_rolls_back_records_and_cursor_together(self):
        lease = self.lease()
        page = self.page("pg_1", [self.upsert("m1", 10), self.upsert("m2", 11)], {"after": 10})
        page["operations"][1]["records"][0]["text"] = ""  # fails validation mid-commit
        with self.assertRaises(ValueError):
            self.sync.commit_page(lease, op_id="op_1", page=page)
        state = self.sync.stream_state(self.conn["connection_id"], "messages", "", "backfill")
        self.assertIsNone(state["cursor"])
        self.assertIsNone(self.sync.head("gmail-acct1", "m1"))

    def test_tx10_quarantined_item_leaves_a_visible_gap_not_a_silent_drop(self):
        lease = self.lease()
        result = self.sync.commit_page(lease, op_id="op_1",
                                       page=self.page("pg_1", [self.upsert("m1", 10)], {"after": 10},
                                                      coverage=[{"start": "1", "end": "10",
                                                                 "state": "complete"}]),
                                       quarantined=[{"source_id": "bad_1", "reason": "unparseable MIME"}])
        self.assertEqual(result["applied"], 1)
        gaps = self.sync.coverage(self.conn["connection_id"], "messages")
        self.assertIn("gap", [entry["state"] for entry in gaps])
        jobs = self.sync.pending_jobs(self.conn["connection_id"], kinds=("quarantine",))
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["payload"]["source_id"], "bad_1")

    def test_sec06_forgotten_item_is_suppressed_without_blocking_the_page(self):
        self.store.forget_source("gmail-acct1", "m9")
        lease = self.lease()
        result = self.sync.commit_page(lease, op_id="op_1", page=self.page(
            "pg_1", [self.upsert("m8", 8), self.upsert("m9", 9), self.upsert("m10", 10)],
            {"after": 10}))
        self.assertEqual(result["applied"], 2)
        self.assertEqual(result["suppressed"], 1)
        self.assertEqual(self.sync.stream_state(
            self.conn["connection_id"], "messages", "", "backfill")["cursor"], {"after": 10})

    def test_sec01_record_from_another_evidence_namespace_is_rejected(self):
        # A compromised or buggy adapter cannot write outside its connection scope.
        lease = self.lease()
        foreign = source_operation("upsert", "m1", source_version=1,
                                   records=[note_record("m1", source="someone-elses-mail")])
        with self.assertRaises(ValueError) as error:
            self.sync.commit_page(lease, op_id="op_1",
                                  page=self.page("pg_1", [foreign], {"after": 1}))
        self.assertIn("namespace", str(error.exception).lower())
        self.assertIsNone(self.sync.head("someone-elses-mail", "m1"))


class StateMachineTests(SyncTestCase):
    def test_sync08_pause_resume_disconnect_transitions(self):
        self.assertEqual(self.sync.status(self.conn["connection_id"])["state"], "active")
        self.sync.pause(self.conn["connection_id"])
        self.assertEqual(self.sync.status(self.conn["connection_id"])["state"], "paused")
        self.sync.resume(self.conn["connection_id"])
        self.sync.disconnect(self.conn["connection_id"])
        final = self.sync.status(self.conn["connection_id"])
        self.assertEqual(final["state"], "disconnected")
        with self.assertRaises(ValueError):
            self.lease()  # no new work after disconnect

    def test_needs_auth_when_check_fails(self):
        class Broken(FixtureAdapter):
            def check(self, context):
                from personal_memory.source_sdk import AdapterError
                raise AdapterError("auth", "revoke")
        sync = SourceSync(self.store, {"fixture.broken": Broken()})
        conn = sync.configure(adapter_id="fixture.broken", source="broken-acct", scope={},
                              retention="mirror")
        self.assertEqual(sync.status(conn["connection_id"])["state"], "active")
        with self.assertRaises(Exception):
            sync.verify(conn["connection_id"])
        self.assertEqual(sync.status(conn["connection_id"])["state"], "needs_auth")

    def test_unsupported_capability_work_is_never_scheduled(self):
        # SDK-03 at the runtime boundary: the fixture declares no events.
        with self.assertRaises(ValueError):
            self.sync.register_trigger(self.conn["connection_id"], kind="events")


class CoverageAndResetTests(SyncTestCase):
    def test_coverage_reports_gaps_and_last_delta(self):
        lease = self.lease()
        self.sync.commit_page(lease, op_id="op_1",
                              page=self.page("pg_1", [self.upsert("m1", 10)], {"after": 10},
                                             coverage=[{"start": "0", "end": "10", "state": "complete"}]))
        report = self.sync.coverage(self.conn["connection_id"], "messages")
        self.assertEqual(report[0]["state"], "complete")
        self.assertIsNotNone(self.sync.status(self.conn["connection_id"])["last_delta_at"])

    def test_stream_roles_track_cursors_independently(self):
        backfill = self.lease(role="backfill")
        self.sync.commit_page(backfill, op_id="b1", page=source_page(
            page_id="p", operations=[self.upsert("m1", 1)],
            next_state=read_state(mode="backfill", cursor={"w": "2020"})))
        live = self.lease(role="incremental")
        self.sync.commit_page(live, op_id="l1", page=source_page(
            page_id="p2", operations=[self.upsert("m2", 2)],
            next_state=read_state(mode="incremental", cursor={"h": 99})))
        bf = self.sync.stream_state(self.conn["connection_id"], "messages", "", "backfill")
        inc = self.sync.stream_state(self.conn["connection_id"], "messages", "", "incremental")
        self.assertEqual(bf["cursor"], {"w": "2020"})
        self.assertEqual(inc["cursor"], {"h": 99})
        self.assertEqual(bf["state_version"], 2)  # each role advanced its own state once
        self.assertEqual(inc["state_version"], 2)


class TombstoneReplayTests(SyncTestCase):
    """Fix 2: a forgotten revision must suppress on replay, never abort the page."""

    def _present(self, source_id, revision=None):
        with self.store.connect() as db:
            if revision is None:
                return db.execute("SELECT COUNT(*) FROM records WHERE source=? AND source_id=?"
                                  " AND deleted=0", ("gmail-acct1", source_id)).fetchone()[0]
            return db.execute("SELECT COUNT(*) FROM records WHERE source=? AND source_id=?"
                              " AND revision=? AND deleted=0",
                              ("gmail-acct1", source_id, revision)).fetchone()[0]

    def test_individual_record_tombstone_is_suppressed_not_fatal(self):
        self.store.deletions.append([rec_id("gmail-acct1", "m1", "1")])
        lease = self.lease()
        result = self.sync.commit_page(lease, op_id="op_1", page=self.page(
            "pg_1", [self.upsert("m1", 10), self.upsert("m2", 11)], {"after": 11}))
        self.assertEqual(result["suppressed"], 1)
        self.assertEqual(result["applied"], 1)
        self.assertEqual(self._present("m1"), 0)
        self.assertIsNone(self.sync.head("gmail-acct1", "m1"))
        self.assertIsNotNone(self.sync.head("gmail-acct1", "m2"))
        self.assertEqual(self.sync.stream_state(
            self.conn["connection_id"], "messages", "", "backfill")["cursor"], {"after": 11})

    def test_forgotten_email_suppressed_across_backfill_incremental_reconcile(self):
        self.store.forget_source("gmail-acct1", "m_forget")
        for index, role in enumerate(("backfill", "incremental", "reconcile")):
            lease = self.lease(owner=f"w{index}", role=role)
            result = self.sync.commit_page(
                lease, op_id=f"op_{role}", page=source_page(
                    page_id=f"pg_{role}",
                    operations=[self.upsert("m_forget", 100), self.upsert(f"new_{role}", 101)],
                    next_state=read_state(cursor={"after": 101}, mode=role,
                                          state_version=self.expected_version)))
            self.assertEqual(result["suppressed"], 1, role)
            self.assertEqual(result["applied"], 1, role)
            self.assertIsNone(self.sync.head("gmail-acct1", "m_forget"), role)
            self.assertEqual(self._present("m_forget"), 0, role)
            self.assertIsNotNone(self.sync.head("gmail-acct1", f"new_{role}"), role)

    def test_forgotten_chunk_does_not_resurrect_through_sibling_records(self):
        # Multi-record email: one individually-forgotten chunk must stay absent.
        self.store.deletions.append([rec_id("gmail-acct1", "m1", "2")])
        lease = self.lease()
        op = source_operation("upsert", "m1", source_version=5, records=[
            note_record("m1", revision="1"), note_record("m1", revision="2"),
            note_record("m1", revision="3")])
        result = self.sync.commit_page(lease, op_id="op_1", page=self.page(
            "pg_1", [op], {"after": 5}))
        self.assertEqual(result["suppressed"], 1)
        self.assertEqual(self._present("m1", "2"), 0)
        self.assertEqual(self._present("m1", "1"), 1)
        self.assertEqual(self._present("m1", "3"), 1)

    def test_replay_of_suppressed_page_is_idempotent(self):
        self.store.deletions.append([rec_id("gmail-acct1", "m1", "1")])
        lease = self.lease()
        page = self.page("pg_1", [self.upsert("m1", 10), self.upsert("m2", 11)], {"after": 11})
        first = self.sync.commit_page(lease, op_id="op_1", page=page)
        replay = self.sync.commit_page(lease, op_id="op_1", page=page)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["records"], first["records"])
        self.assertEqual(self._present("m1"), 0)


    def test_forgotten_email_produces_no_attachment_or_projection_jobs(self):
        self.store.forget_source("gmail-acct1", "m1")
        lease = self.lease()
        item = {"source_id": "m1", "records": [note_record("m1")],
                "attachments": [{"sha256": "a" * 64, "part_id": "p1"}],
                "projections": [{"kind": "thread", "source_id": "m1", "transform": "x"}]}
        result = self.sync.commit_page(lease, op_id="op_1",
                                       page=self.page("pg_1", [self.upsert("m1", 10)], {"after": 10}),
                                       items=[item])
        self.assertEqual(result["suppressed"], 1)
        self.assertEqual(self.sync.pending_jobs(
            self.conn["connection_id"], kinds=("attachment", "projection")), [])
        self.assertIsNone(self.sync.head("gmail-acct1", "m1"))


if __name__ == "__main__":
    unittest.main()
