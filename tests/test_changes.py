"""Change-journal tests (MEMORY_AWARENESS_PLAN.md §4-§5, matrix J01-J08, N01-N05).

Real SQLite transactions throughout: the journal commits inside the same
transaction as canonical evidence, heads and the page receipt, so every test
asserts durable state through a rebuilt Store, not helper call bookkeeping.
"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from personal_memory import changes
from personal_memory.store import Store
from personal_memory.source_sdk import read_state, source_operation, source_page, stream_spec
from personal_memory.source_sync import SourceSync
from tests.test_source_sync import FixtureAdapter, note_record


ENABLED = {"journal": {"enabled": True}}


class JournalFixture(unittest.TestCase):
    """One connection, deterministic pages, journal enabled."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "memory.db"
        self.store = Store(self.path)
        changes.configure(self.store, ENABLED)
        self.sync = SourceSync(self.store, {"fixture.gmail": FixtureAdapter()})
        self.conn = self.sync.configure(adapter_id="fixture.gmail", source="gmail-acct1",
                                        scope={}, retention="mirror")
        self.seq = 0

    def lease(self, role="backfill", owner="w1", ttl=60):
        return self.sync.claim(self.conn["connection_id"], stream="messages", role=role,
                               owner=owner, ttl=ttl)

    def commit(self, lease, operations, cursor=None, complete=True, coverage=(), op_id=None, page_id=None):
        self.seq += 1
        cursor = cursor if cursor is not None else {"after": self.seq}
        page = source_page(page_id=page_id or f"pg-{self.seq}", operations=operations,
                           next_state=read_state(cursor=cursor,
                                                 mode=lease["role"], state_version=lease["state_version"]),
                           complete=complete, coverage=coverage)
        return self.sync.commit_page(lease, op_id=op_id or f"op-{self.seq}", page=page)

    def upsert(self, source_id, version, *, text=None, revision="1", coordinates=None,
               metadata=None, action="upsert", occurred_at="2026-09-15T12:00:00Z"):
        record = note_record(source_id, text=text, revision=revision)
        record["occurred_at"] = occurred_at
        return source_operation(action, source_id, records=[record], source_version=version,
                                metadata=metadata, coordinates=coordinates)

    def events(self, kind=None):
        rows = changes.read(self.store, limit=500)["events"]
        return [row for row in rows if kind is None or row["kind"] == kind]

    def reopen(self):
        return SourceSync(Store(self.path), {"fixture.gmail": FixtureAdapter()})


class JournalIdentityTests(JournalFixture):
    def test_j01_rollback_leaves_no_events_no_records_no_cursor(self):
        # A page whose second operation fails mid-commit must journal nothing.
        lease = self.lease()
        bad = self.upsert("m1", 10)
        broken = self.upsert("m2", 11)
        broken["records"][0]["source"] = "other-namespace"  # rejected inside the transaction
        with self.assertRaises(ValueError):
            self.commit(lease, [bad, broken], cursor={"after": 99})
        data = changes.read(self.store, limit=10)
        self.assertEqual(data["events"], [])
        self.assertEqual(data["high_watermark"], 0)
        state = self.sync.stream_state(self.conn["connection_id"], "messages", role="backfill")
        self.assertIsNone(state["cursor"])
        with self.store.connect() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM records WHERE source_id='m1'").fetchone())

    def test_j02_replay_after_crash_returns_receipt_without_second_event(self):
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)], op_id="op-fixed")
        first = self.events()
        self.assertEqual(len(first), 1)
        # A worker restarting after commit replays the identical operation.
        self.reopen()  # a fresh process sees the durable receipt
        replay = dict(self.lease(role="incremental", owner="w1"), state_version=1)
        self.commit(replay, [self.upsert("m1", 10)],
                    op_id="op-fixed", page_id="pg-1", cursor={"after": 1})
        self.assertEqual(self.events(), first)

    def test_j03_repeated_page_never_duplicates_arrival(self):
        lease = self.lease()
        self.commit(lease, [self.upsert("m1", 10)], op_id="op-a")
        replay = dict(self.lease(owner="w1"), state_version=1)
        self.commit(replay, [self.upsert("m1", 10)], op_id="op-a",
                    page_id="pg-1", cursor={"after": 1})
        self.assertEqual(len(self.events(kind="created")), 1)
        # The durable inbox also dedupes the triggering webhook.
        self.sync.signal(self.conn["connection_id"], event_id="webhook-1")
        self.assertTrue(self.sync.signal(self.conn["connection_id"], event_id="webhook-1")["duplicate"])

    def test_j04_out_of_order_old_revision_produces_no_fresh_event(self):
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 20)], cursor={"after": 20})
        self.assertEqual(len(self.events(kind="created")), 1)
        backfill = self.lease(role="backfill", owner="w3")
        outcome = self.commit(backfill, [self.upsert("m1", 5)], cursor={"after": 5})
        self.assertEqual(outcome["history_only"], 1)
        self.assertEqual(len(self.events(kind="created")), 1)
        head = self.sync.head("gmail-acct1", "m1")
        self.assertEqual(head["source_version"], "20")

    def test_j05_remove_restore_remove_are_three_transitions(self):
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)], cursor={"after": 10})
        created = self.events(kind="created")[0]
        plan = [("remove", 11), ("restore", 12), ("remove", 13)]
        for action, version in plan:
            lease = self.lease(role="incremental", owner="w1")
            state_version = lease["state_version"]
            operation = source_operation(action, "m1", source_version=version,
                                         records=[note_record("m1")] if action == "restore" else None)
            self.commit(lease, [operation], cursor={"after": version})
            # Replaying the same page cannot add a second event for one transition.
            replay = dict(self.lease(role="incremental", owner="w1"), state_version=state_version)
            self.commit(replay, [operation], cursor={"after": version},
                        op_id=f"op-{self.seq}", page_id=f"pg-{self.seq}")
        rows = self.events()
        kinds = [row["kind"] for row in rows]
        self.assertEqual(kinds, ["created", "removed", "restored", "removed"])
        versions = [row["transition_version"] for row in rows]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertEqual(len(set(row["event_id"] for row in rows)), len(rows))
        self.assertEqual(rows[0]["event_id"], created["event_id"])

    def test_j06_split_message_is_one_logical_arrival(self):
        lease = self.lease()
        parts = [note_record("m1", text="part one", revision="1"),
                 note_record("m1", text="part two", revision="2")]
        self.commit(lease, [source_operation("upsert", "m1", records=parts, source_version=7)])
        arrivals = self.events(kind="created")
        self.assertEqual(len(arrivals), 1)
        self.assertEqual(sorted(arrivals[0]["record_ids"]),
                         sorted(changes.record_id("gmail-acct1", "m1", r["revision"])
                                for r in parts))
        for rid in arrivals[0]["record_ids"]:
            self.assertIn("part", self.store.evidence(rid)["text"])

    def test_j07_metadata_update_classified_separately(self):
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)], cursor={"after": 10})
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 11, action="metadata_update",
                                        metadata={"labels": ["read"]})], cursor={"after": 11})
        rows = self.events()
        self.assertEqual([row["kind"] for row in rows], ["created", "metadata_updated"])
        self.assertEqual(rows[1]["classification_basis"], "explicit-metadata-operation")

    def test_j08_forgotten_item_resurfacing_stays_silent(self):
        lease = self.lease()
        self.commit(lease, [self.upsert("m1", 10)])
        created = self.events(kind="created")[0]
        self.store.forget_source("gmail-acct1", "m1")
        before = self.events()
        lease = self.lease(owner="w1")
        outcome = self.commit(lease, [self.upsert("m1", 11)])
        self.assertEqual(outcome["suppressed"], 1)
        self.assertEqual(self.events(), before)
        with self.assertRaises(ValueError):
            self.store.evidence(created["record_ids"][0])

    def test_identical_accepted_state_is_a_noop(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 10)])
        # Different op_id, but the accepted state is unchanged: replayed content,
        # same head version. No second transition, no second event.
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 10)])
        self.assertEqual(len(self.events(kind="created")), 1)
        self.assertEqual(self.events(kind="content_updated"), [])


class NoveltyTests(JournalFixture):
    def test_n01_historical_import_never_fakes_live_arrivals(self):
        for batch in range(1, 6):
            operations = [self.upsert(f"old-{batch}-{i}", batch * 100 + i) for i in range(4)]
            self.commit(self.lease(owner="w1"), operations, cursor={"after": batch})
        rows = self.events()
        self.assertTrue(rows)
        for row in rows:
            self.assertNotEqual(row["novelty"], "live")
        self.assertEqual(len(self.events(kind="backfill_complete")), 0)  # pages never completed a pass
        # The pass itself is bounded: a start-progress event only when the cursor was empty.
        self.assertEqual(len(self.events(kind="backfill_progress")), 1)

    def test_backfill_completion_is_recorded_once_per_pass(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1)], cursor={"after": 1})
        lease = self.lease(owner="w1")
        self.commit(lease, [], cursor={"done": True})
        self.assertEqual(len(self.events(kind="backfill_progress")), 1)
        self.assertEqual(len(self.events(kind="backfill_complete")), 1)

    def test_n02_live_arrival_survives_backfill_race(self):
        # Backfill wins the database race and stores the item as historical.
        lease = self.lease(role="backfill", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)])
        # Incremental later confirms it arrived after the live boundary.
        lease = self.lease(role="incremental", owner="w2")
        self.commit(lease, [self.upsert("m1", 10, coordinates={"arrival": "fresh"})],
                    cursor={"after": 10})
        arrivals = [row for row in self.events() if row["kind"] == "created"]
        self.assertEqual([row["novelty"] for row in arrivals], ["historical", "live"])
        self.assertEqual(arrivals[1]["cause_event_id"], arrivals[0]["event_id"])
        # Exactly one logical arrival identity remains schedulable: the newest event wins.
        self.assertEqual(changes.pending_arrivals(self.store), [("gmail-acct1", "m1", "live")])

    def test_live_event_appears_once_when_incremental_stores_first(self):
        lease = self.lease(role="incremental", owner="w2")
        self.commit(lease, [self.upsert("m1", 10, coordinates={"arrival": "fresh"})],
                    cursor={"after": 10})
        lease = self.lease(role="backfill", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)])  # backfill converges later
        arrivals = [row for row in self.events() if row["kind"] == "created"]
        self.assertEqual([row["novelty"] for row in arrivals], ["live"])
        self.assertEqual(changes.pending_arrivals(self.store), [("gmail-acct1", "m1", "live")])

    def test_n03_incremental_without_coordinates_is_uncertain_not_live(self):
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [self.upsert("m1", 10)], cursor={"after": 10})
        row = self.events(kind="created")[0]
        self.assertEqual(row["novelty"], "uncertain")
        self.assertEqual(row["origin_mode"], "incremental")
        self.assertIn("arrival", row["classification_basis"])

    def test_provider_coordinates_decide_not_the_pass_role(self):
        lease = self.lease(role="incremental", owner="1")
        self.commit(lease, [self.upsert("m1", 10, coordinates={"arrival": "historical"})],
                    cursor={"after": 10})
        row = self.events(kind="created")[0]
        self.assertEqual(row["novelty"], "historical")
        self.assertEqual(row["classification_basis"], "provider-coordinate")

    def test_n04_expired_history_records_explicit_gap(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [], coverage=[{"start": "h1", "end": "h9", "state": "gap",
                                          "note": "history expired"}])
        gaps = self.events(kind="sync_gap")
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["novelty"], "uncertain")
        self.assertIn("history expired", gaps[0]["classification_basis"])
        data = changes.read(self.store, limit=10)
        self.assertTrue(data["resync_required"])
        # Reconciliation discoveries are recovered, never falsely fresh.
        lease = self.lease(role="reconcile", owner="w2")
        self.commit(lease, [self.upsert("m1", 1)])
        self.assertEqual(self.events(kind="created")[0]["novelty"], "recovered")

    def test_n05_bad_source_dates_keep_stable_order_and_uncertainty(self):
        future = self.upsert("m1", 1, coordinates={"arrival": "fresh"},
                             occurred_at="2999-01-01T00:00:00Z")
        undated = self.upsert("m2", 2, coordinates={"arrival": "fresh"}, occurred_at=None)
        lease = self.lease(role="incremental", owner="w1")
        self.commit(lease, [future, undated], cursor={"after": 2})
        rows = self.events()
        self.assertEqual([row["source_item_id"] for row in rows], ["m1", "m2"])  # scan = commit order
        self.assertEqual([row["novelty"] for row in rows], ["uncertain", "uncertain"])
        self.assertIsNone(rows[0]["occurred_at"])  # implausible source date is not authoritative
        self.assertIsNone(rows[1]["occurred_at"])
        self.assertIn("date", rows[0]["classification_basis"])
        self.assertIn("date", rows[1]["classification_basis"])
        self.assertTrue(rows[1]["record_ids"])


class ReadApiTests(JournalFixture):
    def test_read_paginates_with_opaque_scoped_cursor(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert(f"m{i}", i + 1) for i in range(5)])
        page_one = changes.read(self.store, principal="owner-a", limit=2)
        self.assertEqual(len(page_one["events"]), 2)
        self.assertTrue(page_one["has_more"])
        self.assertIsNotNone(page_one["next_cursor"])
        page_two = changes.read(self.store, principal="owner-a", limit=2, cursor=page_one["next_cursor"])
        self.assertEqual(len(page_two["events"]), 2)
        self.assertNotEqual(page_one["events"][0]["event_id"], page_two["events"][0]["event_id"])

    def test_c04_read_is_inspection_not_acknowledgment(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1)])
        changes.read(self.store, principal="owner-a", limit=10)
        # A read cannot advance any consumer scan position.
        self.assertEqual(changes.consumer_positions(self.store), {})

    def test_c08_foreign_cursor_is_rejected_without_leak(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1), self.upsert("m2", 2)])
        other = changes.read(self.store, principal="owner-b", limit=1)
        self.assertIsNotNone(other["next_cursor"])
        with self.assertRaises(ValueError) as error:
            changes.read(self.store, principal="owner-a", cursor=other["next_cursor"])
        self.assertNotIn("m1", str(error.exception))

    def test_filters_scope_events_and_cursors(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1), self.upsert("m2", 2)])
        filtered = changes.read(self.store, principal="owner-a", kinds=["created"],
                                source="gmail-acct1", limit=1)
        self.assertEqual(len(filtered["events"]), 1)
        mismatch = changes.read(self.store, principal="owner-a", kinds=["removed"], limit=10)
        self.assertEqual(mismatch["events"], [])
        self.assertTrue(mismatch["has_more"] is False)
        with self.assertRaises(ValueError):
            changes.read(self.store, principal="owner-a", cursor=filtered["next_cursor"],
                         kinds=["removed"])  # cursor bound to another filter digest

    def test_read_supports_date_window_filters(self):
        # §7: "What new mail arrived?" answers with source and date filters.
        first = self.lease(role="incremental", owner="w1")
        self.commit(first, [self.upsert("old1", 10, occurred_at="2026-09-05T12:00:00Z")],
                    cursor={"after": 10})
        self.sync.release(first)  # the stream lease stays live until released
        second = self.lease(role="incremental", owner="w2")
        self.commit(second, [self.upsert("new1", 11, occurred_at="2026-09-15T12:00:00Z")],
                    cursor={"after": 11})
        recent = changes.read(self.store, limit=50, source="gmail-acct1",
                              since="2026-09-10T00:00:00Z")
        self.assertEqual([e["source_item_id"] for e in recent["events"]], ["new1"])
        window = changes.read(self.store, limit=50, since="2026-09-01T00:00:00Z",
                              until="2026-09-06T00:00:00Z")
        self.assertEqual([e["source_item_id"] for e in window["events"]], ["old1"])
        with self.assertRaises(ValueError):
            changes.read(self.store, limit=50, since="not-a-date")
        # Cursors stay bound to their filter window, like every other filter.
        paged = changes.read(self.store, limit=1, since="2026-09-01T00:00:00Z")
        self.assertIsNotNone(paged["next_cursor"])
        with self.assertRaises(ValueError):
            changes.read(self.store, limit=50, cursor=paged["next_cursor"])

    def test_disabled_journal_records_nothing_and_reads_report_gap_disabled(self):
        changes.configure(self.store, {"journal": {"enabled": False}})
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1)])
        self.assertEqual(self.events(), [])
        data = changes.read(self.store, principal="owner-a")
        self.assertFalse(data["enabled"])

    def test_reference_bounds_are_explicit_never_silent(self):
        lease = self.lease(owner="w1")
        parts = [note_record("big", text=f"chunk {i}", revision=str(i + 1)) for i in range(changes.MAX_INLINE_REFS + 5)]
        self.commit(lease, [source_operation("upsert", "big", records=parts, source_version=1)])
        row = self.events(kind="created")[0]
        self.assertGreater(row["record_count"], len(row["record_ids"]))
        self.assertTrue(row["truncated_refs"])  # explicit flag, never a silent cut
        resolver = changes.event_refs(self.store, row["event_id"])
        self.assertEqual(len(resolver["record_ids"]), row["record_count"])


class EpochFenceTests(JournalFixture):
    def test_events_carry_epoch_and_old_epoch_reads_are_flagged(self):
        lease = self.lease(owner="w1")
        self.commit(lease, [self.upsert("m1", 1)])
        row = self.events()[0]
        self.assertEqual(row["memory_epoch"], 0)
        data = changes.read(self.store, principal="owner-a", epoch=1)
        self.assertTrue(data["resync_required"])
        self.assertEqual(data["events"], [])


class ServiceRouteTests(unittest.TestCase):
    """The single security boundary: routes, roles and capability discovery."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        from personal_memory.service import MemoryService
        self.service = MemoryService(Path(self.tmp.name) / "data", "a" * 40,
                                     principals=[{"token": "r" * 40, "role": "reader"},
                                                 {"token": "g" * 40, "role": "agent"},
                                                 {"token": "i" * 40, "role": "ingest",
                                                  "sources": ["custom-notes"], "connector_id": "example.notes"}],
                                     retrieval_config={"semantic": {"enabled": False}},
                                     source_config={"enabled": False})
        self.addCleanup(self.service.close)
        self.admin = self.service.authenticate("Bearer " + "a" * 40)
        self.reader = self.service.authenticate("Bearer " + "r" * 40)
        self.agent = self.service.authenticate("Bearer " + "g" * 40)
        self.ingest = self.service.authenticate("Bearer " + "i" * 40)

    def call(self, path, args, principal):
        return self.service.dispatch(path, args, principal)

    def test_journal_ships_disabled_and_read_reports_the_gap(self):
        data = self.call("/v1/changes/read", {}, self.reader)
        self.assertFalse(data["enabled"])
        self.assertEqual(data["events"], [])

    def test_admin_configure_enables_the_journal_for_readers(self):
        result = self.call("/v1/awareness/configure", {"journal": {"enabled": True}}, self.admin)
        self.assertTrue(result["enabled"])
        data = self.call("/v1/changes/read", {"limit": 10}, self.reader)
        self.assertTrue(data["enabled"])

    def test_configure_rejects_non_admin_principals(self):
        from personal_memory.service import AccessDenied
        for principal in (self.reader, self.agent, self.ingest):
            with self.assertRaises(AccessDenied):
                self.call("/v1/awareness/configure", {"journal": {"enabled": True}}, principal)

    def test_read_principal_is_credential_derived_not_caller_supplied(self):
        self.call("/v1/awareness/configure", {"journal": {"enabled": True}}, self.admin)
        with self.assertRaises(ValueError):
            self.call("/v1/changes/read", {"principal": "someone-else"}, self.reader)

    def test_cursors_do_not_cross_credentials(self):
        changes.configure(self.service.store, ENABLED)
        with self.service.store.connect() as db:
            for item in ("x1", "x2"):
                changes.append(db, connection_id="c1", source="s", stream="m", partition="",
                               generation=1, source_item_id=item, kind="created",
                               origin_mode="incremental", record_ids=["rec_" + item])
        first = self.call("/v1/changes/read", {"limit": 1}, self.agent)
        self.assertTrue(first["has_more"])
        with self.assertRaises(ValueError):
            self.call("/v1/changes/read", {"cursor": first["next_cursor"]}, self.reader)
        # The owning principal continues cleanly.
        self.assertEqual(len(self.call("/v1/changes/read",
                         {"cursor": first["next_cursor"]}, self.agent)["events"]), 1)

    def test_status_advertises_the_journal_capability_and_state(self):
        status = self.call("/v1/status", {}, self.reader)
        self.assertIn("memory_change_journal", status["capabilities"])
        self.assertIn("change_journal", status)
        self.assertFalse(status["change_journal"]["enabled"])
        aware = self.call("/v1/awareness/status", {}, self.reader)
        self.assertIn("journal", aware)


if __name__ == "__main__":
    unittest.main()
