"""Fix 4 regression: retirement is unified across canonical and derived memory.

A single transaction-aware retirement operation must hide retired evidence and
invalidate dependent learning artifacts, curated entries and awareness results.
New derived memory cannot be created on evidence that is retired (deleted *or*
hidden). An idempotent repair pass cleans up pre-existing active artifacts whose
evidence is already retired, and restoring source visibility never reactivates
conclusions that were invalidated.
"""
import copy
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from personal_memory import curated, reset
from personal_memory.intelligence import Intelligence
from personal_memory.learning import Learning
from personal_memory.store import Store
from test_ingestion import item


def derived_item(source_id, parent_ids, text="A derived summary of the source."):
    record = copy.deepcopy(item(source_id))
    record["text"] = text
    record["provenance"] = {**record["provenance"], "origin": "derived",
                            "parent_record_ids": list(parent_ids)}
    return record


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)
        self.learning = Learning(self.store)
        self.intelligence = Intelligence(self.store)
        self.rid = self.store.ingest_contract([item("one")])["records"][0]["id"]

    def _make_outcome(self, evidence, key="task"):
        return self.learning.outcome(key=key, goal="Repair bicycle", action="Check brakes",
                                      result="Brake test passed", outcome="success",
                                      evidence_ids=[evidence], actor="agent")

    def _visible(self, rid):
        with self.store.connect() as db:
            return not db.execute(
                "SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1",
                (rid,)).fetchone()

    # --- Sole retirement operation cascades to derived memory (supersede) ------

    def test_supersede_invalidates_dependent_learning_artifact(self):
        obj = self._make_outcome(self.rid)
        self.assertEqual(self.learning.browse(kind="outcome", state="recorded")["items"][0]["id"], obj["id"])
        self.store.supersede(self.rid)
        self.assertEqual(self.learning.browse(kind="outcome", state="recorded")["items"], [])

    def test_supersede_invalidates_dependent_curated_entry(self):
        curated.apply(self.store, target="memory", expected_version=0, request_id="c1",
                      operations=[{"action": "add", "content": "prefers concise answers",
                                   "evidence_ids": [self.rid]}],
                      evidence_ids=[], epoch=0, actor="test")
        self.assertEqual(len(curated.read(self.store)["stores"]["memory"]["entries"]), 1)
        self.store.supersede(self.rid)
        self.assertEqual(curated.read(self.store)["stores"]["memory"]["entries"], [])

    def test_supersede_cascades_through_record_dependency_chain(self):
        child = self.store.ingest_contract(
            [derived_item("child", [self.rid])])["records"][0]["id"]
        obj = self._make_outcome(child, key="from-child")
        self.store.supersede(self.rid)
        with self.store.connect() as db:
            self.assertFalse(self._visible(child))
            state = db.execute("SELECT state FROM learning_objects WHERE id=?",
                               (obj["id"],)).fetchone()[0]
        self.assertEqual(state, "invalidated")

    def test_supersede_preserves_replacement_visibility(self):
        replacement = self.store.ingest_contract([item("two")])["records"][0]["id"]
        result = self.store.supersede(self.rid, replacement)
        self.assertTrue(self._visible(replacement))
        self.assertFalse(self._visible(self.rid))
        self.assertEqual(result["hidden_records"], 1)

    # --- Shared "live and visible" creation check ------------------------------

    def test_new_learning_rejected_on_hidden_but_not_deleted_evidence(self):
        self.store.supersede(self.rid)
        with self.assertRaises(ValueError):
            self._make_outcome(self.rid, key="after-supersede")

    def test_new_curated_rejected_on_hidden_evidence(self):
        self.store.supersede(self.rid)
        with self.assertRaises(ValueError):
            curated.apply(self.store, target="memory", expected_version=0, request_id="c2",
                          operations=[{"action": "add", "content": "revived",
                                       "evidence_ids": [self.rid]}],
                          evidence_ids=[], epoch=0, actor="test")

    def test_new_snapshot_rejected_on_hidden_evidence(self):
        self.store.supersede(self.rid)
        with self.assertRaises(ValueError):
            self.intelligence.snapshot(key="snap", record_ids=[self.rid], actor="agent")

    # --- Idempotent repair pass for legacy artifacts ---------------------------

    def _hide_without_cascade(self, rid):
        # Simulates legacy state written before retirement was unified: the
        # evidence is hidden but dependent artifacts were never invalidated.
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))

    def test_repair_invalidates_active_artifacts_citing_retired_evidence(self):
        from personal_memory import lifecycle
        obj = self._make_outcome(self.rid)
        self._hide_without_cascade(self.rid)
        report = lifecycle.repair_retired_dependencies(self.store)
        self.assertGreaterEqual(report["learning"], 1)
        with self.store.connect() as db:
            state = db.execute("SELECT state FROM learning_objects WHERE id=?",
                               (obj["id"],)).fetchone()[0]
        self.assertEqual(state, "invalidated")
        self.assertEqual(lifecycle.repair_retired_dependencies(self.store)["learning"], 0)

    def test_repair_restoration_does_not_reactivate_invalidated_conclusion(self):
        from personal_memory import lifecycle
        obj = self._make_outcome(self.rid)
        self._hide_without_cascade(self.rid)
        lifecycle.repair_retired_dependencies(self.store)
        with self.store.connect() as db:
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (self.rid,))
        again = lifecycle.repair_retired_dependencies(self.store)
        self.assertEqual(again["learning"], 0)
        with self.store.connect() as db:
            state = db.execute("SELECT state FROM learning_objects WHERE id=?",
                               (obj["id"],)).fetchone()[0]
        self.assertEqual(state, "invalidated")

    def test_repair_retires_curated_entries_citing_retired_evidence(self):
        from personal_memory import lifecycle
        curated.apply(self.store, target="memory", expected_version=0, request_id="c3",
                      operations=[{"action": "add", "content": "legacy preference",
                                   "evidence_ids": [self.rid]}],
                      evidence_ids=[], epoch=0, actor="test")
        self._hide_without_cascade(self.rid)
        report = lifecycle.repair_retired_dependencies(self.store)
        self.assertGreaterEqual(report["curated"], 1)
        self.assertEqual(curated.read(self.store)["stores"]["memory"]["entries"], [])
        self.assertEqual(lifecycle.repair_retired_dependencies(self.store)["curated"], 0)


class UpgradeMigrationTests(unittest.TestCase):
    """Stale derived memory is repaired once, transactionally, at initialization."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "memory.db"

    def _create_legacy_database(self):
        # A database written before retirement was unified: the evidence is
        # hidden without the cascade and dependent artifacts are still active.
        store = Store(self.path)
        rid = store.ingest_contract([item("legacy")])["records"][0]["id"]
        outcome = Learning(store).outcome(key="old", goal="Repair bicycle", action="Check brakes",
                                          result="Done", outcome="success", evidence_ids=[rid],
                                          actor="agent")
        subject = store.entity("person", "Owner")["id"]
        Intelligence(store).belief(key="b", subject_id=subject, predicate="rides", value="daily",
                                   evidence=[{"record_id": rid, "quote": "bicycle repair"}],
                                   actor="agent")
        curated.apply(store, target="memory", expected_version=0, request_id="c",
                      operations=[{"action": "add", "content": "prefers cycling",
                                    "evidence_ids": [rid]}],
                      evidence_ids=[], epoch=0, actor="test")
        with store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
        # Databases built before unified retirement carry no migration marker.
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("DROP TABLE IF EXISTS memory_migrations")
        return rid, subject, outcome

    def _markers(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT name,version FROM memory_migrations").fetchall()

    def _state(self, logical_key):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute("SELECT state FROM learning_objects WHERE logical_key=?",
                              (logical_key,)).fetchone()[0]

    def test_upgrade_repairs_stale_derived_memory_before_first_recall(self):
        rid, subject, outcome = self._create_legacy_database()
        reopened = Store(self.path)
        self.assertEqual(Learning(reopened).browse(kind="outcome", state="recorded")["items"], [])
        self.assertEqual(Intelligence(reopened).beliefs(subject_id=subject)["beliefs"], [])
        self.assertEqual(curated.read(reopened)["stores"]["memory"]["entries"], [])
        self.assertIn(("retirement_repair", 1), self._markers())

    def test_interrupted_migration_reruns_and_completes_on_restart(self):
        rid, subject, outcome = self._create_legacy_database()
        with patch.object(Store, "audit", side_effect=RuntimeError("crash mid-migration")):
            with self.assertRaises(RuntimeError):
                Store(self.path)
        # The repair and its marker share one transaction: a crash leaves nothing applied.
        self.assertEqual(self._markers(), [])
        self.assertEqual(self._state("old"), "recorded")
        reopened = Store(self.path)
        self.assertEqual(Learning(reopened).browse(kind="outcome", state="recorded")["items"], [])
        self.assertEqual(self._state("old"), "invalidated")
        self.assertEqual(len(self._markers()), 1)

    def test_reopen_after_migration_performs_no_repeated_work(self):
        self._create_legacy_database()
        Store(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            repairs = db.execute("SELECT count(*) FROM audit WHERE action='retirement_repair'").fetchone()[0]
        self.assertEqual(repairs, 1)
        Store(self.path)
        Store(self.path)
        with closing(sqlite3.connect(self.path)) as db:
            again = db.execute("SELECT count(*) FROM audit WHERE action='retirement_repair'").fetchone()[0]
            markers = db.execute("SELECT count(*) FROM memory_migrations").fetchone()[0]
        self.assertEqual(again, 1)
        self.assertEqual(markers, 1)

    def test_restored_visibility_keeps_invalidated_conclusions_inactive(self):
        rid, subject, outcome = self._create_legacy_database()
        repaired = Store(self.path)
        with repaired.connect() as db:
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (rid,))
        reopened = Store(self.path)
        self.assertEqual(self._state("old"), "invalidated")
        self.assertEqual(Learning(reopened).browse(kind="outcome", state="recorded")["items"], [])


if __name__ == "__main__":
    unittest.main()
