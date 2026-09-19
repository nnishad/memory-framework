"""R1: the semantic revision singleton is a schema invariant.

A fresh empty archive must initialize SemanticIndex successfully (no queued work
has existed to create the singleton row), service warm-up must succeed on an
empty store, and the first ingested record must index and retrieve without a
restart. Setup and migration run in one owned-or-joined transaction: they are
idempotent, never lower an existing clock, never leave a half-migrated schema
after an injected failure, and never commit a transaction the caller owns.
"""
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from personal_memory import semantic
from personal_memory.retrieval import Hybrid
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


def note(source_id, text="the garage engine repairs"):
    return {"source": "bench", "source_id": source_id, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text, "metadata": {}}


class FakeEmbedder:
    key = "sequence-init-model-v1"

    def chunks(self, text):
        yield 0, len(text), text

    def documents(self, texts):
        return [[1.0, 0.5, 0.25] for _ in texts]

    def query(self, text):
        return [1.0, 0.5, 0.25]


class _FailOnStatement:
    """Connection proxy that raises once on the first statement matching a fragment."""
    def __init__(self, db, fragment):
        self.db, self.fragment, self.fired = db, fragment, False

    @property
    def in_transaction(self):
        return self.db.in_transaction

    def execute(self, sql, *args):
        if self.fragment in sql and not self.fired:
            self.fired = True
            raise RuntimeError("injected migration failure")
        return self.db.execute(sql, *args)


class EmptyArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")

    def test_semantic_index_succeeds_on_an_empty_store(self):
        engine = SemanticIndex(self.store, embedder=FakeEmbedder())
        status = engine.status()
        self.assertEqual(status["pending_records"], 0)
        self.assertEqual(status["pending_retirements"], 0)
        self.assertTrue(status["ready"])

    def test_reopening_an_empty_store_does_not_advance_the_revision(self):
        def clock():
            with self.store.connect() as db:
                return db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]
        first = clock()
        SemanticIndex(self.store, embedder=FakeEmbedder())
        SemanticIndex(self.store, embedder=FakeEmbedder())
        self.assertEqual(clock(), first)  # warm-up is not work: no unnecessary increments

    def test_first_record_indexes_and_retrieves_without_restart(self):
        engine = SemanticIndex(self.store, embedder=FakeEmbedder())
        rid = self.store.ingest([note("first")])["records"][0]["id"]
        engine.sync()  # same live process that warmed up on the empty archive
        self.assertIn(rid, [row["id"] for row in engine.candidates("garage", 5)])
        self.assertEqual(engine.status()["pending_records"], 0)

    def test_service_warm_up_succeeds_on_an_empty_store(self):
        # Exercise the real warm-up entry point (Hybrid._ensure_semantic) on an empty
        # archive; only the model is substituted, so the failure must not be swallowed
        # into the permanent degraded state that hid R1 in production.
        with patch.dict(os.environ, {"PERSONAL_MEMORY_DISABLE_SEMANTIC": "0"}):
            h = Hybrid(self.store, start=False)
            self.addCleanup(h.close)
            with patch('personal_memory.semantic.SemanticIndex',
                       lambda store, config=None: SemanticIndex(store, embedder=FakeEmbedder())):
                engine = h._ensure_semantic()
        self.assertIsNotNone(engine)
        self.assertFalse(h._semantic_failed)
        self.assertNotIn("semantic", h.errors)


class MigrationTests(unittest.TestCase):
    """The legacy two-kinds-per-record queue must reshape before any index or
    clock statement references the new seq column."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "legacy.db"
        db = sqlite3.connect(self.path)
        db.execute("""CREATE TABLE semantic_work(record_id TEXT, kind TEXT NOT NULL, id INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL, PRIMARY KEY(record_id, kind))""")
        db.execute("CREATE TABLE semantic_bootstrap(model TEXT PRIMARY KEY, enqueued_at TEXT NOT NULL)")
        self.db = db
        self.addCleanup(db.close)  # close before the temp directory tears down (Windows)

    def _seed(self, rows, bootstrap=()):
        for record_id, kind, id_ in rows:
            self.db.execute("INSERT INTO semantic_work VALUES(?,?,?,?,?,?)",
                            (record_id, kind, id_, 0, 0, "2026-09-01T00:00:00Z"))
        for model in bootstrap:
            self.db.execute("INSERT INTO semantic_bootstrap VALUES(?,?)", (model, "2026-09-01T00:00:00Z"))
        self.db.commit()

    def _assert_migrated(self):
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(semantic_work)")}
        self.assertIn("seq", columns)  # one desired-state row per record, revision-carrying
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='semantic_work_v2'").fetchone())

    def test_empty_legacy_queue_migrates(self):
        self._seed([])
        semantic.ensure_schema(self.db)
        self._assert_migrated()
        with self.db:
            self.assertEqual(self.db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0], 0)

    def test_populated_legacy_queue_keeps_the_newest_signal_and_the_clock(self):
        self._seed([("a", "index", 1), ("a", "retire", 2), ("b", "index", 3)], bootstrap=("old-model",))
        semantic.ensure_schema(self.db)
        self._assert_migrated()
        rows = dict(self.db.execute("SELECT record_id, kind FROM semantic_work"))
        self.assertEqual(rows, {"a": "retire", "b": "index"})  # newest legacy signal wins per record
        value = self.db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]
        self.assertGreaterEqual(value, 3)
        self.assertEqual(self.db.execute("SELECT model, seq FROM semantic_progress").fetchone()[:2],
                         ("old-model", value))  # seeded progress: no re-bootstrap on upgrade

    def test_missing_singleton_is_repaired_above_existing_revisions(self):
        self.db.close()
        db = sqlite3.connect(self.path)
        db.executescript("""DROP TABLE IF EXISTS semantic_work; DROP TABLE IF EXISTS semantic_bootstrap;
            CREATE TABLE semantic_work(record_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
            seq INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL);
            CREATE TABLE semantic_seq(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL);
            CREATE TABLE semantic_history(seq INTEGER PRIMARY KEY, kind TEXT NOT NULL, record_id TEXT NOT NULL);
            CREATE TABLE semantic_progress(model TEXT PRIMARY KEY, seq INTEGER NOT NULL, updated_at TEXT NOT NULL);""")
        db.execute("INSERT INTO semantic_history VALUES(41,'index','x')")
        db.execute("INSERT INTO semantic_history VALUES(42,'retire','y')")
        db.execute("INSERT INTO semantic_work VALUES('y','retire',42,0,0,'2026-09-01T00:00:00Z')")
        db.commit()
        semantic.ensure_schema(db)  # the singleton row vanished while durable revisions remain
        value = db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]
        self.assertGreaterEqual(value, 42)
        next_seq = semantic._signal(db, "index", "z")
        self.assertGreater(next_seq, 42)  # the next revision is strictly newer than anything durable
        db.close()

    def test_setup_never_commits_a_transaction_the_caller_owns(self):
        # DML opens the caller's transaction; if setup committed implicitly, the row and
        # its own DDL would survive a rollback the caller performs afterwards.
        self.db.execute("INSERT INTO semantic_work VALUES('hold','index',9,0,0,'x')")
        self.db.execute("DELETE FROM semantic_bootstrap")  # DML begins an explicit transaction
        self.assertTrue(self.db.in_transaction)
        semantic.ensure_schema(_FailOnStatement(self.db, "no-such-fragment"))
        self.assertTrue(self.db.in_transaction)  # executescript's implicit commit must not fire
        self.db.rollback()
        self.assertEqual(self.db.execute(
            "SELECT count(*) FROM semantic_work WHERE record_id='hold'").fetchone()[0], 0)
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='semantic_seq'").fetchone())  # DDL rolled back too

    def test_injected_migration_failure_leaves_no_half_migrated_schema(self):
        self._seed([("a", "index", 1), ("a", "retire", 2)])
        self.db.execute("BEGIN IMMEDIATE")
        with self.assertRaises(RuntimeError):
            semantic.ensure_schema(_FailOnStatement(self.db, "RENAME TO semantic_work"))
        self.db.rollback()
        legacy = {r[1] for r in self.db.execute("PRAGMA table_info(semantic_work)")}
        self.assertNotIn("seq", legacy)  # nothing durable changed shape
        self.assertIsNone(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='semantic_work_v2'").fetchone())
        semantic.ensure_schema(self.db)  # retry converges on the migrated shape
        self.db.commit()
        self._assert_migrated()


if __name__ == "__main__":
    unittest.main()
