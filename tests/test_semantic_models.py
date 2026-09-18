"""Per-model-revision indexing progress replaces the permanent bootstrap.

The durable queue is shared across model revisions while completion is
per-revision, so each model keeps its own position in an append-only change
history. Activating a model reconciles exactly the changes that happened while
it was inactive - never by rescanning the archive on every tick, and never by
assuming one permanent per-revision bootstrap marker. When retained history is
insufficient, activation falls back to a safe canonical reconciliation.
"""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from personal_memory import semantic
from personal_memory.common import digest
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


def note(source_id, text="the garage engine repairs"):
    return {"source": "bench", "source_id": source_id, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text, "metadata": {}}


class FakeEmbedder:
    key = "model-alpha"

    def __init__(self):
        self.embedded = []

    def chunks(self, text):
        yield 0, len(text), text

    def documents(self, texts):
        for text in texts:
            self.embedded.append(text)
        return [[1.0, 0.5, 0.25] for _ in texts]

    def query(self, text):
        return [1.0, 0.5, 0.25]


class ModelProgressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "memory.db"
        self.store = Store(self.db_path)

    def engine(self, embedder):
        return SemanticIndex(self.store, embedder=embedder)

    def put(self, source_id):
        self.store.ingest([note(source_id)])
        return "rec_" + digest(["bench", source_id, "1"])[:32]

    def work_rows(self, kind=None):
        sql = "SELECT * FROM semantic_work" + (" WHERE kind=?" if kind else "")
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(sql, (kind,) if kind else ())]

    def vector_count(self, record_id=None, model=None):
        sql = "SELECT COUNT(*) FROM vector_chunks WHERE 1=1"
        arguments = []
        if record_id:
            sql += " AND record_id=?"; arguments.append(record_id)
        if model:
            sql += " AND model=?"; arguments.append(model)
        with self.store.connect() as db:
            return db.execute(sql, arguments).fetchone()[0]

    def drain(self, engine, limit=25):
        for _ in range(limit):
            with self.store.connect() as db:
                db.execute("UPDATE semantic_work SET available_at=0 WHERE attempts>0")
            if not engine.sync(batch=64):
                return
        self.fail("queue did not drain")

    def traced_sync(self, engine, batch=64):
        statements = []
        original = self.store.connect

        @contextlib.contextmanager
        def connect():
            with original() as db:
                db.set_trace_callback(statements.append)
                yield db

        self.store.connect = connect
        try:
            processed = engine.sync(batch=batch)
        finally:
            self.store.connect = original
        return processed, statements

    def test_switch_a_b_a_captures_records_added_while_a_inactive(self):
        a_ids = [self.put("a1"), self.put("a2")]
        alpha = FakeEmbedder()
        engine_a = self.engine(alpha)
        self.drain(engine_a)
        self.assertEqual(2, self.vector_count(model="model-alpha"))
        # Activate model B; it adopts the archive under its own revision key.
        beta = FakeEmbedder(); beta.key = "model-beta"
        engine_b = self.engine(beta)
        self.drain(engine_b)
        # New evidence arrives while only B's completion state is advancing.
        late = self.put("a3")
        self.drain(engine_b)
        self.assertEqual(1, self.vector_count(late, model="model-beta"))
        self.assertEqual(0, self.vector_count(late, model="model-alpha"))
        # Re-activating A must reconcile exactly the missed change - not the
        # whole archive, and not nothing.
        alpha2 = FakeEmbedder()
        engine_a2 = self.engine(alpha2)
        self.assertEqual([late], [r["record_id"] for r in self.work_rows("index")])
        self.drain(engine_a2)
        self.assertEqual(1, self.vector_count(late, model="model-alpha"))
        self.assertEqual(1, len(alpha2.embedded))  # only the missed record was embedded

    def test_retirements_while_inactive_are_replayed_on_reactivation(self):
        keep = self.put("keep")
        gone = self.put("gone")
        alpha = FakeEmbedder()
        engine_a = self.engine(alpha)
        self.drain(engine_a)
        beta = FakeEmbedder(); beta.key = "model-beta"
        engine_b = self.engine(beta)
        self.drain(engine_b)
        self.store.forget(gone)                      # retire under B only
        self.drain(engine_b)
        reopened = FakeEmbedder()
        engine_a2 = self.engine(reopened)            # A returns
        self.drain(engine_a2)
        self.assertEqual(0, self.vector_count(gone, model="model-alpha"))
        self.assertEqual(1, self.vector_count(keep, model="model-alpha"))
        self.assertEqual([], self.work_rows())

    def test_interrupted_activation_and_restart_are_safe(self):
        rid = self.put("x")
        rid2 = self.put("y")
        alpha = FakeEmbedder()
        engine_a = self.engine(alpha)
        self.drain(engine_a)
        original = semantic._signal
        state = {"n": 0}
        def interrupt(db, kind, record_id, **kwargs):
            state["n"] += 1
            if state["n"] == 2:
                raise RuntimeError("activation crashed midway")
            return original(db, kind, record_id, **kwargs)
        semantic._signal = interrupt
        try:
            beta = FakeEmbedder(); beta.key = "model-beta"
            with self.assertRaises(RuntimeError):
                # Any failure inside activation rolls the whole reconcile back.
                self.engine(beta)
        finally:
            semantic._signal = original
        with self.store.connect() as db:
            partial = db.execute("SELECT COUNT(*) FROM semantic_work WHERE kind='index'"
                                 " AND record_id IN (SELECT id FROM records)").fetchone()[0]
        self.assertEqual(0, partial)                     # nothing half-published survived
        beta2 = FakeEmbedder(); beta2.key = "model-beta"
        engine_b = self.engine(beta2)                # restart resumes cleanly
        self.drain(engine_b)
        self.assertEqual(1, self.vector_count(rid, model="model-beta"))
        self.assertEqual(1, self.vector_count(rid2, model="model-beta"))
        self.assertEqual([], self.work_rows())

    def test_reopening_an_unchanged_model_does_no_extra_work(self):
        for index in range(3):
            self.put("r%d" % index)
        alpha = FakeEmbedder()
        engine = self.engine(alpha)
        self.drain(engine)
        reopened = self.engine(FakeEmbedder())       # identical key, fresh object
        processed, statements = self.traced_sync(reopened)
        self.assertEqual(0, processed)
        self.assertEqual([], self.work_rows())
        self.assertEqual([], [s for s in statements if "FROM records" in s])  # no rescan

    def test_history_gap_falls_back_to_canonical_reconciliation(self):
        rid = self.put("gap")
        alpha = FakeEmbedder()
        engine = self.engine(alpha)
        self.drain(engine)
        # Simulate aggressive retention pruning: the history this model would
        # replay is gone, so activation must reconcile from canonical state.
        beta = FakeEmbedder(); beta.key = "model-beta"
        engine_b = self.engine(beta)
        self.drain(engine_b)
        with self.store.lock, self.store.connect() as db:
            db.execute("DELETE FROM semantic_history WHERE seq <= ("
                       "SELECT seq FROM semantic_progress WHERE model=?)", ("model-alpha",))
            db.execute("INSERT INTO semantic_history VALUES(999999,'index','rec_unseen')")
        late = self.put("after")
        reopened = FakeEmbedder()
        engine_a2 = self.engine(reopened)
        self.drain(engine_a2)
        self.assertEqual(1, self.vector_count(rid, model="model-alpha"))
        self.assertEqual(1, self.vector_count(late, model="model-alpha"))
        self.assertEqual([], self.work_rows("retire"))

    def test_legacy_bootstrap_markers_and_queue_migrate_in_place(self):
        rid = self.put("legacy")
        hidden = self.put("hidden")
        # Build the pre-revision queue shape exactly as the old code wrote it.
        with self.store.lock, self.store.connect() as db:
            db.execute("DROP INDEX semantic_work_due")
            db.execute("DROP TABLE semantic_work")
            db.executescript("""
                CREATE TABLE semantic_work(
                  id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN('index','retire')),
                  record_id TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                  available_at REAL NOT NULL DEFAULT 0, updated_at TEXT NOT NULL,
                  UNIQUE(kind, record_id));
                CREATE INDEX semantic_work_due ON semantic_work(kind, available_at, id);
                INSERT INTO semantic_work(kind,record_id,updated_at) VALUES('index','rec_stale_pending','x');
                INSERT INTO semantic_work(kind,record_id,updated_at) VALUES('retire','rec_stale_pending','x');
                INSERT INTO semantic_bootstrap VALUES('model-alpha','x');
            """)
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (hidden,))
        alpha = FakeEmbedder()
        engine = self.engine(alpha)
        # The newest legacy signal per record survives as the desired state...
        self.assertEqual({"rec_stale_pending"}, {r["record_id"] for r in self.work_rows()})
        self.assertEqual("retire", self.work_rows()[0]["kind"])
        # ...and the existing marker prevents a pointless archive re-bootstrap.
        self.assertEqual([], [r for r in self.work_rows("index") if r["record_id"] == rid])
        self.drain(engine)
        self.assertEqual(0, engine.sync(batch=64))
        self.assertEqual([], self.work_rows())

    def test_model_configuration_identity_is_revision_scoped(self):
        from personal_memory.semantic import EndpointEmbedder
        base = {"provider": "http", "url": "http://127.0.0.1:9/v1", "model": "ops-minilm"}
        one = EndpointEmbedder(base)
        changed = EndpointEmbedder({**base, "revision": "2026-09-01"})
        self.assertNotEqual(one.key, changed.key)
        self.assertEqual(one.key, EndpointEmbedder({**base}).key)


if __name__ == "__main__":
    unittest.main()
