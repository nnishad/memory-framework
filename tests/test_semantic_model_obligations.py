"""R2 regression: an unfinished indexing obligation is durable per model.

The shared queue row is a signal to distribute, not proof that any model
finished. These tests reproduce the review's exact failure - ingest one record,
initialize A without syncing, initialize B and sync it, then reopen A - and prove
A still returns the record as a candidate after its own sync. They also prove one
model's failure and backoff never postpone, erase or re-trigger another model's
work, and that a stale legacy completion watermark cannot suppress a canonical
reconciliation of a model that never actually finished.
"""
import tempfile
import time
import unittest
from pathlib import Path

from personal_memory.common import digest
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


def note(source_id, text="the garage engine repairs"):
    return {"source": "bench", "source_id": source_id, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text, "metadata": {}}


class RecordingEmbedder:
    """Counts every embedding call and can fail on demand to exercise backoff."""

    def __init__(self, key, fail=False):
        self.key = key
        self.embedded = []
        self.fail = fail

    def chunks(self, text):
        yield 0, len(text), text

    def documents(self, texts):
        out = []
        for text in texts:
            if self.fail:
                raise RuntimeError("embedding endpoint is down")
            self.embedded.append(text)
            out.append([1.0, 0.5, 0.25])
        return out

    def query(self, text):
        return [1.0, 0.5, 0.25]


class ModelObligationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")

    def engine(self, embedder):
        return SemanticIndex(self.store, embedder=embedder)

    def put(self, source_id, text="the garage engine repairs"):
        self.store.ingest([note(source_id, text=text)])
        return "rec_" + digest(["bench", source_id, "1"])[:32]

    def vector_count(self, record_id=None, model=None):
        sql = "SELECT COUNT(*) FROM vector_chunks WHERE 1=1"
        arguments = []
        if record_id:
            sql += " AND record_id=?"; arguments.append(record_id)
        if model:
            sql += " AND model=?"; arguments.append(model)
        with self.store.connect() as db:
            return db.execute(sql, arguments).fetchone()[0]

    def model_work(self, model):
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM semantic_model_work WHERE model=?", (model,))]

    def shared_work(self, kind=None):
        sql = "SELECT * FROM semantic_work" + (" WHERE kind=?" if kind else "")
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(sql, (kind,) if kind else ())]

    def done(self, model, record_id):
        with self.store.connect() as db:
            return db.execute("SELECT 1 FROM vector_done WHERE model=? AND record_id=?",
                              (model, record_id)).fetchone() is not None

    def clear_backoff(self):
        with self.store.connect() as db:
            db.execute("UPDATE semantic_model_work SET available_at=0")
            db.execute("UPDATE semantic_work SET available_at=0")

    def drain(self, engine, limit=25):
        for _ in range(limit):
            self.clear_backoff()
            if not engine.sync(batch=64):
                return
        self.fail("queue did not drain")

    def test_model_a_recovers_the_record_the_review_said_was_lost(self):
        rid = self.put("solo")
        # A initializes but never syncs: its obligation must be durable.
        alpha = RecordingEmbedder("model-alpha")
        self.engine(alpha)
        # B initializes and fully syncs, consuming the shared signal.
        beta = RecordingEmbedder("model-beta")
        engine_b = self.engine(beta)
        self.drain(engine_b)
        self.assertEqual(1, self.vector_count(rid, model="model-beta"))
        self.assertEqual([], self.shared_work("index"))   # B drained the shared inbox
        # Reopening A: its obligation survived B consuming the shared signal.
        alpha2 = RecordingEmbedder("model-alpha")
        engine_a2 = self.engine(alpha2)
        self.assertEqual([rid], [r["record_id"] for r in self.model_work("model-alpha")])
        self.assertFalse(engine_a2.status()["ready"])     # A is not yet complete
        self.drain(engine_a2)
        self.assertEqual(1, self.vector_count(rid, model="model-alpha"))
        self.assertEqual([rid], [c["id"] for c in engine_a2.candidates("garage engine", 5)])
        self.assertEqual(1, len(alpha2.embedded))         # A embedded it exactly once

    def test_model_a_failure_does_not_postpone_or_reset_model_b(self):
        rid = self.put("solo")
        # A's embedding endpoint is down; B completes the same record cleanly.
        alpha = RecordingEmbedder("model-alpha", fail=True)
        engine_a = self.engine(alpha)
        beta = RecordingEmbedder("model-beta")
        engine_b = self.engine(beta)
        self.drain(engine_b)
        self.assertEqual(1, self.vector_count(rid, model="model-beta"))
        self.assertEqual(1, len(beta.embedded))
        # A's attempt fails and backs off, without touching B's completed state.
        self.clear_backoff()
        engine_a.sync(batch=64)
        rows = self.model_work("model-alpha")
        self.assertEqual([rid], [r["record_id"] for r in rows])
        self.assertGreater(rows[0]["attempts"], 0)
        self.assertGreater(rows[0]["available_at"], time.time())
        self.assertEqual(1, self.vector_count(rid, model="model-beta"))
        self.assertEqual(1, len(beta.embedded))           # B did not re-embed for A
        self.assertTrue(self.done("model-beta", rid))
        # Recovering A embeds once and returns the candidate; B stays untouched.
        alpha.fail = False
        self.drain(engine_a)
        self.assertEqual(1, self.vector_count(rid, model="model-alpha"))
        self.assertEqual([rid], [c["id"] for c in engine_a.candidates("garage engine", 5)])
        self.assertEqual(1, len(beta.embedded))           # B never re-embedded

    def test_legacy_watermark_claiming_completion_still_reconciles(self):
        rid = self.put("solo")
        # Model B completed; model A carries a legacy progress watermark falsely
        # claiming it caught up, yet A has no vectors, no obligation and no
        # replay history - the exact state the review found unrecoverable.
        beta = RecordingEmbedder("model-beta")
        engine_b = self.engine(beta)
        self.drain(engine_b)
        self.assertEqual(1, self.vector_count(rid, model="model-beta"))
        with self.store.lock, self.store.connect() as db:
            high = db.execute("SELECT value FROM semantic_seq WHERE id=1").fetchone()[0]
            db.execute("INSERT OR REPLACE INTO semantic_progress VALUES(?,?,?)",
                       ("model-alpha", high, "2026-09-01T00:00:00Z"))
            db.execute("DELETE FROM semantic_model_work WHERE model='model-alpha'")
            db.execute("DELETE FROM semantic_history")
            db.execute("DELETE FROM semantic_work WHERE kind='index'")
        # Reopening A must reconcile from canonical state, not the stale watermark.
        alpha = RecordingEmbedder("model-alpha")
        engine_a = self.engine(alpha)
        self.assertEqual([rid], [r["record_id"] for r in self.model_work("model-alpha")])
        self.drain(engine_a)
        self.assertEqual(1, self.vector_count(rid, model="model-alpha"))
        self.assertEqual([rid], [c["id"] for c in engine_a.candidates("garage engine", 5)])
        self.assertEqual(1, len(alpha.embedded))


if __name__ == "__main__":
    unittest.main()
