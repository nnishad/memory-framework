"""Durable semantic queue: desired-state resolution and publication recovery.

The queue carries one monotonically revised desired-state row per record, so
coalescing can never lose the newest signal (ingest -> remove -> restore), and
acknowledgment only consumes the revision actually processed. Durable vector
creation is distinct from publication into the running search index: a failed
publication keeps retryable work, retries from stored vectors without another
embedding call, is idempotent over partially added chunks, and is reported as
not ready until it lands.
"""
import contextlib
import tempfile
import time
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
    key = "state-model-v1"

    def __init__(self):
        self.embedded = []
        self.on_embed = None

    def chunks(self, text):
        yield 0, len(text), text

    def documents(self, texts):
        for text in texts:
            if self.on_embed:
                self.on_embed(text)
            self.embedded.append(text)
        return [[1.0, 0.5, 0.25] for _ in texts]

    def query(self, text):
        return [1.0, 0.5, 0.25]


class TwoChunkEmbedder(FakeEmbedder):
    key = "state-model-split"

    def chunks(self, text):
        mid = len(text) // 2
        yield 0, mid, text[:mid]
        yield mid, len(text), text[mid:]


class QueueStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.embedder = FakeEmbedder()

    def engine(self, embedder=None):
        return SemanticIndex(self.store, embedder=embedder or self.embedder)

    def put(self, source_id, text="the garage engine repairs"):
        self.store.ingest([note(source_id, text=text)])
        return "rec_" + digest(["bench", source_id, "1"])[:32]

    def remove(self, rid):
        """Mirror the canonical remove mutator: hide the record, enqueue retire."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
            semantic.enqueue(db, "retire", [rid])

    def restore(self, rid):
        """Mirror the source-sync restore: unhide, re-request indexing in one tx."""
        with self.store.lock, self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (rid,))
            semantic.enqueue(db, "index", [rid])

    def work_rows(self, kind=None):
        sql = "SELECT * FROM semantic_work" + (" WHERE kind=?" if kind else "")
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(sql, (kind,) if kind else ())]

    def model_work_rows(self, model):
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(
                "SELECT * FROM semantic_model_work WHERE model=?", (model,))]

    def vector_count(self, record_id=None, model=None):
        sql = "SELECT COUNT(*) FROM vector_chunks WHERE 1=1"
        arguments = []
        if record_id:
            sql += " AND record_id=?"; arguments.append(record_id)
        if model:
            sql += " AND model=?"; arguments.append(model)
        with self.store.connect() as db:
            return db.execute(sql, arguments).fetchone()[0]

    def done(self, engine, rid):
        with self.store.connect() as db:
            return db.execute("SELECT 1 FROM vector_done WHERE model=? AND record_id=?",
                              (engine.key, rid)).fetchone() is not None

    def drain(self, engine, limit=20):
        for _ in range(limit):
            # Backoff is durable wall-clock state; make retries immediate so a
            # drain loop terminates without sleeping through exponential delays.
            with self.store.connect() as db:
                db.execute("UPDATE semantic_work SET available_at=0 WHERE attempts>0")
                db.execute("UPDATE semantic_model_work SET available_at=0 WHERE attempts>0")
            if not engine.sync(batch=64):
                return
        self.fail("queue did not drain")

    def test_ingest_remove_restore_before_first_indexing_pass(self):
        rid = self.put("a")
        self.remove(rid)
        self.restore(rid)                 # all three signals precede any sync()
        engine = self.engine()
        self.drain(engine)
        self.assertEqual(1, self.vector_count(rid))     # the restore was not swallowed
        self.assertEqual([], self.work_rows())
        self.assertTrue(engine.status()["ready"])
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])

    def test_repeated_remove_restore_cycles_converge(self):
        rid = self.put("a")
        engine = self.engine()
        self.drain(engine)
        for _ in range(3):
            self.remove(rid); self.drain(engine)
            self.restore(rid); self.drain(engine)
            self.assertEqual(1, self.vector_count(rid))
            self.assertEqual([], self.work_rows())
        self.remove(rid); self.drain(engine)
        self.assertEqual(0, self.vector_count(rid))     # ending removed stays removed
        self.assertEqual([], self.work_rows())
        self.assertEqual(0, len(engine.rows))

    def test_restore_during_retirement_processing_survives_acknowledgment(self):
        rid = self.put("a")
        engine = self.engine()
        self.drain(engine)
        original = engine._retire_record
        def retire_then_restore(db, record_id):
            original(db, record_id)
            # The restore commits inside the retirement transaction, before its
            # acknowledgment runs: the newer revision must not be acknowledged away.
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (record_id,))
            semantic.enqueue(db, "index", [record_id])
        engine._retire_record = retire_then_restore
        self.remove(rid)
        # Fail publication so, if the newer revision survives the retirement
        # acknowledgment, its retryable row remains as visible evidence.
        real_add = engine._add
        engine._add = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish down"))
        engine.sync(batch=64)                            # processes the retirement
        engine._add = real_add
        rows = self.model_work_rows(engine.key)
        self.assertEqual([rid], [r["record_id"] for r in rows])  # restore survived the ack
        self.assertEqual([], self.work_rows("index"))            # the shared signal was distributed
        self.assertEqual(1, rows[0]["attempts"])         # fresh revision, not the swallowed one
        self.drain(engine)
        self.assertEqual(1, self.vector_count(rid))
        self.assertEqual([], self.work_rows())

    def test_redundant_and_replayed_requests_leave_no_queue_entries(self):
        rid = self.put("a")
        engine = self.engine()
        self.drain(engine)
        before = list(self.embedder.embedded)
        self.restore(rid)          # metadata-style no-op update: record was never hidden
        self.restore(rid)          # identical replay
        self.drain(engine)
        self.assertEqual([], self.work_rows())           # consumed, not permanent entries
        self.assertEqual(before, self.embedder.embedded) # and never re-embedded
        self.assertTrue(self.done(engine, rid))

    def test_new_signal_during_embedding_survives_older_work_completion(self):
        rid = self.put("a")
        engine = self.engine()
        def retire_mid_embed(text):
            self.remove(rid)
        self.embedder.on_embed = retire_mid_embed
        self.drain(engine)
        self.embedder.on_embed = None
        self.assertEqual(0, self.vector_count(rid))      # the newer removal won
        self.assertEqual([], self.work_rows())

    def test_publication_failure_keeps_retryable_work(self):
        rid = self.put("a")
        engine = self.engine()
        def refuse(rows_argument=None):
            raise RuntimeError("index publish failed")
        engine._add = refuse
        engine.sync(batch=64)
        rows = self.model_work_rows(engine.key)
        self.assertEqual([rid], [r["record_id"] for r in rows])  # work retained, not acked
        self.assertEqual([], self.work_rows("index"))            # the shared signal was distributed away
        self.assertGreater(rows[0]["attempts"], 0)
        self.assertGreater(rows[0]["available_at"], time.time())  # bounded retry
        self.assertEqual(1, self.vector_count(rid))               # vectors stayed durable
        self.assertFalse(self.done(engine, rid))              # never marked complete
        status = engine.status()
        self.assertEqual(1, status["pending_records"])
        self.assertFalse(status["ready"])                          # failed publication is not ready

    def test_publication_retry_completes_from_stored_vectors(self):
        rid = self.put("a")
        engine = self.engine()
        real_add = engine._add
        engine._add = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish down"))
        engine.sync(batch=64)
        self.assertEqual(1, len(self.embedder.embedded))
        engine._add = real_add
        with self.store.connect() as db:
            db.execute("UPDATE semantic_model_work SET available_at=0")
        self.drain(engine)
        self.assertEqual(1, len(self.embedder.embedded))  # retry never re-embedded
        self.assertEqual([], self.work_rows())
        self.assertEqual([], self.model_work_rows(engine.key))
        self.assertTrue(self.done(engine, rid))
        self.assertTrue(engine.status()["ready"])
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])
        self.assertEqual(1, len(engine.rows))             # exactly one index entry per chunk

    def test_restart_after_durable_vectors_publishes_without_embedding(self):
        rid = self.put("a")
        engine = self.engine()
        engine._add = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish down"))
        engine.sync(batch=64)
        fresh = FakeEmbedder()
        restarted = self.engine(embedder=fresh)           # vectors load from durable storage
        self.drain(restarted)
        self.assertEqual([], fresh.embedded)               # no embedding repeated
        self.assertEqual([], self.work_rows())
        self.assertTrue(restarted.status()["ready"])
        self.assertEqual([rid], [c["id"] for c in restarted.candidates("garage engine", 5)])

    def test_retirement_during_publication_retry_is_not_resurrected(self):
        rid = self.put("a")
        engine = self.engine()
        engine._add = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish down"))
        engine.sync(batch=64)
        self.remove(rid)                                   # record retires while retry pending
        self.drain(engine)
        self.assertEqual(0, self.vector_count(rid))
        self.assertEqual([], self.work_rows())
        self.assertEqual(0, len(engine.rows))              # no lingering in-memory entries
        self.assertEqual(0, engine.sync(batch=64))         # nothing to resurrect

    def test_partially_published_chunks_complete_without_duplicates(self):
        rid = self.put("a", text="the garage engine needs new brake pads entirely")
        embedder = TwoChunkEmbedder()
        engine = self.engine(embedder=embedder)
        real_add = engine._add
        calls = {"n": 0}
        def flaky(row):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("index disappeared mid-publish")
            real_add(row)
        engine._add = flaky
        engine.sync(batch=64)
        self.assertEqual(2, self.vector_count(rid))        # both chunks are durable
        self.assertEqual(1, len(engine.rows))              # first chunk published, second failed
        self.assertGreater(len(self.model_work_rows(engine.key)), 0)  # retryable work remains
        engine._add = real_add
        with self.store.connect() as db:
            db.execute("UPDATE semantic_model_work SET available_at=0")
        self.drain(engine)
        self.assertEqual(2, len(engine.rows))              # exactly two entries, no duplicates
        self.assertEqual(1, len({row[0] for row in engine.rows.values()}))
        self.assertEqual([], self.work_rows())
        self.assertTrue(engine.status()["ready"])


if __name__ == "__main__":
    unittest.main()
