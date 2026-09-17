"""Incremental semantic work queue (workstream 7).

Index and retirement work is enqueued inside the canonical transaction and
processed in bounded batches; idle polls touch only the queue, never the
archive. Bootstrap covers a pre-existing archive exactly once per model
revision. Final retrieval visibility checks stay independent of index state.
"""
import contextlib
import tempfile
import time
import unittest
from pathlib import Path

from personal_memory.common import digest
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


def note(source_id, text="the garage engine repairs", source="bench"):
    return {"source": source, "source_id": source_id, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text, "metadata": {}}


class FakeEmbedder:
    key = "queue-model-v1"

    def __init__(self):
        self.embedded = []
        self.fail_for = set()
        self.on_embed = None

    def chunks(self, text):
        yield 0, len(text), text

    def documents(self, texts):
        for text in texts:
            for bad in self.fail_for:
                if bad in text:
                    raise RuntimeError("embedding endpoint is down")
            if self.on_embed:
                self.on_embed(text)
            self.embedded.append(text)
        return [[1.0, 0.5, 0.25] for _ in texts]

    def query(self, text):
        return [1.0, 0.5, 0.25]


class SemanticQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.embedder = FakeEmbedder()

    def engine(self, embedder=None):
        return SemanticIndex(self.store, embedder=embedder or self.embedder)

    def put(self, source_id, text="the garage engine repairs"):
        self.store.ingest([note(source_id, text=text)])
        return self.rid(source_id)

    def rid(self, source_id, revision="1"):
        return "rec_" + digest(["bench", source_id, revision])[:32]

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

    def traced_sync(self, engine, batch=16):
        """Run one poll while recording every SQL statement it executes."""
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

    def test_ingest_enqueues_index_work_in_the_canonical_transaction(self):
        self.put("i1"); self.put("i2")
        self.assertEqual(len(self.work_rows("index")), 2)
        # Duplicate replay enqueues nothing new: work coalesces per record.
        self.store.ingest([note("i1")])
        self.assertEqual(len(self.work_rows("index")), 2)
        # A rolled-back batch leaves no orphan work behind.
        with self.assertRaises(ValueError):
            self.store.ingest([note("i3"), {"source": "bench", "source_id": "x", "revision": "1",
                                            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note",
                                            "text": None, "metadata": {}}])
        self.assertEqual(len(self.work_rows("index")), 2)

    def test_idle_poll_touches_only_the_queue(self):
        for index in range(50):
            self.put("r%d" % index)
        engine = self.engine()
        while engine.sync(batch=64):
            pass
        processed, statements = self.traced_sync(engine)
        self.assertEqual(processed, 0)
        archive_scans = [s for s in statements if "FROM records" in s]
        self.assertEqual([], archive_scans)

    def test_failure_keeps_work_durable_and_retries_after_backoff(self):
        good = self.put("good", text="calm waters")
        bad = self.put("bad", text="storm warnings")
        engine = self.engine()
        engine.embedder.fail_for = {"storm"}
        engine.sync(batch=64)
        rows = self.work_rows("index")
        self.assertEqual([bad], [r["record_id"] for r in rows])  # good committed and left the queue
        self.assertGreater(rows[0]["attempts"], 0)
        self.assertGreater(rows[0]["available_at"], time.time())  # durable backoff, not a tight loop
        # A restart loses nothing: the queue is the durable record of work.
        engine.embedder.fail_for = set()
        restarted = self.engine(embedder=engine.embedder)
        with restarted.store.connect() as db:
            db.execute("UPDATE semantic_work SET available_at=0")
        while restarted.sync(batch=64):
            pass
        self.assertEqual(1, self.vector_count(bad))
        self.assertEqual([], self.work_rows())

    def test_restart_does_not_reindex_committed_vectors(self):
        self.put("done", text="already embedded")
        engine = self.engine()
        while engine.sync(batch=64):
            pass
        fresh = FakeEmbedder()
        reopened = self.engine(embedder=fresh)
        processed, _ = self.traced_sync(reopened)
        self.assertEqual(0, processed)
        self.assertEqual([], fresh.embedded)  # no duplicate embedding work

    def test_forget_enqueues_targeted_retirement_without_an_archive_sweep(self):
        a = self.put("a"); b = self.put("b")
        engine = self.engine()
        while engine.sync(batch=64):
            pass
        self.store.forget(a)
        self.assertEqual({a}, {r["record_id"] for r in self.work_rows("retire")})
        processed, statements = self.traced_sync(engine)
        self.assertGreaterEqual(processed, 1)
        self.assertEqual(0, self.vector_count(a))
        self.assertEqual(1, self.vector_count(b))          # neighbours are untouched
        self.assertEqual([], self.work_rows())
        sweeps = [s for s in statements if "WHERE r.deleted=1 OR EXISTS" in s]
        self.assertEqual([], sweeps)

    def test_superseded_evidence_is_revoked_from_the_index(self):
        old = self.put("old", text="passport in the violet folder")
        new = self.put("new", text="passport in the blue drawer")
        engine = self.engine()
        while engine.sync(batch=64):
            pass
        self.store.supersede(old, new)
        while engine.sync(batch=64):
            pass
        self.assertEqual(0, self.vector_count(old))
        self.assertEqual(1, self.vector_count(new))
        self.assertEqual([], self.work_rows())

    def test_retirement_during_embedding_never_publishes_vectors(self):
        live = self.put("live", text="keep this one")
        gone = self.put("gone", text="erase this one")

        def retire_mid_embed(text):
            if text.startswith("erase"):
                self.store.forget(gone)

        self.embedder.on_embed = retire_mid_embed
        engine = self.engine()
        while engine.sync(batch=64):
            pass
        self.assertEqual(0, self.vector_count(gone))
        self.assertEqual(1, self.vector_count(live))
        self.assertEqual([], self.work_rows())  # the doomed record's work is dropped, not retried

    def test_bootstrap_indexes_a_pre_existing_archive_once_per_revision(self):
        ids = [self.put("b%d" % index) for index in range(3)]
        engine = self.engine()
        # The first engine of a revision enqueues the existing archive exactly once.
        self.assertEqual(len(self.work_rows("index")), 3)
        while engine.sync(batch=64):
            pass
        self.assertEqual(3, self.vector_count(model=FakeEmbedder.key))
        later = FakeEmbedder()
        reopened = self.engine(embedder=later)
        self.assertEqual([], self.work_rows("index"))  # no re-bootstrap for the same revision
        self.assertEqual(0, reopened.sync(batch=64))
        changed = FakeEmbedder(); changed.key = "queue-model-v2"
        upgraded = self.engine(embedder=changed)
        self.assertEqual({r["record_id"] for r in self.work_rows("index")}, set(ids))
        while upgraded.sync(batch=64):
            pass
        self.assertEqual(3, self.vector_count(model="queue-model-v2"))

    def test_batches_are_bounded_and_status_reports_the_backlog(self):
        for index in range(10):
            self.put("n%d" % index)
        engine = self.engine()
        self.assertEqual(4, engine.sync(batch=4))
        self.assertLessEqual(len(engine.embedder.embedded), 4)
        self.assertEqual(6, engine.status()["pending_records"])
        while engine.sync(batch=64):
            pass
        self.assertEqual(0, engine.status()["pending_records"])
        self.assertTrue(engine.status()["ready"])


if __name__ == "__main__":
    unittest.main()
