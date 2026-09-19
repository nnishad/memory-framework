"""R3 regression: accelerator publication is failure-safe.

Publication into the running HNSW accelerator is distinct from persisting
durable vectors. In-memory bookkeeping must be published only after the backend
confirms the insertion, so a failed ``add_items`` cannot leave the record
marked complete while the accelerator never received the vector. Because a
backend may mutate before throwing, an ambiguous insertion failure marks the
running accelerator unhealthy and rebuilds it from valid durable chunks - never
re-embedding, never duplicating and never resurrecting a retired label.

``hnswlib`` is intentionally absent from the deterministic environment, so a
fault-injecting fake accelerator module is installed for these tests; the exact
numpy fallback path is exercised separately.
"""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from personal_memory.common import digest
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


def note(source_id, text="the garage engine repairs"):
    return {"source": "bench", "source_id": source_id, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text, "metadata": {}}


class FakeIndex:
    """A brute-force cosine index that can be told to fail at each backend step."""

    def __init__(self, space, dim, control):
        self.space, self.dim, self.control = space, dim, control
        self._max = 0
        self._vecs = {}
        self.adds = 0

    def init_index(self, max_elements, ef_construction, M, random_seed):
        if self.control.get("fail_init"):
            raise RuntimeError("accelerator initialization failed")
        self._max = self.control.get("max_elements", max_elements)

    def set_num_threads(self, n):
        pass

    def get_current_count(self):
        return len(self._vecs)

    def get_max_elements(self):
        return self._max

    def resize_index(self, n):
        if self.control.get("fail_first_resize") and not self.control.get("resized"):
            self.control["resized"] = True
            raise RuntimeError("accelerator resize failed")
        self._max = n

    def add_items(self, vectors, ids):
        self.adds += 1
        label = ids[0]
        if label in self._vecs:
            raise KeyError("label already exists")     # hnswlib rejects duplicates
        if self.control.get("fail_first_add") and "inserted" not in self.control:
            if self.control.get("mutate_then_throw"):
                self._vecs[label] = vectors[0].copy()   # backend mutated, then threw
            self.control["inserted"] = True
            raise RuntimeError("accelerator insertion failed")
        self._vecs[label] = vectors[0].copy()

    def mark_deleted(self, label):
        self._vecs.pop(label, None)

    def set_ef(self, n):
        pass

    def knn_query(self, vector, k=1, filter=None, num_threads=1):
        query = vector[0]
        scored = []
        for label, v in self._vecs.items():
            if filter is not None and not filter(label):
                continue
            scored.append((label, float(v @ query)))
        scored.sort(key=lambda x: -x[1])
        top = scored[:k]
        return (np.array([[s[0] for s in top]], dtype=np.int64),
                np.array([[1.0 - s[1] for s in top]], dtype=np.float32))


def hnsw_module(control):
    module = types.ModuleType("hnswlib")
    def Index(space, dim):
        return FakeIndex(space, dim, control)
    module.Index = Index
    return module


class Embedder:
    key = "publication-model-v1"

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


class AcceleratorPublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.embedder = Embedder()

    def put(self, source_id, text="the garage engine repairs"):
        self.store.ingest([note(source_id, text=text)])
        return "rec_" + digest(["bench", source_id, "1"])[:32]

    def build(self, control):
        with patch.dict(sys.modules, {"hnswlib": hnsw_module(control)}):
            return SemanticIndex(self.store, embedder=self.embedder)

    def clear_backoff(self):
        with self.store.connect() as db:
            db.execute("UPDATE semantic_model_work SET available_at=0")
            db.execute("UPDATE semantic_work SET available_at=0")

    def vector_count(self, record_id=None, model=None):
        sql = "SELECT COUNT(*) FROM vector_chunks WHERE 1=1"
        arguments = []
        if record_id:
            sql += " AND record_id=?"; arguments.append(record_id)
        if model:
            sql += " AND model=?"; arguments.append(model)
        with self.store.connect() as db:
            return db.execute(sql, arguments).fetchone()[0]

    def done(self, rid):
        with self.store.connect() as db:
            return db.execute("SELECT 1 FROM vector_done WHERE model=? AND record_id=?",
                              (Embedder.key, rid)).fetchone() is not None

    def test_failed_insertion_retries_without_duplicate_or_reembedding(self):
        rid = self.put("a")
        control = {"fail_first_add": True}
        engine = self.build(control)
        # The first publication attempt fails inside the accelerator.
        engine.sync(batch=64)
        self.assertEqual(1, self.vector_count(rid))           # vectors persisted durably
        self.assertEqual([], engine.candidates("garage engine", 5))  # not searchable yet
        self.assertFalse(engine.status()["ready"])            # publication incomplete is not ready
        self.assertFalse(self.done(rid))                      # never marked complete
        # Retrying completes publication from the stored vectors, no re-embedding.
        self.clear_backoff()
        while engine.sync(batch=64):
            pass
        self.assertEqual(1, len(self.embedder.embedded))      # never re-embedded
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])
        self.assertEqual(set(engine.rows), set(engine.index._vecs))  # bookkeeping matches backend
        self.assertTrue(engine.status()["ready"])

    def test_partial_mutation_rebuilds_the_accelerator(self):
        rid = self.put("a")
        control = {"fail_first_add": True, "mutate_then_throw": True}
        engine = self.build(control)
        engine.sync(batch=64)
        self.assertFalse(engine.status()["ready"])
        self.clear_backoff()
        while engine.sync(batch=64):
            pass
        # A rebuild replaced the object so the stale mutated label cannot linger
        # untracked, and the record is genuinely searchable exactly once.
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])
        self.assertEqual(1, engine.index.get_current_count())
        self.assertEqual(set(engine.rows), set(engine.index._vecs))
        self.assertEqual(1, len(self.embedder.embedded))

    def test_initialization_failure_keeps_work_retryable(self):
        rid = self.put("a")
        control = {"fail_init": True}
        engine = self.build(control)
        engine.sync(batch=64)
        self.assertIsNone(engine.index)                        # never assigned a broken object
        self.assertFalse(engine.status()["ready"])
        self.assertEqual(0, len(engine.rows))                  # no half-published bookkeeping
        self.assertFalse(self.done(rid))                       # not acknowledged complete
        control["fail_init"] = False
        self.clear_backoff()
        while engine.sync(batch=64):
            pass
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])
        self.assertTrue(engine.status()["ready"])

    def test_resize_failure_keeps_work_retryable(self):
        first = self.put("a")
        second = self.put("b")
        control = {"max_elements": 1, "fail_first_resize": True}
        engine = self.build(control)
        while engine.sync(batch=64):
            pass
        # The record that needed the resize stays retryable; the other is intact.
        self.assertFalse(engine.status()["ready"])
        pending = [rid for rid in (first, second) if not engine.candidates("garage engine", 5)
                   or rid not in [c["id"] for c in engine.candidates("garage engine", 5)]]
        self.assertEqual(1, len(pending))
        self.clear_backoff()
        while engine.sync(batch=64):
            pass
        found = {c["id"] for c in engine.candidates("garage engine", 5)}
        self.assertEqual({first, second}, found)
        self.assertEqual(2, engine.index.get_current_count())
        self.assertTrue(engine.status()["ready"])

    def test_rebuild_does_not_resurrect_a_retired_label(self):
        keep = self.put("keep")
        gone = self.put("gone")
        control = {"fail_first_add": True, "mutate_then_throw": True}
        engine = self.build(control)
        engine.sync(batch=64)                                  # first attempt fails ambiguously
        self.store.forget(gone)                                # retire while unhealthy
        self.clear_backoff()
        while engine.sync(batch=64):
            pass
        self.assertEqual(0, self.vector_count(gone))
        retired_labels = {label for label, row in engine.rows.items() if row[0] == gone}
        self.assertEqual(set(), retired_labels)
        self.assertEqual(set(engine.rows), set(engine.index._vecs))

    def test_exact_vector_path_keeps_maps_consistent(self):
        # With no accelerator installed, bookkeeping stays consistent with the
        # exact-vector map and a record becomes searchable after one sync.
        sys.modules.pop("hnswlib", None)
        engine = SemanticIndex(self.store, embedder=self.embedder)
        self.assertIsNone(engine.hnsw)
        rid = self.put("a")
        while engine.sync(batch=64):
            pass
        self.assertEqual(set(engine.rows), set(engine.vectors))
        self.assertEqual([rid], [c["id"] for c in engine.candidates("garage engine", 5)])


if __name__ == "__main__":
    unittest.main()
