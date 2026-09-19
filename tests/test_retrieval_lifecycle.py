"""R5 regression: Hybrid must own semantic initialization until it stops.

A slow model load (``SemanticIndex`` construction, which activates/reconciles the
durable queue inside its constructor) started on the warm-up or request path must
never resume as an indexing writer after ``close()`` has released the ownership
lease. Close treats an in-progress initializer as owned work: it retains the
lease and reports an incomplete shutdown until the initializer drains, and a
construction that finishes during shutdown is discarded rather than published.

Every test gates the constructor with events so the race is deterministic, and a
second owner competes for the same ``indexing.lock`` to prove ownership.
"""
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from personal_memory.asgi import ProcessLease
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "memory.db")
        # The default-on, lazy semantic path is exercised with a gated fake; scope the
        # kill-switch so construction is enabled regardless of a run-wide setting.
        patcher = patch.dict(os.environ, {"PERSONAL_MEMORY_DISABLE_SEMANTIC": "0",
                                          "PERSONAL_MEMORY_DISABLE_RERANK": "1"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def lock_path(self):
        return self.root / "indexing.lock"

    def build(self, fake_class):
        """A default indexing-owner Hybrid whose semantic engine is built lazily under
        a patched, gated ``SemanticIndex``. No engine is constructed yet."""
        patcher = patch("personal_memory.semantic.SemanticIndex", fake_class)
        patcher.start()
        self.addCleanup(patcher.stop)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=True, index_owner=True)
        self.addCleanup(lambda: self._best_effort_close(backend))
        return backend

    @staticmethod
    def _best_effort_close(backend):
        try:
            backend.close(join_budget=5.0, warmup_grace=0.0)
        except Exception:
            if backend.index_lease is not None:
                backend.index_lease.close()


class Gated(Base):
    """Constructor blocks after entering, before its activation write."""

    def setUp(self):
        super().setUp()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.writes = []
        self.workers = []
        test = self

        class Fake:
            def __init__(self, *args, **kwargs):
                test.entered.set()
                if not test.release.wait(10):
                    raise AssertionError("constructor gate never released")
                # Models the queue/progress activation write the real constructor makes.
                test.writes.append("activate")

            def sync(self, batch=8):
                test.workers.append("sync")
                return False

            def candidates(self, *args, **kwargs):
                return []

            def status(self):
                return {"enabled": True, "ready": True}

            def embedder(self):
                raise NotImplementedError

        self.Fake = Fake


class InitializerOwnershipTests(Gated):
    def test_close_times_out_and_a_second_owner_is_rejected(self):
        backend = self.build(self.Fake)
        builder = threading.Thread(target=backend._ensure_semantic)
        builder.start()
        self.addCleanup(builder.join, 10)
        self.assertTrue(self.entered.wait(5), "lazy construction never began")
        # Shutdown begins while the initializer is mid-construct: with the constructor
        # blocked past the budget, close must report an incomplete shutdown.
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.2, warmup_grace=0.0)
        # Ownership is retained by the still-initializing old writer: a second owner
        # is rejected rather than being handed a released lease.
        with self.assertRaises(RuntimeError):
            ProcessLease(self.lock_path())
        self.release.set()
        builder.join(10)

    def test_no_worker_is_published_during_closing_and_repeat_close_succeeds(self):
        backend = self.build(self.Fake)
        builder = threading.Thread(target=backend._ensure_semantic)
        builder.start()
        self.addCleanup(builder.join, 10)
        self.assertTrue(self.entered.wait(5))
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.2, warmup_grace=0.0)
        # The blocked construction finishes after shutdown had begun.
        self.release.set()
        builder.join(10)
        # A construction that lands during closing is discarded: no engine published,
        # no indexing worker registered against the released resources.
        self.assertIsNone(backend.semantic)
        self.assertEqual([], backend.threads)
        # A later close retries once the initializer drained and releases the lease.
        backend.close(join_budget=5.0, warmup_grace=0.0)
        self.assertIsNone(backend.index_lease)
        released = ProcessLease(self.lock_path())
        released.close()
        # Repeated close after a fully drained lifecycle is harmless.
        backend.close(join_budget=5.0, warmup_grace=0.0)

    def test_second_owner_acquires_only_after_first_fully_drains(self):
        backend = self.build(self.Fake)
        builder = threading.Thread(target=backend._ensure_semantic)
        builder.start()
        self.addCleanup(builder.join, 10)
        self.assertTrue(self.entered.wait(5))
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.2, warmup_grace=0.0)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.lock_path())  # first owner still draining
        self.release.set()
        builder.join(10)
        backend.close(join_budget=5.0, warmup_grace=0.0)  # completes the drain
        second = ProcessLease(self.lock_path())
        second.close()

    def test_no_old_owner_write_after_lease_transfer(self):
        backend = self.build(self.Fake)
        builder = threading.Thread(target=backend._ensure_semantic)
        builder.start()
        self.addCleanup(builder.join, 10)
        self.assertTrue(self.entered.wait(5))
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.2, warmup_grace=0.0)
        self.release.set()
        builder.join(10)
        backend.close(join_budget=5.0, warmup_grace=0.0)
        # Ownership transfers to a new process only now.
        second = ProcessLease(self.lock_path())
        transferred_writes = len(self.writes)
        transferred_workers = len(self.workers)
        # Give any erroneously-resumed old writer time to touch the journal.
        builder.join(0.5)
        threading.Event().wait(0.2)
        self.assertEqual(transferred_writes, len(self.writes),
                         "old owner wrote to the durable journal after lease transfer")
        self.assertEqual(transferred_workers, len(self.workers),
                         "old owner resumed an indexing worker after lease transfer")
        second.close()


class ConstructorFailureDuringClosing(Base):
    def test_constructor_failure_during_closing_still_completes_cleanup(self):
        entered, release = threading.Event(), threading.Event()

        class Fail:
            def __init__(self, *args, **kwargs):
                entered.set()
                release.wait(10)
                raise RuntimeError("injected model load failure")

        patcher = patch("personal_memory.semantic.SemanticIndex", Fail)
        patcher.start()
        self.addCleanup(patcher.stop)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=True, index_owner=True)
        builder = threading.Thread(target=backend._ensure_semantic)
        builder.start()
        self.addCleanup(builder.join, 10)
        self.assertTrue(entered.wait(5))
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.2, warmup_grace=0.0)
        release.set()
        builder.join(10)
        # The constructor raised; the failure is permanent and cleanup still finishes.
        self.assertTrue(backend._semantic_failed)
        backend.close(join_budget=5.0, warmup_grace=0.0)
        self.assertIsNone(backend.index_lease)
        released = ProcessLease(self.lock_path())
        released.close()


class ConcurrentLifecycle(Base):
    def test_concurrent_warmup_search_close_builds_one_engine_no_race(self):
        constructed = []
        gate = threading.Event()

        class Slow:
            def __init__(self, *args, **kwargs):
                constructed.append(1)
                gate.wait(5)

            def sync(self, batch=8):
                return False

            def candidates(self, *args, **kwargs):
                return []

            def status(self):
                return {"enabled": True, "ready": True}

            def embedder(self):
                raise NotImplementedError

            def related_ids(self, *a, **k):
                return set()

        patcher = patch("personal_memory.semantic.SemanticIndex", Slow)
        patcher.start()
        self.addCleanup(patcher.stop)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=True, index_owner=True)
        threads = [threading.Thread(target=backend._ensure_semantic) for _ in range(6)]
        threads.append(threading.Thread(target=backend.start_warmup))
        for t in threads:
            t.start()
        gate.set()
        for t in threads:
            t.join(10)
        self.assertEqual(1, len(constructed), "at most one engine may be constructed")
        # No worker survived the concurrent shutdown below; close cleanly.
        try:
            backend.close(join_budget=5.0, warmup_grace=0.0)
        except RuntimeError:
            if backend.index_lease is not None:
                backend.index_lease.close()


class WarmupAfterClose(Base):
    def test_start_warmup_after_close_does_not_reopen_resources(self):
        constructed = []

        class Fake:
            def __init__(self, *args, **kwargs):
                constructed.append(1)

            def sync(self, batch=8):
                return False

            def candidates(self, *args, **kwargs):
                return []

            def status(self):
                return {"enabled": True, "ready": True}

        patcher = patch("personal_memory.semantic.SemanticIndex", Fake)
        patcher.start()
        self.addCleanup(patcher.stop)
        backend = Hybrid(self.store, {"rerank": {"enabled": False}}, start=True, index_owner=True)
        backend.close(join_budget=5.0, warmup_grace=0.0)
        returned = backend.start_warmup()
        self.assertIsNone(returned, "warm-up must not start after close")
        self.assertIsNone(backend._warmup)
        self.assertEqual([], constructed, "a closed backend must not construct an engine")


if __name__ == "__main__":
    unittest.main()
