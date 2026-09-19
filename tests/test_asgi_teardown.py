"""R4 regression: application teardown must be dependency-aware.

ASGI shutdown must stop accepting requests, drain in-flight HTTP handlers, then
release the service, the managed Hindsight runtime and the process lease in that
dependency order. A dependent that will not close keeps every prerequisite owned
and reports ``lifespan.shutdown.failed``; ownership is never handed to a second
process while a live writer (or an R5 initializer that outlived its budget) can
still touch it. A startup may not claim ownership again until the prior lifecycle
has fully drained, and it must never overwrite a retained handle.
"""
import asyncio
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from personal_memory.asgi import Application, ProcessLease
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store


class Recorder:
    """A closable that logs each attempt and can be told to fail."""

    def __init__(self, name, log, fail=False):
        self.name, self.log, self.fail = name, log, fail
        self.calls = 0

    def close(self):
        self.calls += 1
        self.log.append(self.name)
        if self.fail:
            raise RuntimeError("injected close failure: " + self.name)


class RecordingExecutor:
    def __init__(self, log):
        self.log = log

    def shutdown(self, *args, **kwargs):
        self.log.append("executor")


class TeardownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / "data"
        self.data.mkdir()

    def app(self):
        # Default startup disables the model channels so a real service starts cheaply;
        # the teardown ordering is independent of the retrieval backend internals.
        return Application({"data_dir": str(self.data), "token": "a" * 40,
                            "retrieval": {"semantic": {"enabled": False}},
                            "sources": {"enabled": False}})

    def drive(self, app, inputs):
        async def go():
            inbox, outbox = asyncio.Queue(), asyncio.Queue()
            for message in inputs:
                await inbox.put(message)
            task = asyncio.create_task(app({"type": "lifespan"}, inbox.get, outbox.put))
            replies = []
            try:
                for _ in inputs:
                    replies.append(await asyncio.wait_for(outbox.get(), 60))
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            return replies
        return asyncio.run(go())

    def _drain(self, app):
        # Force-release every owned resource so a retained stand-in lease never keeps
        # a lock file open into Windows temporary-directory cleanup.
        for attr in ("service", "hindsight_runtime", "lease"):
            resource = getattr(app, attr, None)
            if resource is not None:
                try:
                    resource.close()
                except Exception:
                    pass
                setattr(app, attr, None)
        if app.executor is not None:
            try:
                app.executor.shutdown(wait=False)
            except Exception:
                pass
            app.executor = None

    def started(self):
        app = self.app()
        reply = self.drive(app, [{"type": "lifespan.startup"}])[0]
        self.assertEqual("lifespan.startup.complete", reply["type"])
        # These tests drive the ASGI ownership boundary with stand-ins, so release the
        # real retrieval backend now (it owns indexing.lock) before it is replaced.
        app.service.close()
        app.service = None
        self.addCleanup(self._drain, app)
        return app

    def lock(self):
        return self.data / "service.lock"

    def test_shutdown_drains_executor_then_releases_in_dependency_order(self):
        app = self.started()
        log = []
        real_lease = app.lease

        class LeaseRec:
            def close(self):
                log.append("lease")
                real_lease.close()

        app.executor = RecordingExecutor(log)
        app.service = Recorder("service", log)
        app.hindsight_runtime = Recorder("runtime", log)
        app.lease = LeaseRec()
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.complete", reply["type"])
        # HTTP handlers drain before the service, which drains before the runtime
        # it depends on, which drains before the process lease is handed back.
        self.assertEqual(["executor", "service", "runtime", "lease"], log)

    def test_service_close_failure_retains_every_downstream_owner(self):
        app = self.started()
        log = []
        runtime = Recorder("runtime", log)
        app.executor = RecordingExecutor(log)
        app.service = Recorder("service", log, fail=True)
        app.hindsight_runtime = runtime
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.failed", reply["type"])
        # Teardown stopped at the service: the runtime close was never attempted
        # and the process lease is still owned by this (still-live) service.
        self.assertEqual(0, runtime.calls)
        self.assertIsNotNone(app.service)
        self.assertIsNotNone(app.lease)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.lock())
        # Releasing the block lets a later shutdown finish the job.
        app.service.fail = False
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.complete", reply["type"])
        self.assertEqual(1, runtime.calls)
        self.assertIsNone(app.lease)
        probe = ProcessLease(self.lock())
        probe.close()
        # A third shutdown on a fully drained app is harmless.
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.complete", reply["type"])

    def test_runtime_close_failure_retains_process_lease(self):
        app = self.started()
        log = []
        app.executor = RecordingExecutor(log)
        app.service = Recorder("service", log)  # closes cleanly
        app.hindsight_runtime = Recorder("runtime", log, fail=True)
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.failed", reply["type"])
        # The service drained, but the still-owned runtime keeps the lease held.
        self.assertIsNone(app.service)
        self.assertIsNotNone(app.hindsight_runtime)
        self.assertIsNotNone(app.lease)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.lock())

    def test_startup_is_refused_while_prior_cleanup_is_incomplete(self):
        app = self.started()
        log = []
        app.executor = RecordingExecutor(log)
        app.service = Recorder("service", log, fail=True)
        app.hindsight_runtime = Recorder("runtime", log)
        self.assertEqual("lifespan.shutdown.failed",
                         self.drive(app, [{"type": "lifespan.shutdown"}])[0]["type"])
        retained_service, retained_runtime, retained_lease = app.service, app.hindsight_runtime, app.lease
        # A restart cannot double-claim ownership while the prior service is still live.
        with mock.patch("personal_memory.asgi.MemoryService") as builder:
            reply = self.drive(app, [{"type": "lifespan.startup"}])[0]
        self.assertEqual("lifespan.startup.failed", reply["type"])
        builder.assert_not_called()  # no replacement constructed over a retained owner
        self.assertIs(retained_service, app.service)
        self.assertIs(retained_runtime, app.hindsight_runtime)
        self.assertIs(retained_lease, app.lease)

    def test_delayed_retrieval_initializer_holds_both_ownerships(self):
        """R4 + R5: a retrieval initializer that outlives its shutdown budget must keep
        both the indexing journal and the application process lease owned until it drains."""
        store = Store(self.data / "retrieval.db")
        entered, release = threading.Event(), threading.Event()

        class Gated:
            def __init__(self, *args, **kwargs):
                entered.set()
                release.wait(10)

            def sync(self, batch=8):
                return False

            def candidates(self, *args, **kwargs):
                return []

            def status(self):
                return {"enabled": True, "ready": True}

        class DelayedRetrievalService:
            # A MemoryService stand-in that applies its own shutdown budget to the
            # retrieval backend, exactly as the real service.close() propagates R5.
            def __init__(self, retrieval):
                self.retrieval = retrieval

            def close(self):
                self.retrieval.close(join_budget=0.2, warmup_grace=0.0)

        # The lazy construction happens on the builder thread, so the model kill-
        # switches and the gated SemanticIndex must stay patched across that call.
        env = mock.patch.dict(os.environ, {"PERSONAL_MEMORY_DISABLE_SEMANTIC": "0",
                                           "PERSONAL_MEMORY_DISABLE_RERANK": "1"})
        env.start()
        self.addCleanup(env.stop)
        patcher = mock.patch("personal_memory.semantic.SemanticIndex", Gated)
        patcher.start()
        self.addCleanup(patcher.stop)
        hybrid = Hybrid(store, {"rerank": {"enabled": False}}, start=True, index_owner=True)
        builder = threading.Thread(target=hybrid._ensure_semantic)
        builder.start()

        def finish():
            release.set()
            builder.join(10)
            try:
                hybrid.close(join_budget=5.0, warmup_grace=0.0)
            except Exception:
                if hybrid.index_lease is not None:
                    hybrid.index_lease.close()
        self.addCleanup(finish)
        self.assertTrue(entered.wait(5))

        app = self.app()
        log = []
        app.lease = ProcessLease(self.lock())
        app.executor = RecordingExecutor(log)
        app.service = DelayedRetrievalService(hybrid)
        app.hindsight_runtime = Recorder("runtime", log)
        self.addCleanup(self._drain, app)
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.failed", reply["type"])
        # Both ownerships are still held while the old initializer runs.
        self.assertIsNotNone(app.lease)
        self.assertIsNotNone(hybrid.index_lease)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.lock())
        with self.assertRaises(RuntimeError):
            ProcessLease(self.data / "indexing.lock")
        # Drain the initializer; a repeated shutdown now completes and frees both.
        release.set()
        builder.join(10)
        reply = self.drive(app, [{"type": "lifespan.shutdown"}])[0]
        self.assertEqual("lifespan.shutdown.complete", reply["type"])
        self.assertIsNone(hybrid.index_lease)
        probe = ProcessLease(self.lock())
        probe.close()


if __name__ == "__main__":
    unittest.main()
