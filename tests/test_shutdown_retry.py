"""Fix 6: an incomplete shutdown stays retryable instead of losing resources.

A close that cannot finish (a writer that outlives the join budget, a resource
whose close raises) must keep the resource tracked, report the failure, release
nothing it does not own yet, and let a later close() complete the job. A failed
ASGI startup must still release the ownership lease and permit a clean startup
later in the same process, without masking the original startup exception.
"""
import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from personal_memory.asgi import Application, ProcessLease
from personal_memory.backend import load_backend
from personal_memory.service import MemoryService
from personal_memory.store import Store


class StartupError(RuntimeError):
    pass


class LateWriter:
    """Stand-in indexing worker that survives the first shutdown budget."""

    def __init__(self):
        self.exited = False

    def join(self, timeout=None):
        return

    def is_alive(self):
        return not self.exited


class FlakyClose:
    """Resource whose first close fails and whose retry succeeds."""

    def __init__(self):
        self.calls = 0

    def close(self):
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("injected first-close failure")


class CountingClose:
    def __init__(self):
        self.calls = 0

    def close(self):
        self.calls += 1


class FakeHindsightRuntime:
    """Stand-in for hindsight_runtime.Runtime handing out one shared flaky closer."""

    def __init__(self, settings):
        pass

    def start(self):
        return FakeHindsightRuntime.closer

    closer = None


class HybridShutdownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_writer_outlives_first_budget_then_second_close_releases_ownership(self):
        store = Store(self.root / "memory.db")
        backend = load_backend(store, config={})
        writer = LateWriter()
        backend.threads.append(writer)
        # The unfinished shutdown is reported instead of silently retained.
        with self.assertRaises(RuntimeError):
            backend.close(join_budget=0.05)
        # Ownership stays held while a writer lives: a second indexer is rejected.
        with self.assertRaises(RuntimeError):
            ProcessLease(self.root / "indexing.lock")
        writer.exited = True
        backend.close(join_budget=0.05)  # the retry completes
        self.assertIsNone(backend.index_lease)
        released = ProcessLease(self.root / "indexing.lock")
        released.close()
        backend.close(join_budget=0.05)  # repeated clean close is harmless


class ServiceCloseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / "data"

    def options(self):
        return dict(retrieval_config={"semantic": {"enabled": False}},
                    source_config={"enabled": False})

    def test_failed_close_is_retained_retried_and_never_skips_others(self):
        service = MemoryService(self.data, "a" * 40, **self.options())
        flaky, other = FlakyClose(), CountingClose()
        service._closables.extend([other, flaky])
        with self.assertRaises(RuntimeError):
            service.close()
        self.assertEqual(1, other.calls, "a failing resource must not skip the rest")
        self.assertEqual([flaky], list(service._closables),
                         "the failed resource must stay tracked for retry")
        service.close()  # the retry succeeds and drains the list
        self.assertEqual(2, flaky.calls)
        self.assertEqual([], service._closables)
        service.close()  # repeated successful close is harmless
        self.assertEqual(2, flaky.calls)
        self.assertEqual(1, other.calls)


class LifespanShutdownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / "data"

    def app(self):
        return Application({"data_dir": str(self.data), "token": "a" * 40,
                            "retrieval": {"semantic": {"enabled": False}},
                            "sources": {"enabled": False}})

    def drive(self, app, inputs):
        """Send lifespan messages and collect the replies, one per message."""
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

    def test_failed_startup_retains_ownership_until_cleanup_completes(self):
        FakeHindsightRuntime.closer = FlakyClose()
        app = self.app()
        with mock.patch("personal_memory.asgi.MemoryService",
                        side_effect=StartupError("injected stage failure")), \
             mock.patch("personal_memory.hindsight_runtime.Runtime", FakeHindsightRuntime):
            replies = self.drive(app, [{"type": "lifespan.startup"}])
        # The managed runtime refused to close, so dependency-aware cleanup keeps the
        # process lease owned rather than handing it to a second writer: startup.failed,
        # not a raw crash, and not a false release of a still-dependent resource.
        self.assertEqual("lifespan.startup.failed", replies[0]["type"])
        self.assertEqual(1, FakeHindsightRuntime.closer.calls)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.data / "service.lock")  # lease still owned
        # Retrying startup first drains the retained runtime, then claims ownership.
        replies = self.drive(app, [{"type": "lifespan.startup"}])
        self.assertEqual("lifespan.startup.complete", replies[0]["type"])
        replies = self.drive(app, [{"type": "lifespan.shutdown"}])
        self.assertEqual("lifespan.shutdown.complete", replies[0]["type"])
        # The retained flaky resource was retried at the next startup, not dropped:
        # one failing close plus the successful retry during the cleanup-before-restart.
        self.assertEqual(2, FakeHindsightRuntime.closer.calls)
        probe = ProcessLease(self.data / "service.lock")
        probe.close()

    def test_shutdown_retains_ownership_past_a_failing_service_close(self):
        app = self.app()
        self.assertEqual("lifespan.startup.complete",
                         self.drive(app, [{"type": "lifespan.startup"}])[0]["type"])
        # Release the real retrieval backend's indexing lease before substituting the
        # service close: this test drives only the ASGI ownership boundary.
        app.service.close()
        attempts = []

        def flaky_service_close():
            if not attempts:
                attempts.append("failed")
                raise RuntimeError("injected incomplete shutdown")
        app.service.close = flaky_service_close
        hindsight = CountingClose()
        app.hindsight_runtime = hindsight
        replies = self.drive(app, [{"type": "lifespan.shutdown"}])
        self.assertEqual("lifespan.shutdown.failed", replies[0]["type"])
        # Dependency-aware: the service could not close, so its runtime prerequisite
        # is never touched and the process lease stays owned by the live service.
        self.assertEqual(0, hindsight.calls, "shutdown must not release a dependent's owner")
        self.assertIsNotNone(app.lease)
        with self.assertRaises(RuntimeError):
            ProcessLease(self.data / "service.lock")
        # Retry: the retained service, then the runtime and lease drain in order.
        replies = self.drive(app, [{"type": "lifespan.shutdown"}])
        self.assertEqual("lifespan.shutdown.complete", replies[0]["type"])
        self.assertEqual(1, hindsight.calls)
        probe = ProcessLease(self.data / "service.lock")
        probe.close()


if __name__ == "__main__":
    unittest.main()
