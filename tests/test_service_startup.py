"""Startup is transactional: a failed MemoryService never leaks the indexing lease.

Every resource the service owns (search pool, index writers, source poller) is
created in order; if any later stage fails, the already-initialized resources
must be closed in reverse order through the normal shutdown path so a corrected
startup can succeed in the same process against the same data directory.
"""
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from personal_memory.asgi import ProcessLease
from personal_memory.retrieval import Hybrid
from personal_memory.service import MemoryService


class StartupError(RuntimeError):
    pass


class ServiceStartupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name) / "data"

    def options(self, **kwargs):
        base = dict(retrieval_config={"semantic": {"enabled": False}}, source_config={"enabled": False})
        base.update(kwargs)
        return base

    def acquire_lease(self):
        # Same path Hybrid uses: the durable lease over the data directory.
        return ProcessLease(self.data / "indexing.lock")

    def live_memory_threads(self):
        return [t.name for t in threading.enumerate()
                if t is not threading.current_thread() and t.is_alive() and t.name.startswith("memory-")]

    def injected(self, stage):
        def fail(*args, **kwargs):
            raise StartupError("injected failure at " + stage)
        return fail

    def assert_ownership_released(self, stage):
        try:
            lease = self.acquire_lease()
        except RuntimeError:
            self.fail("failure at %s leaked the indexing lease" % stage)
        lease.close()
        self.assertEqual([], self.live_memory_threads(), "failure at %s leaked workers" % stage)

    def test_failure_at_each_initialization_stage_releases_ownership(self):
        stages = [
            "personal_memory.learning.Learning.__init__",
            "personal_memory.adaptive.AdaptiveRecall.__init__",
            "personal_memory.intelligence.Intelligence.__init__",
            "personal_memory.investigate.Investigation.__init__",
            "personal_memory.workflows.Workflows.__init__",
            "personal_memory.workflows.Workflows.start",
            "personal_memory.reset.initialize",
            "personal_memory.native_catalog.initialize",
            "personal_memory.source_runtime.SourceRuntime.__init__",
        ]
        for stage in stages:
            with self.subTest(stage=stage):
                with mock.patch(stage, new=self.injected(stage)):
                    with self.assertRaises(StartupError):
                        MemoryService(self.data, "a" * 40, **self.options())
                self.assert_ownership_released(stage)

    def test_retry_startup_in_the_same_process_after_a_failure(self):
        with mock.patch("personal_memory.native_catalog.initialize", new=self.injected("native_catalog")):
            with self.assertRaises(StartupError):
                MemoryService(self.data, "a" * 40, **self.options())
        service = MemoryService(self.data, "a" * 40, **self.options())
        self.addCleanup(service.close)
        self.assertTrue(service.health()["live"])

    def test_invalid_recall_configuration_fails_before_ownership_starts(self):
        with mock.patch("personal_memory.service.load_backend") as loader:
            with self.assertRaises(ValueError):
                MemoryService(self.data, "a" * 40, **self.options(
                    intelligence_config={"recall": {"bogus": {}}}))
        loader.assert_not_called()
        self.assertFalse((self.data / "indexing.lock").exists())

    def test_invalid_workflow_configuration_fails_before_ownership_starts(self):
        with mock.patch("personal_memory.service.load_backend") as loader:
            with self.assertRaises(ValueError):
                MemoryService(self.data, "a" * 40, **self.options(intelligence_config={"timeout": 99}))
        loader.assert_not_called()
        self.assertFalse((self.data / "indexing.lock").exists())

    def test_corrected_configuration_retries_startup_in_same_directory(self):
        with self.assertRaises(ValueError):
            MemoryService(self.data, "a" * 40, **self.options(
                intelligence_config={"recall": {"bogus": {}}}))
        service = MemoryService(self.data, "a" * 40, **self.options(
            intelligence_config={"recall": {}}))
        self.addCleanup(service.close)
        self.assertTrue(service.health()["live"])
        self.assertEqual([], [n for n in self.live_memory_threads() if n == "memory-source-sync"])

    def test_original_exception_survives_a_failing_cleanup(self):
        real_close = Hybrid.close

        def bad_close(self):
            real_close(self)
            raise StartupError("cleanup also failed")

        with mock.patch("personal_memory.investigate.Investigation.__init__", new=self.injected("investigation")), \
                mock.patch.object(Hybrid, "close", bad_close):
            with self.assertRaises(StartupError) as caught:
                MemoryService(self.data, "a" * 40, **self.options())
        self.assertIn("investigation", str(caught.exception))
        # The cleanup that could run did run: ownership of the directory is free.
        self.acquire_lease().close()

    def test_close_is_idempotent_and_safe_on_a_partial_object(self):
        service = MemoryService(self.data, "a" * 40, **self.options())
        service.close()
        service.close()
        # A constructor that failed before assigning resources must still close.
        MemoryService.__new__(MemoryService).close()
        self.assertEqual([], self.live_memory_threads())

    def test_a_blocked_writer_prevents_premature_ownership_release(self):
        service = MemoryService(self.data, "a" * 40, **self.options())
        release = threading.Event()

        class BlockedEngine:
            last_error = None

            def sync(self, batch=8):
                release.wait(30)
                return 0

        service.retrieval._start_index_thread(BlockedEngine())
        for _ in range(100):
            if any(t.name == "memory-index" for t in threading.enumerate()):
                break
            time.sleep(0.05)
        closer = threading.Thread(target=service.close)
        closer.start()
        time.sleep(0.5)
        try:
            with self.assertRaises(RuntimeError):
                # Ownership may not release while an indexing writer is still live.
                self.acquire_lease().close()
        finally:
            release.set()
            closer.join(30)
        self.assertFalse(closer.is_alive())
        lease = self.acquire_lease()
        lease.close()
        self.assertEqual([], self.live_memory_threads())


if __name__ == "__main__":
    unittest.main()
