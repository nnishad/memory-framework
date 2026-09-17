"""Fix 6 regression: discovery is resolved once and honors retry deadlines.

The supervisor caches adapter declarations and must be the only caller of
``discover`` per refresh interval; the worker receives the already-validated
declarations instead of re-discovering on every page. A persisted backoff must be
honored even when no cached result exists, a discovery auth failure must park the
connection like a read failure, and a changed connection configuration must
invalidate the cached declarations.
"""
import tempfile
import time
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import (AdapterError, read_state, source_operation,
                                        source_page, stream_spec)
from tests.test_source_sync import FixtureAdapter, note_record


class PagedAdapter(FixtureAdapter):
    """A backfill that spans several pages so per-page discovery would be visible."""

    def __init__(self):
        super().__init__()
        self.discoveries = 0

    def discover(self, context):
        self.discoveries += 1
        return [stream_spec("messages", modes=["backfill"], version_order="integer")]

    def read_page(self, context, state):
        seen = (state["cursor"] or {}).get("seen", 0)
        operations = [source_operation("upsert", f"m{seen}",
                                       records=[note_record(f"m{seen}", source=context["source"])],
                                       source_version=seen + 1)]
        return source_page(page_id=f"pg{seen}", operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"seen": seen + 1,
                                                         "done": seen + 1 >= 3},
                                                 mode=state["mode"]))


class ToggleDiscover(FixtureAdapter):
    """Backfill adapter whose next discovery can be forced to fail once."""

    def __init__(self):
        super().__init__()
        self.discoveries = 0
        self.fail_next = False

    def discover(self, context):
        self.discoveries += 1
        if self.fail_next:
            self.fail_next = False
            raise AdapterError("temporary", "upstream discovery unavailable")
        return [stream_spec("messages", modes=["backfill"], version_order="integer")]

    def read_page(self, context, state):
        seen = (state["cursor"] or {}).get("seen", 0)
        operations = [source_operation("upsert", f"m{seen}",
                                       records=[note_record(f"m{seen}", source=context["source"])],
                                       source_version=seen + 1)]
        return source_page(page_id=f"pg{seen}", operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"seen": seen + 1,
                                                         "done": seen + 1 >= 3},
                                                 mode=state["mode"]))


class FailingDiscover(FixtureAdapter):
    def __init__(self, kind="temporary", failures=1):
        super().__init__()
        self.discoveries = 0
        self.kind = kind
        self.failures = failures

    def discover(self, context):
        self.discoveries += 1
        if self.discoveries <= self.failures:
            raise AdapterError(self.kind, "upstream discovery unavailable")
        return [stream_spec("messages", modes=["backfill"], version_order="integer")]

    def read_page(self, context, state):
        return source_page(page_id="done", operations=[],
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"done": True}, mode=state["mode"]))


class DiscoveryCachingTests(unittest.TestCase):
    def _runtime(self, adapter, source="paged-acct"):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        store = Store(self.root / "memory.db")
        runtime = SourceRuntime(store, self.root, config={"enabled": False}, adapters=[adapter])
        self.addCleanup(runtime.close)
        conn = runtime.sync.configure(adapter_id="fixture.gmail", source=source,
                                      scope={"poll_seconds": 300}, retention="archive")
        self.runtime = runtime
        self.store = store
        self.adapter = adapter
        self.cid = conn["connection_id"]
        return adapter

    def _rotate_config(self, tag):
        with self.store.connect() as db:
            db.execute("UPDATE source_connections SET generation=generation+1, scope_hash=? WHERE id=?",
                       (tag, self.cid))

    def _backoff(self):
        with self.store.connect() as db:
            return db.execute("SELECT next_at,config_key FROM source_schedule WHERE connection_id=? AND role='discovery'",
                              (self.cid,)).fetchone()

    def _fail_refresh_into_backoff(self):
        self.runtime.tick()                       # successful discovery under the original config
        self.assertEqual(self.adapter.discoveries, 1)
        self._rotate_config("hash-fp2")           # a genuine configuration change
        self.adapter.fail_next = True
        self.runtime.tick()                       # one immediate refresh, which fails
        self.assertEqual(self.adapter.discoveries, 2)
        # The retry deadline must be recorded against the current config fingerprint
        # so a restart (or a newer config change) can interpret it correctly.
        self.assertIsNotNone(self._backoff()[1])

    def test_declarations_resolved_once_across_multiple_pages(self):
        adapter = self._runtime(PagedAdapter())
        for _ in range(4):  # backfill spans 3 pages, then a skipping terminal pass
            self.runtime.tick()
        with self.store.connect() as db:
            committed = db.execute("SELECT count(*) FROM records WHERE deleted=0").fetchone()[0]
        self.assertEqual(committed, 3)         # all three pages committed
        self.assertEqual(adapter.discoveries, 1)  # ... but discovery ran exactly once

    def test_discovery_backoff_is_honored_without_a_cached_result(self):
        adapter = self._runtime(FailingDiscover("temporary"))
        self.runtime.tick()                # first attempt fails and schedules a backoff
        self.assertEqual(adapter.discoveries, 1)
        self.runtime.tick()                # an immediate second tick must respect the backoff
        self.assertEqual(adapter.discoveries, 1)

    def test_successful_refresh_after_backoff_resumes_processing(self):
        adapter = self._runtime(FailingDiscover("temporary", failures=1))
        self.runtime.tick()                # fails -> backoff
        self.runtime.tick()                # honored, skipped
        self.assertEqual(adapter.discoveries, 1)
        with self.store.connect() as db:   # expire the backoff without real sleeping
            db.execute("UPDATE source_schedule SET next_at=? WHERE connection_id=? AND role='discovery'",
                       (time.time() - 1, self.cid))
        self.runtime.tick()                # now due -> succeeds
        self.assertEqual(adapter.discoveries, 2)

    def test_discovery_auth_failure_parks_the_connection(self):
        self._runtime(FailingDiscover("auth", failures=1))
        self.runtime.tick()
        self.assertEqual(self.runtime.sync.status(self.cid)["state"], "needs_auth")

    def test_changed_connection_config_invalidates_cached_declarations(self):
        adapter = self._runtime(PagedAdapter())
        self.runtime.tick()                       # discovers and caches
        self.assertEqual(adapter.discoveries, 1)
        with self.store.connect() as db:          # rotate credentials/config and keep discovery un-due
            db.execute("UPDATE source_connections SET generation=generation+1, scope_hash=scope_hash||'x' WHERE id=?",
                       (self.cid,))
            db.execute("UPDATE source_schedule SET next_at=? WHERE connection_id=? AND role='discovery'",
                       (time.time() + 3600, self.cid))
        self.runtime.tick()                       # stale cache must be invalidated by the change
        self.assertEqual(adapter.discoveries, 2)

    def test_failed_refresh_backoff_is_bound_to_the_new_config(self):
        # Successful discovery -> config change -> failed refresh -> immediate ticks
        # must respect that refresh's retry deadline rather than retry every tick.
        self._runtime(ToggleDiscover())
        self._fail_refresh_into_backoff()
        for _ in range(3):
            self.runtime.tick()
        self.assertEqual(self.adapter.discoveries, 2)
        # The persisted retry deadline belongs to the current (post-change) config.
        row = self._backoff()
        self.assertIsNotNone(row[1])

    def test_restart_during_backoff_honors_the_deadline(self):
        self._runtime(ToggleDiscover())
        self._fail_refresh_into_backoff()
        # A fresh process (empty in-memory cache) must still respect the persisted,
        # config-bound backoff instead of rediscovering immediately.
        reopened = SourceRuntime(self.store, self.root, config={"enabled": False},
                                 adapter=self.adapter)
        self.addCleanup(reopened.close)
        reopened.tick()
        reopened.tick()
        self.assertEqual(self.adapter.discoveries, 2)
        # Declarations resolved before the restart under the old config are discarded.
        self.assertNotIn(self.cid, reopened.discovered)

    def test_newer_config_change_during_backoff_forces_immediate_attempt(self):
        self._runtime(ToggleDiscover())
        self._fail_refresh_into_backoff()
        # A genuinely newer configuration supersedes the pending backoff and is due now.
        self._rotate_config("hash-fp3")
        self.runtime.tick()
        self.assertEqual(self.adapter.discoveries, 3)

    def test_successful_retry_refreshes_declarations_and_resumes_ingestion(self):
        self._runtime(ToggleDiscover())
        self._fail_refresh_into_backoff()
        with self.store.connect() as db:      # expire the backoff without real sleeping
            db.execute("UPDATE source_schedule SET next_at=? WHERE connection_id=? AND role='discovery'",
                       (time.time() - 1, self.cid))
        self.runtime.tick()                    # retry now succeeds and resumes ingestion
        self.assertEqual(self.adapter.discoveries, 3)
        with self.store.connect() as db:
            committed = db.execute("SELECT count(*) FROM records WHERE deleted=0").fetchone()[0]
        self.assertGreater(committed, 0)


if __name__ == "__main__":
    unittest.main()
