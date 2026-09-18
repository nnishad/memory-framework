"""Source signals are acknowledged only after complete required coverage.

A connection may carry several required incremental streams and partitions.
Catch-up is tracked per stream/partition: acknowledgment of the captured
high-water mark requires every required incremental pass to have completed
converged in this tick - unfinished paging, a pass still in backoff, or any
failure keeps signals pending. Completion is defined by the adapter contract
(the page's `more` continuation flag), not by Gmail-specific cursor fields.
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import (AdapterError, read_state, source_operation,
                                        source_page, stream_spec)
from tests.test_source_sync import FixtureAdapter, note_record


class CoverageAdapter(FixtureAdapter):
    """Independent incremental feeds keyed by (stream, partition).

    Each feed delivers its item list in bounded pages and declares `more`
    while the next_state still represents continuation work - the contract
    fact the runtime uses to decide catch-up.
    """

    def __init__(self, streams, partitions=None, page_size=2, adapter_id="fixture.gmail"):
        super().__init__()
        self._adapter_id = adapter_id
        self.feeds = {}
        self.page_size = page_size
        self.failures = {}    # key -> number of temporary read failures to inject
        self.on_read = None   # hook called inside read_page, once
        self.reads = {}
        self.streams = streams
        self.partitions = partitions or {}

    def spec(self):
        value = super().spec()
        value["adapter_id"] = self._adapter_id
        return value

    def discover(self, context):
        return [stream_spec(name, modes=["incremental"], version_order="integer",
                            partitions=[{"id": p} for p in self.partitions.get(name, [])])
                for name in self.streams]

    def publish(self, stream, source_id, partition=""):
        self.feeds.setdefault((stream, partition), []).append(source_id)

    def read_page(self, context, state):
        key = (context["stream"], context["partition"] or "")
        self.reads[key] = self.reads.get(key, 0) + 1
        if self.on_read:
            hook, self.on_read = self.on_read, None
            hook()
        if self.failures.get(key):
            self.failures[key] -= 1
            raise AdapterError("temporary", "feed is unavailable")
        items = self.feeds.get(key, [])
        cursor = state["cursor"] or {}
        start = int(cursor["page"]) * self.page_size if cursor.get("page") is not None else (
            int(cursor.get("head", 0)) if cursor.get("done") else 0)
        batch = items[start:start + self.page_size]
        operations = [source_operation(
            "upsert", source_id, source_version=start + index + 1,
            records=[note_record(source_id, source=context["source"])])
            for index, source_id in enumerate(batch)]
        end = start + len(batch)
        more = len(items) > end
        next_cursor = {"page": end // self.page_size} if more else {"done": True, "head": end}
        return source_page(page_id="pg-%s-%s-%d" % (key[0], key[1], start),
                           operations=operations,
                           next_state=read_state(cursor=next_cursor, mode=state["mode"],
                                                 state_version=state["state_version"]),
                           complete=True, more=more)


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "memory.db")

    def runtime(self, adapter):
        runtime = SourceRuntime(self.store, self.root, config={"enabled": False}, adapter=adapter)
        self.addCleanup(runtime.close)
        connection = runtime.sync.configure(
            adapter_id="google.gmail", source="gmail-acct1",
            scope={"poll_seconds": 300}, retention="archive")
        return runtime, connection["connection_id"]

    def pending(self, runtime, cid):
        return runtime.sync.pending_signals(cid)

    def schedule_backoff(self, runtime, cid, key):
        runtime._schedule(cid, key, 3600, "forced partition backoff")

    def test_one_stream_converged_while_another_is_still_paging_keeps_signals(self):
        # "alpha" sorts first and stays mid-page; "beta" converges on every
        # pass. Acknowledgment must not depend on pass order.
        feed = CoverageAdapter(["alpha", "beta"], page_size=1)
        for source_id in ("al-1", "al-2", "al-3"):
            feed.publish("alpha", source_id)
        feed.publish("beta", "be-1")
        runtime, cid = self.runtime(feed)
        runtime.tick()                      # cadence; alpha serves page 1 of 3
        self.signal(runtime, cid, "push-1")
        runtime.tick()                      # alpha still paging; beta converged
        self.assertEqual(self.pending(runtime, cid), 1)   # incomplete coverage
        runtime.tick()                      # alpha delivers the last item covered
        self.assertEqual(self.pending(runtime, cid), 0)
        for source_id in ("al-1", "al-2", "al-3", "be-1"):
            self.assertIsNotNone(runtime.sync.head("gmail-acct1", source_id))

    def test_partition_in_backoff_blocks_acknowledgment(self):
        feed = CoverageAdapter(["alpha"], partitions={"alpha": ["p1", "p2"]}, page_size=5)
        feed.publish("alpha", "p1-a", partition="p1")
        feed.publish("alpha", "p2-a", partition="p2")
        runtime, cid = self.runtime(feed)
        runtime.tick()
        self.signal(runtime, cid, "push-1")
        # p1 converges on this pass; p2 is parked in retry backoff and skipped.
        self.schedule_backoff(runtime, cid, '["alpha","p2","incremental"]')
        runtime.tick()
        self.assertEqual(self.pending(runtime, cid), 1)   # p2 never covered
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE source_schedule SET next_at=0,error=NULL,failures=0")
        runtime.tick()                                    # p2 catches up -> ack
        self.assertEqual(self.pending(runtime, cid), 0)
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "p1-a"))
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "p2-a"))

    def test_failure_after_another_stream_succeeded_keeps_signals(self):
        feed = CoverageAdapter(["alpha", "beta"], page_size=5)
        feed.publish("alpha", "al-1")
        feed.publish("beta", "be-1")
        runtime, cid = self.runtime(feed)
        runtime.tick()
        self.signal(runtime, cid, "push-1")
        feed.failures[("beta", "")] = 1                   # alpha succeeds, beta fails
        runtime.tick()
        self.assertEqual(self.pending(runtime, cid), 1)
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE source_schedule SET next_at=0,error=NULL,failures=0")
        runtime.tick()                                    # beta succeeds
        self.assertEqual(self.pending(runtime, cid), 0)
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "be-1"))

    def test_signal_arriving_during_coverage_survives_the_acknowledgment(self):
        feed = CoverageAdapter(["alpha"], page_size=1)
        feed.publish("alpha", "al-1")
        runtime, cid = self.runtime(feed)
        runtime.tick()
        self.signal(runtime, cid, "push-1")

        def arrive_mid_pass():
            feed.publish("alpha", "al-2")
            self.signal(runtime, cid, "push-2")

        feed.on_read = arrive_mid_pass
        for _ in range(6):                                # drain to quiescence
            runtime.tick()
            if self.pending(runtime, cid) == 0:
                break
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "al-2"))
        self.assertEqual(self.pending(runtime, cid), 0)

    def test_restart_mid_catchup_resumes_coverage_and_acks(self):
        feed = CoverageAdapter(["alpha"], page_size=1)
        for index in range(3):
            feed.publish("alpha", "al-%d" % index)
        runtime, cid = self.runtime(feed)
        runtime.tick()
        self.signal(runtime, cid, "push-1")
        runtime.tick()                                    # one page into catch-up
        self.assertEqual(self.pending(runtime, cid), 1)
        # A restart mid-catch-up: a new runtime over the same durable state,
        # same connection, resumes coverage and acknowledges only when complete.
        restarted = CoverageAdapter(["alpha"], page_size=1)
        restarted.feeds = {key: list(items) for key, items in feed.feeds.items()}
        runtime2 = SourceRuntime(self.store, self.root, config={"enabled": False}, adapter=restarted)
        self.addCleanup(runtime2.close)
        for _ in range(6):
            runtime2.tick()
            if self.pending(runtime2, cid) == 0:
                break
        self.assertEqual(self.pending(runtime2, cid), 0)
        for index in range(3):
            self.assertIsNotNone(runtime2.sync.head("gmail-acct1", "al-%d" % index))

    def signal(self, runtime, cid, event_id):
        return runtime.sync.signal(cid, event_id=event_id)


if __name__ == "__main__":
    unittest.main()
