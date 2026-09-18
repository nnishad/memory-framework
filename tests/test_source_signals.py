"""Durable source signals drive ingestion scheduling (workstream 6).

A signal is a durable request to check the source now: it may bypass the normal
polling delay, but never pause, an authentication park or a retry backoff. Only
a converged, successful incremental pass acknowledges the signals it covered;
signals arriving during a pass survive to trigger a follow-up pass.
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import AdapterError, read_state, source_operation, source_page, stream_spec
from personal_memory.source_sync import SourceSync
from tests.test_source_sync import FixtureAdapter, note_record


class FeedAdapter(FixtureAdapter):
    """An incremental source delivering a growing list of items in bounded pages.

    The converged cursor carries a delivered-item watermark, so an item that
    arrives during a pass is delivered on the next pass, never skipped.
    """

    def __init__(self, page_size=1, adapter_id="fixture.gmail"):
        super().__init__()
        self._adapter_id = adapter_id
        self.items = []
        self.page_size = page_size
        self.reads = 0
        self.failures = 0     # temporary failures to inject on the next reads
        self.auth_fail = False
        self.on_read = None   # hook called inside read_page, before serving

    def publish(self, *source_ids):
        self.items.extend(source_ids)

    def spec(self):
        value = super().spec()
        value["adapter_id"] = self._adapter_id
        return value

    def discover(self, context):
        return [stream_spec("messages", modes=["incremental"], version_order="integer")]

    def read_page(self, context, state):
        self.reads += 1
        if self.on_read:
            self.on_read()
            self.on_read = None
        if self.auth_fail:
            raise AdapterError("auth", "credentials were revoked")
        if self.failures:
            self.failures -= 1
            raise AdapterError("temporary", "feed is unavailable")
        cursor = state["cursor"] or {}
        if cursor.get("page") is not None:
            start = int(cursor["page"]) * self.page_size
        elif cursor.get("done"):
            start = int(cursor.get("head", 0))
        else:
            start = 0
        batch = self.items[start:start + self.page_size]
        operations = [source_operation(
            "upsert", source_id, source_version=start + index + 1,
            records=[note_record(source_id, source=context["source"])])
            for index, source_id in enumerate(batch)]
        end = start + len(batch)
        more = len(self.items) > end
        if more:
            next_cursor = {"page": end // self.page_size}
        else:
            next_cursor = {"done": True, "head": end}
        return source_page(page_id="pg-" + self._adapter_id + "-" + str(start),
                           operations=operations,
                           next_state=read_state(cursor=next_cursor, mode=state["mode"],
                                                 state_version=state["state_version"]),
                           complete=True, more=more)


class NotesFeedAdapter(FeedAdapter):
    def __init__(self, page_size=1):
        super().__init__(page_size=page_size, adapter_id="fixture.notes")


class SignalSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "memory.db")
        self.feed = FeedAdapter(page_size=1)
        self.notes = NotesFeedAdapter(page_size=1)
        self.runtime = SourceRuntime(self.store, self.root, config={"enabled": False},
                                     adapter=self.feed, adapters=[self.notes])
        self.addCleanup(self.runtime.close)
        self.conn = self.runtime.sync.configure(
            adapter_id="google.gmail", source="gmail-acct1",
            scope={"poll_seconds": 300}, retention="archive")
        self.other = self.runtime.sync.configure(
            adapter_id="fixture.notes", source="notes-acct2",
            scope={"poll_seconds": 300}, retention="archive")

    def cid(self):
        return self.conn["connection_id"]

    def other_cid(self):
        return self.other["connection_id"]

    def signal(self, connection_id=None, **kwargs):
        return self.runtime.sync.signal(connection_id or self.cid(), **kwargs)

    def record_count(self):
        with self.store.connect() as db:
            return db.execute("SELECT COUNT(*) FROM records WHERE deleted=0").fetchone()[0]

    def force_due(self):
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE source_schedule SET next_at=0,error=NULL,failures=0")

    def test_signal_triggers_ingestion_before_the_next_poll(self):
        self.runtime.tick()  # establishes the 300s poll cadence; nothing published yet
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)
        self.feed.publish("msg-1")
        self.signal(event_id="push-1")
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", "msg-1"))
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_duplicate_signals_do_not_create_duplicate_memory(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        for event in ("push-1", "push-2", "push-3"):
            self.signal(event_id=event)
        self.assertEqual(self.signal(event_id="push-1", payload={"replay": True})["duplicate"], True)
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", "msg-1"))
        self.assertEqual(self.record_count(), 1)
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_failed_processing_preserves_signals_and_backoff_respected(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        self.signal(event_id="push-1")
        self.feed.failures = 1
        self.runtime.tick()  # bypass pass fails -> durable backoff
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)
        self.assertIsNone(self.runtime.sync.head("gmail-acct1", "msg-1"))
        reads_after_failure = self.feed.reads
        self.runtime.tick()  # backoff must not be bypassed by the surviving signal
        self.assertEqual(self.feed.reads, reads_after_failure)
        self.force_due()
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", "msg-1"))
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_authentication_park_holds_signals(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        self.signal(event_id="push-1")
        self.feed.auth_fail = True
        self.runtime.tick()
        self.feed.auth_fail = False
        self.assertEqual(self.runtime.sync.status(self.cid())["state"], "needs_auth")
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)

    def test_pause_holds_signals_and_resume_catches_up(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        self.signal(event_id="push-1")
        self.runtime.sync.pause(self.cid())
        reads_before = self.feed.reads
        self.runtime.tick()
        self.assertEqual(self.feed.reads, reads_before)  # paused: no passes at all
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)
        self.runtime.control(connection_id=self.cid(), action="resume")
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", "msg-1"))
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_signal_arriving_during_a_pass_survives_the_acknowledgment(self):
        self.feed.page_size = 2
        self.runtime.tick()
        self.feed.publish("msg-1")
        self.signal(event_id="push-1")

        def arrive_mid_pass():  # runs inside read_page: this signal must survive
            self.feed.publish("msg-2")
            self.signal(event_id="push-2")

        self.feed.on_read = arrive_mid_pass
        self.runtime.tick()
        # The converged pass acknowledges only the captured high-water mark:
        # push-1 is served, push-2 survives to schedule its own follow-up.
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", "msg-2"))
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_multi_page_catch_up_acknowledges_only_after_completion(self):
        self.runtime.tick()
        self.feed.publish("msg-1", "msg-2", "msg-3")
        self.signal(event_id="push-1")
        self.runtime.tick()  # page 1 of 3: not converged, nothing acknowledged
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)
        self.runtime.tick()  # page 2 of 3 (delay 0 keeps it due): still not converged
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 1)
        self.runtime.tick()  # final page converges -> acknowledged
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)
        for source_id in ("msg-1", "msg-2", "msg-3"):
            self.assertIsNotNone(self.runtime.sync.head("gmail-acct1", source_id))

    def test_repeated_signals_coalesce_into_bounded_work(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        for index in range(5):
            self.signal(event_id="push-%d" % index)
        reads_before = self.feed.reads
        self.runtime.tick()
        self.assertEqual(self.feed.reads, reads_before + 1)  # five signals, one pass
        self.assertEqual(self.runtime.sync.pending_signals(self.cid()), 0)

    def test_one_noisy_source_does_not_starve_other_sources(self):
        self.runtime.tick()
        self.feed.publish("msg-1")
        for index in range(5):
            self.signal(self.cid(), event_id="push-%d" % index)
        feed_reads = self.feed.reads
        notes_reads = self.notes.reads
        self.runtime.tick()
        self.assertEqual(self.feed.reads, feed_reads + 1)
        self.assertEqual(self.notes.reads, notes_reads)  # quiet source keeps its cadence
        # The quiet source still serves its own signals when it has them.
        self.notes.publish("note-1")
        self.signal(self.other_cid(), event_id="push-n1")
        self.runtime.tick()
        self.assertIsNotNone(self.runtime.sync.head("notes-acct2", "note-1"))
        self.assertEqual(self.runtime.sync.pending_signals(self.other_cid()), 0)


if __name__ == "__main__":
    unittest.main()
