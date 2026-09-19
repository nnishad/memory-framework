"""R6 regression: a source signal is acknowledged only after a complete caught-up page.

Catch-up is defined centrally as ``page_committed AND checkpoint_complete AND
NOT page.more`` - not by the paging flag alone. A page that did not advance the
checkpoint (``complete`` false) keeps the captured signal pending no matter what
``more`` says, and a completed-but-still-paging page keeps it pending too. The
same normalized outcome drives both signal acknowledgment and feed scheduling, so
an incomplete page neither drops the request nor causes a tight polling loop.
"""
import tempfile
import time
import unittest
from pathlib import Path

from personal_memory.store import Store
from personal_memory.source_runtime import SourceRuntime
from personal_memory.source_sdk import read_state, source_operation, source_page, stream_spec
from tests.test_source_sync import FixtureAdapter, note_record


class PageAdapter(FixtureAdapter):
    """One required incremental feed whose pages are scripted explicitly.

    Each script entry chooses the operations, the checkpoint-completion flag and the
    continuation flag independently, so an ``incomplete`` page (``complete`` false) is
    distinguishable from a completed-but-paging page. When the script is exhausted the
    last page repeats, modelling a stalled feed. ``legacy`` drops the continuation flag
    entirely, as adapters written before ``more`` existed did.
    """

    def __init__(self, adapter_id="google.gmail"):
        super().__init__()
        self._adapter_id = adapter_id
        self.scripts = []
        self.reads = 0

    def spec(self):
        value = super().spec()
        value["adapter_id"] = self._adapter_id
        return value

    def discover(self, context):
        return [stream_spec("messages", modes=["incremental"], version_order="integer")]

    def add(self, ops=(), complete=True, more=False, cursor=None, page_id=None, legacy=False):
        step = {"ops": list(ops), "complete": complete, "cursor": cursor or {"done": True},
                "page_id": page_id, "more": more, "legacy": legacy}
        self.scripts.append(step)
        return step

    def read_page(self, context, state):
        index = min(self.reads, len(self.scripts) - 1)
        self.reads += 1
        step = self.scripts[index]
        operations = [source_operation("upsert", source_id, source_version=index + 1,
                                       records=[note_record(source_id, source=context["source"])])
                      for source_id in step["ops"]]
        page = source_page(page_id=step["page_id"] or ("pg-%d" % index), operations=operations,
                           next_state=read_state(cursor=step["cursor"], mode=state["mode"],
                                                 state_version=state["state_version"]),
                           complete=step["complete"], more=step["more"])
        if step["legacy"]:  # adapters predating the continuation flag omit it entirely
            page.pop("more")
        return page


class CompletionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "memory.db")

    def runtime(self, adapter):
        runtime = SourceRuntime(self.store, self.root, config={"enabled": False}, adapter=adapter)
        self.addCleanup(runtime.close)
        connection = runtime.sync.configure(adapter_id="google.gmail", source="gmail-acct1",
                                            scope={"poll_seconds": 300}, retention="archive")
        return runtime, connection["connection_id"]

    def signal(self, runtime, cid, event_id):
        return runtime.sync.signal(cid, event_id=event_id)

    def pending(self, runtime, cid):
        return runtime.sync.pending_signals(cid)

    def cursor(self, runtime, cid):
        return runtime.sync.stream_state(cid, "messages", partition="", role="incremental")["cursor"]

    def schedule_row(self, runtime, cid):
        return runtime._schedule_row(cid, "incremental")

    def schedule_due(self, runtime, cid):
        row = self.schedule_row(runtime, cid)
        return row[0] if row else None  # next_at

    def force_due(self, runtime, cid):
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE source_schedule SET next_at=0,error=NULL,failures=0 "
                       "WHERE connection_id=? AND role='incremental'", (cid,))

    def test_incomplete_page_retains_signal_and_holds_checkpoint(self):
        feed = PageAdapter()
        # A nonempty page that applies an item but does NOT complete the checkpoint.
        feed.add(ops=["msg-1"], complete=False, more=False, cursor={"page": 1})
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()
        # The review's exact failure: an incomplete page must not acknowledge the signal.
        self.assertEqual(1, self.pending(runtime, cid))
        self.assertIsNone(self.cursor(runtime, cid))  # checkpoint never advanced
        # A retained signal must not become a tight polling loop: bounded retry, flagged error.
        self.assertGreater(self.schedule_due(runtime, cid), time.time())
        self.assertIsNotNone(self.schedule_row(runtime, cid)[2])

    def test_empty_incomplete_page_retains_signal(self):
        feed = PageAdapter()
        feed.add(ops=[], complete=False, more=False, cursor={"page": 1})
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()
        self.assertEqual(1, self.pending(runtime, cid))

    def test_legacy_page_omitting_more_uses_completion_rule(self):
        # Missing `more` defaults to false, but an incomplete checkpoint still holds.
        feed = PageAdapter()
        feed.add(ops=["msg-1"], complete=False, cursor={"page": 1}, legacy=True)
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()
        self.assertEqual(1, self.pending(runtime, cid))
        self.assertIsNone(self.cursor(runtime, cid))

    def test_replaying_an_incomplete_page_never_completes_it(self):
        # A stable page id re-read at an unchanged cursor replays the same receipt.
        feed = PageAdapter()
        feed.add(ops=["msg-1"], complete=False, more=False, cursor={"page": 1}, page_id="stable")
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        for _ in range(4):
            self.force_due(runtime, cid)
            runtime.tick()
        self.assertEqual(1, self.pending(runtime, cid))
        self.assertIsNone(self.cursor(runtime, cid))

    def test_complete_paging_page_advances_checkpoint_but_keeps_signal(self):
        # A completed page that still declares continuation advances the checkpoint and
        # keeps converging promptly, but does not acknowledge the signal yet.
        feed = PageAdapter()
        feed.add(ops=["msg-1"], complete=True, more=True, cursor={"page": 1})
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()
        self.assertEqual(1, self.pending(runtime, cid))
        self.assertIsNotNone(self.cursor(runtime, cid))
        self.assertLessEqual(self.schedule_due(runtime, cid), time.time())  # converges fast

    def test_complete_final_page_acknowledges_signal(self):
        feed = PageAdapter()
        feed.add(ops=["msg-1"], complete=True, more=True, cursor={"page": 1})
        feed.add(ops=["msg-2"], complete=True, more=False, cursor={"done": True, "head": 2})
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()   # page 1: completed but still paging
        runtime.tick()   # page 2: the complete, non-paging final page
        self.assertEqual(0, self.pending(runtime, cid))
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "msg-1"))
        self.assertIsNotNone(runtime.sync.head("gmail-acct1", "msg-2"))

    def test_recovered_completed_page_after_incomplete_acks(self):
        feed = PageAdapter()
        feed.add(ops=["msg-1"], complete=False, more=False, cursor={"page": 1}, page_id="stuck")
        feed.add(ops=[], complete=True, more=False, cursor={"done": True, "head": 1}, page_id="finish")
        runtime, cid = self.runtime(feed)
        self.signal(runtime, cid, "push-1")
        runtime.tick()   # incomplete -> retained
        self.assertEqual(1, self.pending(runtime, cid))
        self.force_due(runtime, cid)
        runtime.tick()   # a later valid completed page recovers and acknowledges
        self.assertEqual(0, self.pending(runtime, cid))


if __name__ == "__main__":
    unittest.main()
