"""Fix 5 regression: stream and partition identity must reach adapter reads.

The sync runtime already leases, cursors and retries per (stream, partition,
role), but the adapter read context carried neither selector, so one adapter
serving multiple streams or partitions could not tell which slice to read and
the runtime never validated the requested selectors against discovery.
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory.source_sdk import read_state, source_operation, source_page, stream_spec
from personal_memory.source_sync import SourceSync, SyncWorker
from personal_memory.store import Store
from tests.test_source_sync import FixtureAdapter, note_record


class PartitionedAdapter(FixtureAdapter):
    """Two streams; the messages stream declares two partitions."""

    DATA = {("messages", "inbox"): ["inbox-1"],
            ("messages", "sent"): ["sent-1", "sent-2"],
            ("contacts", ""): ["contact-1"]}

    def __init__(self):
        super().__init__()
        self.served = []

    def discover(self, context):
        return [stream_spec("messages", modes=["backfill"],
                            partitions=[{"id": "inbox"}, {"id": "sent"}]),
                stream_spec("contacts", modes=["backfill"])]

    def read_page(self, context, state):
        stream = context.get("stream")
        partition = context.get("partition") or ""
        self.served.append((stream, partition))
        operations = [source_operation("upsert", source_id,
                                       records=[note_record(source_id, source="part-acct")],
                                       source_version=1)
                      for source_id in self.DATA.get((stream, partition), [])]
        return source_page(page_id=f"pg-{stream}-{partition}", operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"done": True}, mode=state["mode"]))


class StreamIdentityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        self.adapter = PartitionedAdapter()
        self.sync = SourceSync(self.store, {"fixture.parts": self.adapter})
        self.conn = self.sync.configure(adapter_id="fixture.parts", source="part-acct",
                                        scope={}, retention="mirror")
        self.cid = self.conn["connection_id"]

    def _sources(self):
        with self.store.connect() as db:
            return {row[0] for row in db.execute(
                "SELECT source_id FROM records WHERE deleted=0")}

    def _state(self, stream, partition=""):
        return self.sync.stream_state(self.cid, stream, partition=partition, role="backfill")

    def test_selectors_reach_read_page(self):
        worker = SyncWorker(self.sync)
        self.assertEqual(worker.run_once(self.cid, stream="messages", partition="inbox",
                                         role="backfill", owner="w")["status"], "committed")
        self.assertEqual(worker.run_once(self.cid, stream="messages", partition="sent",
                                         role="backfill", owner="w")["status"], "committed")
        self.assertEqual(self.adapter.served, [("messages", "inbox"), ("messages", "sent")])

    def test_partitions_produce_distinct_records(self):
        worker = SyncWorker(self.sync)
        worker.run_once(self.cid, stream="messages", partition="inbox", role="backfill", owner="w")
        self.assertEqual(self._sources(), {"inbox-1"})
        worker.run_once(self.cid, stream="messages", partition="sent", role="backfill", owner="w")
        self.assertEqual(self._sources(), {"inbox-1", "sent-1", "sent-2"})

    def test_streams_produce_distinct_inputs(self):
        worker = SyncWorker(self.sync)
        worker.run_once(self.cid, stream="contacts", role="backfill", owner="w")
        self.assertEqual(self._sources(), {"contact-1"})

    def test_partitions_have_independent_cursors(self):
        worker = SyncWorker(self.sync)
        worker.run_once(self.cid, stream="messages", partition="inbox", role="backfill", owner="w")
        self.assertTrue((self._state("messages", "inbox")["cursor"] or {}).get("done"))
        self.assertIsNone(self._state("messages", "sent")["cursor"])

    def test_restart_recovers_each_partition_independently(self):
        SyncWorker(self.sync).run_once(self.cid, stream="messages", partition="inbox",
                                       role="backfill", owner="w")
        # A fresh worker (post-restart) reads the other partition from scratch.
        reopened = SourceSync(Store(self.store.path), {"fixture.parts": PartitionedAdapter()})
        second = SyncWorker(reopened)
        self.assertEqual(second.run_once(self.cid, stream="messages", partition="sent",
                                         role="backfill", owner="w")["status"], "committed")
        self.assertEqual(self._sources(), {"inbox-1", "sent-1", "sent-2"})

    def test_undeclared_partition_is_rejected(self):
        worker = SyncWorker(self.sync)
        result = worker.run_once(self.cid, stream="messages", partition="archive",
                                 role="backfill", owner="w")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self._sources(), set())

    def test_partition_on_stream_without_partitions_is_rejected(self):
        worker = SyncWorker(self.sync)
        result = worker.run_once(self.cid, stream="contacts", partition="x",
                                 role="backfill", owner="w")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(self._sources(), set())

    def test_single_stream_default_partition_still_works(self):
        # Backward compatibility: a stream that declares no partitions is read with
        # the empty default selector, exactly as before stream/partition plumbing.
        class Single(FixtureAdapter):
            def read_page(self, context, state):
                self.seen = context.get("partition")
                return source_page(page_id="pg", operations=[
                    source_operation("upsert", "solo",
                                     records=[note_record("solo", source="single-acct")],
                                     source_version=1)],
                    next_state=read_state(state_version=state["state_version"],
                                          cursor={"done": True}, mode=state["mode"]))

        single = Single()
        sync = SourceSync(self.store, {"fixture.single": single})
        conn = sync.configure(adapter_id="fixture.single", source="single-acct", scope={},
                              retention="mirror")
        result = SyncWorker(sync).run_once(conn["connection_id"], stream="messages",
                                           role="backfill", owner="w")
        self.assertEqual(result["status"], "committed")
        self.assertEqual(single.seen, "")


if __name__ == "__main__":
    unittest.main()
