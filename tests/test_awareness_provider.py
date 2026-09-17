"""Foreground awareness integration tests (MEMORY_AWARENESS_PLAN.md §8, F01-F06).

Two seams: the packet contract at store/service level (visibility recheck,
source bindings, receipt accounting) and the Hermes-side provider path with a
stubbed agent contract, a real HTTP service and real prefetch threads. A
supplied packet is never an exposure: only the optional request-assembled
host hook acknowledges one.
"""
import json
import sys
import tempfile
import threading
import types
import unittest
from collections import namedtuple
from pathlib import Path

from personal_memory import awareness, changes
from personal_memory.client import Client
from personal_memory.server import create_server
from personal_memory.store import Store
from tests.test_memory import record

ENABLED = {"journal": {"enabled": True}}
OCCURRED = "2026-09-16T12:00:00Z"
PACKET_MARKER = "Untrusted personal memory awareness packet"


def _install_agent_stub():
    """Minimal stand-in for the Hermes ABC; the real contract is pinned host-side."""
    if "agent.memory_provider" not in sys.modules:
        sys.modules.setdefault("agent", types.ModuleType("agent"))
        module = types.ModuleType("agent.memory_provider")
        class MemoryProvider:
            pass
        module.MemoryProvider = MemoryProvider
        module.RecallStatus = namedtuple("RecallStatus", ("provider_label", "count"))
        sys.modules["agent.memory_provider"] = module


_install_agent_stub()
from personal_memory.provider import PersonalMemoryProvider  # noqa: E402


def packet_of(text):
    """The awareness JSON block a prefetch carried, or None."""
    if PACKET_MARKER not in text:
        return None
    block = text.split(PACKET_MARKER, 1)[1].split(":\n", 1)[1]
    return json.loads(block)


class PacketContractTests(unittest.TestCase):
    """Store-level packet contract: eligibility, visibility and receipt accounting."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        changes.configure(self.store, ENABLED)

    def journal(self, source_item_id, *, source="gmail-acct1", record_ids=()):
        with self.store.connect() as db:
            return changes.append(db, connection_id="c", source=source, stream="messages",
                                  partition="", generation=1, source_item_id=source_item_id,
                                  kind="created", origin_mode="incremental",
                                  coordinates={"arrival": "fresh"}, occurred_at=OCCURRED,
                                  record_ids=list(record_ids))

    def foreground(self, sources=None):
        awareness.configure_consumer(self.store, id="fg", profile="owner", purpose="foreground",
                                     sources=sources)
        awareness.sweep(self.store, "fg")
        return "fg"

    def test_packet_carries_visible_evidence_ids_resolved_at_read_time(self):
        from personal_memory.ingestion import adapt_existing
        item = adapt_existing(record("alpha"), connector_id="tests", connector_version="1",
                              source_locator="fixture://alpha", observed_at=OCCURRED)
        rid = self.store.ingest([item])["records"][0]["id"]
        self.journal("a1", record_ids=[rid])
        self.foreground()
        packet = awareness.prepare(self.store, consumer_id="fg", session_id="s1")
        event = packet["groups"][0]["events"][0]
        self.assertEqual(event["visible_record_ids"], [rid])

    def test_forgotten_evidence_is_dropped_from_a_packet_without_resurrection(self):
        from personal_memory.ingestion import adapt_existing
        def seeded(name):
            item = adapt_existing(record(name), connector_id="tests", connector_version="1",
                                  source_locator=f"fixture://{name}", observed_at=OCCURRED)
            return self.store.ingest([item])["records"][0]["id"]
        gone, live = seeded("alpha"), seeded("beta")
        self.journal("a1", record_ids=[gone])
        self.journal("b1", record_ids=[live])
        self.foreground()
        self.store.forget(gone)
        packet = awareness.prepare(self.store, consumer_id="fg", session_id="s1")
        items = [row["source_item_id"] for group in packet["groups"] for row in group["events"]]
        self.assertEqual(items, ["b1"])
        self.assertNotIn("alpha", json.dumps(packet))

    def test_consumer_source_bindings_gate_eligibility(self):
        self.journal("g1")
        self.journal("n1", source="notes-acct1")
        self.foreground(sources=["notes-acct1"])
        packet = awareness.prepare(self.store, consumer_id="fg", session_id="s1")
        items = [row["source_item_id"] for group in packet["groups"] for row in group["events"]]
        self.assertEqual(items, ["n1"])

    def test_receipts_separate_supplied_from_exposed(self):
        self.journal("a1")
        self.foreground()
        packet = awareness.prepare(self.store, consumer_id="fg", session_id="s1")
        status = awareness.status(self.store)["receipts"]
        self.assertEqual(status["supplied_unexposed"], 1)
        self.assertEqual(status["exposed"], 0)
        awareness.expose(self.store, packet_id=packet["packet_id"], session_id="s1", turn_id="1")
        status = awareness.status(self.store)["receipts"]
        self.assertEqual(status["supplied_unexposed"], 0)
        self.assertEqual(status["exposed"], 1)


class ProviderAwarenessTests(unittest.TestCase):
    """The prefetch path supplies bounded awareness; the host hook acknowledges it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        base = Path(self.tmp.name)
        self.token = "p" * 40
        self.server = create_server(base / "data", self.token, port=0,
                                    retrieval_config={"semantic": {"enabled": False}},
                                    source_config={"enabled": False})
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.admin = Client(f"http://127.0.0.1:{self.server.server_port}", self.token)
        changes.configure(self.server.store, ENABLED)

    def journal(self, source_item_id, *, source="gmail-acct1"):
        with self.server.store.connect() as db:
            changes.append(db, connection_id="c", source=source, stream="messages", partition="",
                           generation=1, source_item_id=source_item_id, kind="created",
                           origin_mode="incremental", coordinates={"arrival": "fresh"},
                           occurred_at=OCCURRED, record_ids=[])

    def provider(self, *, awareness_cfg="default", **context):
        home = Path(self.tmp.name) / "hermes"
        state = home / "personal-memory"
        state.mkdir(parents=True, exist_ok=True)
        settings = {"url": f"http://127.0.0.1:{self.server.server_port}", "token": self.token,
                    "agent_token": self.token, "data_dir": str(Path(self.tmp.name) / "data"),
                    "prefetch_wait_ms": 2000}
        if awareness_cfg == "default":
            awareness_cfg = {"enabled": True, "consumer_id": "hermes-fg"}
        if awareness_cfg is not None:
            settings["awareness"] = awareness_cfg
        (state / "settings.json").write_text(json.dumps(settings))
        self.admin.call("/v1/awareness/configure", {"id": "hermes-fg", "profile": "owner",
                                                    "purpose": "foreground"})
        provider = PersonalMemoryProvider()
        provider.initialize("session-a", hermes_home=str(home), platform="cli",
                            user_id="owner", **context)
        self.addCleanup(provider.shutdown)
        return provider

    def receipts(self):
        return self.admin.call("/v1/awareness/status")["receipts"]

    def test_f01_unrelated_query_still_carries_a_bounded_packet(self):
        provider = self.provider()
        self.journal("alpha")
        text = provider.prefetch("completely unrelated zebra question", session_id="session-a")
        packet = packet_of(text)
        self.assertIsNotNone(packet)
        items = [row["source_item_id"] for group in packet["groups"] for row in group["events"]]
        self.assertEqual(items, ["alpha"])
        self.assertTrue(packet["token_estimate"])

    def test_f02_repeated_query_refreshes_awareness_independently_of_query_cache(self):
        provider = self.provider()
        self.journal("one")
        first = packet_of(provider.prefetch("same standing question", session_id="session-a"))
        self.assertEqual([row["source_item_id"] for group in first["groups"]
                          for row in group["events"]], ["one"])
        self.journal("two")
        second = packet_of(provider.prefetch("same standing question", session_id="session-a"))
        items = sorted(row["source_item_id"] for group in second["groups"] for row in group["events"])
        self.assertEqual(items, ["one", "two"])

    def test_f03_dropped_prefetch_is_never_acknowledged_and_work_stays_pending(self):
        provider = self.provider()
        self.journal("alpha")
        packet = packet_of(provider.prefetch("question", session_id="session-a"))
        self.assertIsNotNone(packet)
        # The request assembly never confirmed anything for this session.
        self.assertEqual(self.receipts()["supplied_unexposed"], 1)
        self.assertEqual(self.receipts()["exposed"], 0)
        # Next turn: the unacknowledged group is supplied again, not lost.
        again = packet_of(provider.prefetch("another question", session_id="session-a"))
        items = [row["source_item_id"] for group in again["groups"] for row in group["events"]]
        self.assertEqual(items, ["alpha"])

    def test_f04_packet_identity_is_stable_within_one_turn(self):
        provider = self.provider()
        self.journal("alpha")
        provider.on_turn_start(1, "same question")
        first = packet_of(provider.prefetch("same question", session_id="session-a"))
        second = packet_of(provider.prefetch("same question", session_id="session-a"))
        self.assertEqual(first["packet_id"], second["packet_id"])
        absent = provider.on_host_event("request_assembled",
                                        {"turn_id": "1", "request": {"messages": []}})
        self.assertEqual(absent["state"], "not_in_request")
        self.assertEqual(self.receipts()["exposed"], 0)
        ack = provider.on_host_event("request_assembled", {"turn_id": "1"})
        self.assertEqual(ack["state"], "confirmed")
        self.assertTrue(ack["recorded"])
        self.assertEqual(self.receipts()["exposed"], 1)
        # After acknowledgment the same turn must not re-announce the packet.
        replay = provider.on_host_event("request_assembled", {"turn_id": "1"})
        self.assertTrue(replay["already"])
        later = packet_of(provider.prefetch("same question", session_id="session-a"))
        self.assertIsNone(later)

    def test_f05_shared_or_unknown_session_never_receives_private_context(self):
        provider = self.provider(chat_type="group")
        self.journal("alpha")
        self.assertIs(provider.access_allowed, False)
        self.assertEqual(provider.prefetch("question", session_id="session-a"), "")
        self.assertEqual(self.receipts()["supplied_unexposed"], 0)

    def test_f06_default_off_and_missing_host_hook_keep_recall_working(self):
        provider = self.provider(awareness_cfg=None)
        self.journal("alpha")
        text = provider.prefetch("question", session_id="session-a")
        self.assertIn("Untrusted personal memory evidence", text)
        self.assertNotIn(PACKET_MARKER, text)
        self.assertEqual(self.receipts()["supplied_unexposed"], 0)
        # Even if an old host misfires the hook, recall state is untouched.
        ack = provider.on_host_event("request_assembled", {"turn_id": "1"})
        self.assertEqual(ack["state"], "idle")


if __name__ == "__main__":
    unittest.main()
