"""Fix 1: the memory service holds sole ownership of indexing journals.

The awareness worker must never start a second indexing backend: related-memory
search goes through the running service, the service is the only process that
may own the semantic/Hindsight indexing journals, and worker resources close on
normal exit. A retrieval outage still permits analysis of the originals with an
explicit incomplete-retrieval status.
"""
import json
import os
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from personal_memory import awareness, changes, reset
from personal_memory.backend import load_backend
from personal_memory.client import Client
from personal_memory.common import now
from personal_memory.retrieval import Hybrid
from personal_memory.service import MemoryService
from personal_memory.store import Store
from personal_memory.awareness_worker import ServiceRetrieval, process_once


def record(source_id, text, source="gmail-e2e"):
    return {"schema_version": "1.0", "source": source, "source_id": source_id,
            "revision": "1", "kind": "episode", "occurred_at": "2026-09-16T12:00:00Z",
            "observed_at": now(), "text": text, "participants": [],
            "provenance": {"connector_id": "test.connector", "connector_version": "1.0",
                           "source_locator": "test://" + source_id, "origin": "source",
                           "parent_record_ids": []}, "extensions": {}}


def start_hindsight_server(counts):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass

        def answer(self, data):
            raw = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path.endswith("/memories/recall"):
                # One distinct candidate so the worker's related-memory channel is exercised.
                self.answer({"results": [{"document_id": "rec_fake_related",
                                          "scores": {"semantic": 0.9}}]})
            else:
                counts.append(data)
                self.answer({"success": True, "async": False, "items_count": len(data["items"])})

        def do_DELETE(self):
            self.answer({"success": True})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def shutdown():
        server.shutdown()
        server.server_close()

    return server, shutdown


class IndexOwnershipTests(unittest.TestCase):
    def setUp(self):
        os.environ["PERSONAL_MEMORY_DISABLE_SEMANTIC"] = "1"
        os.environ["PERSONAL_MEMORY_DISABLE_RERANK"] = "1"
        self.addCleanup(os.environ.pop, "PERSONAL_MEMORY_DISABLE_SEMANTIC", None)
        self.addCleanup(os.environ.pop, "PERSONAL_MEMORY_DISABLE_RERANK", None)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def journal_arrival(self, store, record_id, source="gmail-e2e"):
        """Append one live-arrival journal event for an ingested record."""
        with store.connect() as db:
            return changes.append(db, connection_id="sconn_fixture", source=source,
                                  stream="messages", partition="", generation=1,
                                  source_item_id=record_id, kind="created",
                                  origin_mode="incremental", record_ids=[record_id],
                                  coordinates={"arrival": "fresh"})

    def test_only_one_process_can_own_the_indexing_journals(self):
        store = Store(self.root / "memory.db")
        owner = load_backend(store, config={}, index_owner=True)
        self.addCleanup(owner.close)
        with self.assertRaises(RuntimeError):
            load_backend(store, config={}, index_owner=True)
        # An ownership-optional local read stays possible: only writers contend.
        reader = load_backend(store, config={})
        self.addCleanup(reader.close)

    def test_service_retrieval_reads_from_the_running_service(self):
        service = MemoryService(self.root / "data", "a" * 40,
                                retrieval_config={"semantic": {"enabled": False}},
                                source_config={"enabled": False})
        self.addCleanup(service.close)
        rid = service.dispatch("/v1/ingest", {"items": [record("m1", "quarterly budget review meeting notes")]},
                               service.principals[0])["records"][0]["id"]
        backend = ServiceRetrieval(lambda path, payload: service.dispatch(path, payload, service.principals[0]))
        found = backend.search(query="budget review", limit=3, depth="fast", exclude_record_ids=[])
        self.assertEqual([row["id"] for row in found["episodes"]], [rid])
        self.assertEqual(backend.search(query="nothing matches this topic", limit=3, depth="fast",
                                        exclude_record_ids=[])["episodes"], [])

    def test_worker_recall_uses_the_service_and_indexing_happens_once(self):
        retains = []
        server, shutdown = start_hindsight_server(retains)
        self.addCleanup(shutdown)
        data_dir = self.root / "data"
        # Deterministic failure-free counting: the service's own index worker thread
        # would race the manual sync below, so only its ownership is under test here.
        starter = patch.object(Hybrid, "_start_index_thread", lambda self, engine: None)
        starter.start()
        self.addCleanup(starter.stop)
        service = MemoryService(
            data_dir, "a" * 40,
            retrieval_config={"semantic": {"enabled": False},
                              "hindsight": {"enabled": True, "url": f"http://127.0.0.1:{server.server_port}",
                                            "bank_id": "fixture", "sources": ["*"]}},
            source_config={"enabled": False})
        self.addCleanup(service.close)
        # The service owns its indexing journal...
        self.assertTrue(hasattr(service.retrieval, "index_lease") or
                        getattr(service.retrieval, "owns_indexing", False))
        # ...and a second writer cannot start against the same data directory.
        with self.assertRaises(RuntimeError):
            load_backend(Store(service.store.path), config={}, index_owner=True)
        store = service.store
        rid = service.dispatch("/v1/ingest",
                               {"items": [record("m1", "quarterly budget review meeting notes")]},
                               service.principals[0])["records"][0]["id"]
        service.dispatch("/v1/awareness/configure",
                         {"id": "bg", "profile": "owner", "purpose": "background"},
                         service.principals[0])
        changes.configure(store, {"journal": {"enabled": True}})
        reset.initialize(store)
        event = self.journal_arrival(store, rid)
        self.assertIsNotNone(event)
        # The worker only searches through the running service.
        worker_retrieval = ServiceRetrieval(
            lambda path, payload: service.dispatch(path, payload, service.principals[0]))
        self.addCleanup(worker_retrieval.close)
        seen = []

        def analyze(packet):
            seen.append(packet)
            return {"summary": "Reviewed the arrival",
                    "citations": packet["events"][0]["record_ids"], "proposals": []}

        outcome = process_once(store, consumer_id="bg", analyze=analyze,
                               retrieval=lambda: worker_retrieval)
        self.assertEqual(outcome["state"], "complete")
        self.assertTrue(seen[0]["evidence"])
        # The service's single owner journals the retain; the worker never does.
        service.retrieval.hindsight.sync(batch=8)
        self.assertEqual(len(retains), 1)
        retained = [item["document_id"] for request in retains for item in request["items"]]
        self.assertEqual(retained, [rid])
        with store.connect() as db:
            done = db.execute("SELECT COUNT(*) FROM hindsight_done").fetchone()[0]
            pending = db.execute("SELECT COUNT(*) FROM hindsight_pending").fetchone()[0]
        self.assertGreaterEqual(done, 1)
        self.assertEqual(pending, 0)
        recall = service.dispatch("/v1/search", {"query": "budget review", "limit": 5},
                                  service.principals[0])
        self.assertIn(rid, [row["id"] for row in recall["episodes"]])

    def test_worker_analyzes_originals_when_the_service_is_unreachable(self):
        store = Store(self.root / "memory.db")
        reset.initialize(store)
        changes.configure(store, {"journal": {"enabled": True}})
        rid = store.ingest([record("m1", "quarterly budget review meeting notes")])["records"][0]["id"]
        self.journal_arrival(store, rid)
        awareness.configure_consumer(store, id="bg", profile="owner", purpose="background")
        # A port that is never bound: the retrieval channel is down.
        down = ServiceRetrieval(Client("http://127.0.0.1:9", "a" * 40, timeout=1))
        self.addCleanup(down.close)
        seen = []

        def analyze(packet):
            seen.append(packet)
            return {"summary": "Reviewed without related memories",
                    "citations": packet["events"][0]["record_ids"], "proposals": []}

        outcome = process_once(store, consumer_id="bg", analyze=analyze,
                               retrieval=lambda: down)
        self.assertEqual(outcome["state"], "complete")
        self.assertTrue(seen[0]["evidence"])
        self.assertEqual(seen[0]["related_memories"], [])
        self.assertEqual(seen[0]["related_memories_status"], "incomplete_retrieval")


if __name__ == "__main__":
    unittest.main()
