"""Measure semantic indexing throughput, idle-poll database work, and recall latency.

Runs entirely against public APIs (ingest, sync, status, search) with a
deterministic hash embedder, so the same script executes against any revision
of the codebase. Used to record the before/after numbers for the incremental
semantic work queue. Usage: python scripts/bench_semantic.py [records]
"""
import hashlib
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from personal_memory.retrieval import Hybrid
from personal_memory.semantic import SemanticIndex
from personal_memory.store import Store


class HashEmbedder:
    """Deterministic dependency-free embedder so the bench measures the
    framework's bookkeeping, not model inference."""

    key = "bench-model-v1"

    def chunks(self, text):
        for start in range(0, len(text), 200):
            end = min(len(text), start + 200)
            yield start, end, text[start:end]

    def documents(self, texts):
        return [[b / 255.0 - 0.5 for b in hashlib.sha256(t.encode()).digest()[:32]]
                for t in texts]

    def query(self, text):
        return self.documents([text])[0]


def record(index):
    text = ("note %d about the garage engine and the violet folder " % index) * 6
    return {"source": "bench", "source_id": "item-%d" % index, "revision": "1",
            "occurred_at": "2026-09-01T00:00:00Z", "kind": "note", "text": text,
            "metadata": {}}


def main(total=1000):
    timings = {"records": total}
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "memory.db")
        started = time.perf_counter()
        for start in range(0, total, 100):
            store.ingest([record(i) for i in range(start, min(start + 100, total))])
        timings["ingest_seconds"] = round(time.perf_counter() - started, 3)

        engine = SemanticIndex(store, embedder=HashEmbedder())
        started = time.perf_counter()
        polls = 0
        while True:
            done = engine.sync(batch=64)
            polls += 1
            if not done and engine.status()["pending_records"] == 0:
                break
        timings["index_all_seconds"] = round(time.perf_counter() - started, 3)
        timings["index_all_polls"] = polls
        timings["index_records_per_second"] = round(total / max(timings["index_all_seconds"], 0.001), 1)

        # Idle polls: the queue is empty and the archive is live. This is the
        # repeated background work a running service pays every wake-up.
        started = time.perf_counter()
        for _ in range(20):
            engine.sync(batch=64)
        timings["idle_sync_ms_per_poll"] = round((time.perf_counter() - started) / 20 * 1000, 3)
        started = time.perf_counter()
        for _ in range(20):
            engine.status()
        timings["status_ms_per_call"] = round((time.perf_counter() - started) / 20 * 1000, 3)

        # Incremental capture: one new record then a single bounded pass.
        store.ingest([record(total + 1)])
        started = time.perf_counter()
        engine.sync(batch=64)
        timings["single_record_sync_ms"] = round((time.perf_counter() - started) * 1000, 3)

        # Signature-tolerant across revisions (index_owner arrived later).
        hybrid_kwargs = {"hindsight": None, "start": False}
        try:
            hybrid = Hybrid(store, {"semantic": {"enabled": True}}, semantic=engine,
                            index_owner=False, **hybrid_kwargs)
        except TypeError:
            hybrid = Hybrid(store, {"semantic": {"enabled": True}}, semantic=engine,
                            **hybrid_kwargs)
        latencies = []
        for word in ("engine", "folder", "garage", "violet", "note"):
            for _ in range(8):
                started = time.perf_counter()
                hybrid.search(word, expand_entities=False)
                latencies.append((time.perf_counter() - started) * 1000)
        latencies.sort()
        timings["recall_p50_ms"] = round(statistics.median(latencies), 2)
        timings["recall_p95_ms"] = round(latencies[int(len(latencies) * 0.95)], 2)
    print(json.dumps(timings, indent=2))


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 1000)
