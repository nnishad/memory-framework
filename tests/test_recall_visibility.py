"""Visibility rules govern final recall hydration.

Candidate rounds can race retirement: evidence may be superseded (hidden) or
forgotten (deleted) after a backend already proposed it. The hydration step in
adaptive and progressive recall must reuse the canonical live-and-visible
predicate so no forgotten or retired evidence - or a claim resting on it -
survives into the answer, while explicit historical retrieval still works and
duplicate/branch bookkeeping is recomputed after filtering.
"""
import copy
import tempfile
import unittest
from pathlib import Path

from personal_memory import reset
from personal_memory.adaptive import AdaptiveRecall
from personal_memory.investigate import Investigation
from personal_memory.store import Store
from test_ingestion import item


def derived_item(source_id, text):
    record = copy.deepcopy(item(source_id))
    record["text"] = text
    return record


class FakeBackend:
    """Stand-in retrieval backend; may mutate the store when a round runs."""

    def __init__(self, record_ids, hooks=None):
        self.record_ids = list(record_ids)
        self.hooks = hooks or {}
        self.calls = []

    def search(self, query, queries=None, depth="balanced", limit=12, **filters):
        self.calls.append(depth)
        hook = self.hooks.get(len(self.calls))
        if hook: hook()
        return {"episodes": [{"id": rid, "text": "stale candidate text", "span_start": 0}
                             for rid in self.record_ids],
                "claims": [], "coverage": [], "diagnostics": {},
                "retrieval_status": "candidates_found"}


class AdaptiveVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest_contract(list(records))["records"]]

    def recall(self, backend):
        return AdaptiveRecall(self.store, backend, {})

    def test_evidence_retired_after_candidates_is_not_returned(self):
        first = self.put(derived_item("one", "A source note about a bicycle repair."))[0]
        backend = FakeBackend([first], hooks={1: lambda: self.store.supersede(first)})
        result = self.recall(backend).search("bicycle repair")
        self.assertEqual(result["episodes"], [])
        self.assertEqual(result["retrieval_status"], "no_relevant_evidence")

    def test_evidence_retired_between_progressive_rounds_is_not_returned(self):
        first, second = self.put(derived_item("one", "bicycle repair notes"),
                                 derived_item("two", "car repair notes"))
        # Round 1 proposes both; the second round launches only after round 1's
        # candidate is retired underneath it.
        backend = FakeBackend([first, second], hooks={1: lambda: self.store.supersede(first)})
        result = self.recall(backend).search("repair", subqueries=["car"], max_calls=2)
        self.assertEqual([e["id"] for e in result["episodes"]], [second])

    def test_forgotten_evidence_never_returns_even_with_history(self):
        gone = self.put(derived_item("one", "bicycle repair"))[0]
        self.store.forget(gone)
        backend = FakeBackend([gone])
        for filters in ({}, {"include_history": True}):
            result = self.recall(backend).search("bicycle repair", **filters)
            self.assertEqual(result["episodes"], [], filters)

    def test_explicit_history_still_returns_retired_evidence(self):
        retired = self.put(derived_item("one", "bicycle repair"))[0]
        self.store.supersede(retired)
        backend = FakeBackend([retired])
        live = self.recall(backend).search("bicycle repair")
        self.assertEqual(live["episodes"], [])
        history = self.recall(backend).search("bicycle repair", include_history=True)
        self.assertEqual([e["id"] for e in history["episodes"]], [retired])

    def test_claims_cannot_remain_when_their_evidence_is_excluded(self):
        rid = self.put(derived_item("one", "A source note about a bicycle repair."))[0]
        claim = self.store.claim("Owner cycles daily", rid)
        backend = FakeBackend([rid], hooks={1: lambda: self.store.supersede(rid)})
        recall = self.recall(backend)
        result = recall.search("bicycle repair")
        self.assertEqual(result["claims"], [])
        # The claim row exists but its supporting evidence is hidden: without the
        # episode, it may not be hydrated even when explicitly proposed.
        with self.store.connect() as db:
            self.assertTrue(db.execute("SELECT 1 FROM claims WHERE id=?", (claim["id"],)).fetchone())

    def test_duplicate_bookkeeping_is_recomputed_after_filtering(self):
        # Same normalized text on two records: retiring the first must leave the
        # second as a clean standalone episode with no reference to hidden evidence.
        first, second = self.put(derived_item("one", "identical repair text"),
                                 derived_item("two", "identical repair text"))
        self.store.supersede(first)
        backend = FakeBackend([first, second])
        result = self.recall(backend).search("identical repair text")
        self.assertEqual([e["id"] for e in result["episodes"]], [second])
        self.assertEqual(result["episodes"][0]["duplicate_record_ids"], [])

    def test_hidden_duplicate_is_dropped_from_a_surviving_episode(self):
        first, second = self.put(derived_item("one", "identical repair text"),
                                 derived_item("two", "identical repair text"))
        self.store.supersede(second)  # the duplicate, not the canonical, retires
        backend = FakeBackend([first, second])
        result = self.recall(backend).search("identical repair text")
        self.assertEqual([e["id"] for e in result["episodes"]], [first])
        self.assertEqual(result["episodes"][0]["duplicate_record_ids"], [])


class InvestigationVisibilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest_contract(list(records))["records"]]

    def branch(self, bid, text):
        return {"id": bid, "intent": text, "queries": [text], "filters": {}}

    def test_branch_excludes_results_retired_during_retrieval(self):
        rid = self.put(derived_item("one", "bicycle repair"))[0]
        backend = FakeBackend([rid], hooks={1: lambda: self.store.supersede(rid)})
        engine = Investigation(self.store, backend, intelligence=None)
        self.addCleanup(engine.close)
        result = engine.search(goal="repair", branches=[self.branch("b1", "bicycle repair")])
        self.assertEqual(result["episodes"], [])
        requirement = result["requirements"][0]
        self.assertEqual(requirement["record_ids"], [])
        self.assertEqual(requirement["status"], "no_evidence_in_context")

    def test_branch_keeps_live_candidates(self):
        live = self.put(derived_item("one", "bicycle repair"))[0]
        backend = FakeBackend([live])
        engine = Investigation(self.store, backend, intelligence=None)
        self.addCleanup(engine.close)
        result = engine.search(goal="repair", branches=[self.branch("b1", "bicycle repair")])
        self.assertEqual([e["id"] for e in result["episodes"]], [live])
        self.assertEqual(result["requirements"][0]["record_ids"], [live])


if __name__ == "__main__":
    unittest.main()
