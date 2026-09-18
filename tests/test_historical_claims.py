"""Fix 7: retired evidence demotes claims to history, never to retraction.

Evidence retirement (supersession, visibility repair) and forgetting are
different facts about a claim. A claim that merely rested on retired evidence
stays inspectable under explicit historical retrieval with status 'retired';
it never returns to current recall. Forgotten and explicitly retracted claims
stay excluded in every mode, and the conservative migration reclassifies only
retirements whose reason is established in the database - an unexplained
'retracted' claim is never resurrected.
"""
import copy
import tempfile
import unittest
from pathlib import Path

from personal_memory import lifecycle, reset
from personal_memory.store import Store
from test_ingestion import item


def note(source_id, text="A source note about a bicycle repair."):
    record = copy.deepcopy(item(source_id))
    record["text"] = text
    return record


class HistoricalClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest_contract(list(records))["records"]]

    def status(self, claim_id):
        with self.store.connect() as db:
            return db.execute("SELECT status FROM claims WHERE id=?", (claim_id,)).fetchone()[0]

    def hide(self, record_id):
        with self.store.lock, self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (record_id,))

    def retract(self, claim_id):
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE claims SET status='retracted' WHERE id=?", (claim_id,))

    def test_retired_evidence_keeps_the_claim_historical_not_current(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("The bicycle was repaired", rid, predicate="repair")
        self.store.supersede(rid)
        self.assertEqual("retired", self.status(claim["id"]))
        # Ordinary recall never sees the demoted claim...
        self.assertEqual([], self.store.search("bicycle")["claims"])
        # ...but explicit historical retrieval keeps it, marked as history.
        history = self.store.search("bicycle", include_history=True)["claims"]
        self.assertIn(claim["id"], [c["id"] for c in history])
        self.assertEqual("retired", [c for c in history if c["id"] == claim["id"]][0]["status"])

    def test_replay_after_retirement_never_reactivates(self):
        rid = self.put(note("one"))[0]
        args = dict(predicate="repair")
        claim = self.store.claim("The bicycle was repaired", rid, **args)
        self.store.supersede(rid)
        with self.assertRaises(ValueError):
            self.store.claim("The bicycle was repaired", rid, **args)
        self.assertEqual("retired", self.status(claim["id"]))

    def test_forgetting_removes_the_claim_from_current_and_history(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("The bicycle was repaired", rid, predicate="repair")
        self.store.forget(rid)
        self.assertEqual("retracted", self.status(claim["id"]))
        self.assertEqual([], self.store.search("bicycle")["claims"])
        self.assertEqual([], self.store.search("bicycle", include_history=True)["claims"])

    def test_explicit_retraction_stays_excluded_in_every_mode(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("The bicycle was repaired", rid, predicate="repair")
        self.retract(claim["id"])
        self.assertEqual([], self.store.search("bicycle")["claims"])
        self.assertEqual([], self.store.search("bicycle", include_history=True)["claims"])


class RetirementMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest_contract(list(records))["records"]]

    def status(self, claim_id):
        with self.store.connect() as db:
            return db.execute("SELECT status FROM claims WHERE id=?", (claim_id,)).fetchone()[0]

    def test_repair_reclassifies_only_established_retirements(self):
        hidden_rid = self.put(note("hidden"))[0]
        hidden_claim = self.store.claim("The bicycle was repaired", hidden_rid, predicate="repair")
        unexplained_rid = self.put(note("visible"))[0]
        unexplained = self.store.claim("The bicycle was tuned", unexplained_rid, predicate="repair")
        forgotten_rid = self.put(note("gone"))[0]
        forgotten = self.store.claim("The bicycle was scrapped", forgotten_rid, predicate="repair")
        # Legacy shape: the retirement cascade had marked all three 'retracted'.
        with self.store.lock, self.store.connect() as db:
            for cid in (hidden_claim["id"], unexplained["id"]):
                db.execute("UPDATE claims SET status='retracted' WHERE id=?", (cid,))
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (hidden_rid,))
        self.store.forget(forgotten_rid)  # forget clears content: exclusion is absolute
        with self.store.lock, self.store.connect() as db:
            report = lifecycle._repair(db, self.store)
        self.assertEqual("retired", self.status(hidden_claim["id"]))       # reason established
        self.assertEqual("retracted", self.status(unexplained["id"]))      # never resurrected
        self.assertEqual("retracted", self.status(forgotten["id"]))        # forgotten stays gone
        self.assertGreaterEqual(report["claims"], 1)
        with self.store.lock, self.store.connect() as db:
            again = lifecycle._repair(db, self.store)
        self.assertEqual(0, again["claims"], "replay must find nothing new to change")
        self.assertEqual("retired", self.status(hidden_claim["id"]))

    def test_history_recall_serves_migrated_claims_and_current_recall_does_not(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("The bicycle was repaired", rid, predicate="repair")
        with self.store.lock, self.store.connect() as db:
            db.execute("UPDATE claims SET status='retracted' WHERE id=?", (claim["id"],))
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
            lifecycle._repair(db, self.store)
        self.assertEqual([], self.store.search("bicycle")["claims"])
        self.assertIn(claim["id"], [c["id"] for c in self.store.search("bicycle", include_history=True)["claims"]])


if __name__ == "__main__":
    unittest.main()
