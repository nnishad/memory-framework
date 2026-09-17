"""Claims validate their evidence before superseding current knowledge.

A new claim is only accepted while its cited evidence is live and visible;
validation and supersession share one write transaction, so a retirement that
races the creation can never publish an unsupported active claim. An idempotent
replay revalidates instead of bypassing the evidence check, a rejected
replacement leaves the current claim untouched, and a valid replacement
supersedes exactly once. Explicit historical retrieval keeps seeing superseded
claims; retraction is reserved for claims whose evidence itself retired.
"""
import copy
import tempfile
import unittest
from pathlib import Path

from personal_memory import reset
from personal_memory.store import Store
from test_ingestion import item


def note(source_id, text="A source note about a bicycle repair."):
    record = copy.deepcopy(item(source_id))
    record["text"] = text
    return record


class ClaimValidationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")
        reset.initialize(self.store)

    def put(self, *records):
        return [r["id"] for r in self.store.ingest_contract(list(records))["records"]]

    def status(self, claim_id):
        with self.store.connect() as db:
            return db.execute("SELECT status FROM claims WHERE id=?", (claim_id,)).fetchone()[0]

    def test_hidden_evidence_cannot_create_a_claim(self):
        rid = self.put(note("one"))[0]
        self.store.supersede(rid)
        with self.assertRaises(ValueError):
            self.store.claim("Owner cycles daily", rid)

    def test_forgotten_evidence_cannot_create_a_claim(self):
        rid = self.put(note("one"))[0]
        self.store.forget(rid)
        with self.assertRaises(ValueError):
            self.store.claim("Owner cycles daily", rid)

    def test_replay_cannot_bypass_evidence_validation(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("Owner cycles daily", rid)
        # Simulate a database written before the retirement cascade existed:
        # the evidence is hidden while the claim row never went through validation.
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
        with self.assertRaises(ValueError):
            self.store.claim("Owner cycles daily", rid)  # identical replay arguments
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM claims").fetchone()[0], 1)

    def test_rejected_replacement_leaves_the_current_claim_unchanged(self):
        live, doomed = self.put(note("one"), note("two", "second note"))
        current = self.store.claim("Uses PostgreSQL", live, subject_id=None, predicate="database")
        self.store.supersede(doomed)
        with self.assertRaises(ValueError):
            self.store.claim("Uses SQLite", doomed, predicate="database", supersedes=current["id"])
        self.assertEqual(self.status(current["id"]), "active")
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM claims").fetchone()[0], 1)

    def test_valid_replacement_supersedes_exactly_once(self):
        one, two = self.put(note("one"), note("two", "second note"))
        first = self.store.claim("Uses PostgreSQL", one, predicate="database")
        second = self.store.claim("Uses SQLite", two, predicate="database", supersedes=first["id"])
        self.assertEqual(self.status(first["id"]), "superseded")
        replay = self.store.claim("Uses SQLite", two, predicate="database", supersedes=first["id"])
        self.assertEqual(replay["id"], second["id"])
        self.assertEqual(self.status(first["id"]), "superseded")
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM claims").fetchone()[0], 2)
        # Explicit historical retrieval still surfaces the superseded claim...
        history = self.store.search("PostgreSQL", include_history=True)
        self.assertIn(first["id"], [c["id"] for c in history["claims"]])
        # ...while current recall only sees the active replacement.
        current = self.store.search("PostgreSQL")
        self.assertEqual([c["id"] for c in current["claims"]], [])

    def test_retirement_cannot_leave_an_unsupported_active_claim(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("Owner cycles daily", rid)
        self.assertEqual(self.status(claim["id"]), "active")
        # Retirement racing creation must not leave the active claim published:
        # retiring the evidence retracts claims that rest on it.
        self.store.supersede(rid)
        self.assertNotEqual(self.status(claim["id"]), "active")
        self.assertEqual([c["id"] for c in self.store.search("bicycle")["claims"]], [])

    def test_upgrade_repair_retracts_claims_on_retired_evidence(self):
        rid = self.put(note("one"))[0]
        claim = self.store.claim("Owner cycles daily", rid)
        from personal_memory import lifecycle
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
            db.execute("DELETE FROM identity_edges")  # keep repair focus on claims
            report = lifecycle._repair(db, self.store)
        self.assertGreaterEqual(report["claims"], 1)
        self.assertNotEqual(self.status(claim["id"]), "active")
        with self.store.connect() as db:
            again = lifecycle._repair(db, self.store)
        self.assertEqual(again["claims"], 0)


if __name__ == "__main__":
    unittest.main()
