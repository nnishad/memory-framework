"""Retirement cascades to identity relationships.

An account<->person link is only as valid as the evidence that supports it.
When that evidence is retired (hidden) or forgotten, the link must not expand
person-based recall, block corrected ownership, or survive an upgrade repair.
Creating or confirming a link from hidden evidence must fail, while genuine
ownership conflicts and time-bounded ownership keep their behavior. Restoring
source visibility never silently reconfirms an invalidated identity.
"""
import tempfile
import unittest
from pathlib import Path

from personal_memory import lifecycle
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store


def record(i, text="vehicle repair", source="whatsapp", when="2024-01-01T12:00:00Z", participants=None):
    return {"source": source, "source_id": str(i), "text": text, "occurred_at": when,
            "metadata": {"participants": participants or []}}


PHONE = [{"namespace": "phone", "address": "+447700900123"}]


class IdentityFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "memory.db")

    def put(self, *records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]

    def account(self, needle):
        return self.store.entities(needle)["entities"][0]["id"]

    def hybrid(self):
        h = Hybrid(self.store, {"rerank": {"enabled": False}, "semantic": {"enabled": False}}, start=False)
        self.addCleanup(h.close)
        return h

    def link(self, account, person, evidence, status="confirmed", **interval):
        return self.store.identity(account, person, evidence, status=status, **interval)


class ExpansionRetirementTests(IdentityFixture):
    def test_retired_identity_evidence_stops_person_based_recall_expansion(self):
        account_rid = self.put(record(1, text="vehicle repair", participants=PHONE))[0]
        own = self.put(record(2, text="passport renewal fee", participants=PHONE))[0]
        account = self.account("+447700900123")
        person = self.store.entity("person", "Amit")["id"]
        self.link(account, person, account_rid)
        # The confirmed link expands the person to every record of the account.
        self.assertIn(own, self.store.related_ids(person))
        backend = self.hybrid()
        found = {e["id"] for e in backend.search("passport", entity_id=person)["episodes"]}
        self.assertIn(own, found)
        # Retire the supporting evidence: the person link must stop expanding.
        self.store.supersede(account_rid)
        self.assertEqual(self.store.related_ids(person), set())
        found = {e["id"] for e in backend.search("vehicle", entity_id=person)["episodes"]}
        self.assertNotIn(account_rid, found)

    def test_hidden_evidence_cannot_expand_even_before_repair(self):
        # Simulate a database upgraded from a build without retirement cascades:
        # the evidence is hidden while the confirmed link is still active.
        rid = self.put(record(1, participants=PHONE))[0]
        account = self.account("+447700900123")
        person = self.store.entity("person", "Amit")["id"]
        self.link(account, person, rid)
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
        self.assertEqual(self.store.related_ids(person), set())


class LinkCreationTests(IdentityFixture):
    def test_hidden_or_forgotten_evidence_cannot_create_a_link(self):
        hidden, gone = self.put(record(1, participants=PHONE), record(2, source="email", participants=PHONE))
        account = self.account("+447700900123")
        person = self.store.entity("person", "Amit")["id"]
        self.store.supersede(hidden)
        self.store.forget(gone)
        for rid in (hidden, gone):
            with self.assertRaises(ValueError):
                self.link(account, person, rid)
            with self.assertRaises(ValueError):
                self.store.identity(account, person, rid, status="candidate")

    def test_conflict_check_ignores_links_on_retired_evidence(self):
        old = self.put(record(1, participants=PHONE))[0]
        account = self.account("+447700900123")
        first = self.store.entity("person", "Amit")["id"]
        second = self.store.entity("person", "Beta")["id"]
        self.link(account, first, old)
        # Pre-upgrade state: the evidence retires without the edge being revoked.
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (old,))
        corrected = self.put(record(2, source="email", participants=PHONE))[0]
        # Corrected ownership can be confirmed once the old support is retired...
        self.link(account, second, corrected)
        # ...while a genuine conflict against live visible support still fails.
        with self.assertRaises(ValueError):
            self.link(account, first, corrected)

    def test_valid_ownership_conflict_still_fails(self):
        one, two = self.put(record(1, participants=PHONE), record(2, source="email", participants=PHONE))
        account = self.account("+447700900123")
        first = self.store.entity("person", "Amit")["id"]
        second = self.store.entity("person", "Beta")["id"]
        self.link(account, first, one)
        with self.assertRaises(ValueError):
            self.link(account, second, two)

    def test_time_bounded_keeps_existing_behavior(self):
        rid = self.put(record(1, participants=PHONE))[0]
        account = self.account("+447700900123")
        person = self.store.entity("person", "Amit")["id"]
        self.link(account, person, rid, valid_from="2025-01-01T00:00:00Z")
        # The 2024 record is outside the confirmed interval: no expansion.
        self.assertEqual(self.store.related_ids(person), set())
        other = self.put(record(2, when="2025-06-01T12:00:00Z", participants=PHONE))[0]
        self.assertIn(other, self.store.related_ids(person))


class RepairAndRestoreTests(IdentityFixture):
    def _stale_state(self):
        rid = self.put(record(1, participants=PHONE))[0]
        account = self.account("+447700900123")
        person = self.store.entity("person", "Amit")["id"]
        edge = self.link(account, person, rid)
        with self.store.connect() as db:
            db.execute("INSERT OR REPLACE INTO record_visibility VALUES(?,1,NULL)", (rid,))
        return rid, person, edge

    def test_upgrade_repair_is_transactional_and_idempotent(self):
        rid, person, edge = self._stale_state()
        report = lifecycle.repair_retired_dependencies(self.store)
        self.assertGreaterEqual(report["identity_edges"], 1)
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT status FROM identity_edges WHERE id=?", (edge["id"],)).fetchone()[0], "revoked")
        again = lifecycle.repair_retired_dependencies(self.store)
        self.assertEqual(again["identity_edges"], 0)
        self.assertEqual(self.store.related_ids(person), set())

    def test_retire_revokes_the_link_directly(self):
        rid, person, edge = self._stale_state()
        self.store.supersede(rid)  # already hidden; supersede re-retires via lifecycle
        with self.store.connect() as db:
            status = db.execute("SELECT status FROM identity_edges WHERE id=?", (edge["id"],)).fetchone()[0]
        self.assertEqual(status, "revoked")

    def test_restoring_visibility_does_not_reconfirm_an_invalidated_identity(self):
        rid, person, edge = self._stale_state()
        lifecycle.repair_retired_dependencies(self.store)
        with self.store.connect() as db:
            db.execute("DELETE FROM record_visibility WHERE record_id=?", (rid,))
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT status FROM identity_edges WHERE id=?", (edge["id"],)).fetchone()[0], "revoked")
        self.assertEqual(self.store.related_ids(person), set())


if __name__ == "__main__":
    unittest.main()
