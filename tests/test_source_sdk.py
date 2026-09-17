"""Adapter SDK contract tests (SOURCE_ADAPTER_IMPLEMENTATION.md §3, SDK-01..SDK-05)."""
import tempfile
import unittest
from pathlib import Path

from personal_memory.ingestion import ConnectorSpec, IngestionConnector
from personal_memory.source_sdk import (
    AdapterError, ContractError, SourceAdapter, adapter_supports, connection_context,
    normalized_item, read_state, source_operation, source_page, stream_spec, validate_page,
    wrap_ingestion_connector,
)


def note_record(source_id, text="a note"):
    return {"schema_version": "1.0", "source": "custom-notes", "source_id": source_id,
            "revision": "1", "kind": "document", "occurred_at": None,
            "observed_at": "2026-09-06T12:00:00Z", "text": text, "participants": [],
            "provenance": {"connector_id": "example.notes", "connector_version": "1.0.0",
                           "source_locator": "notes://" + source_id, "origin": "source",
                           "parent_record_ids": []}, "extensions": {}}


class LegacyNotes(IngestionConnector):
    @property
    def spec(self):
        return ConnectorSpec("example.notes", "1.0.0", "custom-notes")

    def read(self, checkpoint=None):
        yield note_record("one")
        yield note_record("two")


class AdapterSpecDefaultsTests(unittest.TestCase):
    def test_omitted_config_schema_and_capabilities_default_cleanly(self):
        # A minimal declaration is legal: an empty config schema and every
        # capability off, so simple export adapters never pass boilerplate.
        from personal_memory.source_sdk import adapter_spec
        spec = adapter_spec("email.export", "1.0")
        self.assertEqual(spec["config_schema"], {})
        self.assertFalse(adapter_supports(spec, "history"))
        self.assertEqual(sorted(spec["capabilities"]), sorted(
            ("history", "incremental", "reconciliation", "deletions",
             "attachments", "events", "subscriptions")))
        with self.assertRaises(ContractError):
            adapter_spec("email.export", "1.0", config_schema=["not", "an", "object"])


class WrappedConnectorTests(unittest.TestCase):
    def setUp(self):
        self.legacy = LegacyNotes()
        self.adapter = wrap_ingestion_connector(self.legacy)

    def test_sdk01_legacy_connector_reads_as_export_only_page(self):
        # SDK-01: an existing connector imports with unchanged behavior through the wrapper.
        spec = self.adapter.spec()
        self.assertEqual(spec["adapter_id"], "example.notes")
        self.assertFalse(adapter_supports(spec, "incremental"))
        self.assertFalse(adapter_supports(spec, "events"))
        context = connection_context(connection_id="conn_1", source="custom-notes")
        page = self.adapter.read_page(context, read_state(mode="backfill"))
        validate_page(page)
        self.assertTrue(page["complete"])
        source_ids = [op["source_id"] for op in page["operations"]]
        self.assertEqual(source_ids, ["one", "two"])
        # The underlying legacy connector is untouched and still yields records.
        self.assertEqual([r["source_id"] for r in self.legacy.records()], ["one", "two"])

    def test_sdk02_unknown_and_missing_fields_fail_validation_before_writes(self):
        # SDK-02: useful validation errors precede any authoritative progress.
        good = source_page(page_id="pg_1", operations=[source_operation("upsert", "one",
                        records=[note_record("one")])], next_state=read_state(mode="backfill"))
        validate_page(good)
        broken = dict(good); broken["surprise"] = 1
        with self.assertRaises(ContractError) as error:
            validate_page(broken)
        self.assertIn("surprise", str(error.exception))
        missing = dict(good); del missing["page_id"]
        with self.assertRaises(ContractError):
            validate_page(missing)
        wrong_mode = read_state(mode="backfill"); wrong_mode["mode"] = "sideways"
        with self.assertRaises(ContractError):
            validate_page(source_page(page_id="pg_1", operations=[], next_state=wrong_mode))

    def test_sdk03_unsupported_capabilities_report_before_scheduling(self):
        # SDK-03: the wrapped file/export adapter declares no webhooks, deletes or live sync.
        spec = self.adapter.spec()
        self.assertFalse(adapter_supports(spec, "deletions"))
        self.assertFalse(adapter_supports(spec, "subscriptions"))
        self.assertTrue(adapter_supports(spec, "history"))
        with self.assertRaises(AdapterError) as error:
            self.adapter.verify_event({"body": {}})
        self.assertEqual(error.exception.kind, "unsupported")

    def test_sdk04_normalize_is_deterministic_for_a_payload(self):
        # SDK-04: same payload normalized twice yields identical evidence identity/content.
        payload = {"source_id": "one", "record": note_record("one")}
        first = self.adapter.normalize(payload)
        second = self.adapter.normalize(payload)
        self.assertEqual(first, second)
        validate_page(source_page(page_id="pg_1", operations=[source_operation(
            "upsert", "one", records=first["records"])],
            next_state=read_state(mode="backfill")))

    def test_sdk05_state_upgrade_requires_explicit_migration(self):
        # SDK-05: an unknown adapter state version never gets silently reinterpreted.
        class Migrating(LegacyNotesMixin, SourceAdapter):
            def migrate_state(self, old_protocol, state):
                return read_state(state_version=state["state_version"] + 1,
                                  cursor={"migrated": True}, mode=state["mode"])
        adapter = Migrating()
        stale = read_state(state_version=99, cursor={"raw": "opaque"}, mode="incremental")
        with self.assertRaises(AdapterError) as error:
            adapter.accept_state(stale, supported_state_version=1)
        self.assertEqual(error.exception.kind, "cursor")
        migrated = adapter.accept_state(stale, supported_state_version=1, migrate=True)
        self.assertEqual(migrated["cursor"], {"migrated": True})


class LegacyNotesMixin:
    """Minimum complete SourceAdapter for migration-behavior tests."""

    def spec(self):
        return {"adapter_id": "fixture.migrating", "adapter_version": "1.0",
                "protocol_versions": ["1.0"], "capabilities": {key: False for key in
                    ("history", "incremental", "reconciliation", "deletions",
                     "attachments", "events", "subscriptions")},
                "config_schema": {}, "secret_refs": []}

    def check(self, context):
        return {"account_id": "fixture"}

    def discover(self, context):
        return [stream_spec("main", modes=["incremental"])]

    def read_page(self, context, state):
        return source_page(page_id="pg_" + str(state["state_version"]), operations=[],
                           next_state=read_state(state_version=state["state_version"] + 1,
                                                 cursor=state["cursor"], mode=state["mode"]))

    def normalize(self, payload):
        return normalized_item(payload["source_id"], records=[payload["record"]])


class StreamAndStateTests(unittest.TestCase):
    def test_stream_spec_declares_independent_modes_and_limits(self):
        stream = stream_spec("messages", modes=["backfill", "incremental"],
                             partitions=[{"id": "window:2021"}], history_limit="5y")
        self.assertEqual(stream["stream_id"], "messages")
        self.assertEqual(stream["history_limit"], "5y")
        with self.assertRaises(ContractError):
            stream_spec("bad", modes=["telepathy"])

    def test_operations_cover_upsert_metadata_removal_restoration_skip(self):
        for action in ("upsert", "metadata_update", "remove", "restore", "skip"):
            operation = source_operation(action, "sid_1",
                                         records=[note_record("sid_1")] if action in ("upsert", "metadata_update") else None)
            self.assertEqual(operation["action"], action)
        with self.assertRaises(ContractError):
            source_operation("resurrect", "sid_1")

    def test_adapter_error_classification_is_closed(self):
        for kind in ("temporary", "rate_limit", "auth", "cursor", "schema", "permanent"):
            self.assertEqual(AdapterError(kind, "x").kind, kind)
        with self.assertRaises(ValueError):
            AdapterError("optimistic", "x")


if __name__ == "__main__":
    unittest.main()
