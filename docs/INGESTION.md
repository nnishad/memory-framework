# Ingestion contract 1.0

## Strict core, open extensions

The memory core owns a closed envelope. Connectors own namespaced extension data. Extensions cannot
override core fields. Adding a namespace or extra data does not require a database migration.

```json
{
  "schema_version": "1.0",
  "source": "personal-whatsapp",
  "source_id": "group-42/message-123",
  "revision": "1",
  "kind": "message",
  "occurred_at": "2024-02-01T13:05:00Z",
  "observed_at": "2026-09-06T12:00:00Z",
  "text": "+447700900123: I can help with PostgreSQL migrations.",
  "participants": [
    {"namespace":"phone", "address":"+447700900123", "label":"Unknown participant", "relation":"author"}
  ],
  "provenance": {
    "connector_id": "example.whatsapp_export",
    "connector_version": "1.0.0",
    "source_locator": "whatsapp://group-42/message-123",
    "origin": "source",
    "parent_record_ids": []
  },
  "extensions": {
    "example.whatsapp": {
      "version": "1.0",
      "data": {"group_id":"group-42", "reply_to":null, "reactions":[], "future_field":{"structured":true}}
    }
  }
}
```

This example is synthetic. Every displayed top-level field is mandatory, including empty collections.
`occurred_at` is the only nullable core field. Unknown event dates are not replaced by import dates;
event-date filters exclude undated records. Observation time is mandatory and independent.

## What is enforced

1. Only contract `1.0` is accepted. Incompatible future contracts need explicit migration/validation.
2. Unknown core, participant, provenance and extension-envelope fields are rejected.
3. Extensions use namespaces such as `vendor.sensor`. Each has a required version and an arbitrary JSON
   `data` object. Nested arrays, objects and null values are supported. Producer semantics remain separate.
4. `(source, source_id, revision)` identifies an immutable representation. Text, kind, event time,
   participants, origin, evidence-parent or extension changes require a new revision. A new observation,
   source locator or connector version alone creates a receipt without duplicating the record.
5. Provenance includes connector identity/version, source locator, origin and parent list. Origin is
   `source`, `assistant`, or `derived`; it is not a confidence/truth score. Derived records require at
   least one existing live local evidence parent. Missing evidence rejects the transaction.
6. Invalid records reject the entire batch. Batches contain 1–100 records; each record is at most 1 MiB,
   HTTP bodies at most 2 MiB. JSON nesting is at most 24 levels. NaN and infinity are rejected.
7. Extension values round-trip unchanged through the evidence API. They do not overwrite core values.
   Text is indexed. Arbitrary extension semantics are not automatically understood or domain-indexed.

Validation cannot prove that the source is truthful, that every message was imported, or that a device
measured correctly. Coverage and source trust are separate. Empty participants means none were supplied
or observed; it does not prove no people were involved.

## Connector interface

Extend `IngestionConnector`, provide `ConnectorSpec`, and implement `read(checkpoint)` to yield records.
`records(checkpoint)` validates the contract and checks that each source/connector identity matches the
declaration. `submit(client, connector, checkpoint)` validates and sends records. Override `validate_extra`
for producer-specific semantics, such as allowed sensor units. `examples/custom_connector.py` is complete.

Other languages can use the exported JSON Schema and the HTTP API. Python inheritance is optional.
The server repeats core validation; bypassing the SDK does not bypass the contract. Producer-specific
SDK validators are not automatically installed server-side: unknown extension data receives structural
validation and preservation, not a guarantee about its business meaning.

JSON Schema clients should enable date-time format validation. The server additionally checks JSON/body
limits, live evidence references and revision conflicts. The Python validator implements this contract's
specific schema vocabulary; it is not a general-purpose validator for arbitrary third-party schemas.

## HTTP and checkpoint semantics

Send `POST /v1/ingest` with `{"items":[record]}` and the service bearer token. A structural violation
returns HTTP 422 with a useful path:

```json
{"error":"Ingestion contract rejected","path":"$.items[0].provenance","message":"Required field is missing"}
```

Reference/revision conflicts return HTTP 400; oversized HTTP bodies return 413. Acknowledgments include
canonical record IDs and duplicate flags. Persist source checkpoints only after acknowledgment. Replay
stable source IDs/revisions after lost acknowledgments. Do not use import time as record identity.

The native Hermes outbox is durable. Generic `submit` does not add another durable queue; production
connectors own their checkpoint/outbox. Export files can be replayed unchanged. The observation receipt
stores compact provenance and reconstructs the original envelope from immutable canonical content.

## Domain examples

| Source | Shared core | Example extension data |
| --- | --- | --- |
| WhatsApp | Message ID, text, sender account, times, provenance | Group, replies, reactions, media references |
| Email | Message-ID, body, sender/recipients, times | Thread headers, labels, attachments |
| Health | Measurement identity, time, faithful text, device-source provenance | Metric, exact value, units, sampling interval |
| Calendar | Event identity/revision, time, participants | Duration, recurrence, location, response status |
| Future source | The same mandatory envelope | A new producer-owned namespace |

Episodic source records are distinct from derived semantic/procedural/prospective memory claims.
Importers are not required to invent interpretations. Account identifiers are distinct from verified
person identity; shared names or groups are insufficient evidence for person merging.

## Prototype migration

Old stored records stay readable. New HTTP/JSONL writes require contract 1.0. `adapt_existing` is an
explicit parser migration utility whose caller supplies connector identity/version, real observation
time and source locator. Old metadata goes under `personal_memory.legacy`. Bundled import commands and
Hermes captures already apply the contract. Pre-upgrade outbox records use their stored queue time.
Internal `Store.ingest` remains a trusted storage primitive; it is not the external connector API.
