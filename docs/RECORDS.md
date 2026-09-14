# Source records

The current public contract is [INGESTION.md](INGESTION.md). The machine-readable version is
`schemas/ingestion-record-1.0.schema.json`. Closed core fields and open producer-owned extensions replace
the v0.1 unversioned JSONL format. All HTTP ingestion uses `Store.ingest_contract`.

Old stored records remain readable. Internal `Store.ingest` is a trusted storage primitive for existing
local code, not an alternate network ingestion route. New connectors implement the public contract.
