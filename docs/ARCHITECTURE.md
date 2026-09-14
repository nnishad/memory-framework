# Current framework architecture

See FRAMEWORK.md for the integrated v0.7 architecture, workflow queues, authority boundaries and typed knowledge APIs. The foundational ingestion/retrieval notes below remain applicable.

# Architecture

Connectors produce contract 1.0 records. The HTTP boundary validates the complete batch, then commits
immutable revisions, account observations, evidence dependencies and compact provenance receipts to
SQLite. Arbitrary extension JSON is preserved without changing the database schema. Reobservations do
not repeat the whole payload. Original source content remains distinct from derived memory claims.

The native Hermes provider supplies guidance and twelve tools, queues conversations durably and
prefetches bounded context asynchronously. Retrieval combines keyword and configured semantic candidates,
applies source/date/entity filters, fuses ranks and rehydrates local evidence. Unknown/deleted external
IDs are discarded. Confirmed, date-valid account/person links can expand a person's history.

`semantic.py` provides FastEmbed or a compatible HTTP embedding endpoint. FastEmbed chunks use tokenizer
spans; HTTP chunks use an operator-defined character window. SQLite stores vectors. Optional HNSW is
rebuilt from SQLite; exact NumPy ranking is the fallback. Real-model/scale qualification remains open.

`hindsight.py` implements retain, recall and document deletion with a durable sync journal.
`hindsight_runtime.py` makes Hindsight 0.9.2 mandatory in the supported setup/service path.
The default generated configuration is:

```json
{"retrieval":{"hindsight":{"managed":true,"profile":"hermes-personal-memory-<data-path-hash>","bank_id":"hermes-personal-memory","sources":["*"],"backend_id":"embedded:<profile>"}}}
```

There is no `enabled` switch; old `enabled:false` values are ignored during migration. Source
names must match imported source IDs. The default `["*"]` indexes every source. Completed retains
are recorded under canonical document IDs. Deletion retries precede new ingestion. External inferred
facts only select local evidence; they do not merge canonical people or become verified facts. Results
without one document ID are skipped. The stable backend identity prevents a free embedded daemon port
from triggering a full re-index on every restart.

Coverage, indexing readiness and successful recall are distinct states. Pagination can exhaust stored
evidence in a scope, not unimported history. Model guidance is not a mandatory host action gate. Native
skills execute procedures; the host scheduler handles scheduled tasks.

## Upstream references inspected

- [Hermes memory-provider interface](https://hermes-agent.nousresearch.com/docs/developer-guide/memory-provider-plugin)
- [Hindsight recall](https://hindsight.vectorize.io/developer/api/recall)
- [Hindsight retain](https://hindsight.vectorize.io/developer/api/retain)
- [Hindsight OpenAPI](https://hindsight.vectorize.io/openapi.json), inspected version 0.9.2

The actual Hermes abstract interface is tested separately from a live Hermes deployment.

Learning artifacts are held in separate canonical SQLite tables and do not become searchable facts by being proposed. See LEARNING.md for dependency invalidation, evaluation authority and promotion.
