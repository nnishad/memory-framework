# Current framework architecture

See FRAMEWORK.md for the integrated 0.8.0rc8 architecture, workflow queues, authority boundaries and typed knowledge APIs. The foundational ingestion/retrieval notes below remain applicable.

# Architecture

Connectors produce contract 1.0 records. The HTTP boundary validates the complete batch, then commits
immutable revisions, account observations, evidence dependencies and compact provenance receipts to
SQLite. Arbitrary extension JSON is preserved without changing the database schema. Reobservations do
not repeat the whole payload. Original source content remains distinct from derived memory claims.

The native Hermes provider supplies guidance and twenty tools, queues conversations durably and
prefetches bounded context asynchronously. Retrieval combines keyword, managed Hindsight and optional additional semantic candidates,
applies source/date/entity filters, fuses ranks, applies bounded graph expansion, best-effort
cross-encoder reranking and recency weighting, and rehydrates local evidence. Unknown/deleted external
IDs are discarded. Confirmed, date-valid account/person links can expand a person's history.

## Memory flow and cost controls

At turn start, the provider commits the current user input to the durable outbox and flushes only
that capture. Older queued work stays with the background worker, so recall does not wait behind an
unrelated backlog. The new input's canonical record ID is excluded from automatic and explicit recall
for that turn. Durable occurrence counters let completed-turn, compression and session-end hooks fill
missing transcript rows without saving the same occurrence under several lifecycle source IDs.
Tool-call IDs are tracked the same way, so checkpoint and session-end replay do not repeatedly
reprocess the full tool-result history.

Automatic recall launches one bounded fast search and waits up to `prefetch_wait_ms` (200 ms by
default). A default `personal_memory_search` fallback reuses the completed, generation-checked result
instead of issuing the same lookup again. The provider tracks canonical IDs already shown in the
session and omits them from later automatic context. Evidence tool calls request the compact view,
which contains one text copy and source coordinates; the full administrative `/v1/evidence` response
remains the default API contract. Tool names and schemas are unchanged, while concise descriptions and
guidance reduce the fixed model prompt cost.

`semantic.py` provides FastEmbed or a compatible HTTP embedding endpoint. FastEmbed chunks use tokenizer
spans; HTTP chunks use an operator-defined character window. SQLite stores vectors. Optional HNSW is
rebuilt from SQLite; exact NumPy ranking is the fallback. Real-model/scale qualification remains open.
This semantic component is optional because managed Hindsight already supplies vector candidates in
the supported default path and keyword search provides a local deterministic channel. Enable the extra
index when local/offline multilingual similarity, explicit span selection or a second independent
embedding channel justifies its model, storage and indexing cost. When a compatible Hindsight response
includes a semantic score, the relevance gate consumes it; older 0.9.2 responses remain valid.

`hindsight.py` implements retain, recall and document deletion with a durable sync journal.
`hindsight_runtime.py` makes Hindsight 0.9.2 mandatory in the supported setup/service path.
The default generated configuration is:

```json
{"retrieval":{"hindsight":{"managed":true,"profile":"hermes-personal-memory-<data-path-hash>","bank_id":"hermes-personal-memory","sources":["*"],"backend_id":"embedded:<profile>"}}}
```

The supported settings loader forces `enabled:true`; old `enabled:false` values cannot
disable Hindsight. Direct backend fixtures can still omit the component. Source
names must match imported source IDs. The default `["*"]` indexes every source. Completed retains
are recorded under canonical document IDs. Deletion retries precede new ingestion. External inferred
facts only select local evidence; they do not merge canonical people or become verified facts. Results
without one document ID are skipped. The stable backend identity prevents a free embedded daemon port
from triggering a full re-index on every restart. The background worker retains up to eight canonical
documents per request, skips lineage-only fan-in nodes and isolates a bad document after an ambiguous
batch failure so later records can still progress.

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
