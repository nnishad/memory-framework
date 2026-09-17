# Source adapters: implementation and acceptance specification

Status: Phases 1-2 core engine implemented 2026-09-16 (SDK, atomic sync
state, leases/fencing, receipts, heads/coverage, durable inbox, obligations
queue, secrets, worker, bundled email/legacy adapters). HTTP routes, the
`hermes sources` CLI, and live-account/local-model evaluation remain future
work; see the implementation notes at the end. Read alongside
[the architecture and research](SOURCE_ADAPTER_PLAN.md).

## 1. Intended result

An owner connects Gmail once. Historical mail imports while new mail continues
to arrive. The memory framework preserves source evidence, forms connected
memories using local models, and makes those memories available to Hermes through
its existing provider. Restarting a process, receiving duplicate notifications,
or replaying a page must not lose data or duplicate canonical memories.

Adding another data source requires implementing a standard adapter and domain
mapping, not another queue, scheduler, memory store, or retrieval system.

Confirmed requirements:

- Runtime on the Hermes/memory host; Gmail first, using the owner's OAuth app.
- Content processing with configured local models.
- Both one-time import and ongoing ingestion, triggered manually, by schedule,
  or by events where the source supports them.
- Recall includes episodes, attributed facts, relationships, and summaries;
  storing searchable source rows alone does not complete the feature.
- Extend existing contracts compatibly and put shared guarantees in the core.

Proposed defaults awaiting owner confirmation: five years of accessible Gmail
mail including Sent, excluding Spam/Trash initially; five-minute polling; quiet
ingestion. Historical start is a fixed cutoff once configured, not a moving
five-year retention window. Source-deletion retention and attachment size/type
limits must be chosen before activation. Planning can proceed without credentials.

## 2. Feature scope

| Capability | First complete Gmail release | Later extension |
| --- | --- | --- |
| Account setup | OAuth, read-only access, account check, protected secret references | Additional providers and accounts using the same connection model |
| Historical capture | Bounded, resumable windows and visible import progress | Source-specific snapshots/exports |
| Live capture | Incremental polling during and after backfill | Pub/Sub trigger feeding the same runtime |
| Source changes | Edits, metadata changes, removals/restoration under selected policy | Provider-specific equivalent operations |
| Attachments | Durable download and safe text extraction for declared supported formats; explicit unsupported states | OCR/audio and additional extractors |
| Memory formation | Episodes, attributed facts, thread summaries, explicit/inferred links with provenance | Broader cross-domain consolidation |
| Recall | Eligible memory candidates through existing recall, exact evidence access, freshness/coverage | Domain-specific query planning |
| Operations | Status, pause/resume, retry, rescan, disconnect, structured diagnostics | Additional UI surfaces |
| Extensibility | SDK, protocol schema, template, conformance suite, second export adapter | WhatsApp live adapter and Android health collector after discovery |

Sending email, marking it read, and acting on its contents are separate features.
The ingestion credential does not authorize them.

## 3. Adapter SDK and protocol

Introduce a `SourceAdapter` ABC alongside the existing `IngestionConnector`.
Do not add mandatory abstract methods to the old class. A compatibility wrapper
exposes old connectors as export-only streams. Keep the strict record 1.0
envelope and current `/v1/ingest` behavior unchanged.

The SDK should define serializable types with independent protocol versions:

| Type | Required meaning |
| --- | --- |
| `AdapterSpec` | Stable adapter ID, package version, supported protocol versions, config JSON schema, required secret references, capabilities |
| `ConnectionContext` | Immutable connection/source identity, selected scope, the `stream` and `partition` the runtime leased for this read, credential handle, deadlines and cancellation; no arbitrary admin client |
| `StreamSpec` | Stream/partition IDs, supported read modes, cursor semantics, history/deletion limitations |
| `ReadState` | Adapter state version, opaque cursor, fixed scope hash, mode, optional snapshot boundary |
| `SourcePage` | Page identity, operations, next state, completion marker, coverage observations |
| `SourceOperation` | Upsert, metadata update, removal, restoration, or explicit policy skip; stable item identity and source version |
| `NormalizedItem` | Valid 1.0 records, source-head mapping, attachment descriptors, deterministic relationship/projection inputs |
| `AdapterError` | Classified temporary/rate-limit/auth/cursor/schema/permanent failure, safe diagnostic, optional retry hint |

Required methods: `spec`, `check`, `discover`, `read_page`, and `normalize`.
Optional capability interfaces: attachment fetch, webhook verification,
subscription maintenance, and adapter-state migration. Fail capability checks
before scheduling unsupported work. Do not require empty webhook methods on a
file adapter.

`read_page` performs source I/O; `normalize` is deterministic for a source payload
and transformation version. Neither advances authoritative progress. The runtime
validates outputs and owns commits. Source adapters cannot set truth labels,
change owner permissions, or choose runtime code from imported payloads.

A single adapter may declare several streams and each stream several partitions.
The runtime leases, cursors and retries every `(stream, partition, role)` independently
and passes the current selectors to `read_page` as `context["stream"]` and
`context["partition"]` (the empty string for a stream that declares no partitions).
Adapters serving one stream may ignore them; multi-stream adapters must select the
slice named by the context and must never read a neighbouring partition. The runtime
rejects a read whose selectors are not declared by `discover` before any source I/O.

Use entry-point discovery for installed Python adapter packages. Publish JSON
schemas and a bounded authenticated push protocol for other languages. A phone
collector needs durable device-side progress and delivery acknowledgment, but
uses the same canonical commit semantics on the host.

## 4. Core persistence and state machines

Use additive tables in the canonical database; exact names are implementation
choices. Avoid a second authoritative checkpoint file.

| Data | Purpose |
| --- | --- |
| Connections | Adapter/account binding, config version, scope hash, retention policy, secret reference, desired status |
| Stream state | Independent backfill/live/reconciliation cursors and state versions |
| Inbox/pages | Durable notifications or fetched payloads with bounded retention |
| Leases | Worker owner, expiry, fencing token, memory epoch |
| Page receipts | Stable operation ID, digest, sub-batch progress, committed outcome |
| Source heads | Current revision/removal state and comparable upstream version where available |
| Coverage | Completed ranges, unresolved gaps, requested scope, last successful delta |
| Processing obligations | Attachment, projection, formation and cleanup jobs, dependencies, retry state |

Suggested connection states: configured, needs_auth, active, paused, error,
disconnected. Track backfill and live states independently so "backfilling"
does not prevent live work. Jobs transition pending -> leased -> succeeded,
retry_wait, quarantined, or cancelled. Transitions use compare-and-set versions.

Source ingestion performs one bounded atomic commit:

1. Check principal scope, config generation, reset epoch, lease fence, and expected state.
2. Validate all submitted records and operations.
3. Insert evidence and update current source heads/visibility.
4. Enqueue required processing and invalidation obligations.
5. Commit receipt, coverage observations, and page progress.

Reuse `Store.ingest` through an internal transaction-aware helper. The public
method keeps existing validation, acknowledgment shape, and behavior. Explicitly
support zero-record and deletion-only commits in the new sync protocol.

For pages larger than transaction/body limits, retain page-tail progress and
advance the upstream cursor only once all sub-batches have durable outcomes.
Never truncate an oversized record into an apparently complete memory: preserve
the original through blobs and stable bounded parts, or quarantine with a gap.

Preserve the distinction between opaque cursors, immutable representation IDs,
and source chronology. A content hash cannot decide which revision is newer.
Concurrent backfill/live operations reconcile per item, with provider version
ordering when available and a serialized current-state reconciliation otherwise.
Changing source filters requires a new scan generation; absence under a changed
filter does not prove source deletion.

## 5. Runtime and delivery

The worker shares the host and database boundary with the memory service. Reuse
existing runner, workflow, and indexing utilities where applicable. Extract
shared retry/lease helpers if necessary; do not pretend ingestion jobs are
learning proposals or conversation outbox entries.

Hermes `no_agent` cron invokes a bounded enqueue/tick command. Manual sync,
startup recovery, and verified events enqueue the same job types. One scheduler
owns recurring triggers per deployment. Worker retries are queue state, not
duplicate cron jobs.

Provide per-connection API concurrency, byte budgets, rate limits, backoff with
jitter, cancellation, and fair queues for live versus historical work. Perform
network/model calls outside write transactions. Recover unfinished leases after
expiry and reject stale workers with fencing tokens.

Resolve an adapter's `discover` declarations once per configured refresh interval
and pass that single validated result through the page read and the commit path,
so a multi-page pass does not re-discover per page. Keep the cached declarations
keyed by the connection's generation, scope hash and secret reference: rotate that
fingerprint whenever configuration or credentials change so the next tick refreshes
the cache. Honor a persisted discovery backoff even when no cached declarations
remain, and treat a discovery authentication failure like any other auth signal
(park the connection in `needs_auth`) rather than crashing the tick loop.

Webhook receivers acknowledge only after durable inbox insertion. When an event
only signals changes, coalesce triggers with an incrementing generation; a signal
arriving mid-sync must still cause a subsequent pass. Event-only sources retain
payloads and declare replay limitations. Periodic reconciliation repairs missed
signals where the upstream API permits it.

Pausing prevents new jobs; bounded in-flight work may finish unless cancelled.
Disconnect cancels/fences work and stops credential use; deleting stored memories
is a distinct operation. Reset invalidates all old worker epochs and must not
silently refill memory from a still-running connector. Backup/restore includes
new state tables; restored workers start paused for source-state validation.

## 6. Gmail adapter work

Implement as a provider-specific package using the shared SDK:

1. OAuth setup and refresh; verify that refreshed credentials still belong to the
   configured account. Choose callback mechanics for the actual host deployment.
2. Discover mailbox capability and validate selected date/label scope.
3. Record a mailbox history anchor before backfill; start the incremental consumer.
4. Enumerate historical windows with stable IDs, retrieve bounded batches, and
   normalize with the same code as live updates.
5. Process history pages, deduplicating overlapping change entries; apply source
   changes under the configured retention policy.
6. Persist attachment obligations, thread references, participants and timestamps.
7. Reconcile expired cursors and interrupted historical enumeration, retaining
   visible gaps for unavailable upstream data.

Store provider message IDs in an account-specific namespace. Preserve Message-ID,
In-Reply-To, References, Gmail thread ID, original headers, MIME structure,
provider timestamp, and sender-supplied Date distinctly. Missing/invalid dates
must not become today's event date. Metadata-only label/read-state changes
should not force body re-embedding.

Handle multipart/alternative, HTML text extraction, charsets, attachment-only
mail, inline attachments, repeated attachment names, drafts and subsequent
versions. Avoid fetching remote images or executing attachment content.

The old export importer and API adapter may have different source identifiers.
Existing imported mail needs a previewable alias/reconciliation migration; never
merge solely on subject or assume RFC Message-ID is universally unique.

Pub/Sub is a later trigger option. Renewal, restart, duplicate delivery, and
missed-notification recovery must use existing connection state and jobs.

## 7. Memory formation and eligibility

Memory formation is a required release work package, independent of transport
completion. Each committed source change queues bounded, idempotent formation
jobs. Job keys include evidence revision, transform/model/prompt version, and
scope. New transforms schedule reprocessing rather than overwrite immutable evidence.

| Representation | Formation | Recall behavior |
| --- | --- | --- |
| Episode | Group source-backed conversation/event spans with dates and participants | Recall what happened with source references |
| Attributed fact | Extract a statement with exact evidence, speaker, uncertainty, and validity | Distinguish current, historical, conflicting and inferred statements |
| Relationship | Deterministic thread/reply links or explicitly inferred entity/topic connections | Expand relevant connections with provenance |
| Summary | Incrementally summarize thread/time-window memories; retain dependency links | Compact context with access to underlying detail |
| Measurement | Explicit domain mapping of source value/unit/time/subject | Exact domain queries; no semantic approximation of arithmetic |

First release prioritizes email thread/event grouping and source-backed relations.
Topic/entity-based grouping follows only with evaluated precision. A short reply
such as "Friday works" must resolve against its relevant conversation context,
or remain unresolved; it must not become a standalone certain deadline.

Distinguish recall eligibility from truth and from promotion into curated memory.
Existing pending consolidation objects remain excluded under existing defaults.
Introduce an explicit connection-level formation policy for the new feature:

- Deterministic source-linked episodes may become eligible after validation.
- Model-produced summaries/facts remain attributed and inferred; optional
  automatic retrieval eligibility requires an enabled policy and validated
  evidence, schemas, visibility, and dependencies. Quote validation alone does
  not establish entailment, so run quality evaluations and label uncertainty.
- Preserve administrative promotion rules for curated memory, accepted beliefs,
  procedures, and existing consolidation workflows. Eligibility cannot grant
  execution privileges or silently change the meaning of an old API.

This permits continuous formation without asking the owner to approve every
email, while keeping inferred memories distinguishable from verified facts.
Eligibility policy is part of setup and is versioned/audited.

Evidence edits/removals must immediately make affected old representations
ineligible for ordinary recall. Rebuilding replacements may be asynchronous.
Invalidation covers summaries, measurements, graph edges, indexes, and caches.
Local-model outage leaves evidence retrievable with formation marked pending;
it must not cause a cloud fallback or claim memory formation is complete.

## 8. Recall integration and efficiency

Extend `adaptive.py`/`investigate.py` and the existing retrieval/provider path to
retrieve eligible derived-memory candidates as well as original evidence. Apply
authorization and visibility before graph expansion and again on final hydration.
Deduplicate representations that cite the same evidence so an email, its summary,
and its embedding hit do not appear to be three independent confirmations.

Select a small memory context, expand bounded entity/thread/temporal links, and
fetch exact source spans when resolving uncertainty. Return source attribution,
validity, inference status, and relevant coverage. Exact lookup and exhaustive
browse remain available. Keep the Hermes system prompt stable.

Use separate progress indicators: captured, keyword indexed, semantic indexed,
attachments extracted, memory formed, eligible for recall. Store generation and
eligibility changes must invalidate affected caches. A freshness target for raw
mail is different from a freshness target for fully formed memory.

Batch embedding/model work by actual tokenizer budgets where available. Cache
vectors by model plus content; preserve independent evidence/permission mappings.
Update only affected thread summaries, with bounded windows for long threads.
All-archive summarization on every message is prohibited by the cost tests.
Routine API polling and normalization must have zero LLM calls.

## 9. Module and integration map

| Location | Planned responsibility |
| --- | --- |
| New `personal_memory/source_sdk/` | ABC, typed protocol, capabilities, schemas, compatibility wrapper |
| New `personal_memory/source_sync/` | Registry, state transitions, worker, commits, coverage, inbox and recovery |
| Existing `store.py`, `storage.py` | Transaction helpers and additive migrations |
| Existing `service.py`, `asgi.py` | Scoped sync management/commit routes; protocol and size validation |
| Existing `workflows.py`, `learning.py`, `intelligence.py` | Formation jobs, evidence dependencies, eligibility and domain projections |
| Existing `retrieval.py`, `adaptive.py`, `investigate.py`, `provider.py` | Unified eligible-memory recall, visibility and freshness |
| Existing `blobs.py` | Attachment storage with durable extraction obligations |
| Existing `setup.py`, `__main__.py` | Setup and machine-readable source management commands |
| Separate Gmail adapter package | OAuth, Gmail API normalization and source capabilities |
| Managed Hermes plugin and skill | `hermes sources ...`, adapter authoring and operational guidance |

Do not write directly to Hermes core files for source-specific support. Existing
host contract/hash checks stay applicable. If a generic host extension is found
necessary, isolate and test it as a generic change rather than patching in Gmail.

Add source-scoped management/commit operations under a distinct API namespace.
Schema discovery must expose actual capabilities and defaults. Restrict secrets
and installation to operator setup; a data-plane ingest credential may only act
on its assigned connection/streams. Recheck scope inside nested dispatch paths.

## 10. Implementation order and release gates

| Phase | Deliverable | Gate before continuing |
| --- | --- | --- |
| 0 | Confirm scope/policy; record baseline; add synthetic adapter fixtures | Existing API/provider compatibility baseline is documented |
| 1 | SDK and compatibility wrapper | Existing connector works unchanged; schema/contract tests pass |
| 2 | Atomic sync state, leases, inbox, recovery and source heads | Fault-injection tests show no lost acknowledged data or stale commits |
| 3 | Gmail OAuth, historical and live polling | End-to-end evidence recall works during an interrupted/resumed backfill |
| 4 | Attachments, memory formation, eligibility and connected recall | Memory-quality scenarios pass; invalidated memories disappear immediately |
| 5 | Hermes CLI/skill, status, packaging, operational docs | Fresh installation and upgrade work; Hermes follows the documented workflow |
| 6 | Export adapter proof, scale qualification, optional Pub/Sub | Second source requires no source-specific core changes; measured targets hold |

Phases 1–3 alone are an ingestion milestone, not delivery of the requested memory
capability. Release the complete feature only after phase 4 and the operational
checks. Keep changes independently reviewable with migrations and tests beside
the behavior they introduce.

Compatibility gates: old record 1.0 payloads and acknowledgments; legacy
`IngestionConnector`/`submit`; existing `/v1/ingest` checkpoint semantics; existing
provider tool names/schemas; native conversation capture, reset and forget;
existing learning review defaults; unchanged connections when feature disabled.
New settings default off for existing deployments. New connection setup enables
the selected ingestion and formation policies explicitly.

## 11. Test matrix

These are required tests to implement, not tests already run. Use real temporary
SQLite databases and HTTP service boundaries for correctness. Fake upstream APIs
provide controlled faults; local-model quality tests and a small authorized live
Gmail test cover boundaries mocks cannot establish.

| ID | Scenario | Expected result |
| --- | --- | --- |
| SDK-01 | Existing connector instantiated without new methods | Imports with unchanged behavior |
| SDK-02 | Missing/unknown protocol fields or incompatible version | Useful validation error before writes |
| SDK-03 | Adapter declares no webhook/history capability | Unsupported work never scheduled |
| SDK-04 | Same payload normalized twice | Same evidence identity/content; observation receipt can differ |
| SDK-05 | State schema upgrade | Explicit migration or rescan; no cursor reinterpretation |
| TX-01 | Crash before commit | No record/cursor partial state; replay succeeds |
| TX-02 | Crash after commit before acknowledgment | Replay returns committed outcome without duplicate memories/jobs |
| TX-03 | Same operation ID, altered payload | Conflict; no mutation |
| TX-04 | Empty page or deletion-only page | Correct cursor advancement without fabricated evidence |
| TX-05 | Upstream page split across local batches; crash mid-page | Tail resumes; completion cursor never skips items |
| TX-06 | Two workers claim same stream | One active lease; stale fence cannot commit |
| TX-07 | Old backfill revision arrives after live update | Current head remains newest; history is retained as configured |
| TX-08 | Reset/config change during source API call | Old epoch/generation commit rejected |
| TX-09 | Disk full or SQLite contention | No acknowledged partial commit; bounded retry and visible failure |
| TX-10 | Quarantined malformed item among valid items | Valid work progresses only with durable gap/retry tracking |
| SYNC-01 | New event arrives during event coalescing | Follow-up pass occurs; event is not cleared accidentally |
| SYNC-02 | Process dies after webhook receipt acknowledgment | Durable inbox resumes processing |
| SYNC-03 | Duplicate, delayed and out-of-order notifications | Idempotent final state matching authoritative upstream |
| SYNC-04 | 429/5xx/timeouts | Retry hints and bounded backoff; no tight loop |
| SYNC-05 | Revoked token or changed account | Connection needs authorization; no cross-account ingestion |
| SYNC-06 | Expired history cursor | Reconciliation/rescan with deduplication and truthful coverage |
| SYNC-07 | Selected filters change | New scan generation; no false source deletion |
| SYNC-08 | Pause/resume/restart/disconnect | Defined state transitions; no unintended new work |
| SYNC-09 | Partial enumeration or temporary item lookup failure | No deletion inferred from incomplete evidence |
| GM-01 | Mail arrives while five-year backfill runs | Live capture advances independently and meets configured target |
| GM-02 | Message listed in overlapping history collections | One logical canonical outcome |
| GM-03 | Label-only change, Trash, restore, permanent removal | Distinct states; body not re-embedded for metadata-only change |
| GM-04 | Duplicate/missing RFC Message-ID across accounts | Provider/account identities remain distinct |
| GM-05 | Multipart, unusual charset, malformed/missing date | Faithful normalization or explicit gap; no fabricated event date |
| GM-06 | Draft/body revision and older delayed payload | Version handling preserves correct current content |
| GM-07 | Sent mail and selected historical cutoff | Exact declared scope; no unintentional rolling purge |
| GM-08 | Export mail overlaps API mail | Previewable alias mapping; ambiguous candidates stay separate |
| GM-09 | Incremental page finishes with no relevant selected records | Progress commits without repeatedly rescanning the page |
| AT-01 | Download/upload interrupted | Resumes idempotently; incomplete bytes never marked complete |
| AT-02 | Wrong hash/size, oversized attachment | Reject/quarantine visibly; no false extraction success |
| AT-03 | Attachment-only mail, inline media, duplicate filenames | Correct ownership and distinct identity |
| AT-04 | Password-protected/unsupported/corrupt file | Explicit unsupported/error status; source remains available |
| AT-05 | Malicious HTML, remote image, executable attachment | No remote fetch or execution during normalization/extraction |
| MEM-01 | Question paraphrases source wording | Relevant episode/fact recalled with evidence |
| MEM-02 | Deadline changes in a later message | Current answer uses replacement; history question explains change |
| MEM-03 | Two sources report conflicting facts | Conflict and attribution returned; no invented resolution |
| MEM-04 | Short reply depends on earlier thread | Context resolves meaning or memory remains uncertain |
| MEM-05 | Same name, two different people | No automatic identity merge |
| MEM-06 | Confirmed person links email and another source | Bounded cross-source recall retrieves relevant connected evidence |
| MEM-07 | Model emits invented quote/record ID/schema | Output rejected and job diagnosed/retried; not recall eligible |
| MEM-08 | Valid quote but unsupported interpretation | Evaluation detects error; inference never marked verified |
| MEM-09 | Original + summary + semantic hit share evidence | No inflated independent corroboration or duplicate context |
| MEM-10 | Evidence edited/deleted during formation | Old result cannot publish; descendants become ineligible |
| MEM-11 | Local model unavailable/malformed/slow | Capture continues; formation pending/failed visibly; no cloud fallback |
| MEM-12 | New email in a long thread | Bounded affected-context processing, not whole-archive reprocessing |
| MEM-13 | Numeric health fixture with units/overlap | Explicit domain calculations; unsupported semantics reported |
| MEM-14 | Needed detail omitted by summary | Original evidence lookup still retrieves it |
| MEM-15 | Eligibility off vs on for inferred memories | Policy enforced; old promotion APIs unchanged |
| SEC-01 | Connector writes/reads another connection or parent | Access denied at service boundary |
| SEC-02 | Forged/replayed webhook identity | Rejected or deduplicated before scheduling |
| SEC-03 | Email asks Hermes to reveal secrets/install code | Treated as evidence, never authority |
| SEC-04 | Source removed under archive vs mirror policy | Correct visibility/retention; source restoration remains distinct from forget |
| SEC-05 | User forget followed by full rescan | No resurrection through raw data, derived memory, or caches |
| SEC-06 | Forgotten item in a larger source page | Durable suppressed outcome without blocking all later sync |
| SEC-07 | Group/unknown Hermes session | Existing owner/session restriction preserved |
| SEC-08 | Logging and status contain credential-bearing errors | Tokens and secret payloads redacted |
| OPS-01 | Backup/restore including pending jobs | Consistent records/state; no automatic unsafe stale replay |
| OPS-02 | Old database upgraded; feature unused | Existing captures/search/review continue to work |
| OPS-03 | Cached result after deletion/eligibility change | Invalid result cannot be served |
| OPS-04 | Source captured but index/formation delayed | Hermes reports actual readiness and coverage |
| OPS-05 | Second export adapter installed | Works through identical runtime without provider branches in core |
| OPS-06 | Fresh host setup and existing patched Hermes | CLI, skill, service and provider integrate without prompt changes |
| PERF-01 | No-change polling repeated | Zero LLM calls; bounded requests/storage growth |
| PERF-02 | Large backfill plus new mail and interactive recall | No starvation; record measured throughput and p95 latency |
| PERF-03 | Repeated identical content and label updates | No unnecessary embedding/formation work |
| PERF-04 | Large payloads/long threads/failure backlog | Bounded RAM, transaction duration, model tokens and queue retention |

## 12. Evaluation and release evidence

Use three fixture levels: tiny exhaustive crash/replay fixtures; realistic
synthetic mail with threads, duplicates and changes; scale fixtures sized at
1,000, 10,000 and 100,000 messages, with representative attachment sizes. These
are benchmark sizes, not claims about supported performance on this host.

Measure capture lag, memory-formation lag, recall-ready lag, records/bytes per
second, source API calls, embedding/model calls and tokens, database/index/blob
size, peak memory, queue age and p50/p95 recall latency. Fix seeds, workload,
model/version and hardware in each report. Distinguish cold model startup from
steady-state performance.

Release gates:

- No failed deterministic correctness/security/compatibility cases.
- All critical curated memory scenarios (current fact, conflict, identity,
  deletion, provenance, and missing coverage) pass review across repeated runs.
- Record precision/recall@k, citation validity, stale-answer rate and unsupported
  assertion rate on a held-out question set. Freeze acceptable thresholds after
  a baseline and before optimization; do not select thresholds after seeing results.
- At least one authorized Gmail end-to-end test, including restart and new mail
  during backfill, with retrieval through the actual Hermes provider.
- Performance/freshness targets agreed for the actual host and mailbox; no
  hard throughput claims based solely on mocks.
- Installer, adapter template, operational runbook, schema/permission docs and
  recovery procedures updated together. Unsupported formats and source limits
  appear in user-visible status, not only developer logs.

Known pre-existing Windows/full-suite failures must be reproduced and classified
against the baseline. This feature must not add regressions; distinguish unit,
integration, live-account and local-model evaluation results in the report.

## 12. Implementation notes (Phases 1-2 core delivered, 2026-09-16)

The sync correctness layer is implemented and unit-tested. Modules:
`personal_memory/source_sdk.py` (adapter protocol + types + legacy wrapper),
`personal_memory/source_sync.py` (schema, leases, atomic `commit_page`, heads,
coverage, inbox, obligations/jobs, `SyncWorker`), `personal_memory/source_secrets.py`
(`secret://` backed store), and `personal_memory/source_adapters.py` (bundled
`EmailExportAdapter`). `store.py` mounts the sync schema and exposes
`_apply_ingest_item` so the engine and `ingest()` share one write path.

The five review findings are resolved as tested mechanisms:

1. **Formation jobs ride the sync obligations queue**, not `workflows.py`
   (whose `enqueue` hard-gates types). `_enqueue_item_obligations` persists
   per-record work under the same page transaction; connection default
   `formation_policy='deterministic'`.
2. **Attachment obligations are content-addressed** (`attachment:{connection}:{sha256}`),
   so an identical blob across revisions dedupes rather than re-fetching.
3. **Cross-process fencing is database-enforced**: `commit_page` re-validates
   lease owner + monotonic `lease_fence` + connection `generation` + memory
   `epoch` inside the `BEGIN IMMEDIATE` transaction; `threading.RLock` is only
   an intra-process fast path.
4. **Enhancements ship enabled-by-default** where they are correctness-neutral;
   the deterministic formation path is on, and eligibility gating (not a blanket
   off-switch) governs model-dependent enrichment.
5. **Secret backend is concrete**: `SecretStore` keeps values in one 0600
   operator file; DB/specs/ctx carry only `secret://` references and never a
   value in `__repr__`.

Additional decision: **rescans get a fresh scan generation.** `restart_stream`
bumps a per-stream `scan` counter folded into the deterministic `op_id`, so an
explicit rescan of a changed snapshot commits new content instead of colliding
with prior page receipts, while replay within one scan stays idempotent.

Durable inbox signals now drive ingestion scheduling (source-neutral, role-based).
Each supervisor tick captures one high-water mark per active connection before any
pass. A non-empty mark bypasses the ordinary incremental polling delay for one
bounded pass, but never a pause, an authentication park or a schedule row carrying
a retry backoff. Acknowledgment happens only after a converged incremental pass
with no failed incremental pass in the same tick, and only up to the captured
mark, so repeated signals coalesce into one pass, multi-page catch-up defers
acknowledgment until it completes, and a signal arriving during a pass survives
to schedule its own follow-up. Failures leave every signal durably queued.

Efficiency: 250+ records commit per internal transaction (not bound by the HTTP
100 cap), WAL + `busy_timeout` already in `Store`, one transaction per page.
Qualification: 2000 end-to-end records commit in ~0.42s (~4779 rec/s, 8
transactions) with full page replay idempotent under 2s (receipts + content
fingerprint).

Deferred (documented, not silently dropped): HTTP service routes, the
`hermes sources` CLI, the `authorize()` matrix extension, live Gmail end-to-end,
and local-model retrieval evaluation. Pre-existing Windows `TemporaryDirectory`
/ SQLite `PermissionError` teardown failures in `test_native_history`,
`test_production`, `test_attachments`, `test_native_files`, `test_intelligence`
and `test_memory::test_doctor_detects_stale_copied_provider` reproduce identically
on the clean baseline and are not added by this work.

