# Framework 0.8.0rc8

## Architecture and persistence

Canonical source records and source revisions remain the evidence authority. Learning objects
hold outcomes, snapshots, proposed/active lessons, suites, evaluations, consolidation results,
beliefs, relationships, measurements, tasks and procedures. Evidence links and learning-object
dependencies propagate invalidation. All these objects and durable queues live in `memory.db`.
SQLite schema version is 8. `Store` accepts versions 0 through 8 and applies the current
tables, indexes and supported additive migrations on open. Compatibility with an older
application must be checked before rollback; provider rollback does not downgrade the database.
Fresh deployment remains the primary target.

Automatic recall is the existing bounded prefetch path. Explicit progressive recall can plan
queries, expand retrieval rounds, rerank candidates and suppress identical context. Structured
knowledge uses separate read APIs with explicit temporal, entity and numerical semantics.
Unreviewed consolidation results and lesson proposals do not enter ordinary source recall.
Reviewed summaries have a separate FTS index, cleared on dependency invalidation.

External memory is data. Neither a retrieved message, a lesson nor a procedure proposal changes
permissions. Source quotes establish attribution, not entailment or truth. Beliefs and reviewed
summaries remain marked unverified. Active lessons express evaluated applicability on a suite,
not universal correctness or a change to model weights.

## Configuration

Private `personal-memory/settings.json` holds credentials, data location and `intelligence`.
Native Hermes `personal-memory/config.json` holds port, prefetch wait, session access and retrieval
behavior. Preserve generated fields when editing either file. Intelligence configuration stays
private because it can contain model credentials and installed execution/delivery capabilities.
Restart the service after editing it.

A complete local-only starting configuration fragment is:

```json
{
  "intelligence": {
    "timeout": 20,
    "auto_consolidate": true,
    "adapters": {
      "consolidate": {
        "entrypoint": "personal_memory.adapters:extractive",
        "config": {}
      }
    },
    "capabilities": {},
    "learning_policies": {},
    "recall": {}
  }
}
```

This starts the durable worker and locally consolidates source records. It sends no messages
and uses no external model. The bundled extractive adapter produces quoted episode summaries;
it does not infer facts. The optional `personal_memory.adapters:openai_consolidate` adapter
accepts `url` (a compatible API base), `model`, optional `token` and `timeout` in its config.
It can propose structured beliefs using known source-linked entity IDs. The model cannot select
a Python module or assign its own authority. Configuring an external endpoint authorizes that
endpoint to receive the evidence needed for the configured operation.

`recall.planner` and `recall.reranker` take the same `entrypoint`/`config` declaration. Bundled
`openai_plan` and `openai_rerank` use a compatible chat-completions endpoint. Planning yields at
most four search queries; reranking must return a permutation of supplied IDs. Neither may
invent source IDs, broaden explicit filters or mark evidence sufficient. Invalid adapter output
falls back where possible and is identified in diagnostics. Without a planner, clause splitting
is deterministic. Without this adapter, the backend ranking is retained, including any enabled backend
cross-encoder and recency ranking.

Progressive recall launches at most three rounds and stops launching later rounds after the
round budget expires. Planner/reranker subprocesses have three-second timeouts; backend calls
retain their own limits. This is not a hard end-to-end latency guarantee under database contention.
Text budgets count episode/claim characters, not the whole JSON response or model tokens.

## Retrieval defaults

The supported service starts managed Hindsight 0.9.2 before constructing retrieval. Canonical
SQLite remains authoritative; candidates are rehydrated and relevance-gated before return.
Hindsight uses local ONNX embeddings and RRF reranking by default. Without provider credentials,
its `none` mode retains and searches chunks without LLM extraction.

The separate `retrieval` configuration defaults to bounded graph propagation (two hops),
recency weighting (`temporal.weight: 1.0`, half-life 365 days), and best-effort cross-encoder
reranking (`rerank.enabled: true`, model `cross-encoder/ms-marco-MiniLM-L-6-v2`, window 24).
The cross-encoder requires the optional `[rerank]` dependencies and model availability; an
import/load failure disables it for that process. A scoring failure falls back for that
request. Both preserve fusion order before recency ranking and record a diagnostic. Disable it with `rerank.enabled: false` or `PERSONAL_MEMORY_DISABLE_RERANK=1`.
Set `graph.enabled: false` or `temporal.weight: 0` to disable those ranking features. These
backend settings are separate from the optional `intelligence.recall` model adapters above.
ASGI startup warms retrieval in a background thread; startup completion does not wait for it.
Local multilingual embeddings are enabled by default and FastEmbed is a core dependency. Disable them
with `retrieval.semantic.enabled: false` or `PERSONAL_MEMORY_DISABLE_SEMANTIC=1`. The `--semantic` setup
flag and `[semantic]` dependency group remain accepted as compatibility aliases for older automation.

## Strict operation discovery and roles

All endpoints use authenticated JSON POST. `/v1/intelligence-schema` derives exact required and
optional argument names from the implemented methods. Additional method arguments are rejected.
The original ingestion schema remains independently enforced for every source connector.

| Role | Additional authority in this release |
| --- | --- |
| reader | Structured queries, quality, workflow status and schema discovery |
| agent | Snapshots; quoted structured records; tasks/transitions; feedback; consolidation requests; outcomes/proposals |
| evaluator | Read APIs, external evaluation reports and executed evaluation jobs |
| scheduler | Task listing and event claim/ack only |
| executor | Execution of procedures whose capability is in that credential's explicit list |
| admin | Suite/metric/domain registration, belief resolution, consolidation review, procedure binding, retries and other administrative operations |

An agent can execute a procedure only if its principal also lists the required capability.
For example, an operator may add `"capabilities":["approved_inventory_lookup"]` to that
agent principal after installing and reviewing the capability. An empty or missing list grants
no procedure execution. Capability access is checked before enqueueing. Queue execution also
checks that the capability declaration still matches the bound version and that the lesson is
still active. This does not isolate Hermes from its own OS account or the owner of its settings.

Twenty Hermes tools include `personal_memory_knowledge` for structured reads,
`personal_memory_manage` for structured writes, and `personal_memory_execute` for a bound
procedure. The existing owner/session gate and primary-context write guard cover the additions.
Group/unknown remote sessions remain denied. Administrative endpoints are not exposed as agent
management operations; nested operation dispatch repeats authorization for the actual endpoint.

## Consolidation and source coverage

`POST /v1/snapshot` takes `key, record_ids`. The result freezes source fingerprints, coverage
metadata and a cutoff. Reusing its key cannot silently refresh evidence. `POST /v1/workflow/enqueue`
with `type: consolidate` takes `key, snapshot_id`, and optional `segments` of
`{record_id,start,end}`. A job accepts at most 32 spans and 24,000 source bytes; large snapshots
must be partitioned instead of silently truncated.

The automatic scanner partitions every source record into overlapping 2,000-character spans
with a stride of 1,800. Stable per-span job keys make scanner replay idempotent. Its source cursor
advances only after all the source's spans have been queued. A crash before cursor advancement
replays existing keys. Source completeness, queued spans and completed summaries are distinct.
Summarization can omit details; original source content remains independently searchable.

The worker validates the model's JSON shape, referenced identities, exact quotes and snapshot
membership. Output is a pending `consolidation` object. `POST /v1/consolidation/accept` with
`result_id` is an administrative review step: it publishes proposed beliefs as inferred,
unverified records and indexes the summary separately. Repeating the same review is idempotent.
The model cannot classify its own proposal as observed, verified or executable.

## Beliefs, relationships and personalization

`/v1/belief` requires `key, subject_id, predicate, value, evidence`. Evidence is a nonempty array
of `{record_id,quote}`. Optional origin distinguishes reported, observed, inferred and explicit
preference. Values are finite JSON scalars. Context maps and validity intervals represent
applicability. `/v1/beliefs` returns alternatives and conflict sets at a requested time/context;
explicit and more specific contextual preferences rank first. It does not silently choose truth.

`/v1/belief/resolve` is administrative and can supersede only the same subject, predicate and
context. Raw source evidence remains available as history. A later source deletion invalidates
its supported belief; an old alternative is not automatically reactivated as the new truth.

`/v1/relation` records a dated, quoted subject/predicate/object link between existing entities.
`/v1/graph` follows up to three hops with an edge budget, preserving attribution and truncation.
Paths do not merge identities or establish causality. Account ownership still uses the original
temporal identity confirmation/revocation API. Automatic speculative identity merges are absent.

For quantified assertions, the server checks `quantifier` against an operator-configured domain.
`/v1/domain` registers required source names and scope. `/v1/domain/coverage` checks every required
source through a cutoff. `none`/`all` beliefs require this coverage gate. Ordinary free-text values
are not a semantic theorem prover: all resulting beliefs remain unverified, and arbitrary prose
cannot establish global completeness. Coverage itself is an administrator assertion about imports.

## Typed measurements

`/v1/measurement` requires a known subject, metric, numeric value, unit, timestamp and exact
source quotes. Built-in definitions cover mass/weight, temperature, heart rate, HRV and blood
pressure component metrics. Unit conversion is explicit; incompatible units are rejected.
`/v1/metric` registers immutable versioned definitions with canonical unit and positive finite
scale/finite offset per unit. Redefining an ingested metric requires a new metric name.

`/v1/aggregate` requires subject, metric, canonical unit and explicit half-open time window.
It returns count/min/max/unweighted sample mean from matching stored measurements. It neither
infers clinical meaning nor assumes sensor coverage. The health CSV importer preserves
source records and, with `--subject-id <existing-entity-id>`, also maps supported metrics into
this typed API. Without that option it imports source records only. Arbitrary extensions
are not automatically treated as measurements. Identical physical samples supplied with different
keys are not automatically deduplicated; the adapter must use stable sample identity.

## Self-learning and evaluation

The complete governed loop is outcome -> proposal -> immutable suite execution -> promotion ->
use -> feedback/retraction. Outcomes remain reports backed by source evidence. Hermes proposes
scoped semantic/procedural/preference lessons through the existing learning tools.

`/v1/suite` registers `key, cases, evidence_ids`. Each case has `id, kind, input, expected`;
kinds must include target, regression and non-applicable cases. Suites are immutable and have
source dependencies. The expected values are not returned by the agent-facing workflow read API.
OS-level access and inclusion of expected answers in ordinary source records remain outside that
API boundary. Use protected evaluation fixtures that the task model has not been shown.

An evaluator/admin enqueues `type: evaluate` with candidate and suite IDs. Configure the evaluate
adapter in private settings. Each case runs twice in a separate adapter process: with the current
active family revision (or no lesson) and with the candidate. The harness compares canonical JSON
outputs to expected values, without accepting a model's own pass/fail declaration. Every candidate
case must pass. The evaluation binds candidate, suite, adapter and baseline versions. Promotion
checks the expected active revision; stale-baseline results cannot replace a newer active lesson.

Bundled `openai_evaluate` runs a side-effect-free labeled task and returns a JSON object with
`answer`; suite expectations must use that object shape. For free-form tasks, install an evaluator
adapter with an independently validated scoring contract. Exact matching is intentionally strict
and does not measure arbitrary natural-language equivalence. `policy_fixture` is a deterministic
protocol fixture, never a production model-quality evaluator.

For automatic evaluation/promotion, configure an exact applicability scope:

```json
{
  "learning_policies": {
    "YOUR_EXACT_SCOPE": {"suite_id":"ACTUAL_REGISTERED_SUITE_ID","promote":true}
  }
}
```

This fragment belongs inside `intelligence`, with a configured evaluate adapter. The worker
queues pending candidates for that scope, runs the fixed suite and promotes passing revisions.
A reconciliation pass handles crashes between evaluation completion and promotion. Conflicting
or stale promotions are blocked and audited. Without a configured policy, review remains explicit.
These policies cannot grant execution or messaging permissions. No model-weight training occurs.

## Procedures and reminders

An administrator binds an active, evaluated procedural lesson using `/v1/procedure`:
`key, candidate_id, capability, arguments, expected`. Capabilities are installed private-config
entries containing `entrypoint` and `config`. Arguments and expected output are immutable. Model
text is never compiled into Python or shell. `/v1/procedure/execute` accepts only a stable key and
procedure ID; permitted agents/executors receive a durable job ID. Inspect `validation_passed`
before claiming success. A process completing is not itself a successful task outcome.

A capability handler receives `{arguments,idempotency_key}` and must deduplicate external effects
by that key. A crash can occur after a side effect and before its result commits. The framework
provides at-least-once execution, not universal exactly-once external effects. Retractions prevent
future execution; they cannot undo an already completed external action.

Tasks use `/v1/task` and version-checked `/v1/task/transition`. Allowed states are open,
in_progress, blocked, completed and cancelled. Work cannot start/complete before dependencies
complete. Terminal transitions require evidence and persist independent recovery intents.
Completion is evidence-attributed; the framework does not prove that arbitrary external work
actually happened. Completed/cancelled tasks do not resurrect as reminders after an older restore
when the latest intent ledger is applied.

Scheduler credentials can claim due events and acknowledge delivery. The leased outbox retries
expired leases, quarantines after three attempts and supports administrative replay. Delivered
old events do not block later tasks. An optional private `event_handler` runs an installed delivery
adapter with `{event,idempotency_key}` and requires `{"delivered":true}`. Its recipient and authority
must be fixed by operator configuration/host policy, never inferred from task text. No real delivery
handler is bundled: the host supplies its already-authorized channel integration. `event_fixture`
performs no delivery and exists only for tests. Cancellation cannot recall a message already sent.

## Adapter contract and operational limits

Adapters are trusted installed Python `module:function` callables taking `(config,request)` and
returning JSON. They run in separate processes with explicit input, wall-clock timeout, bounded
JSON protocol and bounded output files. On POSIX, timed-out process groups are terminated.
This is failure containment, not an OS security sandbox for malicious installed code. The operator
controls adapter installation and endpoints; model/tool payloads cannot choose an entrypoint.

Jobs use transactionally claimed leases, heartbeats for evaluation cases, stable keys, retry
backoff and quarantine after three attempts. A changed adapter declaration requires a new job.
`/v1/quality` reports job states, worker errors, learning states, task count and feedback. Feedback
is diagnostic and does not rewrite evidence. Monitor backlog, quarantine, memory/disk capacity,
latency and source coverage; quality and completeness do not follow from an empty error count.

All durable artifacts are included in encrypted database backups. Apply the newest independent
ledger on isolated restore to replay source deletions, lesson retractions and terminal task state.
Evidence deletion clears dependent learning payloads and the reviewed-summary index. Credentials,
model/capability settings, encryption keys and external side effects require their own recovery
procedures. Old backups and previously delivered responses are not physically erased by forgetting.

The tested host integration uses the exact pinned Hermes release and controlled fixtures. Real
model quality, full personal-archive load, actual channel delivery, host-loss RPO/RTO and operator
capability correctness require deployment acceptance. No claim of universal inference accuracy,
automatically correct identity discovery, calibrated abstention or all-action memory enforcement
is made by this framework.
