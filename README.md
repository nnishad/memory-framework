# Current remediation status

Version 0.8.0rc8 makes a managed Hindsight 0.9.2 runtime part of the default
Hermes memory service, while retaining the rc7 canonical MEMORY/USER store. The current host patch targets Hermes v2026.9.14; see
[the integration guide](docs/HERMES_INTEGRATION.md) and [RC8 lifecycle notes](docs/RC8_INTEGRATION.md).
**Production qualification remains incomplete.**

# Personal Memory for Hermes — 0.8.0rc8

An evidence-backed personal memory framework with a native Hermes provider, strict
extensible ingestion, structured knowledge, durable learning workflows and governed execution.
This release candidate implements the framework modules below. It does not certify the
accuracy of an untested model, personal archive or deployment.

| Layer | Implemented behavior |
| --- | --- |
| Ingestion | Required versioned core, namespaced extensions, immutable source revisions, provenance receipts, atomic batches/cursors, export adapters and Gmail historical/incremental ingestion |
| Recall | Keyword/Hindsight plus default local multilingual embeddings, optional HNSW, temporal and identity filters, progressive rounds, default graph/recency ranking and best-effort cross-encoder reranking, optional model planner/reranker adapters, duplicate context suppression |
| Knowledge | Quoted beliefs with conflicts and validity, contextual preferences, dated typed relationships, bounded graph traversal, reviewed episode summaries |
| Measurements | Metric/unit validation, immutable custom metric definitions, explicit conversions and SQL aggregates |
| Consolidation | Immutable evidence snapshots, whole-source chunk partitioning, leased jobs, restartable cursor, local extractive or configured model adapter, pending proposals and independent review |
| Learning | Outcomes, immutable lesson versions, separately evaluated suites, active-baseline replay, scoped automatic promotion, retraction and recovery |
| Procedures | Evaluated lessons bound by an administrator to installed capabilities; credential-scoped execution, stable job keys and output validation |
| Tasks | Versioned state transitions, dependencies, deadlines, completion/cancellation evidence, durable terminal intents and leased reminder outbox |
| Operations | Owner/session controls, scoped credentials, worker diagnostics/quarantine, logical forgetting, encrypted backup/isolated restore, native Hermes SQLite backup support |

## Fresh installation

For Gmail OAuth setup, continuous ingestion, and the bounded Windows live test,
see [Gmail ingestion](docs/GMAIL_INGESTION.md). The running memory service owns the
sync worker; historical capture and new-mail polling have separate checkpoints.

Use Python 3.11+ on Linux or macOS with SQLite FTS5 and the pinned Hermes release `v2026.9.14`.
The service uses POSIX file locking; native Windows service deployment is not supported by
the current launcher. From this extracted project:

```sh
sh deployment/install.sh
python scripts/manage_hermes_host_patch.py apply --hermes-root /path/to/hermes-agent
~/.local/share/hermes-memory/venv/bin/python ~/.hermes/personal-memory/run_service.py
```

The service runs in the foreground and starts its private Hindsight runtime automatically;
there is no enable flag or opt-in step. Restart Hermes after setup. Exclusive setup writes
`memory.store: provider`, so Hermes's native `memory` tool, `/memory`, gateway and dashboard
all use the canonical versioned store instead of MEMORY.md/USER.md. Hermes still owns its
active context, skills, scheduler and other host subsystems.

To enable continuous local extractive consolidation, run setup with the service interpreter:

```sh
python -m personal_memory setup --hermes-home ~/.hermes --exclusive --auto-consolidate
```

Restart the service after configuration changes. Source text remains complete in canonical
storage; the worker processes bounded overlapping spans and records each span's provenance.
Model-backed processing, automatic promotion and external delivery are operator-configured.
If Hermes Portal authentication is present, Hindsight automatically uses its supported Nous
provider. Otherwise it runs in local `none` mode (chunk storage plus hybrid recall, without
fact extraction) and needs no API key. No remote message destination is enabled by default.

## Ingest and inspect

```sh
python -m personal_memory import-jsonl --hermes-home ~/.hermes examples/encounters.jsonl
python -m personal_memory import-whatsapp --hermes-home ~/.hermes chat.txt --thread-id stable-group-id --date-order DMY --timezone Europe/London
python -m personal_memory import-email --hermes-home ~/.hermes archive.mbox
python -m personal_memory import-health --hermes-home ~/.hermes measurements.csv
python -m personal_memory doctor --hermes-home ~/.hermes
```

Every new connector must satisfy [INGESTION.md](docs/INGESTION.md) and the bundled JSON
Schema. Unknown namespaced extensions are preserved. Structural acceptance does not verify
source truth or automatically translate arbitrary extensions into typed measurements.
Offline exports do not provide continuous provider sync or attachment/audio/OCR extraction.
`import-health --subject-id <existing-entity-id>` also creates typed measurements for
supported metrics; without that option the importer stores source records only.

Hermes can discover structured-operation arguments through `personal_memory_knowledge`
with operation `schema` and empty arguments. Administrators can call the same HTTP APIs
without putting credentials in command arguments:

```sh
python -m personal_memory request /v1/intelligence-schema empty-object.json --hermes-home ~/.hermes --credential-role admin
```

`empty-object.json` contains `{}`. The selected role must resolve to one configured principal.

## Documentation and validation

- [FRAMEWORK.md](docs/FRAMEWORK.md): current architecture, APIs, configuration and extension contracts.
- [OPERATIONS.md](docs/OPERATIONS.md): deployment, monitoring, backup and recovery.
- [HERMES_INTEGRATION.md](docs/HERMES_INTEGRATION.md): pinned host compatibility and executed integration checks.
- [IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md): capability coverage and remaining qualification boundaries.
- `VALIDATION.json`: historical RC8 executed checks, not a fresh validation of this checkout; older benchmarks retain their original scope.

Tests use synthetic evidence and controlled model/capability fixtures. A real AIAgent loop
is exercised, but no real language-model learning effectiveness, personal archive, live
message delivery or target-hardware production qualification is claimed.

## Synthetic personal archive

The reproducible archive contains 1,226 fictional records, 24 retrieval questions and 30 memory lifecycle checks. Run `python scripts/evaluate_synthetic_personal.py` for lexical retrieval and lifecycle tests. Add `--model-path /absolute/path/to/minilm` for the actual multilingual embedding comparison. See [the measured report](docs/synthetic-personal/REPORT.md), individual results, input JSONL and queries in the same directory. Structured facts and identity confirmations are explicitly supplied by the harness; learning uses fixture adapters. This does not evaluate real LLM extraction or certify production readiness.

## Candidate rejection

The default retrieval backend rejects matches supported only by generic words or weak vector similarity. Empty results expose `no_relevant_evidence`; backend/index failures expose `retrieval_incomplete`. Hermes receives these statuses and explicit unknown-answer guidance. See [gate behavior and limitations](docs/RELEVANCE_GATE.md) and [measured results](docs/synthetic-gated/REPORT.md). Passing the gate does not verify an answer.

## Parallel request plans

Hermes can now call `personal_memory_investigate` with independent evidence intents, language variants and per-branch filters. The server executes up to three branch searches concurrently, merges evidence fairly and optionally exposes source-backed relationship paths. See [execution contract and limits](docs/PARALLEL_INVESTIGATION.md) and [six synthetic plan results](docs/PARALLEL_PLAN_CHECK.json). Planning and final evidence verification remain the Hermes model’s responsibility.

## Hermes activation and upgrades

The canonical native-store contract requires the hash-pinned host patch for v2026.9.14.
The managed provider also installs the structured memory tool surface and request-planning
guidance. Setup stamps the package version; doctor detects an outdated copied provider or
running service. Both Hermes and the memory service must restart after an upgrade. Follow
[Hermes installation and activation](docs/HERMES_INTEGRATION.md).

## Full memory-surface audit

The [v2026.9.14 provider-contract audit](docs/HERMES_914_MEMORY_COVERAGE.md) maps the current
host hooks. The [earlier coverage audit](docs/HERMES_MEMORY_COVERAGE_AUDIT.md) preserves
historical gaps; several have since been addressed by the host bridge. Hook coverage does
not establish complete semantic forgetting or production qualification.

## Awareness of incoming data

The [memory awareness design and rollout plan](docs/MEMORY_AWARENESS_PLAN.md) covers the
shared change journal, bounded next-turn packets, and durable background analysis.
Awareness is opt-in: an administrator enables the journal and configures foreground
and background consumers. The pinned Hermes patch acknowledges packets only after a
successful model request includes them. Run `personal-memory awareness-run` on the
Hermes host for background batches; ingestion itself does not start model runs.

### Hermes integration requirements

The release-pinned [Hermes patch](host-patch/hermes-v2026.9.14.patch) is required for
full awareness behavior. It emits a `request_assembled` event only after Hermes has
successfully sent a model request, which lets the memory service mark a foreground
packet as exposed only when the packet truly reached the model. It also creates
background awareness runs with action toolsets disabled and external memory injection
skipped; the worker supplies bounded source evidence and optional related-memory
retrieval itself.

Apply the patch and deploy the framework together on the host that runs Hermes and
the memory service. Then enable the change journal, configure the foreground and
background consumers, and supervise `personal-memory awareness-run --continuous`.
The worker binds its Hermes run to the same `--hermes-home` profile used to locate
the memory configuration, so profile-specific models, credentials, and cron state
cannot be mixed. Stream discovery is cached and refreshed at the source poll
interval; it does not run on every supervisor tick.
Adapters such as Gmail, WhatsApp, or health data need no Hermes-specific code: they
write through the same source-sync and change-journal contract.

Add `--deliver` only after configuring a private Hermes `cron_recipients` destination:

```sh
personal-memory awareness-run --consumer-id hermes-background --continuous --deliver
```

The awareness model can recommend a notification but cannot select the recipient,
urgency, or text. The worker stores an intent, revalidates cited source evidence,
and invokes Hermes's recipient-scoped delivery bridge. Hermes records provenance for
the exact stored summary before sending; memory confirms the intent only after Hermes
returns positive channel-delivery evidence. Failed or ambiguous sends stay attempted
and become `uncertain` during reconciliation, so they are never blindly resent.
It still needs a live model and channel trial on the deployment host before enabling
real personal notifications.
