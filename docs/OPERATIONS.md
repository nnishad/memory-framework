> Current integration: [Hermes v2026.9.14 guide](HERMES_INTEGRATION.md) and [host bridges](HOST_BRIDGES.md). Earlier findings and test counts below retain their historical scope.

# Operating release 0.8.0rc8

This is a tested release candidate for one owner's local Hermes profile. It is not a
certified deployment of the user's archive. `VALIDATION.json` records historical RC8 executed checks;
this runbook identifies the checks that need the actual host, data and Hermes version.

## Install and start

Use Python 3.11+ on Linux or macOS with SQLite FTS5. The service uses POSIX `fcntl`
locking, so the current launcher does not support native Windows. The systemd template
below is Linux-specific. From the extracted release directory, with Hermes stopped:

```sh
sh deployment/install.sh
python scripts/manage_hermes_host_patch.py apply --hermes-root /path/to/hermes-agent
~/.local/share/hermes-memory/venv/bin/python ~/.hermes/personal-memory/run_service.py
```

The installer selects this provider and sets `memory.store: provider`, keeping the native
`memory` tool enabled while storing MEMORY/USER entries in the canonical versioned store.
Existing MEMORY.md/USER.md files are preserved but are not used in provider-store mode.
The active host patch targets v2026.9.14; see [HERMES_INTEGRATION.md](HERMES_INTEGRATION.md).
Existing settings are preserved, with Hindsight normalized to enabled. Stop an existing service
and back it up before upgrading. Install dependencies in the service virtualenv;
the copied Hermes provider uses only the standard library and the host's provider API.

For supervised operation, edit `deployment/personal-memory.service` for the actual
profile, interpreter and available RAM, copy it into `~/.config/systemd/user/`, then:

```sh
systemctl --user daemon-reload
systemctl --user enable --now personal-memory.service
```

The template is provided, not executed or host-qualified by this release. User-service
filesystem sandbox support varies by host. Verify startup and model-cache permissions.
Use an encrypted local filesystem; SQLite, its WAL, the provider outbox and temporary
backup plaintext are not encrypted by this application. Do not use a network filesystem.
Keep the database and outbox on reliable storage with free-space monitoring.

Service startup is transactional for resource ownership: operator configuration is validated
before the indexing lease or any worker exists, and if a later initialization stage fails, the
already-created resources are closed in reverse order through the normal shutdown path (so
indexing ownership stays held until its writers actually stop). The startup error itself is
always preserved; after a failed start you can correct the configuration and restart in the
same process against the same data directory.

The production server binds loopback, uses one Uvicorn process, bounds HTTP concurrency
and request bodies, rejects browser-origin requests and disables access logs. The
reference `http.server` transport is for tests only. Do not expose either directly to
the internet. This release does not implement multi-tenant row isolation or cluster HA.

## Owner sessions and configuration

Fresh installations are the primary target. Opening a supported older canonical database
applies schema migrations through version 8; this does not imply automatic migration of
every legacy host artifact or install layout. Private bootstrap settings and credentials live in
`personal-memory/settings.json`. The native Hermes wizard/dashboard behavior settings
live in owner-only `personal-memory/config.json`; these override matching private settings.
The supported public keys are `port`, `prefetch_wait_ms`, `session_access`, and `retrieval`.
Restart the service and Hermes after editing them. Native CLI numeric strings are normalized.

CLI and cron sessions run with the owner's OS authority. Remote private sessions require
an exact platform/user-ID match, for example this fragment in `config.json`:

```json
{"session_access":{"owners":{"telegram":["YOUR_STABLE_OWNER_USER_ID"]}}}
```

Unknown recipients, missing platform identity and shared/group rooms receive no memory
prompt, tools, recall or capture. Importing historical group messages into the private
archive remains supported. This policy trusts host-supplied session metadata; it is not
OS isolation, a shared-memory tenancy system or a general control over downstream sending tools.

Completed-turn and session-end hooks capture named tool results, excluding memory tools
and unknown tool names. Parent delegation results retain child-session attribution and
remain unverified evidence. The host's queue before an outbox commit is not durable:
a hard crash can lose work that has not reached the provider. Capture does not imply
that arbitrary assistant statements have become verified personal facts.

## Credentials and connectors

Settings are owner-only JSON under `<hermes-home>/personal-memory/settings.json`.
`token` is the administrator credential; `agent_token` is selected by the Hermes
provider and has role `agent` in `principals`. Additional principals can have:

| Role | Operations |
| --- | --- |
| admin | All operations, including forgetting and declaring coverage |
| agent | Recall, capture, claims, entities and reversible identity links |
| reader | Recall, evidence, browsing and status |
| evaluator | Read access plus evaluation submission/jobs; no promotion or ingestion |
| executor | Explicitly allowlisted installed capabilities only |
| scheduler | Task listing and reminder claim/ack only |
| ingest | Schema, ingestion and checkpoints within its connector/source scope |

An ingest principal is `{"token":"<random-secret-at-least-32-characters>","role":"ingest","connector_id":"example.notes","sources":["custom-notes"]}`.
Generate secrets using `secrets.token_urlsafe(32)`, write settings atomically with
owner-only permissions, then restart the service. Rotate each client with its matching
principal. Remove retired tokens after clients switch. Never put tokens in shell
arguments, source control, prompts or logs. OS access to the settings file grants
access to all these credentials; roles do not sandbox Hermes against its own OS user.

Checkpointed ingestion sends `items` plus:

```json
{"checkpoint":{"connector_id":"example.notes","source":"custom-notes","expected_cursor":null,"cursor":{"offset":100}}}
```

The cursor advances in the same SQLite transaction as the batch. Resume by POSTing
`{"connector_id":"example.notes","source":"custom-notes"}` to `/v1/checkpoint`.
A lost acknowledgement can be replayed with the same batch and cursor. A different
batch cannot reuse that cursor. Changed source content, event time or extensions need
a new revision. Connectors should retain their input and retry transient failures with
backoff. Built-in export importers use idempotent replay rather than cursor skipping.

Registered extension schemas can be placed under `extension_schemas` as
`namespace -> version -> JSON Schema`. Known namespaces enforce versions and their
schemas; unknown namespaces remain preserved. Remote schema references are rejected.
Extra JSON data is retained as evidence, not automatically indexed as numeric facts or
converted into verified semantic claims. New domain adapters own those transformations.

## Monitor and repair

Run `python -m personal_memory doctor --hermes-home <profile>` using the service
interpreter. `--offline` checks local storage without contacting the service.
`/v1/health` is an authenticated POST for liveness and aggregate counters;
`/v1/ready` returns HTTP 503 while configured indexes have backlog or errors.
Queries can still return partial evidence during backlog and include diagnostics.
Neither endpoint asserts source completeness or that a query answer is true.

Monitor service restarts, disk capacity, doctor failures, source coverage, pending
captures, oldest pending capture time, dead letters, index backlog and remote deletion
backlog. Decide alert thresholds from the actual import volume and latency budget.
External source content is not emitted into diagnostic error messages.

Permanent invalid captures move to the local outbox `dead_letters` table. Inspect this
private table using a local SQLite tool; its payload can contain personal data. Repair
connector/schema/references, then replay using `Outbox.replay_dead_letter(entry_id)`
with the profile's configured `Client`. Do not change canonical source content under
the same revision; submit a corrected revision. Transient network failures remain in
`pending` with backoff. A rejected record must not block later valid records.

Semantic index maintenance is incremental. Canonical writes enqueue durable
index/retire requests in the same transaction; each background poll processes
only due queue entries in bounded batches, so an idle poll touches the queue
and never scans the archive. Failed records stay queued with durable retry
backoff, survive restarts and coalesce per record. A pre-existing archive is
bootstrapped into the queue exactly once per model revision key. Embedding
failures are recorded per record and retried. Model-weight/config changes
produce a different index key; old indexes are rebuildable derived state. Do not
edit canonical records to repair an index. A changed remote embedding
deployment must use a new explicit `revision`. Retrieval-time visibility checks
stay independent of index freshness: a stale vector never returns retired
evidence. Exact NumPy search is linear; use and qualify the optional HNSW extra
for larger archives. Restart reconstructs HNSW from SQLite vectors.
Measured on a Windows development box with a deterministic hash embedder
(`python scripts/bench_semantic.py <records>` prints the JSON before/after evidence):
background work is constant in archive size where the former scanner grew with
it — at 5,000 records an idle poll dropped from 9.65 ms to 3.64 ms and a
readiness status call from 7.22 ms to 2.84 ms, while indexing throughput
(~70-80 records/s, embedding- and fsync-bound) and recall latency stayed
unchanged.

Hindsight 0.9.2 is installed and managed by default. Service startup creates a private,
profile-specific embedded pg0 daemon and indexes every source; there is no enable flag.
With an authenticated Hermes Portal it automatically selects Hindsight's `nous` provider.
Without provider credentials it selects Hindsight's `none` mode, which retains chunks and
supports hybrid recall without LLM extraction. Explicit `HINDSIGHT_API_LLM_*` environment
settings take precedence. The first startup downloads Hindsight's local embedding model.

Failed or unacknowledged retains remain journaled; forgetting also schedules remote deletion
for uncertain writes. A failed Hindsight deletion blocks further retention. Hindsight results
only propose canonical document IDs and cannot bypass local visibility rules or tombstones.
Normal indexing sends up to eight documents per synchronous retain request. Contract failures and
ambiguous timeouts fall back to per-document isolation; clear connection/service outages back off the
batch. Lineage-only fan-in records are not sent to Hindsight because they contain no independent
evidence.
`doctor` requires the package and a live default engine. The embedded pg0 database is derived
retrieval state, not the source of truth; canonical SQLite and its deletion ledger remain the
recovery authority. Operators choosing `managed:false` must provide an HTTP(S) `url` and own
that external service's retention, replicas, credentials and backups.

## Reset and stale Hindsight profiles

`python -m personal_memory reset --hermes-home <profile> --confirm` performs a canonical
logical reset, advances the write epoch, invalidates derived memory, and synchronously
attempts to clear the configured Hindsight bank. Retains, document deletions and the bulk
clear share one engine lifecycle lock, so an in-flight retain can never republish evidence
into a bank the reset already cleared. Check `external_engine` in the result: canonical
reset can complete even if external clearing fails. A failed clear persists a durable
cleanup obligation; the indexing worker retries it (also after a restart) and `/v1/ready`
reports "external cleanup pending" until the engine confirms the bank is empty. Restart
sessions to discard already loaded prompts. Native transcripts, external sources and
backups are outside this reset; it is not secure physical erasure.

## Unified retirement

Explicit supersession (`/v1/supersede`) and source-synchronization replacement run through
one transaction-aware retirement operation. Retiring evidence hides the record and its
descendants from current recall while preserving them for historical inspection, and it
cascades to every dependent artifact in the same transaction: awareness evidence, learning
artifacts, curated entries, identity links and derived intelligence. An account<->person
identity link is retired with its supporting evidence: hidden or forgotten evidence cannot
create or confirm a link, cannot block a corrected ownership claim, and cannot expand
person-based recall; restoring the evidence's visibility never reconfirms the revoked
identity. A record is only "live and visible" when it is undeleted and not hidden; that
single check gates creation of beliefs, learning
artifacts, snapshots, curated memory and consolidation results, so historical evidence can
still be inspected but can never silently support a current conclusion. Progressive recall
(`/v1/recall`) and parallel investigation (`/v1/investigate`) recheck that same predicate
during final hydration, so evidence retired while a retrieval round was in flight never
reaches the answer; explicit `include_history` recall still returns retired evidence.

Restoring a source record's visibility does not reactivate conclusions that were already
invalidated. An idempotent repair pass scans active artifacts whose evidence is already
retired and invalidates them; re-running it changes nothing. Queued consolidation applies
the same pass before accepting a proposal.

`python -m personal_memory prune-hindsight --hermes-home <profile>` reports stale
framework-owned embedded profiles/instances. Add `--apply` to remove eligible stale entries;
the active profile is preserved. Stop the service before applying removal so a daemon does
not hold those files. This is separate from clearing the active bank.

Retrieval models warm in the background at ASGI startup. See [FRAMEWORK.md](FRAMEWORK.md)
for the default graph/recency ranking, optional cross-encoder dependencies and disable settings.

## Back up and rehearse recovery

Create a 32-byte key once in a separately protected location; losing it makes its
backups unrecoverable. Keep a separately secured recovery copy of the key.

```sh
python -m personal_memory backup-keygen /secure-location/memory-backup.key
python -m personal_memory backup --hermes-home <profile> --key-file /secure-location/memory-backup.key --destination /encrypted-backups/snapshot.enc
python scripts/recovery_drill.py --hermes-home <profile> --key-file /secure-location/memory-backup.key --destination /private-drills/unique-drill-directory
```

Backups use SQLite online snapshots and streaming authenticated AES-256-GCM. The outbox
is snapshotted before memory to support at-least-once replay of pre-cutoff captures.
Snapshots include stored indexes and audit/cursor state. Credentials, model weights,
settings, Hermes conversations outside this framework and the key are maintained
separately. Do not call a backup successful unless its command completes successfully.
Copy encrypted backups off-host and set a retention policy appropriate to the owner.

Restore always writes to a new isolated directory, authenticates the archive and
checks hashes plus SQLite integrity and foreign keys. It refuses an existing target.
The drill directory contains decrypted data: protect and remove it after review.

For recovery, stop Hermes and the memory service and preserve the current data directory.
Restore to an isolated directory, supplying the newest independent deletion ledger:

```sh
python -m personal_memory restore /encrypted-backups/snapshot.enc --key-file /secure-location/memory-backup.key --destination /private-drills/recovered --deletion-ledger /preserved-current-data/memory.deletions.db
```

`memory.deletions.db` records content-free deletion intents before canonical deletion
commits. Startup replays outstanding intents. Restore merges the supplied ledger and
reapplies deletions, including record IDs absent from the old snapshot, before returning.
Protect a current off-host copy of this ledger according to the required data-loss window.
Do not replace the latest ledger with the old backup's ledger before recovery. If the
latest deletion history has been lost, an older backup cannot reconstruct it; keep that
restore isolated until later deletions can be reconciled. The bundled snapshot ledger
only covers its own cutoff. Tests cover a failed canonical commit and an older restore.

Verify counts, evidence samples and deletion results, then install `memory.db` and
`memory.deletions.db` into the configured data directory and optional `outbox.db` into
the profile state directory. Restore settings and credentials separately. Start the
service, wait for readiness, restart Hermes and validate recall/replay. Never copy old
WAL/SHM files alongside restored snapshots. Preserve the replaced directory until acceptance.
Logical forgetting does not securely erase historical backups, free pages, prior responses
or copied facts with no recorded dependency. Apply the owner's backup retention policy.

The pinned Hermes native backup code now recognizes all three `.db` files and takes
SQLite snapshots; this was tested on the real host implementation. External data paths
are declared by the provider; the host supports external restore paths under the OS home.
`doctor` checks this path constraint. Native ZIP encryption, snapshot ordering and the
complete native import UI are separate concerns: use the framework's encrypted backup
and deletion-aware isolated restore for the documented recovery procedure.

## Upgrade, rollback and acceptance

This artifact is for fresh deployment. Before a future upgrade, rehearse a backup restore
and review that release's compatibility instructions. Provider-selection rollback
(`python -m personal_memory rollback`) restores Hermes configuration and preserves data;
it does not downgrade a database schema.

Historical storage/retrieval validation recorded 55 tests, 13 actual-release bridge checks, a complete AIAgent loop with a
local deterministic model-protocol fixture, and CLI install/import/backup/restore smoke
checks. A 10,000-record keyword-only workload with eight clients measured p50 129.65 ms
and p95 175.99 ms over 100 queries; acknowledged records survived SIGKILL and restart.
Encrypted backup plus isolated restore took 0.77 seconds on the ephemeral test host.
These measurements are not full-archive semantic performance or real-model answer quality.

Deployment acceptance requires:

1. The actual target host/model: run tests with `HERMES_PROVIDER_CONTRACT` pointing to
   `agent/memory_provider.py`, then verify provider discovery, all tools, turn capture,
   compression checkpoint, session switch and recall in the actual running host.
2. A representative archive slice: label source-backed answers, dates, unknown people,
   reused phone numbers, contradictory facts and questions with no supporting evidence.
   Run `scripts/evaluate_archive.py` with local JSONL cases containing `case_id`, `query`,
   `expected_record_ids` and optional `filters`. Review actual Hermes answers/actions too.
3. Full-volume import/replay, concurrent search, host restart and disk/network failure
   drills on intended hardware. Record peak RAM, index build time, p95 latency and a
   measured recovery time/data-loss window. The synthetic load report does not establish these deployment objectives.
4. Actual embedding/extraction versions, a completed dependency vulnerability audit,
   storage encryption, role/token rotation, backup-key recovery and deletion recovery.
5. Numeric health transformations and units validated against the source. Memory recall
   is not a clinical interpretation engine. Procedural/prospective notes need the host's
   skills/scheduler for execution. Automatic inferred claims need evidence review.

No GitHub Actions workflow is present in this checkout, and the recorded validation does not
claim an executed CI matrix. Local service and Hermes-runtime dependency audits are included;
zero reported known vulnerabilities only describes those scanned environments. `constraints-tested.txt` pins tested direct dependencies,
not every transitive dependency. Capture a platform-specific lock/SBOM for deployment.

References: [Hermes MemoryProvider](https://hermes-agent.nousresearch.com/docs/developer-guide/memory-provider-plugin),
[SQLite online backup](https://www.sqlite.org/backup.html),
[Python HTTP server limitations](https://docs.python.org/3/library/http.server.html),
[Qdrant multilingual ONNX model](https://huggingface.co/Qdrant/paraphrase-multilingual-MiniLM-L12-v2-onnx-Q).


## Current framework operations

Schema version is 8. See FRAMEWORK.md for the full configuration and API contracts.
Run setup with `--auto-consolidate` to enable local continuous consolidation. Optional model,
capability and delivery adapters live under private settings `intelligence`; they cannot be
selected by agent tool arguments. Changed adapter declarations need new jobs/procedure bindings.

`/v1/quality` reports workflow states, worker errors, feedback and pending task count. Inspect
quarantined jobs before administrative `/v1/workflow/retry`; inspect reminder failures before
`/v1/task/event/retry`. An API request can succeed while a queued job later fails. Poll workflow
status and inspect result validation before reporting completion. No logs contain model request
bodies or credentials. Adapter errors are represented by type, not raw potentially sensitive text.

Worker leases are renewed for long evaluation sequences. Interrupted jobs retry with stable keys
and stop after three failed attempts. Capabilities and delivery handlers must deduplicate external
effects by the supplied idempotency key. Review the handler's correctness and recipient policy
before installing it; subprocess timeouts are not an OS sandbox for malicious trusted plugins.

The current independent intent ledger contains source deletions, lesson retractions and terminal
task intents. Preserve its newest version when restoring an old encrypted backup. Restore applies
these intents before returning the isolated database. Do not combine an old canonical database
with an old ledger and assume later cancellations or forgetting are preserved.

Historical performance numbers above belong to the earlier storage/retrieval build. Historical RC8
functional counts and fixtures are recorded in VALIDATION.json; subsequent source changes
are not certified by that report. Full personal-archive throughput,
model learning quality and actual reminder delivery are not established by those numbers.
