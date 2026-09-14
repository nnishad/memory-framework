> Current release: [rc6 remediation and acceptance](RC6_INTEGRATION.md). Earlier findings and test counts below retain their historical scope.

# Operating release 0.7.0rc1

This is a tested release candidate for one owner's local Hermes profile. It is not a
certified deployment of the user's archive. `VALIDATION.json` records executed checks;
this runbook identifies the checks that need the actual host, data and Hermes version.

## Install and start

Use Python 3.11+ on Linux with SQLite FTS5. From the extracted release directory:

```sh
sh deployment/install.sh
~/.local/share/hermes-memory/venv/bin/python ~/.hermes/personal-memory/run_service.py
```

The installer selects this provider and disables the built-in fact/profile stores,
preserving their files. Existing settings are preserved. Stop an existing service
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

The production server binds loopback, uses one Uvicorn process, bounds HTTP concurrency
and request bodies, rejects browser-origin requests and disables access logs. The
reference `http.server` transport is for tests only. Do not expose either directly to
the internet. This release does not implement multi-tenant row isolation or cluster HA.

## Owner sessions and configuration

This release targets a fresh installation. It does not require an existing database or
perform a legacy-install migration. Private bootstrap settings and credentials live in
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

Embedding failures are recorded per record and retried. Model-weight/config changes
produce a different index key; old indexes are rebuildable derived state. Do not edit
canonical records to repair an index. A changed remote embedding deployment must use
a new explicit `revision`. Exact NumPy search is linear; use and qualify the optional
HNSW extra for larger archives. Restart reconstructs HNSW from SQLite vectors.

Hindsight 0.9.2 is installed and managed by default. Service startup creates a private,
profile-specific embedded pg0 daemon and indexes every source; there is no enable flag.
With an authenticated Hermes Portal it automatically selects Hindsight's `nous` provider.
Without provider credentials it selects Hindsight's `none` mode, which retains chunks and
supports hybrid recall without LLM extraction. Explicit `HINDSIGHT_API_LLM_*` environment
settings take precedence. The first startup downloads Hindsight's local embedding model.

Failed or unacknowledged retains remain journaled; forgetting also schedules remote deletion
for uncertain writes. A failed Hindsight deletion blocks further retention. Hindsight results
only propose canonical document IDs and cannot bypass local visibility rules or tombstones.
`doctor` requires the package and a live default engine. The embedded pg0 database is derived
retrieval state, not the source of truth; canonical SQLite and its deletion ledger remain the
recovery authority. Operators choosing `managed:false` must provide an HTTP(S) `url` and own
that external service's retention, replicas, credentials and backups.

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

Executed here: 55 tests, 13 actual-release bridge checks, a complete AIAgent loop with a
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

The CI workflow is included but has not been run on GitHub. It tests supported Python
versions and audits installed dependencies; it is a release gate. Local service and Hermes-runtime dependency audits are included;
zero reported known vulnerabilities only describes those scanned environments. `constraints-tested.txt` pins tested direct dependencies,
not every transitive dependency. Capture a platform-specific lock/SBOM for deployment.

References: [Hermes MemoryProvider](https://hermes-agent.nousresearch.com/docs/developer-guide/memory-provider-plugin),
[SQLite online backup](https://www.sqlite.org/backup.html),
[Python HTTP server limitations](https://docs.python.org/3/library/http.server.html),
[Qdrant multilingual ONNX model](https://huggingface.co/Qdrant/paraphrase-multilingual-MiniLM-L12-v2-onnx-Q).


## Current framework operations

Schema version is now 6. See FRAMEWORK.md for the full configuration and API contracts.
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

Historical performance numbers above belong to the earlier storage/retrieval build. Current
functional counts and fixtures are recorded in VALIDATION.json. Full personal-archive throughput,
model learning quality and actual reminder delivery are not established by those numbers.
