# Memory integration remediation — 0.8.0rc6

Target: Hermes v2026.8.31, commit `29112bef099274229cadff79cdff7bf7b99c4b77`. This release implements the reviewed integration changes below. It remains a release candidate. Fixtures use fictional data; no real personal archive, language model or external recipient was used.

The provider is a plugin. Native integration requires the separate, explicit, reversible patch across 19 host files. The provider ABC is unchanged; setup never edits Hermes core. Hermes retains authority over permissions, executable skills, scheduling and working context.

## Remediation matrix

| Reviewed gap | Implemented behavior | Boundary |
|---|---|---|
| Cron continuity bypass | Output and notepad revisions carry canonical dependencies, producer identity and recipient scope. Cross-job reads check both producing and receiving jobs. Long output is chunked. | Legacy/untracked content, changed recipients and forgotten evidence are withheld. |
| Native history authorization | Trusted caller scopes surround conversation/tool execution. Native search, body hydration, resume, metadata-only search and cross-profile fallback enforce access. Authorized reads reconcile touched sessions first. | Generated titles, previews and cached system prompts lack field-level lineage and are conservatively redacted. Direct filesystem/SQL reads are outside the API boundary. |
| Mutable delivery scope | Scope is immutable per job/run. Delivery requires exact outgoing text, the recorded run, current recipients and live evidence. Missing content or run fails closed. | Native cron messaging checks resolved recipients. Untracked cron media is withheld. Other MCP, shell and network tools require their own execution policy. |
| Silent capture failure | Host callbacks return durable queued/withheld/failed receipts. Native callers receive failures; provider status exposes receipts. Native search supplies canonical evidence IDs. Hierarchical provenance groups retain more than 100 parents. | Queued is not committed. Delivery artifacts commit synchronously. Unknown provenance remains withheld. |
| Incomplete deletion/recovery | Touched native history is reconciled before returning content. Attachment bytes share canonical backup/deletion storage. Global reset markers survive in the independent deletion ledger. | Logical redaction does not erase active prompts, native/external files, disk pages or independently retained backups. |
| Native skill lifecycle | Revisioned file migration, current-state pointers, historical retirement, explicit export of active evaluated procedural lessons, and loader checks for retraction/content integrity. | Export never promotes a candidate or grants permissions. Ordinary operator-installed skills remain native instruction sources. |
| Reset/built-in/compression gaps | Journaled reset, stale-write epoch fence, incomplete-reset readiness failure, source-backed built-in migration, evidence-oriented compressor wording and interrupted-turn observations. | Interrupted capture verifies available tool exposure or withholds. Actual interrupted-loop behavior has not been separately qualified end to end. |
| Ingestion expansion | Strict core and namespaced extensions; typed health import for an explicit subject; resumable attachments with connector scope and SHA-256 verification. | OCR, speech recognition and arbitrary-format extraction remain ingestion-adapter responsibilities. |

## Installation and migration

From the extracted project, install using the README and explicitly apply the pinned patch:

```sh
python scripts/manage_hermes_host_patch.py apply --hermes-root /path/to/hermes-agent
python -m personal_memory sync-hermes-files --hermes-home /path/to/profile
python -m personal_memory doctor --hermes-home /path/to/profile
```

Restart Hermes and the service before running doctor. It checks the installed version and all pinned host hashes. Plugin setup alone does not activate native protections. Existing host patches must be rolled back using their matching release bundle before applying a replacement patch; mixed or locally edited source is rejected.

Initial native file migration is explicit. It rejects symlinks, out-of-profile paths, ambiguous names, invalid UTF-8, changing files and files over 1 MiB. Files use 60,000-character parts beneath canonical roots; the current pointer publishes only after complete ingestion. Ordinary edits preserve history while retiring current visibility. Explicit forgetting invalidates descendants.

Knowledge operation `native_state` pages observed skill, built-in memory, notepad and todo state. It reports observations rather than competing executable tasks. Native todo capture is the last observed tool result. No exhaustive inventory is claimed without migration. Exclusive mode disables the old built-in memory tool; additive-mode recall without provable canonical exposure withholds derived capture.

```sh
python -m personal_memory export-native-skill --hermes-home /path/to/profile --candidate-id lrn_example --name reviewed-procedure
python -m personal_memory import-health --hermes-home /path/to/profile measurements.csv --subject-id known-entity-id
python -m personal_memory attach --hermes-home /path/to/profile fictional.pdf --record-id rec_example --mime application/pdf
```

Managed skill export requires an active, independently evaluated procedural lesson and explicit administrator action. Local edits are not overwritten. Missing registry/service, retraction or changed content makes the managed skill unavailable.

## Attachment interface

`/v1/blob/begin`, `/put`, `/complete`, `/list` and `/read` bind bytes to a live canonical parent. Upload uses 512 KiB chunks, idempotent retries and out-of-order resume; completion verifies declared size and SHA-256. Maximum size is 1 GiB per attachment. Agent/reader credentials may read; only administrators or the parent record's scoped ingestion connector may upload. Knowledge operation `attachments` lists originals. The API exposes chunk reads. Extracted text must be ingested as derived evidence with canonical parent IDs. No attachment is executed or automatically interpreted.

## Reset, backup and downgrade

Administrator `POST /v1/reset` requires `{"scope":"canonical"}`. A durable intent precedes reset; source families are tombstoned, canonical identity/learning metadata is scrubbed, and the write epoch advances. Interrupted reset resumes on restart/retry. Pending reset makes readiness fail and normal memory requests unavailable. Old sessions and queued captures cannot write using the old epoch.

Preserve the latest `memory.deletions.db`. Isolated restore merges this ledger before opening the recovered store. Reset markers also scrub identity metadata from old backups, while applied markers preserve valid post-reset data on restart. Encrypted backups contain canonical data, attachments, optional outbox and managed-skill registry. Native skill files, credentials and backup keys are maintained separately.

Stop both Hermes and the service before installing restored databases. Restore `skill-exports.json` to the profile's personal-memory directory alongside separately backed-up native skills, then start new sessions. Already-loaded/in-flight prompts are not erased by reset.

Canonical storage is schema 7. rc6 migrates supported older schemas; earlier framework code refuses schema 7. Host-patch rollback restores source only, not database format. Data downgrade requires a separately preserved pre-upgrade backup and compatible replay of current deletion intents.

## Validation limits

`VALIDATION.json`, current rc6 logs and host reports record executed checks. Upstream suites use Hermes `scripts/run_tests.sh`. Two native-state failures reproduce the previously documented baseline: SQL trace query-count instrumentation and `/proc/.../fd` availability. They remain reported failures.

Not established: real-LLM recall or learning accuracy, years-of-archive throughput, complete browser/WebSocket deployment authentication, actual external delivery, or macOS/Windows production behavior. Native history reconciliation currently takes a coherent full SQLite snapshot when its database/WAL stamp changes; large-archive latency remains unqualified. Historical semantic/HNSW benchmarks retain their original scope.

The memory plugin is not an OS sandbox. Direct filesystem access, trusted plugins and arbitrary network tools retain their own authority and isolation requirements.
