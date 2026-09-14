> Current release: [rc6 remediation and acceptance](RC6_INTEGRATION.md). Earlier findings and test counts below retain their historical scope.

# rc5 update

Native read guards, transport identity, parent delegate packets, reported skill/review outcomes and recipient-scoped cron hooks are implemented in the separate pinned host patch. See [HOST_BRIDGES.md](HOST_BRIDGES.md) for installation, evidence and remaining limits. The rc4 inventory below is historical; its listed gaps are partially superseded by rc5.

# Gap remediation — 0.8.0rc4

Status: **partial remediation, not full production readiness**. Target remains Hermes v2026.8.31, commit 29112bef099274229cadff79cdff7bf7b99c4b77. No upstream Hermes source was changed in this release. No existing user installation or real personal data was used.

## Added in rc4

The administrative native-history connector now reads coherent snapshots of the pinned Hermes state.db, restricts imports to explicitly selected sessions, batches records, detects edits/removals, resumes failed revision retirement, and supplies stable native message identity. Exact session/role/timestamp/content copies link to the first imported observation. Source-item tombstones block future revisions and are reconciled on restore. See `NATIVE_HISTORY.md` for operation and limitations.

## Implemented in rc3

- A durable, profile-local exposure ledger stores canonical evidence IDs seen through provider searches, investigations, evidence reads and returned prefetch results. It contains no source text. It survives restart and follows explicit parent-session compression/branch transitions; a reset does not inherit the parent scope.
- Automatic completed-turn captures separate user statements from generated assistant output. Assistant replies, generated tool observations, delegation summaries, checkpoints and session-end assistant content carry the session's known parent evidence IDs. Canonical source deletion therefore follows these tracked copies through the existing dependency cascade.
- Outbox delivery checks parent tombstones before sending. Forgotten dependencies cause pending payloads to be discarded while retaining an idempotency fingerprint. Existing dead letters are also checked for forgotten dependencies and their payloads removed. Transient network failures are never interpreted as evidence of deletion.
- Native `session_search`, built-in `memory`, unknown tool results and malformed framework recall results do not become independent tool evidence. Without canonical provenance, the session's subsequent generated captures are withheld. Required checkpoints explicitly fail instead of reporting incomplete data as safely archived.
- `on_turn_start` commits user input locally before a final answer exists. Session-end capture preserves available user/assistant message content, including structured content passed by the host, with an explicit unverified completion state.
- The status tool exposes capture withholding. Native connector version metadata now follows the framework package version.

## Semantics and limits

The exposure ledger is deliberately conservative: every generated observation depends on all known evidence exposed in that session. Deleting one source can remove an entire generated response, including unrelated text in that response. Independently supplied user statements remain separate. This is source-dependent logical deletion, **not semantic forgetting everywhere**.

The ingestion contract permits at most 100 parents per record. This implementation refuses generated captures above that budget rather than silently dropping dependency IDs. A hierarchical lineage representation is still needed for long sessions. User input remains capturable. Restarting the provider does not bypass withholding. Starting a fresh session should also discard the old host context.

IDs must be supplied by actual retrieval responses. Previously untracked copies, facts read through arbitrary files/terminal code, native injected summaries, and externally reconstructed histories cannot acquire trustworthy lineage retroactively. The framework cannot delete content already shown to a user or copied to native state/history, WAL files, external sources or old backups. Outbox reconciliation requires the service to become reachable; a stopped/offline queue may retain payloads until reconciliation. Physical erasure and backup retention are separate operations.

Existing captures are not silently reclassified. Upgrading a previously used installation requires a separate migration/reconciliation of historical records and queued captures. The current project is a fresh development, so this release does not run such migration against user data.

## Outstanding work

| Area | Remaining implementation or qualification |
|---|---|
| Native history | Selected-session connector and framework-side reconciliation implemented; native session_search replacement, native transcript erasure and live resume provenance remain |
| Long sessions | Hierarchical dependencies beyond the 100-parent contract, deduplication across input/completion/checkpoint/end observations |
| Local surfaces | Authenticated TUI/desktop owner binding; transport-specific API/ACP/gateway qualification |
| Delegation | Parent-prepared scoped evidence packets or restricted read access, child trace lineage |
| Learning | Typed native skill/review events, revision mappings, evaluated procedure export; retain review-fork isolation |
| Tasks and cron | Native todo/job/run/notepad mappings and delivery-recipient-aware recall |
| Built-in stores | Explicit MEMORY.md/USER.md migration and committed-write/retraction synchronization |
| Reset and restore | Provider-aware dashboard reset with explicit store scope and all-store deletion reconciliation |
| Compression | Provider-aware authority wording and complete resumed-context provenance |
| Media | Original attachment/blob ingestion and retention; raw message JSON alone is insufficient |
| Quality | Real-model planning/indirect-recall evaluations and complete desktop, cron, delegate and restore runs |

SOUL.md, project instructions and scheduler execution state remain host authority. Their memory integration must not allow retrieved facts or inferred procedures to grant execution authority.

## Validation

`tests/test_capture_lineage.py` exercises tracked-copy deletion, preservation of independent user statements, offline queue deletion, dead-letter cleanup, restart persistence, native untracked-recall exclusion, checkpoint refusal, interrupted input, session isolation, compression inheritance and overflow behavior. The full suite, pinned release bridge and actual AIAgent protocol fixture are reported in `../VALIDATION.json` and the adjacent JSON reports. The AIAgent fixture is a deterministic local model protocol server, not a real-language-model quality result.
