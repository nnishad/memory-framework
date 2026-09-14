# RC7 canonical Hermes integration

Target: Hermes Agent v2026.9.11 / v0.21.2, commit
`939e45c91d751fadd94dcd1b873ac3cb44846213`.

RC7 makes the framework the single source of truth for Hermes's small curated personal
memory. Setup in exclusive mode enables the native `memory` tool while selecting
`memory.store: provider`; it no longer disables Hermes's memory surface. The provider supplies
a MemoryStore-compatible view backed by `/v1/curated/read`, `/v1/curated/apply` and
`/v1/curated/reset`.

Each target (`memory` and `user`) has a monotonically increasing version. Mutations require an
expected version, current reset epoch and idempotency key. Batches validate and commit in one
SQLite transaction. Evidence-linked entries retire automatically when their source is forgotten.
A concurrent edit returns a conflict and fresh live entries instead of overwriting another session.

The system-prompt snapshot is frozen when an agent is constructed. Successful mid-conversation
edits update only its live tool view; a new agent/session rebuilds the prompt from canonical state.
Background review receives a separate store instance/version, while writes still pass through the
same canonical concurrency and approval controls.

Hermes terminal tool results are observed immediately. Stable tool-call identities deduplicate
later transcript replay, and the local outbox exposes queued, committed, withheld and failed
receipts. Observation failures are intentionally independent of the underlying tool result.

The v2026.9.11 host patch also carries authenticated native-history filtering, bounded delegation
packets, review-change receipts, cron/notepad continuity, run-specific delivery authorization,
canonical artifact registration and managed-skill validation across Hermes's decomposed modules.

The release-specific patch is integrity-pinned in `host-patch/manifest.json`. The patch fixture
verifies check, apply, idempotent reapply, rollback, idempotent rerollback and rejection of local
edits. Framework smoke checks cover version conflicts, atomic reset and prompt-snapshot stability.
The host fixture exercises the real AIAgent/native-memory handler with a local service and confirms
that its live canonical view updates without changing the existing prompt snapshot.
The `0.8.0rc7` wheel was also built, installed into an isolated directory and imported successfully.

This is still a release candidate. Real-model quality, live multi-platform delivery and full
production soak testing remain deployment responsibilities.
