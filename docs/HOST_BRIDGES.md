> Current release: [rc6 remediation and acceptance](RC6_INTEGRATION.md). Earlier findings and test counts below retain their historical scope.

# Hermes host integration — 0.8.0rc8

The framework includes an explicit host patch for NousResearch/hermes-agent v2026.9.11,
commit 939e45c91d751fadd94dcd1b873ac3cb44846213. Generic optional callbacks live in
Hermes; backend policy remains in the provider. Plugin installation does not edit Hermes core.

## Installation and rollback

Stop Hermes before modifying its source. Install the framework/provider using the main README, then run from this extracted project, replacing the example checkout path:

```sh
python scripts/manage_hermes_host_patch.py check --hermes-root /path/to/hermes-agent
python scripts/manage_hermes_host_patch.py apply --hermes-root /path/to/hermes-agent
```

Restart the memory service and Hermes. The installer verifies the release anchor and every touched file, checks the complete patch before applying, rejects local edits/mixed states, and verifies resulting hashes. Reapplying is a no-op. It does not fetch a release or force-overwrite another version. Review `host-patch/hermes-v2026.9.11.patch` and its hash manifest before installation.

Rollback with Hermes stopped:

```sh
python scripts/manage_hermes_host_patch.py rollback --hermes-root /path/to/hermes-agent
```

Rollback restores source only. It preserves stored records and removes the extended host protections described here; do not resume sensitive native history assuming these guards remain enabled.

## Connected paths

| Surface | Implemented behavior |
|---|---|
| Native curated memory | Hermes's native memory tool is backed by versioned canonical entries with atomic batches, evidence links, reset epochs and conflict responses. Each agent gets a frozen prompt snapshot and a separate live edit view. |
| CLI/gateway/dashboard | `/memory` reads the active canonical store; gateway commands use the identity-scoped resident agent; dashboard status and targeted reset call provider-owned APIs. |
| Tool observations | Terminal tool results are captured immediately with stable call identity and durable queued/committed/withheld/failed receipts; capture failure does not alter tool execution. |
| Native history | For synced profile state.db archives, native message reads, search results/neighbour snippets and model/display resume consult canonical deletion status. Affected compressed summaries and untracked generated echoes are withheld. Exact unsynced copies are checked against imported fingerprints. |
| Desktop/TUI | Actual stdio transports establish local identity. WebSocket identity comes from server authentication, not model arguments. Local legacy-token sockets and explicitly allowlisted authenticated identities can access memory. Shared chat contexts remain denied. |
| Delegation | The parent retrieves at most four evidence records, rechecks their live status, supplies a bounded untrusted evidence packet and records exposure. Children retain skip_memory and receive no memory credential. |
| Skills/reviews | Successful skill changes and review tool deltas become idempotent, source-dependent observations and reported learning outcomes. Available skill revisions include a profile-contained path and content hash. Review harness prompts are excluded. |
| Cron | Job/run identity and resolved recipients determine recall access. Explicit private recipient scopes are recorded and checked before delivery. Completed runs become durable reported outcomes. |

## Access configuration

Merge into the selected profile's `personal-memory/config.json`; preserve other existing settings:

```json
{
  "session_access": {
    "owners": {"desktop:your-auth-provider": ["stable-owner-id"]},
    "cron_recipients": [
      {"platform": "telegram", "chat_id": "explicit-private-chat-id", "thread_id": "", "chat_type": "private"}
    ]
  }
}
```

Use actual authenticated identity/provider values, not display names. Unlisted external cron destinations receive no personal recall. Local-delivery jobs may recall within the selected profile. A memory-bearing run whose resolved recipients change has delivery withheld. Legacy-token desktop ownership assumes a directly connected loopback deployment; reverse-proxy and remote desktop topologies require separate authentication qualification.

Run the selected-session native sync described in NATIVE_HISTORY.md before relying on native read guards. Older sync state needs one resync to register its database binding. Only the profile state.db binding is qualified. If status cannot be checked, guarded historical reads fail closed.

## Validation and remaining limits

145 framework tests pass. Seven host bridge scenarios exercise actual SessionDB and AIAgent construction. A deterministic local model protocol fixture completes a memory tool round trip and durable turn capture. The focused upstream Hermes suite passes 388 tests; two failures also reproduce with the original state/search source: SQL trace-count instrumentation and an unavailable /proc process directory. Logs are included. Patch apply, repeat apply, rollback, repeat rollback and local-edit rejection pass.

These are integration tests, not full production certification. Full desktop RPC/UI, real outbound cron delivery, complete child-agent execution, real-model recall quality and platform matrices remain unqualified. Recipient scope storage currently uses job identity; overlapping runs of the same job need additional qualification. Native task/todo/notepad mapping is not added by cron outcome capture.

Deletion is logical at covered read boundaries, not physical erasure of SQLite/WAL/backups, external copies or already-loaded prompts. Arbitrary raw file/SQL reads are outside these guards. Retrieval filtering may return fewer results without refilling. Skill observation does not provide a complete current-skill catalog, automatic procedure export, or permission to execute learned actions. Existing independent evaluation/promotion gates remain in force. The 100-parent provenance limit remains; generated capture above it is withheld. Prior coverage audits and benchmark reports describe their stated earlier versions.
