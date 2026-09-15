> Current integration: [Hermes v2026.9.14 guide](HERMES_INTEGRATION.md) and [host bridges](HOST_BRIDGES.md). Earlier findings and test counts below retain their historical scope.

# Native Hermes history integration

Framework: 0.8.0rc8. Current pinned host: Hermes v2026.9.14. The administrative connector
described here was introduced in rc4. It does not replace native `session_search` or run a
background watcher. The current host bridge also synchronizes authorized native history on
read; see [HOST_BRIDGES.md](HOST_BRIDGES.md).

## Operation

Install/update the framework and restart the service and Hermes using the existing integration guide. Run synchronization with explicitly selected native session IDs:

```sh
personal-memory sync-hermes-history /path/to/profile/state.db \
  --hermes-home /path/to/profile \
  --archive-id stable-profile-archive \
  --session-id selected-session-1 \
  --session-id selected-session-2
```

Keep `archive-id` stable for the same database and its restores. Use a different archive identity for an unrelated database. It is an operator-supplied namespace, not an authentication credential. Session selection authorizes which conversation records are imported; the connector does not infer the owner of group messages or read every profile automatically.

The connector implements `IngestionConnector`, supplies every required 1.0 envelope field, and preserves native metadata in the versioned `hermes.history` extension. Imported records use `source=hermes-history`. Normal personal-memory search, investigation and evidence tools can retrieve them and retain canonical record IDs.

The native database is opened read-only. SQLite online backup provides a coherent temporary snapshot; the network ingestion phase does not hold a read transaction on the live writer. Snapshot construction has a 30-second deadline. Memory use is bounded by paginated reads and ingestion batches of at most 100 records and approximately 1.5 MB. An individual record must satisfy the existing 100,000-character/1-MiB ingestion contract; oversized or malformed messages stop the scan explicitly. Original attachment blobs are not downloaded or decoded.

A complete selected-session scan is necessary to detect edits, native deletion and rewind. An append cursor alone cannot do this. Unchanged records are checked in batches and do not generate ingestion requests. Partial scans never run the deletion sweep.

## Provenance and generated messages

User rows are imported as observations of what was written. Compacted visible history is included; compressed summaries, rewound inactive rows, empty content and unsupported roles are excluded. Reasoning/private scratchpad columns and API prompt sidecars are not imported.

Assistant and tool rows require a trusted lineage file mapping native numeric message IDs to existing canonical evidence IDs:

```json
{
  "42": ["rec_existing_canonical_source_id"]
}
```

Pass this file with `--lineage-file /path/to/lineage.json`. The example ID is a placeholder; the service checks that actual parents exist. Unknown parents stop reconciliation. Forgotten parents suppress the derived import. Omitting the file withholds new generated imports; it does not erase previously imported native messages merely because their lineage was omitted on this run.

This file is supplied by trusted host/operator code. It is not an instruction for a model to invent provenance. Automatic export of native per-message lineage from all Hermes execution paths remains outstanding.

Exact copies sharing archive, session, role, timestamp and content are linked to the first imported observation, matching the host's display-history equivalence. Each copy retains its own native row identity and becomes a tracked derivative. This handles tested compaction copies without merging contacts or guessing cross-session identity. Near matches, rewritten summaries, cross-session copies and independently imported archives are not automatically equated.

## Deletion and revisions

A native edit creates a new canonical revision. The coordinator durably records the old revision's retirement before removing it, and retries unfinished retirements on the next run. `doctor` reports outstanding retirements. Coordinator maps, exact-copy fingerprints and retirement intents live in the profile's existing `personal-memory/outbox.db`, included by the framework backup path. They contain IDs/hashes and synchronization metadata, not another copy of message text.

If a native item disappears or is rewound out of visible history, a completed sync forgets its framework source item and tracked dependents. Exact surviving compaction copies preserve their canonical source lineage instead of being treated as unrelated messages. Reintroducing the deleted native ID or another exact tracked copy does not silently revive it.

If an imported record is explicitly forgotten through the framework, the next sync suppresses reimport and promotes that item's retention boundary to all revisions. Changed text under the same native row ID cannot evade the deletion. This is deliberately conservative: native undo/restore does not override a completed retention deletion.

For explicit source-item deletion across current and future revisions:

```sh
personal-memory forget rec_actual_record_id \
  --hermes-home /path/to/profile --all-revisions
```

The administrator-only `POST /v1/forget-source` accepts `source` and `source_id` directly, including for an already deleted or not-yet-imported item. It does **not** delete the whole source collection. `POST /v1/source/status` provides batched source-item retention status.

Source-family tombstones are content-free hashed identifiers in the independent deletion ledger. Restore replays them against historical revisions in the restored database, and ingestion rejects future revisions of that item. Do not downgrade to a framework version that predates these tombstones: older versions do not enforce the new retention semantics.

## Boundaries still open

- Synchronization is explicit, not automatically scheduled or attached to every native write.
- Native state.db, session files, caches, previously delivered answers and backups are not physically erased by this connector. Native `session_search` and resume can still expose native history outside the framework's deletion boundary.
- Existing provider captures and historical imports do not automatically share native message IDs; independent or previously unattributed copies are not globally forgotten.
- Generated history without source evidence is withheld, not relabeled as verified source data.
- All-store forgetting, authenticated desktop/TUI bindings, cron/delegate/review/skill bridges, complete media ingestion and real-model quality qualification remain separate work.

Tests use fictional data only. `HERMES_NATIVE_HISTORY_RUNTIME.json` records an executed run using the actual pinned `SessionDB`, including native session creation, message append and native session deletion. It does not claim an end-to-end desktop or live-model run.
