# Hermes integration

Current target: Hermes Agent `v2026.9.14` / v0.21.3, commit
`345cd2b057a452236de401d3534b8502a7465e8d`.

The active pin and file hashes are in `host-patch/manifest.json` and the packaged
`personal_memory/host_contract.json`. Older patch files are historical bundles; the patch
manager uses the active manifest. See [the provider-contract audit](HERMES_914_MEMORY_COVERAGE.md).

## Install

Install the framework and managed provider, then apply the release-specific host patch while
Hermes is stopped:

```sh
sh deployment/install.sh
python scripts/manage_hermes_host_patch.py check --hermes-root /path/to/hermes-agent
python scripts/manage_hermes_host_patch.py apply --hermes-root /path/to/hermes-agent
```

Start the generated service launcher and restart Hermes:

```sh
"$HOME/.local/share/hermes-memory/venv/bin/python" \
  "${HERMES_HOME:-$HOME/.hermes}/personal-memory/run_service.py"
```

Exclusive setup configures:

```yaml
memory:
  provider: personal-memory
  store: provider
  memory_enabled: true
  user_profile_enabled: true
```

This keeps Hermes's native `memory` tool available while replacing its file persistence with
the canonical framework store. Do not also edit MEMORY.md/USER.md; they are not consulted in
provider-store mode.

## Runtime contract

- Provider initialization happens before native-store creation.
- `/v1/curated/read` supplies the load-time prompt snapshot and live tool view.
- `/v1/curated/apply` requires the current target version, reset epoch and an idempotency key.
- A prompt snapshot never changes inside an existing agent. New/reset sessions rebuild it.
- `/memory` in CLI and gateway uses the active agent's store; gateway access therefore inherits
  verified platform/user/chat identity.
- Desktop status and targeted reset route through optional provider-owned callbacks.
- Terminal tool observations are best-effort side effects with durable receipts; capture failure
  never changes the underlying tool result.
- Background review forks the store view rather than sharing a mutable entry list.
- Managed memory-backed skills are verified before their contents are returned.

The older native-history, cron/notepad, delivery and reset protections remain described in
[HOST_BRIDGES.md](HOST_BRIDGES.md). The v2026.9.14 patch ports their shared bridge into the
release's decomposed host modules.

## Upgrade and rollback

The patch installer verifies release hashes, rejects mixed/local modifications and is idempotent.
It does not fetch or force-overwrite Hermes. To restore the exact release source:

```sh
python scripts/manage_hermes_host_patch.py rollback --hermes-root /path/to/hermes-agent
```

Rollback preserves canonical data but removes the host integration, so stop the memory service or
return Hermes's memory configuration to file mode before resuming normal use.

See [RC8_INTEGRATION.md](RC8_INTEGRATION.md) for the default Hindsight lifecycle and
[RC7_INTEGRATION.md](RC7_INTEGRATION.md) for the canonical-store concurrency details.
