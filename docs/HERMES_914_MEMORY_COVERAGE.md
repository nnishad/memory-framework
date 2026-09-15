# Hermes v2026.9.14 memory-surface coverage audit

Audit date: 2026-09-14. Framework: `hermes-personal-memory` v0.8.0rc8. Hermes pin:
**v2026.9.14 / v0.21.3**, commit `345cd2b057a452236de401d3534b8502a7465e8d`, patch
`patch_sha256=62477753…` as audited, re-based to `d34da13e…` on 2026-09-15
(`HOST_BRIDGES.md`), `memory_bridge.py` sha256 `d8cd3e15…6acb`. The re-base adds one
`from pathlib import Path` to the attestation block, so no conclusion below moves.

This is a **read-only source audit** of the real patched 9.14 tree (exported at
`/home/hermes/hermes-e2e/hermes-src-914`). It supersedes the question "does the 2026.8.31
audit still describe us" — that document (`HERMES_MEMORY_COVERAGE_AUDIT.md`) is retained as
history. Every Hermes reference below is `file:line` from the pinned source; every framework
reference is `file:line` from this repo.

Evidence extractions (regenerable):
`.e2e/hermes_914_memory_surface.txt` (ABC + manager skeletons + call sites),
`.e2e/hermes_914_bridge_calls.txt` (native callers of `memory_bridge`),
`.e2e/hermes_914_provider_calls.txt` (completeness sweep of every provider hook).

Line references and test counts below belong to the dated audit and can drift as source
changes. Hook coverage is not proof of every end-to-end memory behavior or production quality.

## Verdict

**Our provider satisfies the entire v2026.9.14 memory contract.** Hermes reaches a memory
provider through exactly three channels, and we cover all of them with no missing hook:

1. the `MemoryProvider` ABC lifecycle (dispatched by `agent/memory_manager.py`);
2. the optional host boundary resolved **by string** in `agent/memory_bridge.py`
   (`_profile_hook(home, "<name>", …)` and `getattr(provider, "<attr>")`);
3. tool-schema injection (`get_tool_schemas` → `MemoryManager.get_all_tool_schemas`).

Every ABC method we either override with real behaviour or intentionally inherit; every
bridge-resolved hook and attribute exists on `PersonalMemoryProvider`. The single ABC method
we do **not** override (`identity_signature`) returns `{}` correctly for our design (§F).

Because the bridge resolves optional hooks via `getattr`/`hasattr` rather than the ABC, a
missing attribute fails **silently** (feature quietly disabled), not loudly. That is exactly
why this audit enumerates the string surface explicitly and checks each name — see §B.

---

## The 9.14 contract, as it actually reads

`agent/memory_provider.py` (222 lines) declares 4 abstract members and ~24 overridable hooks.
`agent/memory_manager.py` (872 lines) owns fan-out and **filters kwargs by provider
signature** (`_signature_params`, `_has_var_kwargs`, `_accepts_require_checkpoint`,
`_provider_sync_accepts`, `_provider_memory_write_metadata_mode`), so a provider only
receives optional arguments it declares. `agent/memory_bridge.py` (315 lines, shipped in our
host patch) is the host-side boundary that enforces profile scoping, native-history
federation, cron continuity and delivery, and skill verification — all by calling
provider-named hooks.

---

## §A — ABC lifecycle coverage matrix

Status: **Override** = we implement real behaviour; **Inherit** = ABC default is correct for
us (reasoning in §F); file:line = our handler in `personal_memory/provider.py`.

| ABC member (9.14) | Kind | Our impl | Status |
|---|---|---|---|
| `name` | abstract | provider.py:42 → `"personal-memory"` | Override |
| `is_available` | abstract | provider.py:45 (config-file presence, no network) | Override |
| `initialize` | abstract | provider.py:53 (home, scope, clients, outbox, epoch, lineage) | Override |
| `get_tool_schemas` | abstract | provider.py:98 → `SCHEMAS` (20 tools) | Override |
| `unavailable_reason` | overridable | provider.py:50 | Override |
| `system_prompt_block` | overridable | provider.py:95 (GUIDANCE) | Override |
| `prefetch` | overridable | provider.py:213 (bounded fast hybrid, cached) | Override |
| `queue_prefetch` | overridable | provider.py:283 | Override |
| `recall_status` | overridable | provider.py:229 (`RecallStatus`) | Override |
| `sync_turn` | overridable | provider.py:286 | Override (`turn_author` not declared — §G) |
| `handle_tool_call` | overridable | provider.py:176 (guard → route → validate → invalidate) | Override |
| `shutdown` | overridable | provider.py:610 (drain outbox, close) | Override |
| `on_turn_start` | overridable | provider.py:585 (durable user-input capture + flush) | Override |
| `identity_signature` | overridable | — | **Inherit `{}`** (§F — correct) |
| `on_session_end` | overridable | provider.py:592 | Override |
| `on_session_switch` | overridable | provider.py:603 (rebind id, inherit lineage, invalidate) | Override |
| `on_pre_compress` | overridable | provider.py:303 (`require_checkpoint` accepted) | Override |
| `on_delegation` | overridable | provider.py:408 | Override |
| `get_config_schema` | overridable | provider.py:101 | Override |
| `save_config` | overridable | provider.py:107 | Override |
| `on_memory_write` | overridable | provider.py:577 (raises if capture withheld) | Override |
| `create_native_memory_store` | overridable | provider.py:133 → `CanonicalMemoryStore` | Override |
| `observe_tool_result` | overridable | provider.py:372 | Override |
| `curated_memory_status` | overridable | provider.py:140 | Override |
| `dashboard_memory_status` | overridable | provider.py:145 (uninitialized-safe) | Override |
| `dashboard_memory_reset` | overridable | provider.py:150 (uninitialized-safe) | Override |
| `backup_paths` | overridable | provider.py:125 (uninitialized, no network) | Override |

Manager call sites these hooks serve (from `.e2e/hermes_914_memory_surface.txt`
L309-416): `add_provider`/`initialize_all`/`create_native_memory_store`
(agent_init.py:1274/1282/1298), `build_system_prompt` (system_prompt.py:480),
`on_turn_start`/`prefetch_all`/`describe_recall` (turn_context.py:774/782/787),
`handle_tool_call` (inline_tool_executors.py:232, tool_executor.py:1528),
`observe_tool_result` (inline_tool_executors.py:64), `sync_all` (run_agent.py:902),
`on_session_end` (run_agent.py:863/873, cli_session_mixin.py:481),
`on_session_switch` (conversation_compression.py:1433/3098, cli_session_mixin.py:583/815,
cli_commands_mixin.py:350, tui_gateway/methods_tools.py:771),
`commit_session_boundary_async` (cli_session_mixin.py:579),
`on_pre_compress` (conversation_compression.py:2633/2644),
`on_delegation` (delegate_tool_results.py:338), `flush_pending` (cli.py:764,
run_shutdown.py:1169), `identity_signature` (run_agent_cache.py:87).

**Signature-compatibility proofs (manager filters by our signature):**
- `on_pre_compress`: we set `pre_compress_checkpoint_api_version = 2` (provider.py:20), so the
  manager classifies us as a checkpoint provider and feeds `evidence_messages` when present
  (memory_manager.py:690-697); `_accepts_require_checkpoint` sees our declared
  `require_checkpoint` kwarg (provider.py:303) and passes it. `conversation_compression.py:2644`
  calls the manager with `evidence_messages=`; that never reaches our signature unfiltered.
- `sync_turn`: `_provider_sync_accepts` (memory_manager.py:475) sends `messages` (we declare
  it) and **omits `turn_author`** (we don't declare it) — see §G, benign by design.
- `on_memory_write` / `observe_tool_result`: metadata is passed by keyword because our
  signatures declare `metadata` (memory_manager.py:712-731).
- **Session-boundary ordering:** `commit_session_boundary_async` (memory_manager.py:614-647)
  submits `on_session_end(snapshot)` and `on_session_switch(new_session_id, reset=True,
  reason=…)` as **one serialized task** on the manager's single background worker, so the
  LLM-bound extraction always runs *strictly before* provider rebinding (the fix for #16454
  transcript mis-attribution). Our handlers honour this: `on_session_end` (provider.py:592)
  captures the old transcript, then `on_session_switch` (provider.py:603) rebinds the id and
  inherits lineage from `parent_session_id`; the extra `reason`/`rewound` kwargs are absorbed
  by our `**kwargs`. ✓

---

## §B — Bridge duck-typed hook coverage matrix

These are **not** in the ABC. `memory_bridge.py` resolves them by name at runtime; if a name
were absent the feature would be silently dead, so each is verified present on
`PersonalMemoryProvider`. Callers are native Hermes files.

| Hook / attribute (resolved string) | Bridge fn → native caller | Our impl | Status |
|---|---|---|---|
| `on_host_event(event,payload)` | `emit()` (memory_bridge.py:52) ← `review_completed` (background_review.py:1225), `emit_memory_event` (cron/scheduler.py:2317) | provider.py:538 | Override |
| `delegation_context(goal)` | `delegation_packet` (memory_bridge.py:65) ← delegate_tool.py:207 | provider.py:527 | Override |
| `access_allowed` (attr) | `agent_memory_scope` (memory_bridge.py:173-174); `register_cron_artifact` (:220) | provider.py:37/62 (attr, default-deny) | Present |
| `check_session_epoch()` | `agent_memory_scope` (memory_bridge.py:177) | provider.py:427 | Override |
| `home` (attr) | `native_scope` (memory_bridge.py:179), continuity (:245) | provider.py:54 (set in initialize) | Present |
| `host_context` (attr) | `_notepad_provider` (memory_bridge.py:234) | provider.py:63 | Present |
| `requires_native_scope(home)` | `authorize_history` (:195), `_notepad_provider` (:229) | provider.py:451 (→ `True`) | Override |
| `authorize_native_history(home,ctx)` | `authorize_history` (:200/202) | provider.py:454 → host_bridge.authorize_native | Override |
| `filter_native_history(home,db,rows)` | `filter_history` (:123) ← hermes_state_messages.py:819/848/952 | provider.py:413 → host_bridge.filter_native | Override |
| `native_read_evidence(home,db,rows)` | `filter_history` trace (:127) | provider.py:419 → host_bridge.native_evidence | Override |
| `filter_native_continuity(...)` | `filter_cron_continuity` (:212), `filter_notepad_notes` (:245) ← notepad.py:166, scheduler_prompt.py:113 | provider.py:458 → host_bridge.continuity | Override |
| `register_native_artifact(job,content,kind)` | `register_cron_artifact` (:219), `commit_notepad_change` (:259) ← scheduler.py:2679, notepad.py:107/117 | provider.py:492 | Override |
| `check_native_delivery(...)` | `check_cron_delivery` (:144) ← scheduler_delivery.py:1688 | provider.py:423 → host_bridge.check_delivery | Override |
| `filter_native_session_metadata(home,data)` | `filter_session_metadata` (:314) ← hermes_state.py:466, hermes_state_titles.py:135 | provider.py:472 | Override |
| `reset_native_memory(home,scope)` | `reset_provider_memory` (:264) | provider.py:431 | Override |
| `verify_exported_skill(home,path,content)` | `verify_memory_skill` (:295) ← skills_tool.py:541, skills_tool_plugin.py:92 | provider.py:445 | Override |
| `retire_native_notepad(home,job_id)` | `retire_notepad` (:302) ← notepad.py:147 | provider.py:479 | Override |
| `authorize_tool_delivery(...)` | `authorize_memory_delivery` (:307) | provider.py:462 | Override |

All 18 string-resolved hooks/attributes are present. `requires_native_scope=True` means
native history access is **always** gated behind an authenticated in-profile scope
(memory_bridge.py:195-204, host_bridge.authorize_native) — a deliberate secure default, not an
open door.

---

## §C — Host-event vocabulary

The only events that reach `on_host_event` from non-test 9.14 source are
`review_change` (`memory_bridge.py:96`) and `cron_completed` (`cron/scheduler.py:2318`). Our
handler (provider.py:539) accepts `{review_change, skill_change, cron_completed,
turn_interrupted}` — a superset — writes a durable receipt to `host_event_receipts`, and
**raises on any unknown event** rather than ignoring it. `skill_change`/`turn_interrupted` are
internal transitions we drive ourselves (e.g. `observe_tool_result` → `skill_change`,
provider.py:404). The unrelated `emit`/`_emit` in `codex_responses_adapter`, `skill_usage`,
and `tui_gateway/*` are different functions and never enter the memory boundary.

## §D — Tool-surface and reserved-name check

We expose 20 tools, all prefixed `personal_memory_*` (tools.py:14-136): search, evidence,
entities, entity, remember, capture, timeline, status, browse, connections, identity,
identity_revoke, recall, outcome, propose, lessons, knowledge, manage, execute, investigate.
`MemoryManager.add_provider` rejects any provider tool that shadows
`toolsets._HERMES_CORE_TOOLS` (memory_manager.py:356-369). That reserved set
(`.e2e/hermes_914_provider_calls.txt` L738) includes `memory` and `session_search`, which
remain **Hermes built-ins we mirror/filter**, not tools we replace:
- writes to the built-in `memory` tool mirror to us via `notify_memory_tool_write` →
  `on_memory_write` **only when the store is not `framework_backed`**
  (inline_tool_executors.py:139). Our `create_native_memory_store` returns a
  `CanonicalMemoryStore` with `framework_backed = True` (host_store.py:18), so native memory
  writes land directly in the service and there is **no double capture**; the background-review
  fork calls `.fork()` on it (background_review.py:938; host_store.py:111).
- `session_search` reads are guarded by `native_recall` (inline_tool_executors.py:115) →
  `filter_history`/`native_read_evidence`, and its result is classified (not recaptured) by
  `observe_tool_result` (provider.py:390).
No name collisions exist. ✓

## §E — `initialize` kwargs contract

`agent_init._memory_provider_init_kwargs` (agent_init.py:1196-1232) always supplies
`hermes_home`, `platform`, and — for scoped contexts — `host_context`, plus
`host_memory_api=HOST_MEMORY_API(2)`, `host_memory_root`, `agent_identity`,
`agent_workspace`, gateway identity params, and `session_title`. Our `initialize`
(provider.py:53-93) consumes exactly these: it requires `hermes_home`, derives scope via
`context_allowed`, records `host_context`, and persists `host-runtime.json` when
`host_memory_api==2`. CLI path sends no `host_context` (it stays `None`), which our scope
logic handles as "defer to `session_allowed`". ✓

> Correction from the 2026-09-15 production deployment: the two attestation kwargs are set inside
> `with suppress(Exception)` and that block raised `NameError: Path`, so neither kwarg ever arrived
> and `host-runtime.json` was never written. "Always supplies" above describes the patch text, not
> the runtime behaviour — reading a diff is not the same as executing it. Fixed by the v2026.9.14
> re-base recorded in `HOST_BRIDGES.md`, and verified live by `doctor` → `pinned_host_patch` PASS.

## §F — Why `identity_signature = {}` is correct for us

`gateway/run_agent_cache.py` folds `_memory_provider_identity_signature()` into the cached
agent key (run_agent_cache.py:65,87). Honcho overrides this because it freezes per-user alias
tables at init. Two facts make `{}` safe for us: (1) the cache signature already includes
`user_id`/`user_id_alt` (run_agent_cache `_agent_config_signature`), and (2) we freeze **no
per-user credential** — a single profile `agent_token` from `settings.json` is used, and the
`memory_epoch` is fetched live from `/v1/epoch` (provider.py:76). There is no provider-held
identity that can drift without a config change, so there is nothing extra to bust. Inheriting
the default is a reasoned choice, not an oversight.

---

## §G — Findings: full contract coverage, with honest boundaries

**No missing hook and no silent-disable gap exists** between our provider and Hermes
v2026.9.14. The items below are deliberate architectural boundaries or a small number of
accurately-scoped caveats, not integration breaks:

1. **Per-turn author identity is not stored (`sync_turn` omits `turn_author`; `on_turn_start`
   ignores the author trio).** By design this is nearly moot: `context_allowed`
   (host_bridge.py:17) denies every shared surface (`group`, `supergroup`, `channel`, `guild`,
   `public`, `room`), so an authorized session is effectively single-writer (private/direct
   owner, `local_owner` tui/desktop, or an explicit private cron recipient). Writer identity
   is bound at `initialize` (provider.py:81). *If* multi-participant private sessions are ever
   allowed, add `turn_author` to `sync_turn` and read the author trio in `on_turn_start`.

2. **Automatic-recall budget is environment-sensitive (two-layer timeout).** Prefetch is
   bounded at *two* independent layers: (a) our provider's own `prefetch_wait_seconds`
   (default 200 ms from `prefetch_wait_ms`, provider.py:70-73) governs how long `prefetch()`
   waits for its cached background search before returning "recall is pending"; (b) the
   manager wraps each *external* provider call in `_external_prefetch_timeout` on a daemon
   thread (memory_manager.py:290-306, `_prefetch_provider` :404-), so a stuck provider is
   skipped on later turns rather than blocking the turn. The observed first-turn gate can
   still miss 200 ms when Hindsight GPU `depth=fast` search exceeds it — Hermes-version-
   independent and already measured in `E2E_REPORT.md §14`; the explicit
   `personal_memory_search` path is unaffected. Left intentionally strict by owner decision.

3. **Subagents, background-review forks, and native todo/MEMORY.md remain host-owned.**
   `delegate_task` children run with `skip_memory=True` and get a parent-prepared
   `delegation_context` evidence packet (provider.py:527); review forks are identity-scoped
   and call `.fork()` on our framework-backed store. The framework deliberately does **not**
   duplicate scheduler execution state or the context compressor's summaries — the host stays
   authoritative, per the framework's authority-boundary design.

4. **Hindsight LLM wiring is a deploy-env concern**, not a contract gap: LLM-backed extraction requires a working provider. Without provider credentials,
   automatic `none` mode supports retention and recall without LLM extraction (see
   `hindsight_runtime.py` auto-provider selection).

## §H — Delta vs the 2026.8.31 audit

The `HERMES_MEMORY_COVERAGE_AUDIT.md` (rc2 / v2026.8.31) listed these as "code gaps"; the
9.14 tree plus current code show them **closed**:

| 2026.8.31 "code gap" | Resolved by (now implemented) |
|---|---|
| `on_turn_start` was a no-op | provider.py:585 durable user-input capture + outbox flush |
| `on_memory_write` not overridden | provider.py:577 mirror + `sync_native_files` |
| native `session_search` recaptured | provider.py:390 classification + `native_recall` trace |
| no native-history federation / synced deletion | host_bridge.py `filter_native`/`native_evidence`/`sync_on_read` lineage + `source/status forgotten` gating |
| no cron continuity / notepad bridge | provider.py:458/479/492 + host_bridge.continuity/check_delivery (immutable run scopes, live-lineage delivery proof) |
| skills not bridged | provider.py:445 `verify_exported_skill`, host_bridge.skill_revision, `skill_change` events, `sync_native_files` |
| delegated child had no memory access | provider.py:527 `delegation_context` evidence packet (parent-side, scope-preserving) |
| dashboard reset only touched built-in files | provider.py:150 `dashboard_memory_reset` → `/v1/curated/reset` (versioned, epoch-guarded) |

The "deliberate boundary" rows of that audit (compressor ownership, SOUL.md/authority files,
parallel-plan autonomy, multimodal provenance depth) remain **intended** boundaries.

## §I — Recommended attention (none blocking)

- Optional: accept `turn_author` / author trio **if** shared private multi-writer sessions are
  ever enabled (G-1). Currently gated off by policy.
- Keep this doc and the 8.31 audit cross-linked so future churn reviews start from the
  string-resolved hook list in §B — the silent-disable surface is the highest-risk area when
  Hermes bumps.
- Re-run the three `.e2e` extractions + the 17-assertion `check_hermes_release.py`,
  `check_host_bridges.py`, `check_native_history.py`, and the current test suite on every Hermes
  pin change; §A/§B enumerate what must still resolve by name.

**Conclusion:** against v2026.9.14 the memory bridge is complete — the audit found no missing member in the three enumerated provider-reach channels.
This result does not establish that every memory use case is enforced end to end; current
changes still require runtime checks against the pinned host.
