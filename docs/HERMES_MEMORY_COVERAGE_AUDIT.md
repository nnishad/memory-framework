> Current integration: [Hermes v2026.9.14 guide](HERMES_INTEGRATION.md) and [host bridges](HOST_BRIDGES.md). Earlier findings and test counts below retain their historical scope.

# Hermes memory coverage audit

Historical baseline for 0.8.0rc2. See `GAP_REMEDIATION.md` for the 0.8.0rc3/rc4 changes and remaining work. The source audit below is retained as the original finding, not a claim that every gap remains unchanged.

Audit date: 2026-09-09. Framework: 0.8.0rc2. Hermes: **v2026.8.31 / v0.21.0**, commit **29112bef099274229cadff79cdff7bf7b99c4b77**. This audit does not claim compatibility with later commits on main.

**Verdict: our framework integrates the principal memory-provider lifecycle, but it does not have full interaction with every Hermes memory surface. End-to-end forgetting, desktop/TUI access, native history, skills, delegated execution and scheduled-job state have material gaps or deliberate boundaries.**

The previous 124 passing tests and 17 bridge checks establish the paths they exercise. They do not establish complete product-wide memory coverage. “No core patch required” remains true for the ordinary provider integration and parallel-investigation tool; it does not mean every Hermes subsystem uses the framework.

## How Hermes actually divides memory

The [pinned release](https://github.com/NousResearch/hermes-agent/releases/tag/v2026.8.31) includes persistent provider memory, native conversation history, built-in MEMORY.md/USER.md stores, a skill library, compressed working context, scheduled-job continuity and per-job notepads. These are separate storage and execution paths.

The provider contract supplies lifecycle hooks and custom tools. It is not a universal storage interception layer. In particular, the [provider ABC](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/agent/memory_provider.py) explicitly says that subagents do not have their own provider session; the parent receives completed delegation output.

## Coverage matrix

“Verified” means existing executed bridge/runtime checks or the new boundary probes. “Code” means traced in the pinned source, without a complete host-surface run.

| Hermes memory use | Current framework interaction | Assessment |
|---|---|---|
| Provider discovery, initialization, tool injection | Managed plugin loads; 20 tools advertised through MemoryManager | Verified |
| Stable memory instructions | Provider guidance enters the system prompt | Verified |
| Automatic recall before a turn | `prefetch` uses a bounded fast hybrid search; cached results are invalidated by generation | Verified; automatic recall does not itself generate parallel plans |
| Explicit personal search / investigation | Hybrid, progressive and parallel-plan tools dispatch to the service | Verified; real model plan selection remains unqualified |
| Completed user/assistant turns | `sync_turn` durably enqueues text exchanges | Verified; echoed recalled facts lack source dependency links |
| Tool observations | Named tool results captured; framework tools and built-in `memory` excluded | Partial; native `session_search` is not excluded and was reproduced as recaptured |
| Start-of-turn observation | Hermes calls `on_turn_start`; our provider inherits its no-op | Code gap; no durable capture at the beginning of a turn |
| Interrupted or failed turns | Hermes skips normal completed-turn sync for interrupted turns | Code gap; complete interrupted user/assistant capture is not guaranteed |
| Pre-compression checkpoint | Provider stores normalized user/assistant rows to durable outbox | Verified durability; assistant echoes still lack origin lineage |
| Compressed working context | Hermes context compressor/engine owns summaries and reinjection | Deliberate host boundary; not replaced by our memory backend |
| Session switch and shutdown | Hooks update session IDs, invalidate cache, drain outbox | Verified |
| Session-end transcript | Our callback captures named tool outputs, not a general historical transcript import | Partial |
| Built-in MEMORY.md / USER.md | Exclusive setup disables their prompt/tool use | Replacement by configuration; existing files are not automatically migrated or erased |
| Built-in memory-write notifications | Hermes exposes `on_memory_write`; our provider does not override it | Code gap if built-in stores are enabled alongside our provider |
| Native history / `session_search` | Hermes reads profile `state.db`; compression recovery explicitly points there | Separate store; no indexed native-history connector or synchronized deletion |
| Child/subagent memory | Delegate construction uses `skip_memory=True`; parent stores task + final result | Parent summary verified; no direct child-provider recall or complete child transcript ingestion |
| Background memory/skill review | Hermes intentionally creates review forks with `skip_memory=True` | Separate lifecycle; no framework outcome/evaluation bridge from that review |
| Skills / procedural files | Hermes owns SKILL.md files, skill manager, skill usage/provenance | Not synchronized with framework evaluated lessons and procedures |
| Native `todo` working state | Hermes holds a revisioned per-session plan and reinjects it after compression | Separate from framework durable tasks; no task-ID/state mapping |
| Cron agent persistent recall | Cron constructs an agent with `skip_memory=False`, `platform='cron'`; our policy allows that profile | Code-compatible; no full scheduled-job run performed in this audit |
| Cron continuity and durable notepad | Scheduler injects prior output and per-job `cron/notepad.db` separately | No typed synchronization, cursor mapping or deletion propagation |
| CLI and authorized private gateway use | CLI allowed; configured private owner identities allowed; shared rooms denied | Verified policy/bridge; not every real messaging transport tested |
| TUI / desktop | Pinned host uses `tui` / `desktop`; our default identity-less policy denies both | Reproduced policy gap for normal local surfaces |
| API / ACP / other host surfaces | Availability depends on actual host platform and trusted identity metadata | Unqualified; cannot assume CLI policy applies |
| SOUL.md / project instructions | Hermes loads identity and project context through its prompt builder | Deliberate authority boundary; should not become ordinary inferred memories |
| Dashboard “reset memory” | Native endpoint deletes MEMORY.md and/or USER.md | Does not clear framework DB, native transcripts or derived copies |
| Framework/native backup | Provider paths and SQLite snapshot support already tested | Backup inclusion covered; full native import + all-store deletion reconciliation not qualified |
| Multimodal content | Normal sync flattens content to text; capture is not an attachment/blob ingestion system | Partial; original media and complete modality provenance are not guaranteed |

## Confirmed high-priority findings

### 1. Forgetting does not cover independent remembered copies

**Reproduced with a real local HTTP service and durable outbox.** A fictional original source was ingested; its content was repeated in an assistant turn; the original was forgotten. The text remained searchable under `source=hermes`. A separate probe confirms that assistant text is also stored in checkpoints without parent-source dependencies.

This does not mean deletion fails for the original record: that record is removed correctly, and tracked framework dependents are handled. It means there is no complete lineage from retrieval to generated answer to recaptured conversation/checkpoint. A claim that a fact was forgotten everywhere would be false.

Required work: carry retrieved record IDs into turn/checkpoint provenance, retain those dependencies through derived artifacts, and distinguish “delete this source” from “forget this information across copies.” Native transcript and backup retention must be part of that contract. Do not silently delete an independently supplied user statement merely because it contains similar text.

Framework evidence: `personal_memory/provider.py::sync_turn`, `on_pre_compress`; executed observations in `MEMORY_BOUNDARY_PROBES.json`.

### 2. Native session search bypasses the intended recalled-evidence capture exclusion

**Reproduced.** `_capture_tool_results` excludes `personal_memory_*` and `memory`, but not `session_search`. A fictional native-history search result became a new `hermes-tools` source. Its native session/message identity is not mapped into a deletion dependency.

Required work: classify retrieval tools by capability, not only a narrow name prefix; ingest native transcript evidence through stable session/message identities with source lineage. Native [session search](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/tools/session_search_tool.py) and framework search must have an explicit federation and forgetting policy.

### 3. Standard local TUI/desktop access is not integrated

**Policy probe and host code agree.** The host resolves desktop chat to `desktop` and standalone TUI to `tui`. Our policy trusts only identity-less `cli` and `cron`; other surfaces require an explicit private/direct chat and a configured stable owner ID. Therefore a normal local desktop/TUI context without those fields is denied.

Required work: carry authenticated local-owner/session provenance into provider initialization. Do not “fix” this by accepting arbitrary `platform` strings or every GUI/API request. The host surface and authorization boundary both matter.

Host evidence: `tui_gateway/server.py::_resolve_session_platform`, `_resolve_agent_platform`; framework evidence: `personal_memory/access.py::session_allowed`.

### 4. Native self-learning and framework learning are separate

The [background review fork](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/agent/background_review.py) deliberately excludes external memory providers to keep synthetic review prompts out of real user memory. Native skill creation/usage does not automatically produce our outcome → proposal → evaluation → promotion records. Conversely, an evaluated framework procedure does not automatically create or update a Hermes skill.

Required work: a narrow, explicit skill/review event bridge that records real outcomes and revisions without ingesting the review harness as user conversation. Preserve the host's review isolation. Merely setting `skip_memory=False` would undo an intentional protection.

### 5. Delegated agents cannot directly use our memory tools

The [delegate tool](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/tools/delegate_tool.py) starts children with `skip_memory=True`. The parent callback gives us the delegated task and final result, not a child-scoped retrieval session or every intermediate observation.

Required work: parent-prepared evidence packets or a constrained, read-only, task-scoped delegation bridge; preserve original user authorization and provenance. Global owner-memory access should not be inferred from being a child process.

### 6. Job/task continuity needs its own bridge

The [scheduler](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/cron/scheduler.py) can use provider recall, but its continuity output and [notepad](https://github.com/NousResearch/hermes-agent/blob/29112bef099274229cadff79cdff7bf7b99c4b77/cron/notepad.py) are distinct. Native todo state and framework durable tasks likewise have different IDs and lifecycle semantics. Our `cron` access rule trusts the profile and does not itself inspect the delivery recipient.

Required work: stable job/run/task IDs, typed observations of state transitions, and a delivery-aware evidence policy for scheduled messages. Keep scheduler execution state authoritative in Hermes rather than duplicating it as competing runnable tasks.

## Additional gaps and deliberate boundaries

- **Interrupted turns:** `run_agent.py` skips normal provider sync when interrupted. Implement explicit partial-turn events with completion state if full archival capture is required.
- **Legacy file migration:** exclusive setup leaves old MEMORY.md/USER.md files on disk. Migration needs provenance and deduplication; disabling the stores is not migration.
- **Native reset UI:** `/api/memory/reset` in `hermes_cli/web_server.py` only removes built-in text files. A provider-aware UI must state exactly which stores it resets and must not claim global deletion.
- **Compression prompts:** the default compressor contains wording treating MEMORY.md/USER.md as authoritative. That wording is not tailored to an exclusive external provider. Provider-aware context descriptions would reduce misleading assumptions.
- **Authority files and operational state:** SOUL.md, project instructions, scheduler execution state, native session state and skills need not all be physically moved into one database. Their useful facts can be exposed through typed, source-backed interfaces while their owners retain authority.
- **Parallel plans:** tool execution is integrated; every natural-language request is not automatically decomposed. Automatic prefetch remains one bounded fast lookup, and the model chooses explicit investigation based on guidance.
- **Health/media:** the memory contract can accept more connectors, but our provider alone does not extract every original attachment, watch metric or relationship from a conversation.

## Evidence and reproducibility

1. Source inspection covers the pinned release's provider lifecycle, agent initialization, turn context, sync/end behavior, compressor, delegation, review forks, session search/state, skills, cron, todo, dashboard reset and local-surface routing.
2. The pre-existing 17 bridge checks and actual AIAgent protocol-fixture report remain valid for their tested scope. They are not real-LLM quality tests.
3. `scripts/audit_memory_boundaries.py` adds four probe groups against isolated fictional data. Run from the framework root:

```sh
HERMES_PROVIDER_CONTRACT=/path/to/hermes/agent/memory_provider.py \
  python scripts/audit_memory_boundaries.py
```

The probe report intentionally records **gaps reproduced**, rather than mislabeling them passing safety checks. It imports the existing HTTP test harness and real pinned provider ABC; it is not a full desktop, cron or subagent end-to-end run.

`HERMES_MEMORY_SOURCE_MAP.json` records source symbols, hashes and immutable GitHub references used in this audit. No behavior was changed during this audit. Full-memory-interaction acceptance remains **not achieved**.

## Implementation priority

1. Retrieval-to-answer/checkpoint lineage and native-history federation, including an explicit all-store forgetting contract.
2. Authenticated owner binding for TUI/desktop and qualification of gateway/API surfaces.
3. Scoped child-agent evidence access and typed native skill/review events.
4. Cron continuity/notepad and native task-state bridges.
5. Interrupted/multimodal capture, legacy migration and provider-aware dashboard reset/compression wording.
6. End-to-end tests across these paths with a real model, including indirect recall, conflicting identities, query drift and deletion after recapture.
