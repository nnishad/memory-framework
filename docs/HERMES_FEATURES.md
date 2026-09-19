# Personal Memory — Feature Reference for the Hermes Agent

> Audience: anyone integrating, operating, or prompting the Hermes agent against this
> memory framework. Every feature below is grounded in the code under
> `personal_memory/`. Versions cited are read from `host_contract.json` and
> `provider.py`/`tools.py`.
>
> Framework version: **0.8.0rc8** · Pinned Hermes release: **v2026.9.14**
> (`commit 345cd2b057a452236de401d3534b8502a7465e8d`) · `host_memory_api: 2` ·
> `native_store_contract: 1` · managed Hindsight backend **0.9.2** · ingestion
> **contract 1.0**.

---

## 1. What this framework is

An **evidence-backed personal memory** service that the Hermes agent uses to remember,
retrieve, structure, learn from, and reason about a single owner's history across
conversations, email, files, and native chat history. Its defining principle is that
**all knowledge is traceable to canonical source evidence**, and nothing — a recall,
a belief, a lesson, a notification — outranks the evidence it was derived from.

Design guarantees that shape every feature:

- **Local SQLite is the single source of truth.** The external Hindsight engine and the
  vector index only *rank candidates*; returned text always comes from the local store
  (`hindsight.py` note: *"External facts are candidates; returned text is local evidence."*).
- **Memory content is untrusted data, never instructions.** Repeated across tool
  descriptions, GUIDANCE, and the awareness prompt.
- **Ranked results are leads, not proof.** Two distinct outcomes — `no_relevant_evidence`
  (search did not establish it) and `retrieval_incomplete` (retrieval failed) — both mean
  *unknown*, never *absent* (`tools.py` GUIDANCE, `retrieval.py`).
- **Forgetting is real and cascading.** Deleting evidence retires every derived artifact
  (`lifecycle.py`).
- **Evidence-first writes.** Claims, beliefs, measurements, and curated memory all require
  live, visible canonical evidence and an exact quote for reported/observed facts.

---

## 2. How Hermes connects to the framework

The framework exposes itself to Hermes as a **memory provider** plus a set of **agent
tools**, talking to a standalone service over loopback HTTP. The provider module
(`provider.py`) is the *only* file Hermes imports directly; the service and CLI never
depend on Hermes.

There are **three access channels** from Hermes into the provider:

1. **ABC lifecycle hooks** — `PersonalMemoryProvider(MemoryProvider)` subclass methods
   dispatched by Hermes's `agent/memory_manager.py`.
2. **Duck-typed host boundary** — optional hooks resolved by Hermes's shipped
   `agent/memory_bridge.py` via `getattr(provider, name, None)`; a missing name fails
   safe (silently skipped).
3. **Tool-schema injection** — `get_tool_schemas()` hands Hermes the 20 agent tools.

Registration: Hermes loads the plugin and calls `register(ctx)`, which runs
`ctx.register_memory_provider(PersonalMemoryProvider())`.

### 2.1 Required ABC lifecycle hooks (`provider.py`)

| Hook | What it gives Hermes |
|---|---|
| `name` | Stable provider id `"personal-memory"`. |
| `is_available` | Enabled only when `$HERMES_HOME/personal-memory/settings.json` exists. |
| `initialize(session_id, **kwargs)` | Loads settings; computes whether memory access is `allowed` for this context/session; writes `host-runtime.json` attestation when `host_memory_api==2` and a `host_memory_root` is supplied; builds the service `Client`, the durable `Outbox`, and the `ExposureLedger`; sets `prefetch_wait_ms` (default 200, range 0..2000); optionally arms the awareness consumer `hermes-foreground` (opt-in). |
| `system_prompt_block` | Returns `GUIDANCE` — the standing instructions on how to use memory. |
| `get_tool_schemas` | The 20 tools (§3), deep-copied. |
| `handle_tool_call` | Routes a tool call to `/v1/*` via `ROUTES`; **blocks writes in non-primary contexts**; reuses a cached prefetch when possible. |
| `prefetch` / `queue_prefetch` | Bounded async recall (`fast` depth, limit 4) on a dedicated `personal-memory-recall` thread so context is warm before the turn; 10 s generation-checked cache. |
| `sync_turn` | Reconciles the turn's transcript into durable captures. |
| `on_pre_compress` | Checkpoints memory before Hermes compresses context (`pre_compress_checkpoint_api_version = 2`, so Hermes passes `evidence_messages` + `require_checkpoint`). |
| `observe_tool_result` | Records tool results into the exposure ledger. |
| `on_delegation` / `delegation_context` | Passes a memory packet to a delegated sub-agent. |
| `on_turn_start` / `on_session_end` / `on_session_switch` | Lifecycle capture points (fills gaps without double-capturing). |
| `shutdown` | Drains workers/outbox cleanly. |
| `recall_status` | Reports retrieval/queue state back to the host. |
| `on_memory_write` | Reacts when the host's native memory surfaces write. |
| `create_native_memory_store` | Returns a `CanonicalMemoryStore` (§6) for Hermes's native MEMORY/USER surfaces. |

### 2.2 Optional host-boundary hooks (channel 2, `provider.py` + `host_bridge.py`)

Resolved by name via `getattr`, so Hermes only calls what the provider implements. All
are present in this build:

`filter_native_history`, `native_read_evidence`, `check_native_delivery`,
`check_session_epoch`, `reset_native_memory`, `verify_exported_skill`,
`requires_native_scope` (→ `True`), `authorize_native_history`,
`filter_native_continuity`, `authorize_tool_delivery`,
`filter_native_session_metadata`, `retire_native_notepad`, `register_native_artifact`,
`sync_native_files`, and the event sink `on_host_event`.

`on_host_event` accepts the event vocabulary `review_change`, `skill_change`,
`cron_completed`, `turn_interrupted`, `request_assembled` and raises on unknown events.
In practice Hermes reaches it via background review (`review_change`) and the cron
scheduler (`cron_completed`).

### 2.3 The transport path

```
Hermes ─(hooks / tool call)─▶ PersonalMemoryProvider
        │                                   │
        │                       Client.call(path, args)   (client.py, HTTPS or loopback HTTP only)
        │                                   │  trace id via X-Personal-Memory-Trace
        ▼                                   ▼
   outbox.db (durable queue)        ASGI Application (asgi.py) ──▶ MemoryService.dispatch (service.py)
   async writes, retries                    │  auth + role scopes + epoch fence
                                            ▼
                                    /v1/* handlers → Store · Hybrid retrieval ·
                                    Intelligence · Learning · Workflows · Awareness ·
                                    Curated · Hindsight · SourceSync
```

`asgi.py` runs a single Uvicorn worker on loopback, a `ProcessLease` guaranteeing one
owner per data directory, 16 concurrent request slots, a 2 MiB body cap, strict JSON
(no duplicate keys, no non-finite numbers), and rejects browser `Origin`. Errors map
contract failures to HTTP 422 with the caller's own validation detail so a small model
can correct its call, and curated version conflicts to 409.

---

## 3. The 20 agent-facing tools (`tools.py`)

Hermes sees exactly these tools (all prefixed `personal_memory_*`, so no collision with
Hermes core tools). Each maps to a `/v1/*` route through `ROUTES`.

### Retrieval & reading
1. **`search`** — hybrid retrieval. `depth` ∈ fast/balanced/deep; multi-`queries` (≤4)
   for paraphrases/subquestions; entity/source/time filters; `include_history`;
   `expand_entities`. → `/v1/search`.
2. **`recall`** — progressive evidence retrieval, up to 3 rounds with subqueries and a
   `text_budget` (1 k–48 k). → `/v1/recall`.
3. **`investigate`** — request-specific parallel search plan: 1–6 branches, each with its
   own intent/queries/filters, executed server-side **without another planning LLM call**.
   → `/v1/investigate`.
4. **`evidence`** — read one original record by ID (content flagged untrusted). → `/v1/evidence`.
5. **`browse`** — paginate stored history by stable record id with unchanged filters;
   exhaustive scoped reads. → `/v1/browse`.
6. **`timeline`** — bounded entity history including superseded claims. → `/v1/timeline`.
7. **`status`** — availability, retrieval capabilities, source coverage, queued captures. → `/v1/status`.

### Writing memory
8. **`capture`** — ingest a complete **contract 1.0** record (stable source identity,
   revision, provenance, observation time; extra fields go in namespaced versioned
   extensions). → `/v1/ingest`.
9. **`remember`** — save an evidence-linked claim (`category` semantic/procedural/
   prospective/observation; `evidence_kind` reported/observed/inferred needs an exact
   quote); `supersedes` corrects a prior claim. → `/v1/claim`.

### Entities & identity
10. **`entities`** — find provisional/known entities by literal label (equal names ≠ identity). → `/v1/entities`.
11. **`entity`** — create/update a provisional entity and link evidence. → `/v1/entity`.
12. **`connections`** — inspect account↔person links incl. candidates and revoked. → `/v1/connections`.
13. **`identity`** — link an observed account to a person with evidence (`candidate`/
    `confirmed`, time-valid). → `/v1/identity`.
14. **`identity_revoke`** — revoke a wrong link without deleting account/person. → `/v1/identity-revoke`.

### Learning
15. **`outcome`** — record an externally-evidenced task outcome (success/failure/partial/
    unknown/cancelled) with evidence and used-memory ids. → `/v1/learning/outcome`.
16. **`propose`** — propose a scoped lesson from outcome ids (inactive until evaluated and
    promoted). → `/v1/learning/propose`.
17. **`lessons`** — browse learning artifacts (candidates/outcomes/evaluations). → `/v1/learning/browse`.

### Structured knowledge hubs (read + write + execute)
18. **`knowledge`** — a single **read** hub whose `operation` selects the endpoint:
    `attachments`, `native_state`, `beliefs`, `graph`, `aggregate`, `tasks`, `workflow`,
    `quality`, `coverage`, `procedures`, `summaries`, `schema`, `sources`, `changes`,
    `awareness_status`. → `/v1/intelligence/read`.
19. **`manage`** — a single **write** hub: `snapshot`, `belief`, `relation`,
    `measurement`, `task`, `transition`, `feedback`, `consolidate` (each requiring
    `evidence` = array of `{record_id, quote}`). → `/v1/intelligence/write`.
20. **`execute`** — run an administrator-bound procedure through an operator-installed
    capability; returns a durable job id. → `/v1/procedure/execute`.

**Schema safety note** (`tools.py::parser_safe`): model-facing schemas strip
`pattern`/`format` and nested `maxLength ≥ 2000` so llama.cpp / vLLM grammar compilers
accept them; the full contract is still enforced server-side on every call.


---

## 4. Feature areas in detail

### 4.1 Hybrid retrieval (`retrieval.py`, `semantic.py`, `relevance.py`, `adaptive.py`, `investigate.py`)

The latency core. A `search` fuses several candidate channels, then re-ranks and gates:

```
query ─▶ keyword (SQLite FTS5, OR-expanded terms)
      ├─▶ semantic (FastEmbed/ONNX vectors, HNSW accelerator)      [default ON]
      ├─▶ Hindsight recall (external ranking engine, depth budget)  [if configured]
      ▼
   RRF fusion  (score = weight / (60 + rank))
      ▼
   bounded graph expansion (HippoRAG-style personalized PageRank)   [default ON]
      ▼
   cross-encoder rerank (top rerank_window=24 → [1,2])              [default ON]
      ▼
   temporal recency bonus  (1 + weight·0.5^(age/half_life))         [default ON]
      ▼
   relevance gate (lexical anchor OR semantic ≥ floor, default 0.5)
      ▼
   evidence rehydration → return LOCAL canonical text
```

- **Depth** selects pool size: fast/balanced/deep = **32 / 96 / 256** candidates.
- **Semantic index** (`semantic.py`): durable per-model work queues, default model
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`; supports an
  OpenAI-compatible `EndpointEmbedder`; an HNSW accelerator (cosine, M=24,
  ef_construction=160); tokenizer-span chunking; `enqueue(db, kind, record_ids)`
  coalescing so a retire/update never loses index work.
- **Lazy, fail-quiet engines**: `_ensure_semantic` / `_ensure_reranker` build on first use
  and, on permanent failure, silently fall back to non-semantic retrieval rather than
  erroring the turn. A warmup thread (`memory-warmup`, owned by the backend, started by
  `asgi.py`) primes models off the request path so the first post-restart turn is fast.
- **Kill switches**: `PERSONAL_MEMORY_DISABLE_SEMANTIC`, `PERSONAL_MEMORY_DISABLE_RERANK`
  (env) turn levers off independently.
- **Relevance gate** (`relevance.py`): accepts on any content-anchor overlap OR semantic
  similarity ≥ `semantic_minimum`; always returns `answer_verified: false`.
- **`recall`** and **`investigate`** wrap the same engine: recall drives up to 3
  progressive rounds; investigate runs 1–6 independent branches in parallel server-side
  and re-applies `live_and_visible` at final hydration so evidence retired mid-flight
  never reaches the answer.

A search result carries `episodes`, `claims`, `coverage`, `retrieval_status`,
`evidence_sufficiency`, `generation`, `diagnostics`, `connections`, and an honest
`warning`.

### 4.2 Canonical store & ingestion contract (`store.py`, `ingestion.py`, `catalog.py`)

- **Store**: SQLite (WAL, FTS5), `schema_version` 8, ingestion **contract "1.0"**.
- **Stable identity**: `record_id = "rec_" + digest(source, source_id, revision)[:32]`.
- **Contract 1.0** (`ingestion.py`): a closed set of core fields plus **namespaced,
  versioned extensions** (regex-validated namespace keys). Provenance `origin` ∈
  {`source`, `assistant`, `derived`}; `derived` records require `parent_record_ids`.
  A dependency-free validator enforces depth/NaN guards and a 1 MiB per-record cap.
- **Idempotent ingest**: replay-safe; receipts report `duplicate`. `ingest()` at the
  service layer enforces the current **memory epoch** so a stale writer cannot commit.
- **Versioning**: `supersede` moves the current head forward; `checkpoint` records
  cursor progress atomically with records; a **deletion-ledger** replays on open
  (`src_`/`lrn_`/`done_`/`cancel_` prefixes).
- **Entities / accounts / identity** (`catalog.py`): accounts keyed by
  `acct_+digest(namespace,address)` with strict email/phone normalization; person
  entities; `identity_edges` link account↔person backed by a live evidence record, with
  `candidate`/`confirmed`/`revoked` states and validity intervals. **Only confirmed,
  time-valid links expand recall**; a confirming link is blocked by any live conflicting
  ownership interval. `browse`/`timeline`/`connections` expose this history honestly
  (stable-id order, explicit truncation flags).

### 4.3 Structured knowledge (`intelligence.py`)

A typed layer over evidence, every fact **grounded by an exact quote** (`_quote`) against
live evidence:

- **Beliefs** (subject/predicate/value with conflict detection), **relations**
  (subject–predicate–object), **measurements** (unit-converted via `metric_definitions`
  + defaults like kg / °C / bpm), **aggregates** (SQL min/max/avg over time windows).
- **Tasks** — durable task runtime with dependencies, a **state machine**, versioned
  `transition`, and `due_events`; tasks never grant permission to send reminders.
- **Coverage domains**, **quality** signals, **feedback**, **summaries** (FTS),
  **procedures** (bound to operator capabilities), immutable **metric** history, and a
  **graph** read (1–3 hops).
- Published beliefs are explicitly **unverified** (`truth: unverified`) until the
  consolidation/evaluation pipeline supports them.

### 4.4 Learning (`learning.py`, `workflows.py`, `skill_export.py`)

An evidence-gated, independence-enforced pipeline:

```
outcome  ─▶ propose (candidate; family+revision unique)
        ─▶ evaluate  (3..100 cases; proposer ≠ evaluator)
        ─▶ promote   (passing eval bound to exact candidate_digest;
                      proposer ≠ promoter; revision must advance;
                      one active lesson per family)
        ─▶ retract / invalidate (recursive via learning_dependencies)
```

- **Evaluation suites** require all three case categories (target / regression /
  non_applicable). **Consolidation** jobs run extractive summaries (≤ 24 000 evidence
  bytes per job) whose quotes are validated against source spans; acceptance publishes
  pending, unverified beliefs.
- **Workflows** (`workflows.py`) execute as **leased durable jobs** on a `memory-workflows`
  worker; adapters run in an **isolated subprocess** (`python -m personal_memory.runner
  module:fn`, 64 KiB stdout cap, timeout kill); quarantine after 3 attempts.
- **Procedures** require a **capability the operator installed**; executing a remembered
  lesson never grants new permission.
- **Skill export** (`skill_export.py`): an administrator can export an *active evaluated
  procedural lesson* to `$HERMES_HOME/skills/<name>/SKILL.md`, tracked by a registry with
  sha256 pinning (refuses on local edits, symlink escape, or family mismatch); Hermes
  verifies via `verify_exported_skill`.

### 4.5 Awareness & the change journal (`changes.py`, `awareness.py`, `awareness_worker.py`)

A proactive "what should the model pay attention to" system, **opt-in and default OFF**.

- **Change journal** (`changes.py`): a durable, transaction-aware feed. When evidence,
  source state, and the journal entry commit together, a rollback can never leave a
  notification without its evidence. Events carry references + minimal metadata only;
  content resolves at read time after visibility checks. Each event has `kind`
  (created / content_updated / metadata_updated / removed / restored / sync_gap /
  backfill_* / enrichment), `origin_mode`, and deterministic **novelty**
  (historical / live / recovered / uncertain). Reads are paginated, epoch-bound, and
  **never acknowledge** a consumer position.
- **Consumers & decisions** (`awareness.py`): server-bound consumers
  (`hermes-foreground`, background digests) turn journal events into one of four
  scheduling decisions — `record_only`, `next_turn`, `background_digest`,
  `background_prompt`. Derived self-feedback never re-wakes the model; uncertain
  discoveries never schedule a live prompt. Batch membership is **frozen at
  materialization**, leased with a **fence** (a stale worker's completion becomes
  impossible), and bounded by per-consumer policy (packet groups/chars, windows).
- **Prepare / expose / complete / deliver**: `prepare` builds a bounded next-turn packet
  (re-checks visibility, carries per-record `readiness`, reuses a prior background
  analysis instead of paying for another model run). `expose` records that a packet
  actually entered a model request (session receipt only — never batch/delivery state).
  Model analysis runs **outside** the transaction via `awareness_worker.hermes_analyze`
  (Hermes's cron model, read-only). Results are validated (citations must reference
  visible batch evidence) before completion. **Delivery** is a separate durable intent
  with quiet-hours, urgent bypass, idempotency keys, and an explicit *uncertain* state on
  ambiguous sends (no blind resend).
- **Source-pause policy** (`on_source_pause` = hold | process): a source whose every
  connection is paused parks its analysis as `held` instead of churning.
- Compaction and replay are administrator-only, bounded, and never silently erase an
  unread range or re-fire a notification that was already attempted.

### 4.6 Native Hermes surfaces (`host_store.py`, `curated.py`, `native_history.py`, `native_files.py`)

- **`CanonicalMemoryStore`** (`host_store.py`, `framework_backed=True`): a
  Hermes-`MemoryStore`-compatible, versioned curated store for the **MEMORY** and
  **USER PROFILE** prompt sections. Keeps a frozen prompt snapshot with live refresh,
  applies batch edits with `expected_version` → **409 conflict**, sanitizes through
  Hermes's `tools.threat_patterns.scan_for_threats`, and enforces char limits
  (memory 2200 / user 1375). `apply_batch` + `create_native_memory_store` suppress
  double-capture and support background-review forks.
- **Curated memory** (`curated.py`): small, operator-visible prompt entries kept separate
  from raw records/claims, each with explicit **live** evidence dependencies. Every edit
  is one transaction: validate, retire replaced entries, publish the new head, and record
  an idempotency receipt. Forgetting cited evidence auto-retires the entries that depended
  on it (`invalidate`).
- **Native history reconciliation** (`native_history.py`): a **read-only** adapter that
  snapshots Hermes's own `state.db` messages (online SQLite backup, never editing the
  native writer), converts eligible messages to contract records, and reconciles
  resumably with tombstones for removed/rewound items. Unknown generated provenance is
  **withheld**; compaction copies are matched by content fingerprint and treated as
  derived, not new facts.
- **Native file snapshots** (`native_files.py`): versioned snapshots of profile-contained
  `SKILL.md` and builtin `MEMORY.md`/`USER.md` files with chunk lineage and retraction;
  rejects symlinks / paths outside the profile and files > 1 MiB.

### 4.7 External sources (`source_sync.py`, `source_runtime.py`, `source_sdk.py`, `source_adapters.py`, `gmail.py`, `gmail_oauth.py`, `importers.py`)

- **Durable source-sync**: connections, streams, **leases with monotonic fencing**,
  replay receipts, current-version heads, coverage, and jobs — all in the same
  SQLite transaction so a crash replays safely. Adapters never commit directly; every
  authoritative write goes through `commit_page`. Stream roles (backfill / incremental /
  reconcile) determine the live head by chronology, not content equality.
- **Gmail connector** (`gmail.py`, `gmail_oauth.py`): anchors history id, consumes live
  history immediately while backfilling historical windows concurrently; enrichment is a
  separate bounded, zero-LLM stage.
- **Importers** (`importers.py`): EML/MBOX (Message-ID keyed), WhatsApp text, health CSV.
- Ingested source evidence flows into the journal (§4.5), the canonical store (§4.2), and
  feeds awareness/coverage/status identically to agent captures.

### 4.8 Hindsight external backend (`hindsight.py`, `hindsight_runtime.py`)

- HTTP adapter to the managed Hindsight 0.9.2 engine. **Local evidence owns truth and
  identity**; Hindsight only ranks candidate documents and never overwrites the contact
  registry or bypasses local tombstones/temporal filters.
- A lifecycle lock serializes retain/delete/clear against the remote bank; a durable
  **bank-clear obligation** is persisted before the remote request so a canonical reset
  leaves no orphaned remote documents; deletions outrank new extraction; malformed
  documents are isolated without blocking the batch. Recall maps depth → budget/max-tokens
  and preserves raw cosine similarity when the engine exposes it.

### 4.9 Forgetting, retirement & lineage (`lifecycle.py`, `lineage.py`, `deletions.py`, `reset.py`, `recovery.py`)

- **Unified retirement** (`lifecycle.retire`): hides a record and its derived descendants
  and, in one cascade, invalidates dependent awareness results, learning artifacts,
  curated entries, **revokes identity links**, and turns active claims resting on the
  evidence into `retired` history (retirement ≠ retraction).
- **`live_and_visible` / `require_live_evidence`** gate all new derived memory creation;
  a single startup **versioned repair** (`apply_upgrade`) cleans pre-existing stale
  artifacts without rescanning the archive on every open.
- **Forget / supersede / forget_source**: user forgetting, upstream replacement, and
  permanent source-level tombstones are distinct operations.
- **Exposure ledger & lineage** (`lineage.py`): tracks which records each session saw so
  generated captures inherit correct provenance; dependencies are compacted into
  canonical fan-in nodes (≤ 100 parents) and **never truncated**; a forgotten parent
  withholds the derived capture.

### 4.10 Access control & authorization (`service.py`, `access.py`, `host_bridge.py`)

- **Service roles**: admin / agent / reader / ingest / evaluator / executor / scheduler /
  awareness; tokens ≥ 32 chars; `authorize()` enforces per-role scopes (e.g. executor
  needs explicit capabilities, ingest needs sources + connector id).
- **Context/session gating** (`access.py`, `host_bridge.context_allowed`): memory is a
  **single-owner** boundary. Group/channel/guild/public chats are denied; CLI/cron under
  the owner's OS account are allowed; cron recipients must have an explicit private
  `platform`/`chat_id`/`thread_id`; run scope is immutable.
- `handle_tool_call` **blocks writes in non-primary contexts**; delivery, native-history
  authority, skill revision, and cross-profile reads are all re-checked at the bridge.

### 4.11 Observability & robustness (`trace.py`, `provider.py`, `asgi.py`)

- One **trace id** per top-level call, propagated over `X-Personal-Memory-Trace` and bound
  to a `ContextVar`, so `grep trace=<id>` reconstructs a call across both the Hermes
  provider process and the service process. `@traced`/`log_call` emit **a single INFO
  line per top-level call**, cheap when debug is off.
- Health/status surfaces retrieval diagnostics, outbox/queue backlog, dead-letters,
  Hindsight sync state, awareness readiness, and source coverage — so the agent can
  honestly say when memory is incomplete rather than guessing.


---

## 5. End-to-end functional flows

### 5.1 A normal Hermes turn (read path)
1. Hermes calls `initialize` → provider builds `Client`/`Outbox`/`ExposureLedger`,
   resolves `access_allowed`, and fires a **prefetch** (bounded `fast` recall, limit 4)
   on the `personal-memory-recall` thread.
2. `system_prompt_block` injects `GUIDANCE`; `get_tool_schemas` exposes the 20 tools.
3. The model calls `personal_memory_search` → `handle_tool_call` routes to `/v1/search`
   (reusing the cached prefetch if the query matches) → service `dispatch` (role + epoch
   checks) → `Hybrid.search` (§4.1) → local evidence returned with diagnostics.
4. Whatever records reach the model are registered in the **ExposureLedger** so any later
   generated capture inherits correct provenance.

### 5.2 Writing memory (durable, non-blocking)
1. Model calls `capture`/`remember`/`manage` → provider validates context is primary
   (writes blocked otherwise).
2. The item is **committed locally to `outbox.db` first** (`Outbox.enqueue`, idempotent
   by content fingerprint + epoch), then delivered to `/v1/ingest` by the outbox worker
   with exponential backoff; contract failures dead-letter for explicit replay; forgotten
   parents are discarded as `withheld`.
3. The service ingests contract-1.0 records into the canonical store, enqueues semantic
   and Hindsight index work, and (if the journal is enabled) appends a change event — all
   in one transaction.

### 5.3 Awareness loop (opt-in)
```
evidence commits ─▶ change journal ─▶ sweep materializes frozen batches
   ─▶ claim (lease + strictly higher fence) ─▶ worker resolves visible originals
   ─▶ hermes_analyze (Hermes cron model, read-only, outside the transaction)
   ─▶ validate citations ─▶ complete ─▶ optional durable delivery intent
Foreground variant: prepare() builds a bounded next-turn packet that expose() marks as
actually entering a model request — exposure ≠ processing ≠ delivery.
```

### 5.4 Native store round-trip
Hermes MEMORY/USER edits flow through `create_native_memory_store` → `CanonicalMemoryStore`
→ `curated.apply` with `expected_version`; a concurrent edit returns 409 so the agent
reloads rather than clobbering. Forgetting the cited evidence auto-retires the curated
entries that depended on it.

---

## 6. Default-on vs opt-in (quick reference)

| Capability | State | Notes |
|---|---|---|
| Keyword (FTS5) retrieval | **On** | Always available baseline. |
| Semantic retrieval | **On** | Lazy; falls back silently on failure; `PERSONAL_MEMORY_DISABLE_SEMANTIC`. |
| Cross-encoder rerank | **On** | `PERSONAL_MEMORY_DISABLE_RERANK`. |
| Graph expansion | **On** | Bounded, depth-driven. |
| Temporal recency bonus | **On** | Multiplicative decay. |
| Prefetch on turn start | **On** | Bounded `fast` recall; `prefetch_wait_ms` default 200. |
| Durable outbox writes | **On** | Local-commit-before-network, idempotent. |
| Hindsight backend | **Opt-in** | Requires configured sources + url/bank; else external ranking skipped. |
| Change journal | **Opt-in** | `changes.configure(journal.enabled)`; disabled records nothing. |
| Awareness consumers | **Opt-in** | `settings["awareness"]["enabled"]` default **False**. |
| Auto-consolidation | **Opt-in** | `intelligence.auto_consolidate` boolean. |
| Learning promotion | **Admin-gated** | Candidate ≠ active; promotion needs a passing eval + independence. |
| Skill export | **Admin-gated** | Only active evaluated procedural lessons; registry-pinned. |
| Native history import | **Admin/explicit** | Read-only, selected sessions; unknown provenance withheld. |
| Group/channel memory access | **Denied** | Single-owner boundary. |

---

## 7. Configuration & attestation

- **Settings file**: `$HERMES_HOME/personal-memory/settings.json` gates availability and
  carries `awareness` and retrieval options; `prefetch_wait_ms` bounds turn latency.
- **Service settings**: `data_dir`, `token`, `retrieval`, `backend`, `principals`,
  `extension_schemas`, `intelligence`, `sources`.
- **Attestation**: on `initialize` with `host_memory_api==2` and a `host_memory_root`,
  the provider writes `host-runtime.json` recording the live Hermes runtime it is bound
  to. `host_contract.json` pins the supported Hermes release
  (**v2026.9.14**, commit `345cd2b…`), per-file before/after sha256 (including the shipped
  `agent/memory_bridge.py` patch), `patch_sha256`, `host_memory_api: 2`,
  `native_store_contract: 1`, and `framework_version: 0.8.0rc8`.
- **Env toggles**: `PERSONAL_MEMORY_DISABLE_SEMANTIC`, `PERSONAL_MEMORY_DISABLE_RERANK`,
  `PERSONAL_MEMORY_LOG_LEVEL`.

---

## 8. Feature checklist (what Hermes can do with this framework)

- Recall across conversations, email, files, and native history with **hybrid keyword +
  semantic + external-rank + graph + rerank + temporal** retrieval.
- **Progressive** (`recall`) and **parallel planned** (`investigate`) deep searches.
- **Persist** new evidence (`capture`) and evidence-linked **claims** (`remember`) with
  correction by supersession.
- **Model entities, accounts, and person identity** with time-valid, evidence-backed links
  and honest revocation.
- Maintain **structured knowledge**: beliefs, relations, unit-converted measurements,
  aggregates, dependency-aware tasks with a state machine, coverage, quality, summaries.
- **Learn** from outcomes through an independence-enforced, admin-promoted lesson system,
  and export evaluated procedures as **skills**.
- Run **proactive awareness**: attention-ranked next-turn packets, background digests, and
  durable, quiet-hours-aware **notifications** — all evidence-cited and never self-waking.
- Read/write Hermes's **native MEMORY / USER** surfaces through a conflict-safe,
  framework-backed store, and reconcile **native chat history and files** read-only.
- **Ingest external sources** (Gmail, EML/MBOX, WhatsApp, health CSV) with durable,
  crash-safe, fenced sync.
- **Forget and retire** cascadingly so no stale conclusion survives lost evidence, with
  full **provenance lineage** and **exposure tracking**.
- **Authenticate and scope** every operation by role and single-owner context, and
  **observe** system health honestly through status/tracing.

---

*This document is derived directly from the source in `personal_memory/`. When a hook,
tool, default, or constant changes, update the corresponding section and re-verify
against `provider.py`, `tools.py`, `service.py`, `host_contract.json`, and the relevant
subsystem module.*
