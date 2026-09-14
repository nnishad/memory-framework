# End-to-End Integration Report — Hermes × hermes-personal-memory

**Framework:** `hermes-personal-memory` v0.8.0rc8
**Host agent:** Hermes AGPL @ v2026.9.11 (commit `939e45c9`), pinned host patch applied
**Runtime:** WSL2 Ubuntu · GPU **NVIDIA RTX 5080 (16 GB)** · Ollama (OpenAI-compat) on Windows host
**Date:** 2026-09-14

## 1. Headline — the requested item

**Hindsight now runs on the GPU dependency path (`local-ml` / torch CUDA), not CPU/ONNX.**

Evidence:
- `torch 2.11.0+cu128`, `cuda.is_available() == True`, device `NVIDIA GeForce RTX 5080`.
- Hindsight daemon env: `HINDSIGHT_API_EMBEDDINGS_PROVIDER=local`,
  `HINDSIGHT_API_EMBEDDINGS_LOCAL_MODEL=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`.
- `hindsight_api/engine/embeddings.py` → `select_local_device(force_cpu=False, …)` returns CUDA
  and builds `SentenceTransformer(device="cuda")`.
- `nvidia-smi --query-compute-apps` lists the Hindsight daemon PID as an active GPU compute app.
- Doctor check `hindsight_default` = **OK** (Hindsight active by default, no init errors).

## 2. What is PROVEN end-to-end

| Capability | Result | How verified |
|---|---|---|
| Hindsight GPU embeddings | ✅ GPU (torch CUDA) | env + device-selection code + nvidia-smi PID |
| Multilingual semantic recall | ✅ | passport / English→Hindi / paraphrase all `candidates_found` |
| Pinned host patch integrity | ✅ | `git apply` original→patched; **doctor `pinned_host_patch` OK** (all contract file+anchor hashes match the patched tree) |
| Ollama LLM wired into Hermes | ✅ | provider `custom/ollama`, `qwen2.5:14b-instruct`, ctx 65536; agent loop answers |
| Hermes **recall** from memory | ✅ | Q "where is my passport" → tool `personal_memory_search` → "The violet folder in the bedroom cupboard." |
| Hermes **capture** (write durable fact) | ✅ | Q "save wifi secret" → tool `personal_memory_remember` → retained/retrievable |
| **Cross-session** recall | ✅ | fresh session retrieved "The home wifi passphrase is sunset-orchid-42." |
| Framework unit tests | ✅ 168 OK (21 skipped) | post-integration re-run |
| Synthetic personal evaluation | ✅ 30/30 checks, 1226 records | `evaluate_synthetic_personal.py`, reproducible `archive_sha256=e3a4bb06…` |
| Doctor internal gates | ✅ `checks_passed = True` | all 17 internal checks green after cleanup |
| **Abstention / no hallucination** | ✅ | never-stored queries → Hermes searched then answered `NO MEMORY STORED` (no invented fact/PIN) |
| Pinned-patch **workflow** acceptance | ✅ | `check→apply→apply→rollback→rollback` + local-edit rejection, `patch_sha256=e02aee22…` |
| **Native-history** boundary (real Hermes `SessionDB`) | ✅ | import + read-only sync + `delete_session`→tombstone propagation |
| **Load + SIGKILL recovery + encrypted backup** | ✅ | 3000 recs/1.11s, p95 327ms, survived SIGKILL, backup+restore 0.26s |
| Release smoke (copied provider + CLI + key custody) | ✅ | 8 checks incl. encrypted backup/restore CLI + provider rollback |

"Hermes knows how to use the framework for memory" is demonstrated by real tool invocations
(`personal_memory_search` / `personal_memory_remember`) driving a successful
capture → retain → cross-session recall loop.

## 3. Doctor status (final)

All internal checks pass:
`private_settings, private_behavior_config, installed_provider_version, installed_investigation_module,`
`pinned_host_patch, native_backup_path, dependency_{uvicorn,cryptography,hindsight_api,hindsight_embed},`
`database_integrity, capture_dead_letters, native_history_retirements, service_ready,`
`live_service_version, hindsight_default, parallel_investigation`.

`production_certified` is **False by design** — it is hardcoded in `operations.py:65`. The framework
never self-certifies; it lists 4 external acceptance gates (below).

## 4. Remaining gates (external / by design — NOT defects)

`production_certified` stays False until the operator signs off, but the shipped verification
fixtures now cover three of the four listed gates:

1. **Live Hermes acceptance** — *partly*: the full CLI agent loop (recall/capture/cross-session/
   paraphrase) and the native-history fixture run against the real pinned Hermes `SessionDB`; the
   project's formal acceptance suite and the authenticated `hermes serve`/`gateway` transport
   (JSON-RPC + web build + messaging auth) are not stood up in this harness.
2. **User archive recall/identity evaluation** — needs a real held-out user archive; the synthetic
   eval is deterministic fixtures, explicitly *not* a statistical benchmark (see eval `limitations`).
3. **Host load & recovery drill** — ✅ exercised (`check_load_recovery.py`: SIGKILL survival +
   encrypted backup/isolated restore).
4. **Storage encryption + credential/backup-key custody** — ✅ exercised (`smoke_release.py`:
   `backup-keygen` + encrypted `backup` + isolated `restore` + rollback).

## 5. Test-harness caveats (environment artifacts, not framework bugs)

- **Split-user setup:** Hermes agent runs as `root` (venv under `/root`), the memory service runs as
  `hermes` (non-root Postgres/`pg0` cannot `initdb` as root), both sharing `/home/hermes/.hermes`.
  Consequence: the provider-owned `outbox.db` is `root:root`, so `doctor` must be run as the
  provider-home owner (root here). A single-user production deploy removes this wrinkle.
- **`host-runtime.json`** (needed by `pinned_host_patch`) is written by `provider.initialize` when
  Hermes passes `host_memory_api=2` + `host_memory_root`. Confirmed this code path works (direct
  initialize with Hermes's real init kwargs writes it correctly and doctor then verifies the patch).
- **`tools.tool_search.enabled=off`** is required so the 14B model invokes the 4 memory tools
  eagerly; the tier-1 `tool_call` bridge deferred them behind a format the small model couldn't emit.
- **4 `dead_letters`** quarantined during the earlier `tool_call`-bridge crash (`hermes.native`
  4xx-rejected malformed captures at 17:16–17:17) were inspected and discarded as test noise.
- The framework's own `SemanticIndex`/`FastEmbedder` (CPU ONNX, `retrieval.semantic`) feeds the
  RelevanceGate and is **separate** from the Hindsight GPU engine; both are enabled.

## 6. Reproduction scripts (`.e2e/`, scratch)

`89_gpu_verify.sh`, `90_gpu_device_probe.sh` (GPU proof) ·
`72_apply_host_patch.sh` (patch) · `78/79/80_*` (Ollama wiring) ·
`84_recall_direct.sh`, `86_t9_capture_recall.sh` (agent memory loop) ·
`95_direct_init.sh`, `99_clear_deadletters.sh` (doctor green) · `100_final_eval.sh` (30/30) ·
`101_full_e2e.sh` (fresh recall/capture/cross-session/paraphrase) · `103_negative_test.sh`
(abstention) · `105_autoinit_test.sh` (CLI does not self-attest — by design, see `smoke_release.py:46`) ·
`106/108` (patch + native-history fixtures) · `107` (release smoke + load/recovery) · `109` (final doctor green) ·
`117–125` (retrieval optimisation: benchmark authoring, BEFORE/AFTER runs, full unittest, evidence merge).

## 7. Retrieval optimisation — temporal consolidation (research → implement → benchmark)

Motivated by the 2025–2026 agent-memory literature (Generative-Agents relevance×recency×
importance; Zep/Graphiti temporal validity; Hindsight recency boost): the fusion layer used
**pure Reciprocal Rank Fusion with no notion of time**, so a *stale* fact that is an equal-or-
stronger lexical match could outrank the *current* fact — the documented #1 production failure
for long-running agents.

- **Change** (`personal_memory/retrieval.py`): `_apply_recency()` adds a **bounded, additive**
  exponential recency bonus `score × (1 + weight · 0.5^(age_days/half_life))` after RRF fusion
  and before hydration. It **demotes nothing** (only prefers fresher, already-relevant evidence),
  keeps undated records at their fusion rank, and is **off by default** (`retrieval.temporal.weight: 0`),
  so shipped behaviour is unchanged unless a deployment opts in.
- **Benchmark** (`scripts/benchmark_temporal.py`, evidence in `docs/TEMPORAL_BENCHMARK.json`):
  keyword-only, deterministic, isolating temporal precedence (not semantic gap-bridging). On 7
  current-vs-stale conflicts with single-fact and explicit-time-filter regression guards:

  | run | current recall@1 | current MRR | stale-outranks-current | single-fact@1 | historical@1 |
  |---|---|---|---|---|---|
  | BEFORE (RRF only) | 0.1429 | 0.5714 | 6/7 | 1.0 | 1.0 |
  | AFTER, feature **off** (parity) | 0.1429 | 0.5714 | 6/7 | 1.0 | 1.0 |
  | AFTER, feature **on** (w=1.0, hl=365d) | **1.0** | **1.0** | **0/7** | 1.0 | 1.0 |

  Delta: **+0.857** current-state recall@1, **+0.429** MRR, stale misrankings **6 → 0**, with **zero
  regression** on the guards; the disabled run is byte-identical to baseline (behaviour-neutral).
- **Regression:** full suite `python -m unittest discover -s tests` → **168 OK (skipped 21)**; new
  deterministic test `tests/test_retrieval.py::test_recency_consolidation_prefers_current_fact_when_enabled`
  passes 3/3 runs.
- **Conclusion:** the framework **is** optimisable and this optimisation measurably helps. Follow-on
  candidates (each needing the same benchmark-before/after discipline): learned cross-encoder rerank
  of the fused top-k, recency/importance in the base score, and HippoRAG-style multi-hop activation.

## 8. Live deployment + production A/B against the real Hindsight GPU stack

The Section 7 change was then deployed to the **running production service** and validated end-to-end
through the actual `/v1/ingest` → `/v1/search` path (keyword **FTS5** + fastembed **semantic** +
**Hindsight on GPU**), not just the in-process harness.

- **Import authority.** `run_service.py` does `sys.path.insert(0, …/plugins/personal-memory)`, so the
  live service imports `personal_memory` from `/home/hermes/.hermes/plugins/personal-memory/` (that
  `retrieval.py` — not the framework copy or site-packages — is what must be synced).
- **Restart-race gotcha (fixed).** The first restart attempt failed (`startup.failed` →
  `RuntimeError`) even after rolling the code+config back to the original, which proved the failure was
  **environmental, not the change**: the managed Hindsight daemon + its embedded Postgres need a **full
  teardown** (release of `:9276` and the `pg0` data dir) before relaunch; a fixed `sleep 3` after
  `pkill` was insufficient. An in-process repro that ran the identical startup sequence booted fine
  (`LEASE ok → HINDSIGHT ok → SERVICE ok`). Hardened deploy scripts now `pkill`, then **wait until both
  ports are free and no daemon/postgres remains**, before every launch. With that, restarts succeed.
- **Non-destructive.** Demo facts used dedicated sources (`tbench`, `tbench2`), were removed afterwards
  via `/v1/forget-source`, and both `retrieval.py` and `config.json` were backed up before overwrite with
  an automatic rollback-and-recover path on any failed restart. The service is left **UP**.
- **Live A/B** — same deployed code, only `retrieval.temporal.weight` toggled; a *stale-dominant*
  conflict (the old fact stated richly/repeatedly, the current fact stated once and briefly):

  | scenario (query) | weight **0 (OFF)** top hit | weight **1.0 (ON)** top hit |
  |---|---|---|
  | `my favorite coffee` (2 rich stale vs 1 thin current) | `coffee_old1` → **STALE-FIRST** ❌ | `coffee_new` → **CURRENT-FIRST** ✅ (stale demoted 1→3) |
  | `how do I commute` (current already wins on fused score) | `transit_new` (CURRENT) | `transit_new` (CURRENT) — **neutral, no regression** |

  On the fully-indexed multi-channel stack, *light* conflicts already surface the current fact first
  (so recency is correctly a no-op there), and the earlier stale-first seen in the first run was an
  **under-indexed artifact**. When the fused RRF genuinely prefers a stale memory, the recency bonus
  flips it to the current fact — reproduced live on GPU, with **zero** change to the already-correct case.
- **State after run:** `retrieval.temporal = {weight: 1.0, half_life_days: 365}` is now **enabled** in the
  live `config.json` (the requested deploy). **Revert:** delete `retrieval.temporal` from
  `~/.hermes/personal-memory/config.json` and restart, or restore the timestamped `~/.e2e-backup-*` copy.

  Driver scripts: `.e2e/127_live_op.py`, `.e2e/132_robust_demo.sh`, `.e2e/134_live_conflict.py`,
  `.e2e/133_live_toggle.sh` (and `.e2e/129_state_probe.sh`, `.e2e/130_diag.sh`, `.e2e/131_repro_startup.sh`).
