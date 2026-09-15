# Production Deployment Report — `192.168.68.67` (judaadu-nandan)

**Framework:** `hermes-personal-memory` v0.8.0rc8 (git clone of the public repo, commit `f02d158` + the fixes listed in §5)
**Host agent:** Hermes @ v2026.9.14 (commit `345cd2b057a452236de401d3534b8502a7465e8d`), pinned host patch applied (30 files)
**LLM:** llama.cpp `http://192.168.68.65:8080/v1`, model `Cobra91310/Ornith-1.5-9B-MTP-NVFP4-newhead:NVFP4`, `n_ctx=262144`, speculative decoding on
**Embeddings:** GPU through the `local-ml` extra (torch CUDA); the Hindsight daemon is a live CUDA compute app
**Install:** fresh, production-shaped — `personal_memory setup --exclusive --auto-consolidate --semantic`, memory service under a systemd user unit
**Date:** 2026-09-15

## 1. Headline

**The complete loop runs on real software with no cloud dependency:** a real Hermes agent
turn on llama.cpp called the memory tools, the service ingested the record, Hindsight extracted
and consolidated it through the same llama.cpp endpoint, and a later turn recalled the stored
secret verbatim — `4417 Delta` — with a trace id on every step.

```
$ hermes -z "Search my personal memory for the access code of the hillside storage unit. …"
4417 Delta
```

## 2. What is PROVEN end-to-end

| Capability | Result | Evidence |
|---|---|---|
| llama.cpp serves the agent | ✅ | tool-calling probe; `/slots` → `n_ctx: 262144`, `/v1/models` → `meta.n_ctx: 262144` (≫ the 64K Hermes floor) |
| Hermes provider registered | ✅ | `hermes plugins doctor personal-memory` → manifest `0.8.0rc8 (exclusive)` OK, runtime discovery/import/registration passed |
| Host patch integrity | ✅ | `manage_hermes_host_patch.py check→apply→check`: `original` → `patched`, idempotent; `agent/memory_bridge.py` sha256 `d8cd3e159146…` matches the manifest |
| Host patch attestation | ✅ | one patched Hermes run writes `personal-memory/host-runtime.json` (`{"api": 2, "root": "/home/jugaadu/hermes-agent"}`) and `doctor` re-hashes all 30 files against it (§6) |
| Agent → memory tool surface | ✅ | all 20 tools compile and answer; journal shows `/v1/epoch`, `/v1/curated/read`, `/v1/ingest records=1`, `/v1/generation`, `/memories/recall`, `/v1/search episodes=2`, `/v1/claim`, `/v1/entity` with `role=agent` |
| Real capture by the agent | ✅ | turn 2 stored claim `clm_02a317a601f71839ec3a061f6b1b2232` against `rec_a38b5c21732541d31f2b178bc91075d1`, created entity `ent_9133002652514e328687dd37d698cdf9`, then self-verified via timeline + search |
| Hindsight really calls the LLM | ✅ | `/llm-requests/stats`: 5 `success`, provider `openai`, operation `consolidation`, 18,796 tokens, 0 failures; bank = 4 nodes (2 observation + 2 world), 3 documents, `pending_consolidation: 0` |
| GPU embeddings | ✅ | venv python is a CUDA compute app (244 MiB); `semantic.index="hnsw"`, `ready=true`, `failed_records=0` |
| Trace correlation in production | ✅ | one INFO line per top-level call with a 12-hex trace id; the id is echoed in the response header and additively inside ≥400 bodies (observed inside live 422 bodies) |
| Contract enforcement + honest failure | ✅ | agent's empty `predicate` rejected by `/v1/claim`; agent read the error, corrected, retried, succeeded |
| Abstention | ✅ | queries for never-stored facts return no candidates rather than an invented answer |
| Service under systemd | ✅ | `ready=True problems=[]`, `NRestarts=0`, one cgroup for service + daemon + Postgres, `Memory 1.8G` against `MemoryHigh=10G`, `Tasks 90/1024` |
| Full regression on this host | ✅ | `python -m unittest discover -s tests` → **Ran 183 tests OK (skipped=21)** in 33.6 s |

## 3. Release gates on this host (`.e2e/224_host_gates_fixed.sh`, all re-run after the §6 re-base in `.e2e/244_host_gates_after_rebase.sh`)

| Gate | Result |
|---|---|
| `check_host_bridges.py` | rc=0 — `actual_AIAgent: true`, `actual_SessionDB: true`, 7 boundary checks |
| `check_native_history.py` | rc=0 — `passed: true`, `hermes_tag: v2026.9.14`, real `SessionDB.delete_session` → tombstone propagation |
| `check_agent_runtime.py` | rc=0 — full `AIAgent` loop against the deterministic protocol fixture, 3 requests, 8 checks |
| `check_load_recovery.py` | rc=0 — 10,000 records ingested in 2.56 s, p50 327 ms / p95 8,009 ms, **records survived SIGKILL**, encrypted backup + isolated restore 0.40 s |
| `check_hermes_release.py` | rc=0 — **17 passed, 0 failed** against the real v2026.9.14 tree |
| `smoke_release.py` | rc=0 — 8 checks incl. copied ASGI launcher, idempotent replay, backup/restore CLI, provider rollback |
| `personal_memory doctor` | rc=0 — **`checks_passed: true`**, 17/17 checks, once the §6 patch re-base landed |

## 4. Environment facts worth carrying forward

- The remote login shell is **fish**: every remote action must be a script piped through
  `Get-Content -Raw .e2e/<f> | ssh 192.168.68.67 'tr -d "\r" | bash -s'`.
- `scp` from the Windows working tree introduces CRLF; normalise on the host with
  `sed -i 's/\r$//'` before diffing, or the diff shows the whole file as changed.
- Under systemd the service logs to **journald**, not `~/.hermes/personal-memory/service.log`
  (that file only exists because the pre-systemd run redirected into it). Harnesses must read
  `journalctl --user -u personal-memory`.
- `/v1/ready` and `/v1/health` are **POST-only**; a GET poll returns 405 and will spin forever.
- Hermes config keys are dotted (`hermes config set model.provider custom`); `model` is a dict
  (`provider|default|base_url|api_key|context_length`) and a self-hosted endpoint needs
  `provider: custom` with an explicit `base_url` — the `llamacpp` alias otherwise demands the
  Hermes-managed server.
- The shipped `deployment/personal-memory.service` template is too small for this workload as
  measured (`RSS 4.2 GB`, 117 threads, cold start ≫60 s). The installed unit adds
  `EnvironmentFile`, `ReadWritePaths=%h/.hindsight %h/.pg0 %h/.cache`, `TimeoutStartSec=900`,
  `TasksMax=1024`, `MemoryHigh=10G`/`MemoryMax=14G`. **Recommend folding these back into the template.**

## 5. Defects found by this deployment and fixed

### 5.1 llama.cpp rejected the model-facing tool schema (blocked every agent turn)

Two distinct 400s, root-caused by bisecting the live schema against the endpoint
(`.e2e/204–208`, 30+ probes):

1. `Unable to generate parser for this template … Pattern must start with '^' and end with '$'`
   — the ingestion contract's `string()` helper emits an unanchored `"pattern": "\S"` and
   `occurred_at` carries `"format": "date-time"`.
2. `Failed to initialize samplers: failed to parse grammar` — carried **only** by
   `personal_memory_capture`, and inside it only by `provenance.source_locator`'s
   `maxLength: 2000`. Measured on this endpoint:

   | probe | result |
   |---|---|
   | nested `maxLength` 1999 | PASS |
   | nested `maxLength` 2000 / 4000 / 100000 | FAIL |
   | three nested props summing to 2001 | PASS (per-string limit, not aggregate) |
   | `participants` item string raised to 2000 | FAIL (applies inside arrays too) |
   | root-level `text` `maxLength: 100000` | PASS (root object is exempt) |

**Fix** (`personal_memory/tools.py`): `parser_safe()` now drops `pattern`/`format` and drops a
nested (depth ≥ 2) `maxLength` at or above 2000, keeping root-level bounds so a long record body
is still capped. **The published contract is untouched** — `/v1/ingestion-schema` and every
server-side validation still enforce all constraints; only the model-facing copy is simplified,
so a model can no longer be grammar-blocked from calling the tool.
Regression guarded by `tests/test_ingestion.py::ModelFacingSchemaTests` (3 model-independent tests).
Verified after deploy: `PASS ALL` for the 20-tool payload and `failing tools: []`.

### 5.2 `check_agent_runtime.py` issued a real ingest on a 1-second probe budget

The readiness poll builds `Client(..., timeout=1)` and then reuses that client for
`/v1/ingest`. With managed Hindsight the ingest waits for model extraction, so the gate died with
an uncaught `TimeoutError`. Fixed by rebuilding the client with a real budget after the poll.

### 5.3 Two `test_native_history` tests failed on this host (pre-existing, not a regression)

Proved pre-existing by running them in a clean worktree at `f02d158` with zero local edits —
identical failures. Diagnosis from the in-test service's own logs:

```
Loading weights:   0%|  | 0/105 …
op=/v1/search ms=3015.1 ok=0 error=TimeoutError
    ordered,scores=self._apply_rerank(query,ordered,scores)
```

The cross-encoder **lazy-loads inside the first request that has ≥2 candidates**, which is exactly
these two compaction tests (their siblings get 0–1 hits and never touch it). The `HTTPFixture`
client budget was a hard-coded 3 s, so cold model load looked like a failure. Fixed by giving the
fixture a 60 s budget; no assertion changed. Result: module 11/11 OK, suite 181 OK.
`/v1/status` corroborates it: `rerank.loaded: false` at startup. **Recommendation:** preload the
reranker during service start (or in a background thread) so the first real query does not pay it.

### 5.4 Hindsight's inline retain exceeded its client timeout under shared-endpoint load

`/memories` (synchronous retain) measured **4.7 s cold-idle → 34–120 s** once the agent's own
generation was competing for the same single llama.cpp slot; one call died at
`ms=120100.9 error=TimeoutError`, and Hermes correctly retried the capture. Fixed in deployment by
setting the supported, validated fields `retrieval.hindsight.retain_timeout: 300` and
`recall_timeout: 30` (both capped at 600 by `hindsight_runtime.normalize`).
**Recommendation for real scale:** give the memory extractor its own llama.cpp instance/port so
extraction cannot queue behind agent generation — or move the retain off the tool-call path.

## 6. Resolved with sign-off — `doctor.pinned_host_patch`

`doctor` requires `~/.hermes/personal-memory/host-runtime.json`, written by
`provider.initialize()` **only when** the patched host passes `host_memory_api=2` +
`host_memory_root`. Those kwargs never arrived, because our own patch references `Path` in a module
that never imports it:

```python
    with suppress(Exception):                     # agent/agent_init.py:1208
        from agent.memory_bridge import HOST_MEMORY_API
        import agent.memory_bridge as _host_memory_bridge
        kwargs["host_memory_api"] = HOST_MEMORY_API
        kwargs["host_memory_root"] = str(Path(...))   # ← NameError, swallowed
```

`Path` was the **only** use of that name in `agent_init.py` and the file has no `pathlib` import
(verified on the host: `Path bound in agent_init namespace: False`). The defect is identical in all
three pinned patches (`v2026.8.31`, `v2026.9.11`, `v2026.9.14`), so it is ours, not upstream drift.
`check_hermes_release.py` hides it because that gate writes the attestation file itself.

**Impact was certification proof, not data safety:** the two kwargs feed only the attestation write;
boundary enforcement is driven by `host_context`, and the boundary gates that exercise it
(`check_host_bridges`, `check_native_history`, `check_hermes_release` 17/17) all passed against the
real patched tree.

**Fix, scoped to the deployed release by decision:** one line, `from pathlib import Path`, as the
first statement inside the suppress block of `hermes-v2026.9.14.patch`. `.e2e/236` validated the
re-generator before trusting it: `git diff` of the *unmodified* patched tree reproduced the shipped
patch byte-for-byte, so the re-based patch is a git regeneration, not a hand-spliced hunk. Its delta
is 11 lines, all inside `agent/agent_init.py` — the added line plus the post-image offsets it shifts
in that file's five later hunks. `host-patch/manifest.json` and `personal_memory/host_contract.json`
were repinned together:

| pin | before | after |
|---|---|---|
| `patch_sha256` | `62477753…` | `d34da13e…` |
| `agent/agent_init.py` post-image | `c17ce444…` | `03324e96…` |

The v2026.8.31 and v2026.9.11 archives keep their original hashes and remain defective; that is
recorded in `docs/HOST_BRIDGES.md` so nobody re-disables the guard by assuming otherwise.

Verification (`.e2e/237`–`245`): bundle self-consistency on the host; a full
`rollback → check → apply → check` cycle that re-verifies all 30 post-apply hashes
(`git status` went to 0 dirty files at the pristine step, proving rollback is exact);
`check_host_patch.py` run for the first time host-side against a `git archive` export of
`345cd2b0` — 6 checks, `passed: true`, regenerating `docs/HOST_PATCH_CHECK.json`; all four
distributed copies of the contract byte-identical (repo, memory-service venv, Hermes venv, plugin
mirror); one real Hermes turn that again answered `4417 Delta` **and** wrote
`{"api": 2, "root": "/home/jugaadu/hermes-agent"}` (mode 0600); then `doctor` →
`checks_passed: true`, 17/17, rc=0. Suite: **183 tests OK (skipped=21)** after adding
`tests/test_production.py::HostPatchBundleTests`, which pins patch ↔ manifest ↔ contract agreement
and the self-containment of the attestation hunk, so a future re-base cannot leave a stale contract
behind. `.gitattributes` gained `eol=lf` on `*.patch`: without it a Windows checkout rewrites the
integrity-pinned payload to CRLF and the bundle fails its own `patch_sha256`.

## 7. Deliberate divergences from the original brief

- The brief mentioned an "ollama embedding server"; the chosen and configured path is
  **GPU embeddings via `local-ml`**. Ollama (`qwen3-embedding:4b`, `:11434`) is untouched and unused.
- Paraphrase recall on this host (3 of 5 keyword-free queries) sits inside the repo's own measured
  norms (`docs/synthetic-personal/REPORT.md`, multilingual_hybrid top-1 16/23), so it is recorded as
  expected behaviour, not a host regression.
- `check_host_patch.py` needs `--original-root`/`--anchor-root` pristine worktrees, so it had never
  run on a real host. `git -C ~/hermes-agent archive HEAD | tar -x -C /tmp/hermes-orig` supplies them
  from the pinned commit; it then passed 6 checks and regenerated `docs/HOST_PATCH_CHECK.json`.

## 8. Repo change set from this deployment

| File | Change |
|---|---|
| `personal_memory/tools.py` | depth-aware `parser_safe()`: drop `pattern`/`format` + nested `maxLength ≥ 2000` |
| `tests/test_ingestion.py` | `ModelFacingSchemaTests` (3 tests) guarding the model-facing surface and the untouched contract |
| `scripts/check_agent_runtime.py` | real client budget after the 1 s readiness poll |
| `tests/test_memory.py` | `HTTPFixture` client timeout 3 s → 60 s (cold cross-encoder load) |
| `host-patch/hermes-v2026.9.14.patch` | attestation block imports `pathlib.Path` (§6) |
| `host-patch/manifest.json`, `personal_memory/host_contract.json` | repinned `patch_sha256` + `agent_init.py` post-image |
| `tests/test_production.py` | `HostPatchBundleTests` (2 tests): bundle hashes agree, hunk binds what it uses |
| `docs/HOST_PATCH_CHECK.json`, `docs/HERMES_RELEASE_CHECK.json` | regenerated gate evidence for the new pin |
| `docs/HOST_BRIDGES.md` | host-attestation contract + re-base record; release pin read from the manifest |
| `docs/HERMES_914_MEMORY_COVERAGE.md` | correction: the audited attestation kwargs never arrived |
| `.gitattributes` | `eol=lf` for `*.patch`, so the pinned payload survives a Windows checkout |
| `.e2e/*` | 70+ numbered reproduction scripts, their captured outputs, and this report |

Suite on this host after the change set: **183 tests OK (skipped=21)**.

Host-side state (outside git): `~/.hermes/config.yaml` model + memory block (backup
`config.yaml.bak-setup`), `~/.hermes/personal-memory/config.json` timeout fields (backup
`config.json.bak-retain`), `~/.config/systemd/user/personal-memory.service` (enabled),
`~/.hermes/venvs/hermes`, patched `~/hermes-agent`.
