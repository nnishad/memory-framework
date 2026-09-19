# Personal Memory — Production Readiness Audit (0.8.0rc8)

> Derived from a full read of `personal_memory/` plus the project's own
> `VALIDATION.json`, `docs/ROADMAP.md`, `docs/IMPLEMENTATION_PLAN.md`, and
> `docs/GAP_REMEDIATION.md`. Every item below is grounded in code or a first-party
> report. This is a read-only assessment; no source was changed.

## Verdict

**Not production-certified — and the framework says so itself.** `VALIDATION.json`
sets `release_status: "release_candidate"`, `production_certified: false`;
`operations.doctor()` hard-codes `production_certified: False` with four external
gates. That is an honest, deliberate posture, not an unfinished build.

- **Code completeness: high.** A marker sweep found **no** `TODO`/`FIXME`/
  `NotImplementedError`/stub bodies in shipped code — only abstract-method signatures
  (`...`) and intentional `pass #` comments. All 57 modules are implemented and wired.
- **Correctness posture: strong.** 168 framework tests pass, 0 skipped in the RC8 run;
  dependency audit clean (pip-audit, 0 known vulns); durability races previously found
  were fixed test-first.
- **What is missing is *qualification*, not *implementation***: real-model quality,
  real external delivery, full-volume load/recovery, and credential/key custody. Plus a
  small set of genuine functional boundaries (§B) that are documented limits.

**Ready for production?** For a single owner on a qualified host, after the §C gates are
actually executed on real models/data: **yes, with eyes open**. As-is, out of the box:
**no** — it has never been run against a real LLM, real delivery channel, or
representative personal archive.

---

## A. Confirmed gaps in functionality (real, but scoped/known)

| # | Gap | Evidence | Impact |
|---|---|---|---|
| A1 | **Attachment content is stored but not text-extracted** | `source_runtime._attachment` completes the blob job with `'text_extraction':'not performed'` | Attachment bytes are retrievable by id but not searchable — no OCR/audio/text extraction. ROADMAP lists OCR/audio adapters as an extension. |
| A2 | **EML/MBOX one-shot importer drops attachment bytes** | `importers.py`: `"attachment_contents_imported":False`, `"[No text body; attachments not imported]"` | Legacy email import keeps only attachment *names*. (The live Gmail source adapter *does* fetch bytes via the blob pipeline — see A1.) |
| A3 | **Native `session_search` is not replaced** | `VALIDATION.json` `native_session_search_replaced: false`, `native_recall_recapture_withheld: false` | Native session_search results lack canonical lineage; recaptured content is withheld rather than tracked. |
| A4 | **No global/semantic forgetting** | `global_semantic_forgetting: false`; `reset.py` returns `physical_erasure: False` excluding native files/transcripts, external sources, backups, already-loaded prompts | Forgetting is logical + source-dependent. Content already shown to a user, copied into native state, WAL, backups, or external sources cannot be erased by the framework. |
| A5 | **Assistant echo retains copied content after source forgetting** | `VALIDATION.json` `confirmed_gaps[0]` | A generated assistant reply that copied source text keeps that text after the source is forgotten (lineage is by dependency, not by content match). |
| A6 | **Historical records are not auto-migrated** | `historical_records_migrated: false`; GAP_REMEDIATION §"Semantics and limits" | Upgrading an existing installation needs a separate migration/reconciliation; this RC targets fresh development only. |
| A7 | **100-parent lineage cap** | GAP_REMEDIATION; ingestion contract | Long sessions need hierarchical lineage; partially mitigated by `lineage.compact_parents` fan-in nodes (≤100 parents each, never truncated). |
| A8 | **Identity-less TUI/desktop contexts are denied** | `confirmed_gaps[3]`; `access.session_allowed` | No authenticated owner binding for TUI/desktop; only owner OS-account CLI/cron and explicit private chat identities are admitted. |
| A9 | **Cross-process store locking is cooperative** | `store.py` `Store.lock = threading.RLock()` | In-process only; cross-process safety relies on SQLite WAL + `busy_timeout` + the single `ProcessLease`. Correct by design, but not a distributed lock. |

---

## B. Architectural limits that are *by design* (not defects)

These are stated boundaries a deployer must accept, not bugs to fix:

- **No truth oracle.** Beliefs/consolidations remain `unverified`; conflicting sources are
  preserved, never silently resolved. Lessons are scoped advice; procedures still require
  an operator-installed capability. (`IMPLEMENTATION_PLAN.md`)
- **No arbitrary code execution.** Model-generated code is never compiled/run; only
  administrator-bound capabilities execute.
- **Ranked results are leads, not proof.** `no_relevant_evidence` and
  `retrieval_incomplete` both mean *unknown*, never *absent*.
- **Memory is untrusted data.** It cannot grant permission or override user instructions.
- **Single-owner boundary.** Group/channel/guild/public contexts are denied by design.

---

## C. Deployment-qualification gates (must be executed before "production")

From `VALIDATION.json.remaining_gates` + `doctor().external_gates`. None of these are
code gaps — they are real-world acceptance tests never run in this package:

1. **Real-model quality** — `llm_calls_tested: false`, `actual_llm_evaluated: false`,
   `live_llm_extraction: false`. All quality numbers come from a deterministic local
   model-protocol fixture + real MiniLM embeddings on 20–1226 synthetic records, not a
   live LLM.
2. **Representative user archive** — recall/identity/**abstention calibration**:
   `general_answerability_verified: false`, `unsupported_queries_abstention_calibrated: false`.
3. **Real external delivery** — `real_external_delivery: false`; task/reminder/cron
   notification delivery has never hit an actual channel adapter.
4. **Full-volume load & host-loss recovery** — `LOAD_RECOVERY_CHECK` objectives; only a
   100-record synthetic SIGKILL recovery was exercised.
5. **Storage encryption + credential/backup-key custody** — off-host backup and key
   management unqualified.
6. **CI matrix** — `ci_matrix_executed: false`; tested on Python 3.12.13 / 3.13.9 only.
7. **Installed adapter qualification** — real model/capability/delivery adapters and
   additional source/domain connectors.

---

## D. Test/CI observations (informational)

- **Windows suite has ~14 environmental teardown failures** (`WinError 32` unlinking a
  still-locked SQLite db during `tempfile` cleanup) — documented as cleanup-only, not
  logic regressions. One real Windows bug was found historically (`skill_export.py`
  `os.open(dir, O_RDONLY)` → `Errno 13`), since addressed.
- **Two deploy-gate test quirks are known non-regressions**: `test_production` real-uvicorn
  readiness is flaky under a 503 race; `test_retrieval` concurrent-semantic-init asserts
  one engine but yields zero under the `PERSONAL_MEMORY_DISABLE_SEMANTIC` kill-switch.
- Legit skips are environment gates only: production extras (`jsonschema`, `cryptography`,
  `uvicorn`) and the pinned-Hermes-contract tests (`HERMES_PROVIDER_CONTRACT`).

---

## E. Recommended pre-production checklist

- [ ] Run the full suite on the **target OS** (Linux host) with production extras installed; confirm 0 non-environmental failures.
- [ ] Execute a **live LLM** end-to-end turn (search → capture → claim → consolidate) and capture real quality/abstention numbers.
- [ ] Qualify a **real delivery adapter** for tasks/notifications (or leave delivery disabled).
- [ ] Run a **load + host-loss recovery drill** at expected archive volume; measure latency (esp. cold-start warmup ~7.7 s and the 200 ms prefetch SLA).
- [ ] Establish **backup-key custody** and an **off-host encrypted backup**; test `restore` with the current deletion ledger.
- [ ] Decide on **A1–A8** accept/mitigate: at minimum document that attachments are not text-searchable (A1) and that forgetting is logical, not physical (A4/A5).
- [ ] Verify `doctor` passes on the host: `pinned_host_patch`, `installed_provider_version`, `hindsight_default`, `parallel_investigation`, `live_service_version`.

---

*Re-verify this audit against `VALIDATION.json`, `operations.doctor()`, and the modules
cited whenever the framework version or the pinned Hermes release changes.*
