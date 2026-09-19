# Attachment Text Extraction (S1) — Design

**Date:** 2026-09-19
**Sub-project:** S1 of the production-readiness remediation program (covers audit gaps **A1** and **A2**)
**Status:** Approved design — pending implementation plan
**Source audit:** `docs/PRODUCTION_READINESS.md`

---

## 1. Goal

Turn stored-but-unread attachment bytes into **searchable, provenance-linked evidence** so a query
like *"find the invoice number in that PDF/image attachment"* works. Today the framework fetches and
stores attachment bytes faithfully but never reads them: the source runtime completes the attachment
job with a hardcoded `'text_extraction': 'not performed'`
([`source_runtime.py:334`](../../../personal_memory/source_runtime.py)), and the offline email
importer records attachment **filenames only** and drops the bytes
([`importers.py:44,57-58`](../../../personal_memory/importers.py)).

Two audit gaps are addressed:

- **A1** — Attachments stored but not text-extracted (source-runtime path; bytes already captured).
- **A2** — `importers.emails` (EML/MBOX) drops attachment bytes entirely (`attachment_contents_imported: False`).

## 2. Locked decisions

| Decision | Choice | Rationale |
|---|---|---|
| Extraction engine | **Vision-LLM primary** — OpenAI-compatible `/chat/completions` with the image as a base64 data URL | User's model at a configurable endpoint supports images; most robust for layout/tables/handwriting |
| Where text lives | **Derived evidence record** (`origin='derived'`, parented to the attachment's record) | Reuses ingestion → FTS/semantic/graph indexing, retrieval, and lifecycle forgetting-cascade for free |
| Execution model | **Separate durable `extraction` job** in the existing `source_jobs` queue | Reuses lease/fencing/retry/quarantine; decouples slow network work from fast byte storage |
| Default state | **ON as a kill-switch** (`enabled: true`); real gate is "`base_url`+`model` configured" | Honors the standing *default-on with graceful degradation* preference; skips safely when unconfigured |
| PDF handling | **Text-layer first** (deterministic, no model); **vision-render scanned pages** as fallback | Born-digital PDFs are exact and cheap; only scans pay the model cost |
| New dependencies | **`pypdfium2` + `Pillow`** as core `[project] dependencies`, pinned in `deployment/constraints-tested.txt` | pypdfium2 does PDF text + page rendering in one poppler-free wheel; Pillow encodes rendered pages to PNG and downscales oversized images |

## 3. Current-state evidence (grounding)

- **Attachment storage is complete and independent.** `SourceRuntime._attachment(cid)` claims a
  `kind='attachment'` job, fetches raw bytes via `adapter.attachment(...)`, verifies the size, and
  stores them chunked + sha256-verified through `blobs.begin/put/complete`, then calls
  `complete_job(job, {'blob_id':..., 'stored':True, 'text_extraction':'not performed'})`.
- **Blob store has no text column.** `memory_blobs(id, record_id, filename, mime, size, sha256, state)`
  and `memory_blob_chunks(blob_id, chunk_index, data BLOB)`. `blobs.read(store, blob_id, index)` returns
  base64 chunks; `blobs.listing(record_id)` already flags `content_is_untrusted: True`.
  Blob identity is **record-scoped**: `blob_id = 'blob_' + digest([record_id, filename, mime, size, sha256])[:32]`.
- **Provenance + lifecycle already support derived records.** Records carry
  `provenance.origin` and `provenance.parent_record_ids`
  ([`source_sync.py:510-511`](../../../personal_memory/source_sync.py)); `lifecycle.py` cascades
  retirement to **derived descendants**. A derived record parented to the attachment's record therefore
  inherits correct forgetting behavior.
- **Durable job machinery exists.** `SourceSync.claim_job(owner, *, kinds=(), ttl=300, connection_id=None)`,
  `complete_job(job, result=None)`, `fail_job(job, *, reason, retry_after=60, quarantine=False)`,
  and `_job_guard(db, job, state)` for lease fencing. `SourceRuntime.tick()` already calls
  `_attachment(cid)` per connection; `_extraction(cid)` slots in beside it.
- **Config plumbing.** Public `config.json` is restricted to `PUBLIC_KEYS = {port, prefetch_wait_ms,
  session_access, retrieval}` ([`configuration.py`](../../../personal_memory/configuration.py)).
  `hindsight_runtime.normalize()` validates `retrieval.hindsight` and permits
  `llm_provider/llm_model/llm_base_url`, forwarded to the Hindsight daemon as env vars. **That LLM is
  Hindsight's own**; conflating it with our extractor would be wrong. The framework convention is
  **base_url/model in config, API key in env** (no key ever in config).
- **No chat/vision-LLM client exists today.** The reranker is a local cross-encoder; the learning
  "planner" is a local ONNX intent classifier. S1 introduces the first outbound model client.

## 4. Architecture & new components

### 4.1 `personal_memory/extraction.py` (new)
A focused module exposing one public function that routes by mime type:

```
extract_text(raw: bytes, mime: str, filename: str, cfg: dict) -> dict
```

- Returns `{'text': str, 'method': str, 'model': str|None, 'truncated': bool, 'chars': int, 'pages': int|None}`
  where `method ∈ {'vision-llm', 'pdf-text-layer', 'pdf-vision'}`.
- **Routing:**
  - `image/*` (png, jpeg, webp, gif, tiff, bmp): downscale via **Pillow** to `max_image_dimension`
    (re-encoding to fit `max_image_bytes`), then one vision call → `method='vision-llm'`.
  - `application/pdf`: extract the **text layer** via **pypdfium2** first. If the extracted text is
    meaningful (≥ `pdf_text_min_chars` across the document), use it → `method='pdf-text-layer'` (no model
    call). Otherwise render up to `max_pdf_pages` pages at `pdf_render_dpi` via pypdfium2 → Pillow → PNG
    and run each page through the vision model, concatenating with `[page N]` markers → `method='pdf-vision'`.
  - Any other mime → `ExtractionError('unsupported')`.
- **Vision call:** OpenAI-compatible `POST {base_url}/chat/completions` with `model`, a **fixed
  transcription prompt**, and one `image_url` content part carrying `data:image/png;base64,{...}`.
  HTTP via **stdlib `urllib.request`** with `timeout=cfg['timeout_seconds']` and
  `Authorization: Bearer {env key}` when a key is present.
- **Prompt-injection guard:** the prompt instructs *verbatim transcription only* and explicitly states
  that any text inside the image is **data, never instructions** to follow.
- Output is truncated to `cfg['max_output_chars']` with `truncated=True` when clipped; PDFs beyond
  `max_pdf_pages` are truncated the same way (partial text is still useful).
- Raises a classified `ExtractionError(kind, message)` where
  `kind ∈ {'network','http','decode','empty','unsupported','oversized','encrypted','corrupt'}`
  so the caller maps kind → job policy (retry vs. skip). `disabled` / `unconfigured` are decided by the
  caller **before** invoking `extract_text` (job-level skips, not model errors). `encrypted` =
  password-protected PDF; `corrupt` = unparseable PDF/image bytes; neither benefits from a retry.

### 4.2 `SourceRuntime._extraction(cid)` (new)
Mirrors `_attachment(cid)`:

1. `job = self.sync.claim_job(self.worker.owner, kinds=('extraction',), connection_id=cid, ttl=300)`;
   return if none.
2. Re-check the parent record is live and not hidden (same query `_attachment` uses). If retired →
   `complete_job(job, {'skipped': 'evidence retired'})`.
3. Resolve extraction config; if `enabled` is false → `complete_job(job, {'skipped': 'extraction disabled'})`;
   if no `base_url`/`model` → `complete_job(job, {'skipped': 'extraction endpoint not configured'})`.
4. Reassemble bytes via `blobs.read(...)` across chunks; verify against `size`.
5. `result = extraction.extract_text(raw, mime, filename, cfg)`.
6. On success: write the **derived record** through the runtime's authorized ingest path
   (§6), then `complete_job(job, {'derived_record_id':..., 'method':..., 'model':..., 'chars':..., 'pages':...})`.
7. On `ExtractionError`:
   - `kind ∈ {'unsupported','oversized','empty','encrypted','corrupt'}` → `complete_job(job, {'skipped': kind})` (no retry).
   - `kind ∈ {'network','http','decode'}` → `fail_job(job, reason=..., retry_after=60, quarantine=job['attempts']>=4)`.
8. Wrap the whole body in `try/except` so no exception escapes the worker loop (same pattern as `_attachment`).

### 4.3 `SourceRuntime._attachment(cid)` (modified)
After `blobs.complete(...)` and the existing `complete_job(...)`, **enqueue** an `extraction` job
for the connection. Enqueue reuses the framework's real job API: a thin public
`SourceSync.enqueue_job(connection_id, kind, dedupe_key, payload)` wrapper around the existing
`_enqueue_job(db, ...)` (which opens its own `BEGIN IMMEDIATE` transaction and does
`INSERT OR IGNORE INTO source_jobs`). Call it with `kind='extraction'`,
`dedupe_key='extraction:' + blob_id`, `payload={record_id, blob_id, filename, mime, size}`.
`extraction` is added alongside the existing `attachment` and `projection` kinds, and the UNIQUE
`dedupe_key` makes the enqueue **idempotent** (re-attaching the same blob enqueues at most one job).
Attachment storage remains fast and fully independent of extraction.

### 4.4 `SourceRuntime.tick()` (modified)
Call `self._extraction(cid)` alongside the existing `self._attachment(cid)` per active connection,
guarded by the same `if not self.stop.is_set():` check.

### 4.5 Dependencies (new core)
- `pypdfium2` — PDF text-layer extraction **and** page rendering in one wheel; bundles PDFium, no system
  packages (poppler-free, Windows-friendly).
- `Pillow` — encodes rendered PDF pages to PNG for the vision data URL, and downscales oversized images.

Both are added to `[project] dependencies` in `pyproject.toml` and pinned in
`deployment/constraints-tested.txt`, so `deployment/install.sh`
(`pip install -c deployment/constraints-tested.txt '.[production]'`) installs them with no extra step.

## 5. Configuration

New namespace nested under the already-public `retrieval` key (so `PUBLIC_KEYS` is unchanged):

```json
"retrieval": {
  "attachment_extraction": {
    "enabled": true,
    "base_url": "http://192.168.68.67:8080/v1",
    "model": "<vision-model-id>",
    "timeout_seconds": 60,
    "max_image_bytes": 8388608,
    "max_image_dimension": 1568,
    "max_output_chars": 20000,
    "max_pdf_pages": 20,
    "pdf_render_dpi": 150,
    "pdf_text_min_chars": 32
  }
}
```

- **`enabled` defaults `true`** and acts as a **kill-switch**. Extraction actually runs only when a
  valid `base_url` **and** `model` are present; otherwise the job completes with `{skipped: 'extraction
  endpoint not configured'}` (graceful degradation — no error, attachment storage unaffected).
- **Env kill-switch** `PERSONAL_MEMORY_DISABLE_EXTRACTION=1` also disables extraction, matching the
  framework's dual config+env convention for rerank (`retrieval.rerank.enabled=false` /
  `PERSONAL_MEMORY_DISABLE_RERANK=1`). When set, jobs complete `{skipped:'extraction disabled'}`.
- **API key from env** `PERSONAL_MEMORY_EXTRACTION_API_KEY`; **never** accepted in config. A missing key
  is allowed (local servers may ignore auth); a present key is sent as a bearer token.
- **Validation** added to `hindsight_runtime.normalize()` (or a small dedicated normalizer it calls):
  - `enabled` must be bool.
  - `base_url`, if present, must be a string starting `http://` or `https://`, length ≤ 4000.
  - `model`, if present, must be a non-empty string ≤ 200.
  - `timeout_seconds` must be int/float in `0 < x <= 300`.
  - `max_image_bytes` must be int in `1 .. 1 GiB` (matches the blob cap).
  - `max_image_dimension` must be int in `64 .. 8192` (longest edge after downscale).
  - `max_output_chars` must be int in `1 .. 100000`.
  - `max_pdf_pages` must be int in `1 .. 500`.
  - `pdf_render_dpi` must be int in `50 .. 600`.
  - `pdf_text_min_chars` must be int in `0 .. 100000` (text-layer sufficiency threshold).
  - Unknown keys in `attachment_extraction` raise `ValueError` (closed set, mirrors `retrieval.hindsight`).
- `base_url`/`model` are **not** required at config time (so the default-on, unconfigured host stays valid);
  they are required at extraction time, enforced by the `unconfigured` skip.

## 6. Derived-record shape

Written through the runtime's authorized ingest path (the same channel `source_sync._apply_operation`
uses), so namespace and provenance rules are enforced:

| Field | Value |
|---|---|
| `source` | the connection's evidence namespace (same as the parent record) |
| `source_id` | `'attachment-text:' + blob_id` — **deterministic**, so re-runs upsert (idempotent, no duplicates) |
| `kind` | `'attachment_text'` |
| `occurred_at` | inherited from the parent record's `occurred_at` |
| `text` | the extracted text (truncated to `max_output_chars`) |
| `provenance.origin` | `'derived'` |
| `provenance.parent_record_ids` | `[<attachment's record_id>]` — drives the lifecycle forgetting-cascade |
| `metadata.extraction` | `{method:'vision-llm'|'pdf-text-layer'|'pdf-vision', model, mime, filename, blob_id, chars, pages, truncated, content_is_untrusted:true, prompt_injection_guard:true}` |

Because it is an ordinary record, it is indexed by FTS + semantic + graph automatically and returned by
`search`/`recall`/`investigate` like any other evidence, with provenance pointing back to the source record.

## 7. Data flow

```
source bytes ─▶ _attachment: store blob (UNCHANGED) ─▶ enqueue 'extraction' job {record_id, blob_id, mime, size}
                                                                   │
tick ─▶ _extraction: claim job ─▶ record live? ─▶ enabled? ─▶ base_url+model configured?
                                                                   │ yes
                       blobs.read (reassemble + verify size) ─▶ extract_text
                         route: image/* → vision-LLM ; application/pdf → text-layer, else render+vision
                                                                   │ text
                       write DERIVED record (origin=derived, parent=[record_id],
                                             source_id='attachment-text:'+blob_id)
                                                                   │
                       complete_job {derived_record_id, method, model, chars}
                                                                   │
                       ──▶ normal ingestion ──▶ FTS + semantic + graph index ──▶ searchable
                       ──▶ lifecycle.py ──▶ retiring parent cascades to this derived record
```

## 8. Format scope

**v1 (in scope):**
- `image/*` — `png, jpeg, webp, gif, tiff, bmp` — via the vision model (downscaled to `max_image_dimension` first).
- `application/pdf` — text-layer extraction (born-digital) **or** per-page vision rendering (scanned),
  bounded by `max_pdf_pages` / `pdf_render_dpi`.

**Explicitly skipped** (`complete_job {skipped:...}`, never retried): unsupported mime, image still
exceeding `max_image_bytes` after downscale, empty/whitespace output, encrypted PDF, corrupt bytes,
retired parent, extraction disabled, endpoint unconfigured.

**Deferred follow-ups (out of S1):**
- Office formats (`.docx/.xlsx/.pptx`).
- Per-page hybrid PDFs (text on some pages, scans on others) — v1 decides text-layer vs. vision at the
  **document** level, not per page.

## 9. Error-handling matrix

| Condition | Policy | Attachment bytes |
|---|---|---|
| Endpoint not configured | `complete_job {skipped:'extraction endpoint not configured'}` | safe (already stored) |
| `enabled: false` or `PERSONAL_MEMORY_DISABLE_EXTRACTION=1` | `complete_job {skipped:'extraction disabled'}` | safe |
| Unsupported mime | `complete_job {skipped:'unsupported'}` | safe |
| Image still > `max_image_bytes` after downscale | `complete_job {skipped:'oversized'}` | safe |
| Encrypted / password-protected PDF | `complete_job {skipped:'encrypted'}` | safe |
| Corrupt / unparseable PDF or image | `complete_job {skipped:'corrupt'}` | safe |
| Empty/whitespace output | `complete_job {skipped:'empty'}` | safe |
| PDF longer than `max_pdf_pages` | extract first `max_pdf_pages`; derived record `truncated=true` | safe |
| Parent retired mid-flight | `complete_job {skipped:'evidence retired'}` | safe |
| Network error / timeout | `fail_job(retry_after=60)`; `quarantine` after 4 attempts | safe |
| Non-2xx HTTP | `fail_job(retry_after=60)`; `quarantine` after 4 attempts | safe |
| Malformed model response | `fail_job(retry_after=60)`; `quarantine` after 4 attempts | safe |

**Invariant:** extraction failures never block, lose, or quarantine the attachment-storage job. The two
jobs are independent; bytes are committed before extraction is ever attempted.

## 10. Security & trust

- Extracted text is **untrusted input**. The derived record carries `content_is_untrusted: true` and
  `prompt_injection_guard: true` in metadata; its text is evidence and is never interpreted as instructions.
- The transcription prompt explicitly tells the model to treat all in-image text as data.
- API key is read **only** from env `PERSONAL_MEMORY_EXTRACTION_API_KEY`; config rejects any key field.
- `base_url` must be `http(s)`; the request is a plain outbound POST from the service process (the existing
  browser-origin rejection in `asgi.py` is unrelated and unchanged).
- Derived records are `origin='derived'` + `kind='attachment_text'`, so they are never conflated with
  user- or assistant-authored content.

## 11. Observability

- `@traced` on the outbound model call; a single INFO line per extraction (trace.py convention) with
  trace-id, blob_id, model, chars, latency, and outcome (`derived|skipped:<kind>|failed`).
- Job result and derived-record metadata both record `method`, `model`, `chars`, `truncated`.

## 12. Phase 2 — A2 (offline importer byte capture)

`importers.emails` yields records with attachment **filenames only**, no bytes, and no `record_id`
(the id is assigned at ingest). Fix = two-phase import:

1. Ingest the email record as today (unchanged generator output).
2. For each attachment, `blobs.upload(client, path_or_bytes, record_id, mime)` against the newly assigned
   `record_id`, then **enqueue the same `extraction` job** from §4.3.

This reuses all Phase-1 machinery (extraction client, job, derived record, config, indexing, cascade);
only the **byte-capture path** in the importer/CLI is new. `importers.emails` gains an option to surface
attachment payloads (bytes or on-disk temp paths) instead of only filenames, and sets
`attachment_contents_imported: True` when captured. Sequenced **after** Phase 1 so S1 ships incrementally.

## 13. Testing plan (TDD; no live network in CI)

The model is stubbed with a local fake HTTP transport (monkeypatched `urllib.request.urlopen` or an
in-process socket server). The real `192.168.68.67:8080` endpoint is exercised **only** in manual live
validation, never in automated tests.

**`extraction.py` unit tests**
- Request shape: correct `model`, base64 `data:{mime}` URL, transcription prompt, bearer header iff env key set, timeout applied.
- Success returns text + method + model + chars.
- Empty/whitespace output → `ExtractionError('empty')`.
- Image still exceeding `max_image_bytes` **after downscale** → `ExtractionError('oversized')`.
- Unsupported mime → `ExtractionError('unsupported')`.
- Network error / timeout → `ExtractionError('network')`.
- Non-2xx → `ExtractionError('http')`.
- Malformed JSON body → `ExtractionError('decode')`.
- Output longer than `max_output_chars` → `truncated=True`, clipped.
- **PDF text-layer:** a born-digital PDF returns text with `method='pdf-text-layer'` and **no** model call.
- **PDF scanned:** an image-only PDF renders pages and calls the model per page → `method='pdf-vision'`,
  `[page N]` markers, `pages` set.
- **PDF page cap:** a PDF longer than `max_pdf_pages` processes only the first N and sets `truncated=True`.
- **Encrypted PDF** → `ExtractionError('encrypted')`; **corrupt PDF/image bytes** → `ExtractionError('corrupt')`.
- **Image downscale:** an image larger than `max_image_dimension` is downscaled before the call.

**Config validation tests**
- Defaults: `enabled` true; unconfigured `base_url`/`model` accepted at load.
- Rejects: bad `enabled` type, non-http(s) `base_url`, empty/oversized `model`, out-of-range
  `timeout_seconds`/`max_image_bytes`/`max_output_chars`, unknown key in `attachment_extraction`.
- Key never read from config; env key used as bearer.
- Env kill-switch `PERSONAL_MEMORY_DISABLE_EXTRACTION=1` forces the `extraction disabled` skip even when `enabled: true`.

**Job-wiring tests (`source_runtime`)**
- Attachment completion enqueues exactly one `extraction` job with the right payload.
- `_extraction` claims it, writes the derived record with correct `source_id`, `origin='derived'`,
  `parent_record_ids=[record_id]`, `kind='attachment_text'`, and completes the job.
- Disabled → `{skipped:'extraction disabled'}`; unconfigured → `{skipped:'extraction endpoint not configured'}`.
- Unsupported/oversized/empty → `{skipped:<kind>}` (no retry, no derived record).
- Network/http/decode → `retry_wait`, then `quarantined` after 4 attempts.
- Parent retired mid-flight → `{skipped:'evidence retired'}`.
- **Idempotency:** re-running extraction on the same blob yields the **same** derived record id; no duplicate.

**Integration tests**
- End-to-end: attachment (fake image) → extraction (stub model) → derived record → `search`/`recall`
  returns the attachment text with provenance to the parent.
- **Lifecycle cascade:** retiring the parent record also retires/forgets the derived `attachment_text` record.

**Phase 2 (A2) tests**
- `importers.emails` surfaces attachment payloads; post-ingest `blobs.upload` + extraction enqueue occur;
  `attachment_contents_imported` becomes `True`; the derived record is searchable.

## 14. Non-goals

- No local OCR engine (Tesseract/EasyOCR/PaddleOCR) and no OpenCV preprocessing in S1 — scanned PDFs and images go through the vision model.
- No Office-format extraction (`.docx/.xlsx/.pptx`) and no per-page hybrid PDF handling (deferred, §8).
- No change to the retrieval pipeline, the closed `workflows.py` job types (`consolidate`/`evaluate`),
  or `AdaptiveRecall`'s `{planner, reranker}` config keys.
- No change to `PUBLIC_KEYS`; extraction config nests under the existing `retrieval` key.
- New **core** dependencies are limited to `pypdfium2` (PDF text + rendering) and `Pillow` (PNG encode + downscale); no other new runtime deps.

## 15. Success criteria

1. An image **or PDF** attachment on a configured host produces a searchable `attachment_text` derived
   record whose provenance points at the source record. Born-digital PDFs use the deterministic text
   layer with **no** model call; scanned PDFs and images use the vision model.
2. Attachment byte storage is provably unaffected by any extraction outcome (all §9 rows leave bytes safe).
3. Re-running extraction is idempotent (no duplicate derived records).
4. Retiring a parent record cascades to its derived attachment text.
5. On an unconfigured host the feature is inert: jobs complete `{skipped:'extraction endpoint not configured'}`,
   no errors, no network calls.
6. All new tests pass offline (stubbed model); `pip-audit` stays clean.
7. A2: EML/MBOX import captures attachment bytes and produces the same searchable derived text.
8. `pypdfium2` and `Pillow` are declared core dependencies and pinned in `deployment/constraints-tested.txt`;
   a fresh `.[production]` install runs both image and PDF extraction with no extra steps.
