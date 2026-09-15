# Phase 1 Hermes-loop answer-quality baseline

- hermes: `v2026.9.14` @ `345cd2b057a4`
- provider contract sha256: `f1fe9f9941d0bdab…`
- model: `Cobra91310/Ornith-1.5-9B-MTP-NVFP4-newhead:NVFP4`
- total scenarios: 7  | passed: 7  | failed: 0
- aggregate tokens: 19758 in / 5718 out over 21 api calls
- mean turn latency: 12.04s
- abstention correct: 1 / hallucinations: 0

## Per-scenario

| scenario | class | correct | latency (s) | tokens (in/out) | api_calls | detail |
| --- | --- | :-: | --: | --: | --: | --- |
| paraphrase-paint | paraphrase | yes | 10.25 | 2585/610 | 2 | must_any_ok=True must_none_hits=[] |
| cross-session-cabin | cross-session | yes | 10.38 | 3538/581 | 3 | must_any_ok=True must_none_hits=[] |
| correction-codename | correction | yes | 15.0 | 3653/1290 | 4 | must_any_ok=True must_none_hits=[] recency=ok (mentioned=True qualified=True) |
| contradiction-beverage | contradiction | yes | 14.64 | 2513/1223 | 3 | must_any_ok=True must_none_hits=[] conflict_hints=['both', 'conflict', 'contradict', 'record'] |
| compression-vault | compression | yes | 16.16 | 3121/1126 | 3 | must_any_ok=True must_none_hits=[] |
| interrupted-reservation | interrupted | yes | 11.34 | 2481/664 | 4 | must_any_ok=True must_none_hits=[] |
| unknown-passport | abstention | yes | 6.49 | 1867/224 | 2 | abstain_markers_hit=True fabricated=False |

## Answers (truncated)

### paraphrase-paint

```
Based on your stored preference — your favorite color is **cerulean** — a **cerulean** (cerulean-blue) shade of paint would be the one you'd enjoy most for a bedroom wall.

That's the only color preference I have on record, so it's the honest answer I can give you. If you want, tell me the room's vibe (calm/cozy, bright/airy, grounding) and I can suggest specific cerulean tones and finishes (matte vs. eggshell) that fit a bedroom best.
```

### cross-session-cabin

```
Based on my memory, here's what I found:

**WiFi password:** `Larchwood-42` — but this is a *reported* claim from a past conversation ("cabin-chat"), with no date, no verification, and no network name recorded.

A few caveats worth flagging before you rely on it:

- **No SSID stored.** I don't have the network name (e.g., "Larchwood" or similar), so you'd need to read that off the router when you arrive.
- **The recorded trip was "next month,"** not this weekend — so if the password changed since then, this may be stale.
- **It's unverified.** The claim came from a conversation, not from actually connecting at the cabin.

If you want, I can save the SSID too when you have it, or flag the password as "verify before the weekend" so it doesn't get treated as current. Want me to do either?
```

### correction-codename

```
The current codename is **Osprey**.

History: it was **Falcon** (recorded 2024-01-01), then the codename was changed to **Osprey** on 2025-06-15. Since the latest update is from June 2025, Osprey is the current codename.
```

### contradiction-beverage

```
Two conflicting records exist in memory, and I can't tell which is current without more context:

- **"I have not touched coffee in years; my morning drink is green tea."** (source: `bev-tea`)
- **"I start every morning with a large black coffee."** (source: `bev-coffee`)

Both are stored as equally-weighted first revisions with no timestamp, so they directly contradict each other. If you can tell me which is the current truth (or when the change happened), I'll reconcile them and remove the stale one so future questions give a clean answer.

Which is it — green tea or coffee?
```

### compression-vault

```
The combination to your attic vault is **amber-crystal-7**.

That's what's stored in your memory (source: `eval`, record `vault-target`). Nothing has changed since it was recorded, so that's the current value.
```

### interrupted-reservation

```
Everything my memory holds about your Luigi's reservation is exactly one record, and it's deliberately incomplete:

**Luigi's reservation**
- Source: `eval` (source_id: `luigis`), revision 1, document
- What's recorded: *"I booked a table at Luigi's for May 3rd, but I did not confirm whether it is lunch or dinner."*
- Verified facts:
  - There is a booked table at Luigi's.
  - Date: May 3rd.
  - Restaurant: Luigi's.
- Unconfirmed / unknown:
  - Meal time — lunch or dinner — is **not** established.
  - Time of day, number of guests, and party size are not recorded.

There's nothing else in the beliefs store, no conflicts, and no additional claims about this reservation. The only thing I actually know is that a table was booked for May 3rd at Luigi's; everything else remains unconfirmed.

Wa
```

### unknown-passport

```
I don't have any information about your passport number. My memory search came back with no relevant evidence — there's nothing stored about it.

If you've shared this before in a different session, it may not be indexed yet, or it may never have been recorded. I won't invent or guess a passport number, since that's sensitive identification data.

To get me to remember it: just tell me the number and I'll store it as a factual record so I can retrieve it next time.
```

## Calibration note

Step 2 measured MiniLM multilingual similarity 0.4894 on a zero-lexical-overlap paraphrase pair; the shipped default 0.5 floor would reject it. This baseline therefore uses semantic_minimum=0.35 in the ephemeral profile and flags the shipped default as an open calibration question.

## Scope statement

Real `hermes -z` oneshot turns against the ephemeral Hermes profile + deployed personal-memory provider + host llama.cpp model. Store seeded via direct /v1/ingest; canonical reset between scenarios. Production store and archive untouched.

## Proven in-loop vs still-mock-only

Proven through the real Hermes MemoryManager + deployed provider on `192.168.68.67`, no mocks:

- **Fix 1** — bare `personal_memory_search` runs `depth=balanced/limit=8` on a fresh round-trip; explicit `depth=fast/limit=4` reuses the automatic prefetch with zero new `/v1/search` calls. Two named checks pass in `scripts/verify_correctness_fixes.py`.
- **Fix 2** — `on_pre_compress` bumps the injection epoch and previously-suppressed evidence re-injects afterwards; a claim whose `status`/`valid_to` changes produces a different `_claim_fingerprint`; `already_in_context` reflects `_row_retained` accurately. Three named checks pass.
- **Fix 3** — `_host_message_id` returns `id:host-42` for a stable host id and the durable `message_capture_ids` table dedups across `sync_turn` and `on_session_end` even when the ordinal counter would collide; the no-host-id fallback still dedups to one capture. Four named checks pass.
- **Fix 4** — `RelevanceGate.assess` admits a lexical-overlap-free candidate purely from the semantic floor and denies below it; end-to-end a zero-content-word paraphrase ("What shade of paint do I love the most?" vs. "My favourite colour is cerulean.") is accepted via the semantic channel with `lexical_anchors=False` and `semantic_similarity≈0.49`. Two named checks pass.
- **Fix 5** — `personal_memory_browse`/`timeline` add no injected-exposure rows (retained-map unchanged before/after); `on_session_switch` scoped to `{old, new}` leaves a third session's state intact. Three named checks pass.
- **Live answer-quality** — seven `hermes -z` turns against the host llama.cpp model cover paraphrase, cross-session, correction, contradiction, compression, interrupted-record and unknown/abstention; all seven score correct on the re-run after the correction-scorer fix, with `hallucinations=0` and the abstention case explicitly refusing to invent a passport number.

Still covered only by deterministic mocks / unit fixtures (Phase 1 does not upgrade these):

- Retrieval-quality claims about Hindsight-adjacent behavior when the external engine is unavailable — `test_retrieval.py` uses `DeterministicEmbedder`, not a live semantic model at the boundary the service sees.
- LongMemEval / LoCoMo dataset scores — deferred to a later phase; Phase 1 uses project scenarios only.
- Adaptive controller (Section 2), event/conflict/identity/generalization extraction (Section 3), supported-connection paths (Section 4), verified-learning loop (Section 5), and evidence-per-token accounting (Section 6) — measured only as aggregate tokens / api_calls in this baseline; their behavior is unchanged and remains mock-covered.
- Cross-host production-store behavior — deliberately out of scope per design rule (3): evals run against ephemeral stores; only the live service deploy in Step 1 touched production, and that was file-sync + restart, not a data write.

## Open calibration question

The shipped default `relevance.semantic_minimum=0.5` sits above the measured MiniLM similarity (`0.4894`) for a genuinely related, zero-lexical-overlap paraphrase. The mechanism works at that threshold but the default suppresses valid semantic matches. This is a Phase-1 finding to carry into the Section 6 evidence-per-token / Section 2 adaptive-controller work; it is not a correctness bug in Fix 4 and is not fixed here.

## Correction-scorer history (honest baseline note)

Run #1 scored 6/7 with the correction case marked incorrect because the model wrote *“it was previously ‘Falcon’ from early 2024, now Osprey”* and the initial `must_none=["Falcon"]` heuristic penalised any mention of the superseded value. The behaviour was correct; the scorer was wrong. Run #2 uses `recency_qualification` (mention of the old value is only allowed when a temporal marker like “previously”/“was”/a past date is also present) and produces the 7/7 reported above. No provider code changed between the two runs.

