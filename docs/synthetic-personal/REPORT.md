# Synthetic personal memory evaluation

Fictional synthetic sources. No real LLM. Structured facts and identity confirmations supplied by the harness, not automatically inferred.

Archive: 1,226 records. Lifecycle checks: 30/30 passed.

| Retrieval | Single-source top 1 | All evidence top 5 | Median ms |
|---|---:|---:|---:|
| lexical | 15/23 | 20/24 | 8.42 |
| multilingual_hybrid | 16/23 | 23/24 | 88.43 |

Top-1 excludes the two-source contradiction case. Top-5 includes all 24 questions and requires every expected source, including both conflicting reports.

## Retrieval misses

- lexical: मेरी गाड़ी का इंजन किसने ठीक किया था? Expected ['garage']; retrieved ['hindi'].
- lexical: Gaadi repair karane ke liye Mira ne kaunsi jagah batayi thi? Expected ['garage']; retrieved ['hinglish', 'school', 'same-name', 'group-unknown', 'recommend'].
- lexical: मेरा पासपोर्ट कहाँ रखा है? Expected ['passport']; retrieved ['hindi'].
- lexical: Where is my blue bicycle key? Expected ['hindi']; retrieved ['old-address', 'passport', 'new-address', 'same-name', 'book'].
- multilingual_hybrid: Where is my blue bicycle key? Expected ['hindi']; retrieved ['background-0077', 'background-0069', 'background-0146', 'background-0058', 'background-0539'].

## Unsupported questions

- lexical: What is my submarine registration number? Returned 5 candidates; sufficiency `not_established`.
- lexical: What is my blood type? Returned 5 candidates; sufficiency `not_established`.
- lexical: What is my private helicopter tail number? Returned 5 candidates; sufficiency `not_established`.
- multilingual_hybrid: What is my submarine registration number? Returned 5 candidates; sufficiency `not_established`.
- multilingual_hybrid: What is my blood type? Returned 5 candidates; sufficiency `not_established`.
- multilingual_hybrid: What is my private helicopter tail number? Returned 5 candidates; sufficiency `not_established`.

## Lifecycle checks

- PASS: idempotent archive replay
- PASS: missing mandatory provenance rejected
- PASS: candidate identity does not expose group history
- PASS: confirmed contact connects old group evidence
- PASS: phone reuse separates owners
- PASS: unknown dates not assigned through bounded ownership
- PASS: overlapping confirmed phone ownership rejected
- PASS: historical address preserved
- PASS: current address excludes expired value
- PASS: conflicting reports surfaced
- PASS: explicit correction resolves conflict
- PASS: contextual preference takes priority
- PASS: fabricated evidence quote rejected
- PASS: two-hop graph preserves sourced connections
- PASS: health units normalize without mixing people
- PASS: incompatible health unit rejected
- PASS: task cannot bypass unfinished dependency
- PASS: dependency completion unlocks task
- PASS: stale task update rejected
- PASS: procedural candidate passes executed fixture suite
- PASS: proposer cannot self-promote
- PASS: regressive lesson fails executed fixture suite
- PASS: failed lesson cannot replace active procedure
- PASS: bound procedure executes validated fixture
- PASS: reviewed consolidation summary searchable
- PASS: forget removes source search and derived summary
- PASS: reimport cannot resurrect forgotten source
- PASS: forget invalidates learned procedure
- PASS: injection text remains data and leaves other evidence live
- PASS: memory survives database reopen

## Limits

- Curated retrieval questions are not a held-out statistical benchmark.
- Structured facts, identity confirmation and graph edges are supplied explicitly, not extracted by a real LLM.
- Learning/execution use deterministic fixture adapters; this does not measure real-world skill improvement.
- Unsupported queries may return unrelated candidates; abstention is not calibrated.
- No real LLM prompt-injection or answer-generation evaluation.
- 1,226 records do not qualify five years of full-volume production traffic.

## Reproduce

Run from the project root:

```sh
python scripts/evaluate_synthetic_personal.py --model-path /absolute/path/to/minilm --output-dir docs/synthetic-personal
```

Omit `--model-path` for lexical and lifecycle-only benchmark runs. FastEmbed is a core runtime dependency; `--model-path` selects the local model used by this benchmark semantic channel. The archive and questions are generated deterministically; isolated temporary databases are removed after each run.
