# Retrieval relevance gate

The default Hybrid backend now filters candidate sources before returning episodes, claims or graph neighbours. Common English grammatical words, generic numeric labels and bare numbers cannot establish lexical relevance. A content-word match or a semantic cosine score at least 0.5 admits a candidate for inspection. Rank fusion scores are never treated as confidence. The configurable floor is an engineering heuristic tested with multilingual MiniLM, not a universal threshold or a calibrated probability.

Operator retrieval configuration:

```json
{"relevance":{"semantic_minimum":0.5}}
```

A higher floor rejects more semantic candidates and can lose valid paraphrases. Keyword-only operation loses queries with no shared content words. Names, topical overlap, other languages' function words and semantically related but non-answering passages can still pass. This gate fixes obvious unrelated matches; it cannot determine general answerability. Real model/entailment evaluation and a separate held-out archive remain necessary before claiming reliable abstention.

Responses include:

- `candidates_found`: inspect the cited text; it may not answer the requested attribute.
- `no_relevant_evidence`: the bounded search found no candidates passing the gate; the answer remains unknown, not disproved.
- `retrieval_incomplete`: an engine failed or the semantic index was not ready, with no surviving candidates.

`evidence_sufficiency` remains `not_established` for every status. Rejected sources cannot leak claims through keyword matches. Progressive recall preserves the status; Hermes prefetch preserves it and provider guidance tells the model to avoid guessing. Source coverage and backend diagnostics remain available. Direct catalog browsing and custom operator backends are outside this default Hybrid gate.

The original synthetic report in `docs/synthetic-personal` is the unchanged pre-gate baseline. `docs/synthetic-gated` records the new run, including false rejections. The first trial used two lexical anchors and a 0.5 semantic floor; it rejected all three unsupported questions but reduced multilingual all-evidence top-5 retrieval to 21/24. The revised gate uses one content anchor, excludes bare numbers and retains 0.5. A 0.4 trial preserved 23/24 valid questions but still admitted candidates for the submarine question, so it was rejected. These settings were developed against these curated questions; results are developmental, not held-out validation.

Final measured result: all three unsupported questions return zero candidates for both lexical and semantic retrieval. Semantic all-evidence top-5 remains 23/24; lexical drops from the historical 20/24 to 17/24. The new run uses exact NumPy indexing, whereas the prior baseline used HNSW; this is not a controlled latency comparison. The unresolved English-to-Hindi bicycle-key question still retrieves non-answering topical candidates.
