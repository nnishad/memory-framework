# Request-driven parallel memory investigation

Hermes interprets the request and writes a plan using the native `personal_memory_investigate` tool. The server does not pretend that keyword splitting understands intent, and it does not require a second planning-model call. The tool is available in the provider's 20-tool registry and at the authenticated read endpoint `POST /v1/investigate`.

For “Who from the old cycling group can repair my bicycle, and who recommended them?”, Hermes can supply:

```json
{
  "goal": "Find a bicycle repair contact from past group conversations and their recommender",
  "branches": [
    {"id": "group_offer", "intent": "Find a sourced repair offer", "queries": ["bicycle brakes repair", "cycle mechanic group"]},
    {"id": "recommendation", "intent": "Find a personal recommendation", "queries": ["bicycle repair recommended", "cycle mechanic recommendation"]}
  ],
  "graph_hops": 2,
  "limit": 18,
  "text_budget": 16000,
  "timeout": 10
}
```

Hermes reads the returned sources, resolves a discovered name/account through existing entity and identity tools, then refines an unresolved branch using that evidence. It must not invent entity IDs or treat shared group membership as identity confirmation. Query translations and transliterations are supported as explicit variants: the server does not generate them itself.

## Execution and efficiency

- One request contains up to six independently scoped branches with up to three variants each. Equivalent searches within the request execute once.
- A service-wide pool runs at most three branch searches concurrently. A twelve-job admission bound prevents unbounded queue growth across requests. Overload returns an explicit busy error rather than starting extra work.
- Each branch uses the existing hybrid backend, including configured embeddings. Its three variants execute within one backend call. Embedding execution may serialize behind the model's own lock; concurrency is not a claim of linear speedup.
- Results merge in round-robin order so a noisy branch cannot consume the entire context. Stable record IDs deduplicate shared evidence. Equal text in different sources remains separate to preserve provenance and filters.
- Canonical records and branch filters are checked again after parallel retrieval, under the store writer lock. Sources forgotten while searches run cannot reappear in the merged response.
- The response deadline is 1–25 seconds. Pending calls are cancelled; already-running calls may finish later within the globally bounded pool. Backend timeout behavior still matters, and service shutdown waits for workers.
- Record text plus graph evidence uses the declared text-character budget. JSON metadata and plan text are additional overhead. Each branch retrieves at most eight candidates; source browsing remains available for exhaustive work.

## Connecting dots

When explicitly requested, graph traversal starts from at most three non-account entities attached to selected, unfiltered evidence. It follows up to two hops of stored, source-backed reported relations, with at most twelve edges per seed, and returns relation endpoints, predicate, source quotes, validity dates and seed IDs. `graph_truncated` exposes clipping.

Graph traversal uses current relation validity. It never widens source/time/entity-filtered branches. For historical or explicitly scoped graph investigations, Hermes uses the existing structured `personal_memory_knowledge` graph operation with a deliberate entity and date. Relationships must already have been extracted or supplied with evidence; this executor does not invent missing links.

Health aggregation, dated beliefs, conflicting values, tasks and evaluated procedures remain typed operations in `personal_memory_knowledge`. The model can select these alongside search instead of interpreting numerical similarity as a health calculation.

## Evidence requirements and stopping

Each branch returns its intent, actual queries, filters, selected record IDs, incomplete status and remaining unselected candidates. `unresolved_requirements` identifies branches without evidence in the returned context or with failures. It is not a list of all unanswered questions: every branch also appears in `verification_required`, and `answer_verified` is always false. Hermes must check whether the evidence answers the requested attribute.

Primary query content anchors, stronger lexical support for alternate variants, and the backend semantic floor reduce query-expansion drift. For example, a blood-type query expanded to “ABO blood group” must not match only “group”. This remains a heuristic; overly broad or incorrect plans can still retrieve topical non-answers.

Provider guidance instructs Hermes to refine only unresolved needs using discovered evidence and to avoid repeating unchanged broad plans. After two unsuccessful investigation calls it should report the uncertainty or perform an explicitly needed scoped browse. This is model guidance, not an enforced cross-tool counter. Memory evidence cannot authorize external actions.

## Validation

The test suite covers concurrent execution using a synchronization barrier, shared-search deduplication, fair merging, source/time filters, deletion during retrieval, partial failure, response deadlines, output budgets, queue admission, graph scoping, endpoint authorization and query-expansion drift.

`python scripts/check_parallel_plans.py` runs six explicitly authored plan fixtures against the 1,226-record fictional archive using only the lexical backend. The report is `docs/PARALLEL_PLAN_CHECK.json`. This tests the execution interface and demonstrated plans, not autonomous planning quality from a real Hermes model. The framework still needs representative real-model planning, answerability and archive-scale latency evaluation before those capabilities can be called production-qualified.
