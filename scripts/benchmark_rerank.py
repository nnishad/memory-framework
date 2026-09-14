"""Cross-encoder re-ranking benchmark: does the *answering* passage rise to rank 1?

Isolates a different weakness than the temporal benchmark. Reciprocal-rank fusion
(keyword FTS5 here) ranks by term statistics, so a lexically-similar **distractor**
that repeats the query's salient noun but does NOT answer it can outrank the passage
that actually answers. A learned cross-encoder scores (query, passage) relevance and
should promote the answer. Runs keyword-only (no semantic / GPU) so the ONLY variable
is the re-ranker; the fusion path under test is identical to production.

  --rerank off  -> baseline (fusion order only)
  --rerank on   -> enable the cross-encoder re-ranker of the fused top-k

Each "rerank" case has one answer doc that shares a content term with the query (so
keyword retrieval surfaces it and the relevance gate accepts it) but a distractor with
stronger term overlap. "guard" cases are ones keyword retrieval already ranks first and
that re-ranking must not break. Requires sentence-transformers + a downloadable model
for --rerank on; run it on the GPU venv.
"""
import argparse
import json
import math
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store

# Same recent date for everything: recency is NOT the axis under test (temporal stays off).
WHEN = "2025-01-15T12:00:00Z"
CORPUS = [
    # --- rerank targets: answer (a_*) is retrievable but a distractor (d_*) wins on lexics ---
    ("a_penicillin", "The nurse flagged my allergy chart: I am allergic to penicillin, so that medication was removed."),
    ("d_penicillin", "I keep tracking every allergic reaction because allergic symptoms and allergic episodes have been allergic concerns for years."),
    ("a_rao", "Dr. Rao is my cardiologist; I see Dr. Rao every quarter for my heart."),
    ("d_rao", "The hospital lists every cardiologist on staff, and the cardiologist on call changes almost weekly."),
    ("a_julia", "My thesis was written entirely in Julia, which I chose for the numerics."),
    ("d_julia", "Every language I teach treats each language as a system; a formal language is still a language."),
    ("a_ssid", "Our home wireless network name is CasaRoja and every device connects to CasaRoja."),
    ("d_ssid", "The wifi network kept dropping my home connection, so I factory reset the wifi network router at home twice."),
    ("a_renew", "I renewed my gym membership for twelve months starting this January."),
    ("d_renew", "The gym is open twenty four hours; I go to the gym for hours and the gym is my second home."),
    ("a_pass", "My grandmother's birthday is the eleventh of June; I call her every year on June."),
    ("d_pass", "Birthdays are nice; I love birthday cake and a birthday party with many birthday balloons."),
    ("a_pin", "The PIN for my office locker is 4417; my locker PIN is stored in the safe."),
    ("d_pin", "My office has a PIN code policy; the office PIN must be changed and the office PIN is confidential."),
    # --- guards: keyword retrieval already ranks the answer first; rerank must not break ---
    ("g_pepper", "My pet cat is called Pepper and Pepper sleeps on my chair."),
    ("g_tokyo", "I will be travelling to Tokyo in December; Tokyo is where my conference is held."),
    ("g_bike", "The bicycle I ride every day is a red Trek bicycle kept in the garage."),
]

# kind, query, expected, distractor, category
CASES = [
    ("rerank", "What medication am I allergic to?",            "a_penicillin", "d_penicillin"),
    ("rerank", "Who is my cardiologist?",                      "a_rao",        "d_rao"),
    ("rerank", "Which language did I write my thesis in?",     "a_julia",      "d_julia"),
    ("rerank", "What is my home wifi network name?",           "a_ssid",       "d_ssid"),
    ("rerank", "How long is my gym membership?",               "a_renew",      "d_renew"),
    ("rerank", "When is my grandmother's birthday?",           "a_pass",       "d_pass"),
    ("rerank", "What is the PIN for my office locker?",        "a_pin",        "d_pin"),
    ("guard",  "What is my cat called?",                       "g_pepper",     None),
    ("guard",  "Where is my December conference?",             "g_tokyo",      None),
    ("guard",  "What colour is my bicycle?",                   "g_bike",       None),
]


def ndcg_at(rank, k):
    # single relevant document: DCG = 1/log2(rank+1) if within k, IDCG = 1
    return (1.0 / math.log2(rank + 1.0)) if (rank is not None and rank <= k) else 0.0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--label", default="run")
    p.add_argument("--rerank", choices=["on", "off"], default="off")
    p.add_argument("--model", default="cross-encoder/ms-marco-MiniLM-L-6-v2")
    p.add_argument("--window", type=int, default=24)
    p.add_argument("--limit", type=int, default=5)
    a = p.parse_args()

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "memory.db")
        store.ingest([{"source": "notes", "source_id": sid, "text": text, "occurred_at": WHEN}
                      for sid, text in CORPUS])
        config = {"rerank": {"enabled": a.rerank == "on", "model": a.model, "window": a.window}}
        hybrid = Hybrid(store, config, start=False)
        init_errors = dict(hybrid.errors)
        try:
            for kind, query, want, distract in CASES:
                start = time.monotonic()
                answer = hybrid.search(query, limit=a.limit, expand_entities=False)
                ids = [r["source_id"] for r in answer["episodes"]]
                rank = ids.index(want) + 1 if want in ids else None
                dr = (ids.index(distract) + 1) if (distract and distract in ids) else None
                results.append({"kind": kind, "query": query, "expected": want, "distractor": distract,
                                "retrieved": ids, "rank": rank, "distractor_rank": dr,
                                "elapsed_ms": round(1000 * (time.monotonic() - start), 2)})
        finally:
            hybrid.close()

    targets = [r for r in results if r["kind"] == "rerank"]
    guards = [r for r in results if r["kind"] == "guard"]
    k = a.limit
    def r1(group):
        return round(sum(r["rank"] == 1 for r in group) / len(group), 4) if group else None
    report = {
        "label": a.label,
        "rerank": a.rerank,
        "model": a.model if a.rerank == "on" else None,
        "window": a.window,
        "retriever": "keyword_fts5_only",
        "initialization_errors": init_errors,
        "records": len(CORPUS),
        "target_count": len(targets),
        "recall_at_1": r1(targets),
        "mrr": round(sum(1 / r["rank"] if r["rank"] else 0 for r in targets) / len(targets), 4) if targets else None,
        "ndcg_at_k": round(sum(ndcg_at(r["rank"], k) for r in targets) / len(targets), 4) if targets else None,
        # reranker opportunity: answer retrieved within top-k but NOT already first
        "retrieved_but_not_first": sum(bool(r["rank"] and 1 < r["rank"] <= k) for r in targets),
        "distractor_beats_answer": sum(bool(r["distractor_rank"] and r["rank"] and r["distractor_rank"] < r["rank"]) for r in targets),
        "guard_recall_at_1": r1(guards),
        "cases": results,
    }
    Path(a.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "cases"}, indent=2))
    print("--- cases ---")
    for r in results:
        print(f"  [{r['kind']:6}] rank={r['rank']} dist_rank={r['distractor_rank']} {r['query']} -> {r['retrieved']}")


if __name__ == "__main__":
    main()
