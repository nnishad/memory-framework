"""Temporal-consolidation benchmark: does the current fact win for 'now' questions?

Isolates one retrieval weakness documented in the 2025-2026 agent-memory literature:
pure rank-based fusion (RRF) has no notion of recency, so a *stale* fact that is at
least as strong a lexical match as the *current* fact can outrank it. The benchmark
runs keyword-only (no embedding / GPU) so it is deterministic and dependency-light,
and the fusion path under test is identical to production.

  --temporal-weight 0  -> baseline behaviour (recency ignored)
  --temporal-weight 0.5 --half-life-days 180 -> enable recency re-rank

Current-state questions expect the NEWEST record; historical questions pass an
explicit time filter and must keep returning the OLD record (regression guard).
"""
import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store

# (source_id, occurred_at, text). Each conflict pair shares the query's anchor term in
# BOTH records, so both are always retrieval candidates; the STALE record repeats the
# anchor more times so it wins the naive lexical/RRF tie. That isolates temporal
# precedence as the only thing that can correct the ranking (semantic gap-bridging is
# a separate axis handled by the embedding engines, deliberately not tested here).
CORPUS = [
    ("office_old",   "2022-08-01", "My office is at 14 Market Street. My office address is 14 Market Street, the office I use daily."),
    ("office_new",   "2025-08-01", "My office is now at 91 River Road."),
    ("phone_old",    "2021-03-05", "My phone number is 555-0100. Save my phone number 555-0100 as my primary phone number."),
    ("phone_new",    "2025-06-10", "My phone number is now 555-0199."),
    ("city_old",     "2019-01-15", "I live in Pune. The city I live in is Pune."),
    ("city_new",     "2024-11-02", "I live in Bangalore now."),
    ("job_old",      "2020-05-01", "I work at Infosys. The company I work for is Infosys."),
    ("job_new",      "2025-02-20", "I work at Google."),
    ("car_old",      "2018-07-01", "I have a Honda car. My car is a Honda."),
    ("car_new",      "2025-04-12", "I have a Tesla car."),
    ("gym_old",      "2022-01-01", "My gym is Fitness World. I go to the Fitness World gym."),
    ("gym_new",      "2025-09-01", "My gym is Golds Gym."),
    ("wifi_old",     "2021-01-01", "The wifi password is oldpassword123. Use the wifi password oldpassword123."),
    ("wifi_new",     "2025-05-01", "The wifi password is newsecret2025."),
    # Non-conflict singletons (regression: recency must not break clear lookups)
    ("passport",     "2024-03-01", "My passport is stored in the violet folder in the bedroom cupboard."),
    ("allergy",      "2024-02-01", "My allergy is to penicillin. My allergy medication is penicillin."),
    ("pet",          "2024-11-01", "My pet cat is called Pepper."),
]

# kind: current (expect newest), historical (explicit filter, expect old), single (expect the one)
CASES = [
    ("current", "Where is my office?",              "office_new", "office_old", {}),
    ("current", "What is my phone number?",         "phone_new",  "phone_old",  {}),
    ("current", "Which city do I live in?",         "city_new",   "city_old",   {}),
    ("current", "Who do I work for?",               "job_new",    "job_old",    {}),
    ("current", "What car do I have?",              "car_new",    "car_old",    {}),
    ("current", "Which gym do I go to?",            "gym_new",    "gym_old",    {}),
    ("current", "What is my wifi password?",        "wifi_new",   "wifi_old",   {}),
    ("single",  "Where is my passport stored?",     "passport",   None,         {}),
    ("single",  "What allergy do I have?",          "allergy",    None,         {}),
    ("single",  "What is my cat called?",           "pet",        None,         {}),
    ("historical","Where was my office in 2022?",   "office_old", "office_new", {"after": "2022-01-01T00:00:00Z", "before": "2023-01-01T00:00:00Z"}),
    ("historical","What was my phone number in 2021?", "phone_old", "phone_new", {"after": "2021-01-01T00:00:00Z", "before": "2022-01-01T00:00:00Z"}),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--label", default="run")
    p.add_argument("--temporal-weight", type=float, default=0.0)
    p.add_argument("--half-life-days", type=float, default=180.0)
    a = p.parse_args()

    results = []
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "memory.db")
        store.ingest([{"source": "notes", "source_id": sid, "text": text,
                       "occurred_at": date + "T12:00:00Z"} for sid, date, text in CORPUS])
        config = {"temporal": {"weight": a.temporal_weight, "half_life_days": a.half_life_days}}
        hybrid = Hybrid(store, config, start=False)
        try:
            for kind, query, want, stale, filters in CASES:
                start = time.monotonic()
                answer = hybrid.search(query, limit=3, expand_entities=False, **filters)
                ids = [r["source_id"] for r in answer["episodes"]]
                rank = ids.index(want) + 1 if want in ids else None
                stale_rank = (ids.index(stale) + 1) if (stale and stale in ids) else None
                results.append({"kind": kind, "query": query, "expected": want, "stale": stale,
                                "retrieved": ids, "rank": rank, "stale_rank": stale_rank,
                                "elapsed_ms": round(1000 * (time.monotonic() - start), 2)})
        finally:
            hybrid.close()

    current = [r for r in results if r["kind"] == "current"]
    single = [r for r in results if r["kind"] == "single"]
    historical = [r for r in results if r["kind"] == "historical"]
    def r1(group):
        return round(sum(r["rank"] == 1 for r in group) / len(group), 4) if group else None
    report = {
        "label": a.label,
        "temporal_weight": a.temporal_weight,
        "half_life_days": a.half_life_days,
        "records": len(CORPUS),
        "current_state_recall_at_1": r1(current),
        "current_state_mrr": round(sum(1 / r["rank"] if r["rank"] else 0 for r in current) / len(current), 4),
        "stale_outranks_current_count": sum(bool(r["stale"] and r["stale_rank"] and r["rank"] and r["stale_rank"] < r["rank"]) for r in current),
        "single_fact_recall_at_1": r1(single),
        "historical_recall_at_1": r1(historical),
        "cases": results,
    }
    Path(a.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "cases"}, indent=2))


if __name__ == "__main__":
    main()
