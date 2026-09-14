"""HippoRAG-style multi-hop graph activation benchmark.

Measures a different weakness than the temporal and rerank benchmarks. The base
retrieval channels (keyword FTS5, semantic ANN, hindsight) only ever surface a record
that already shares a term or an embedding neighbourhood with the query. A fact that is
relevant purely because of *how it connects* - seed entity -> bridge record -> bridge
entity -> far answer - is invisible to every direct channel. The personalised-PageRank
propagation in `Hybrid._propagate_graph` is meant to activate exactly those records.

Two axes are reported, because propagation and the hard `RelevanceGate` (accepts only
`lexical OR semantic>=0.5`) operate at different stages of `search`:

  1. ACTIVATION (candidate level, gate-independent, deterministic, no model needed):
     does multi-hop propagation reach a *term-less* gold record that one-hop expansion
     cannot? This is the mechanism's actual job, measured directly against the graph.

  2. END-TO-END (production `search`, full pipeline): does the default-on lever regress
     or dilute the returned episodes? A genuinely term-less bridge cannot clear the gate
     in a keyword-only harness, so keyword end-to-end recall of the bridge stays FLAT at
     zero - the guard instead proves the lever adds NO false positives and never demotes
     a direct answer. Run with `--semantic` on the GPU venv to exercise the live
     multi-channel path where a semantically-close bridge can clear the floor.

  --mode off     -> graph disabled (the pre-feature floor)
  --mode onehop  -> --hops 1, approximates the previous bounded one-hop expansion
  --mode multi   -> --hops 2, the production default (multi-hop personalised PageRank)
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

WHEN = "2025-01-15T12:00:00Z"

# source_id -> text. Vocabulary is deliberately disjoint per case so a query matches ONLY
# its seed record; the "two_hop" gold is reachable purely through the entity graph below.
RECORDS = {
    # --- health: seed --Cardiology-- bridge --SaintAnne-- gold (gold shares no query term) ---
    "h_seed": "I reported chest pain and palpitations at the intake desk.",
    "h_bridge": "The cardiology department processes all new referrals.",
    "h_gold": "Northwind renews the supplemental cover automatically every April.",
    # --- travel: seed --Lounge-- bridge --Amex-- gold ---
    "t_seed": "My flight was cancelled at the boarding gate this morning.",
    "t_bridge": "The travel desk arranges the airport lounge access.",
    "t_gold": "The corporate Amex settles every invoice the travel desk raises.",
    # --- one-hop guard: seed --Filter-- gold (gold term-less; one hop already reaches it) ---
    "o_seed": "I descale the espresso machine every Sunday morning.",
    "o_gold": "The water filter for the bean hop needs replacing soon.",
    # --- direct guard: the answer IS the keyword hit; the lever must never demote it ---
    "d_direct": "The side door code for the studio is 8123.",
}

# entity label -> source_ids that mention it. Records sharing an entity are graph neighbours;
# a bridge record links two otherwise-unconnected entities, enabling multi-hop reach.
EDGES = {
    "ent:cardiology": ["h_seed", "h_bridge"],
    "ent:saintanne": ["h_bridge", "h_gold"],
    "ent:lounge": ["t_seed", "t_bridge"],
    "ent:amex": ["t_bridge", "t_gold"],
    "ent:filter": ["o_seed", "o_gold"],
    "ent:studio": ["d_direct"],
}

# kind, query (matches ONLY the seed's terms), gold source_id
CASES = [
    ("two_hop", "chest pain palpitations intake", "h_gold"),
    ("two_hop", "flight cancelled boarding gate", "t_gold"),
    ("one_hop", "descale espresso machine Sunday", "o_gold"),
    ("direct", "side door code studio 8123", "d_direct"),
]


def build_store(tmp):
    """Ingest the corpus and materialise the declared entity graph."""
    store = Store(Path(tmp) / "memory.db")
    store.ingest([{"source": "notes", "source_id": sid, "text": text, "occurred_at": WHEN}
                  for sid, text in RECORDS.items()])
    with store.connect() as db:
        ids = {row["source_id"]: row["id"] for row in db.execute("SELECT source_id, id FROM records WHERE deleted=0")}
    for label, sources in EDGES.items():
        entity = store.entity("graph", label, record_id=ids[sources[0]])["id"]
        for sid in sources[1:]:
            store.entity("graph", label, entity_id=entity, record_id=ids[sid])
    return store, ids


def seed_records(store, query, top):
    """Reproduce the production seed set: fused keyword candidates, RRF-weighted, top-k."""
    found = store.search(query, limit=32)["episodes"]
    return dict([(r["id"], 1.0 / (60 + rank)) for rank, r in enumerate(found, 1)][:top])


def run(mode, semantic, limit):
    hops = {"off": 2, "onehop": 1, "multi": 2}[mode]
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        store, ids = build_store(tmp)
        config = {"temporal": {"weight": 0.0}, "rerank": {"enabled": False},
                  "graph": {"enabled": mode != "off", "hops": hops, "seed_top": max(1, limit)}}
        if semantic:
            config["semantic"] = {"enabled": True}
        hybrid = Hybrid(store, config, start=False)
        try:
            for kind, query, gold in CASES:
                start = time.monotonic()
                reached = {item["id"] for item in hybrid._propagate_graph(seed_records(store, query, hybrid.graph_seed_top), None)}
                episodes = [r["id"] for r in hybrid.search(query, limit=limit)["episodes"]]
                results.append({"kind": kind, "query": query, "expected": gold,
                                "gold_activated": ids[gold] in reached,
                                "in_episodes": ids[gold] in episodes,
                                "episodes": [sid for sid, rid in ids.items() if rid in episodes],
                                "elapsed_ms": round(1000 * (time.monotonic() - start), 2)})
        finally:
            hybrid.close()
    return results, dict(hybrid.errors), hybrid.graph_weight


def rate(group, key):
    return round(sum(bool(r[key]) for r in group) / len(group), 4) if group else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    p.add_argument("--label", default="run")
    p.add_argument("--mode", choices=["off", "onehop", "multi"], default="multi")
    p.add_argument("--semantic", action="store_true", help="enable the live semantic channel (GPU venv)")
    p.add_argument("--limit", type=int, default=5)
    a = p.parse_args()

    results, errors, weight = run(a.mode, a.semantic, a.limit)
    two_hops = [r for r in results if r["kind"] == "two_hop"]
    one_hops = [r for r in results if r["kind"] == "one_hop"]
    direct = [r for r in results if r["kind"] == "direct"]

    report = {
        "label": a.label, "mode": a.mode, "graph_enabled": a.mode != "off", "hops": {"off": 2, "onehop": 1, "multi": 2}[a.mode],
        "retriever": "semantic+keyword" if a.semantic else "keyword_fts5_only", "graph_weight": round(weight, 4),
        "records": len(RECORDS), "two_hop_cases": len(two_hops),
        # headline: multi-hop activates term-less bridges no direct channel reaches (gate-independent)
        "two_hop_activation_recall": rate(two_hops, "gold_activated"),
        "one_hop_activation_recall": rate(one_hops, "gold_activated"),
        # end-to-end under the current hard gate (keyword-only => term-less bridges are gated out)
        "two_hop_in_episodes_recall": rate(two_hops, "in_episodes"),
        "direct_recall": rate(direct, "in_episodes"),
        "initialization_errors": errors, "cases": results,
    }
    Path(a.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "cases"}, indent=2))
    print("--- cases ---")
    for r in results:
        print(f"  [{r['kind']:7}] activated={r['gold_activated']!s:5} in_episodes={r['in_episodes']!s:5} {r['query']!r} -> {r['episodes']}")


if __name__ == "__main__":
    main()
