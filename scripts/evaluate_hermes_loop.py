#!/usr/bin/env python3
"""Phase 1 Step 3: LLM-in-the-loop answer-quality baseline over the seven project
scenario classes, driven through the real `hermes -z` oneshot with the deployed
personal-memory provider + the ephemeral ASGI service. Nothing here touches the
production store; every scenario runs inside a throwaway HERMES_HOME.

Run under the memory-service venv (the only interpreter with uvicorn+hindsight_api+
fastembed on this host):

    <svc-venv>/bin/python scripts/evaluate_hermes_loop.py \
        --hermes-root ~/hermes-agent \
        --hermes-bin  ~/.hermes/venvs/hermes/bin/hermes \
        --prod-config ~/.hermes/config.yaml \
        --service-env ~/.hermes/personal-memory/service.env \
        --plugin-mirror ~/.hermes/plugins/personal-memory \
        --output docs/PHASE1_BASELINE.json \
        --report docs/PHASE1_REPORT.md

Design: seed the store via direct HTTP /v1/ingest (deterministic, no LLM in the
write path), then run one real `hermes -z` turn against the pinned host model and
score the answer against per-scenario gold expectations. Reset the canonical store
between scenarios so topic bleed does not confound results.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("--hermes-root", required=True)
parser.add_argument("--hermes-bin", required=True)
parser.add_argument("--prod-config", required=True)
parser.add_argument("--service-env", required=True)
parser.add_argument("--plugin-mirror", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--report", required=True)
parser.add_argument("--hermes-commit")
parser.add_argument("--turn-timeout-sec", type=int, default=180)
args = parser.parse_args()

root = Path(args.hermes_root).resolve()
project = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root), str(project), str(Path(args.plugin_mirror).resolve())]

from personal_memory.client import Client
from personal_memory.common import atomic_json
from personal_memory.setup import install


NOW = datetime.now(timezone.utc)


def iso(when):
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_item(source, source_id, text, occurred_at=None):
    return {
        "schema_version": "1.0",
        "source": source,
        "source_id": source_id,
        "revision": "1",
        "kind": "document",
        "occurred_at": occurred_at,
        "observed_at": iso(NOW),
        "text": text,
        "participants": [],
        "provenance": {
            "connector_id": "evaluate.hermes_loop",
            "connector_version": "0.1.0",
            "source_locator": f"eval://{source}/{source_id}",
            "origin": "source",
            "parent_record_ids": [],
        },
        "extensions": {},
    }


# ---------------------------------------------------------------------------
# Seven scenario classes (paraphrase, cross-session, correction, contradiction,
# compression, interrupted, unknown/abstention). Each carries its own gold so
# scoring is deterministic; the *answer* is produced by the real host model.
# ---------------------------------------------------------------------------
SCENARIOS = [
    {
        "id": "paraphrase-paint",
        "class": "paraphrase",
        "seed": [make_item("eval", "paraphrase-paint", "My favourite colour is cerulean.")],
        "query": "Which shade of paint would I enjoy most for a bedroom wall?",
        "expect": "answer",
        "must_any": ["cerulean"],
        "must_none": ["turquoise", "cobalt blue"],
        "gold": "cerulean",
    },
    {
        "id": "cross-session-cabin",
        "class": "cross-session",
        "seed": [
            make_item("cabin-chat", "cabin-wifi", "The WiFi password at my parents' lake cabin is Larchwood-42."),
            make_item("trip-planner", "cabin-trip", "I am going up to the lake cabin again next month."),
        ],
        "query": "What network credentials do I need for the weekend trip to the lake cabin?",
        "expect": "answer",
        "must_any": ["Larchwood-42"],
        "must_none": [],
        "gold": "Larchwood-42 (surfaced from a different provenance source than the trip plan)",
    },
    {
        "id": "correction-codename",
        "class": "correction",
        "seed": [
            make_item("eval", "codename-old", "The project codename is Falcon.", occurred_at=iso(NOW.replace(year=NOW.year - 2, month=1, day=1))),
            make_item("eval", "codename-new", "Project update: the codename has changed; it is now Osprey.", occurred_at=iso(NOW.replace(year=NOW.year - 1, month=6, day=15))),
        ],
        "query": "What is the current codename of my project?",
        "expect": "answer",
        "must_any": ["Osprey"],
        "must_none": [],
        # A correction answer is only fully correct if it presents the newer value as
        # current AND any mention of the superseded value is qualified by a temporal
        # marker. Bare "Falcon" without a qualifier means the model did not resolve
        # the correction.
        "recency_qualification": {
            "superseded": "Falcon",
            "qualifiers": ["previously", "previous", "was ", "used to", "prior", "old", "earlier", "2024", "changed", "before", "initially", "superseded", "replaced", "originally"],
        },
        "gold": "Osprey presented as current; any mention of Falcon is qualified as past/superseded.",
    },
    {
        "id": "contradiction-beverage",
        "class": "contradiction",
        "seed": [
            make_item("eval", "bev-coffee", "I start every morning with a large black coffee."),
            make_item("eval", "bev-tea", "I have not touched coffee in years; my morning drink is green tea."),
        ],
        "query": "What do I usually drink in the morning?",
        "expect": "answer",
        # Correct = at least one beverage surfaced AND the answer acknowledges conflict.
        "must_any": ["coffee", "tea"],
        "must_conflict_hints": ["but", "however", "both", "conflict", "inconsist", "contradict", "different", "changes", "unclear", "record"],
        "must_none": [],
        "gold": "Both beverages surfaced with a conflict marker; the model must not silently pick one.",
    },
    {
        "id": "compression-vault",
        "class": "compression",
        "seed": (
            [make_item("eval", "vault-target", "The combination to my attic vault is amber-crystal-7.")]
            + [
                # 18 plausible but irrelevant decoys. Under host-side context
                # compression these crowd the transcript; the target must still surface.
                make_item("eval", f"vault-decoy-{i}",
                          f"Unrelated fact number {i}: a shelf in the storage unit holds jarred pears and {i} ball bearings.")
                for i in range(18)
            ]
        ),
        "query": "What is the combination to my attic vault?",
        "expect": "answer",
        "must_any": ["amber-crystal-7"],
        "must_none": ["ball bearing", "jarred pear"],
        "gold": "amber-crystal-7",
    },
    {
        "id": "interrupted-reservation",
        "class": "interrupted",
        "seed": [
            make_item("eval", "luigis", "I booked a table at Luigi's for May 3rd, but I did not confirm whether it is lunch or dinner."),
        ],
        "query": "Tell me everything my memory holds about my Luigi's reservation.",
        "expect": "answer",
        "must_any": ["Luigi"],
        # Interrupted record has an unresolved slot; correct answer must not fabricate one.
        "must_none": ["for dinner", "for lunch", "7pm", "7 pm", "8pm", "8 pm"],
        "gold": "May 3rd at Luigi's with the meal ambiguity preserved (not resolved).",
    },
    {
        "id": "unknown-passport",
        "class": "abstention",
        "seed": [],
        "query": "What is my passport number?",
        "expect": "abstain",
        "abstain_markers": [
            "don't have", "do not have", "don't recall", "not stored", "no record",
            "not in my memory", "not saved", "i cannot find", "can't find",
            "no information", "don't see", "not available", "have no", "unable to find",
            "nothing stored", "not in memory",
        ],
        "forbid_patterns": [r"\b[A-Z]{1,2}\d{6,8}\b", r"passport number is", r"it is \w+\d"],
        "must_any": [],
        "must_none": [],
        "gold": "Model must abstain; no fabricated passport identifier.",
    },
]


def ephemeral_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def source_env_file(path):
    env = {}
    p = Path(path)
    if not p.is_file():
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def seed(client, items):
    if not items:
        return []
    result = client.call("/v1/ingest", {"items": items})
    return [r["id"] for r in result.get("records", [])]


def reset_store(reset_client):
    # /v1/reset wipes the canonical store + clears the external engine. Cheap
    # enough between scenarios that topic bleed never confounds a later case.
    try:
        reset_client.call("/v1/reset", {"scope": "canonical"})
    except Exception as error:
        print(f"reset failed: {type(error).__name__}: {error}", file=sys.stderr)


def run_hermes_turn(hermes_bin, home, query, usage_path, timeout):
    """One real `hermes -z` turn under the ephemeral profile. Returns (answer, rc, latency_s)."""
    env = dict(os.environ)
    env["HERMES_HOME"] = str(home)
    env.pop("PERSONAL_MEMORY_DISABLE_SEMANTIC", None)
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            [hermes_bin, "-z", query, "-t", "memory", "--yolo", "--usage-file", str(usage_path)],
            capture_output=True, text=True, timeout=timeout, env=env,
        )
        answer = proc.stdout.strip() or proc.stderr.strip()
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        answer = "<TIMEOUT>"
        rc = -1
    latency = time.monotonic() - t0
    return answer, rc, latency


def read_usage(path):
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return {}


def score(scenario, answer, usage, rc, latency):
    text = answer.lower()
    result = {"scenario_id": scenario["id"], "class": scenario["class"], "answer": answer[:800],
              "rc": rc, "latency_sec": round(latency, 2),
              "input_tokens": usage.get("input_tokens"), "output_tokens": usage.get("output_tokens"),
              "cache_read_tokens": usage.get("cache_read_tokens"), "api_calls": usage.get("api_calls"),
              "completed": usage.get("completed"), "model": usage.get("model")}
    must_any = scenario.get("must_any", [])
    must_none = scenario.get("must_none", [])
    if scenario["expect"] == "abstain":
        abstain_hit = any(m in text for m in scenario["abstain_markers"])
        fabricated = any(re.search(p, text) for p in scenario["forbid_patterns"])
        result["correct"] = abstain_hit and not fabricated
        result["abstained"] = abstain_hit
        result["hallucinated"] = fabricated
        result["detail"] = f"abstain_markers_hit={abstain_hit} fabricated={fabricated}"
    else:
        hit_all = all(any(tok.lower() in text for tok in [m]) for m in must_any)
        none_hit = [m for m in must_none if m.lower() in text]
        result["correct"] = bool(hit_all) and not none_hit
        result["detail"] = f"must_any_ok={hit_all} must_none_hits={none_hit}"
        if "must_conflict_hints" in scenario:
            hints = [h for h in scenario["must_conflict_hints"] if h in text]
            result["conflict_acknowledged"] = bool(hints)
            result["correct"] = result["correct"] and bool(hints)
            result["detail"] += f" conflict_hints={hints}"
        rq = scenario.get("recency_qualification")
        if rq:
            superseded = rq["superseded"].lower()
            mentioned = superseded in text
            qualified = any(q.lower() in text for q in rq["qualifiers"])
            result["superseded_mentioned"] = mentioned
            result["superseded_qualified"] = qualified
            if mentioned and not qualified:
                result["correct"] = False
                result["detail"] += f" recency=UNQUALIFIED ({superseded} present without any qualifier)"
            else:
                result["detail"] += f" recency=ok (mentioned={mentioned} qualified={qualified})"
    return result


def main():
    tmp_root = Path(tempfile.mkdtemp(prefix="p3-eval-"))
    home = tmp_root / "profile"
    home.mkdir(parents=True)
    port = ephemeral_port()

    prod_cfg = Path(args.prod_config)
    prod = {}
    try:
        import yaml
        prod = yaml.safe_load(prod_cfg.read_text()) or {}
    except Exception as error:
        print(f"could not parse prod config for model block: {error}", file=sys.stderr)
    import yaml
    sub = {"model": prod.get("model"), "agent": prod.get("agent", {})}
    if isinstance(sub.get("agent"), dict):
        d = sub["agent"].get("disabled_toolsets") or []
        sub["agent"]["disabled_toolsets"] = [x for x in d if x != "memory"]
    sub = {k: v for k, v in sub.items() if v is not None}
    yaml.safe_dump(sub, (home / "config.yaml").open("w"), sort_keys=False)

    install(home, port=port, exclusive=True)

    # Turn on the default-off semantic lever for the ephemeral profile + calibrate
    # the relevance floor to the pinned MiniLM's operating point (shipped default
    # 0.5 suppresses the paraphrase case; see Step 2 finding).
    for name in ("settings.json", "config.json"):
        path = home / "personal-memory" / name
        cfg = json.loads(path.read_text())
        cfg.setdefault("retrieval", {})["semantic"] = {"enabled": True}
        cfg["retrieval"]["relevance"] = {"semantic_minimum": 0.35}
        atomic_json(path, cfg)

    env_overlay = source_env_file(args.service_env)
    env_overlay["HERMES_HOME"] = str(home)
    for k, v in env_overlay.items():
        os.environ[k] = v

    svc_log = tmp_root / "service.log"
    proc = subprocess.Popen([sys.executable, str(home / "personal-memory" / "run_service.py")],
                            stdout=svc_log.open("w"), stderr=subprocess.STDOUT,
                            env={**os.environ, "HERMES_HOME": str(home)})
    settings = json.loads((home / "personal-memory" / "settings.json").read_text())
    client = Client(settings["url"], settings["token"], timeout=30)
    reset_client = Client(settings["url"], settings["token"], timeout=180)
    ready_deadline = time.monotonic() + 240
    ready = False
    while time.monotonic() < ready_deadline and not ready:
        if proc.poll() is not None:
            raise RuntimeError("service died during startup; see " + str(svc_log))
        try:
            r = client.call("/v1/ready")
            ready = bool(r.get("ready")) if isinstance(r, dict) else False
        except Exception:
            time.sleep(2)
    if not ready:
        raise RuntimeError("service not ready in time")

    results = []
    try:
        for sc in SCENARIOS:
            reset_store(reset_client)
            seed_ids = seed(client, sc["seed"])
            # Warm the lazy semantic index inside the service so a per-scenario
            # first-turn build cost is not attributed to the model's wall clock.
            try:
                client.call("/v1/search", {"query": sc["query"], "depth": "balanced", "limit": 4})
            except Exception:
                pass
            usage_path = tmp_root / f"usage-{sc['id']}.json"
            answer, rc, latency = run_hermes_turn(args.hermes_bin, home, sc["query"], usage_path, args.turn_timeout_sec)
            usage = read_usage(usage_path)
            scored = score(sc, answer, usage, rc, latency)
            scored["seed_record_ids"] = seed_ids
            scored["gold"] = sc["gold"]
            results.append(scored)
            print(f"[{sc['class']:12s}] correct={scored['correct']} latency={scored['latency_sec']}s tokens={scored['input_tokens']}/{scored['output_tokens']} api_calls={scored['api_calls']} detail={scored['detail']}")
    finally:
        try:
            proc.terminate()
            for _ in range(30):
                if proc.poll() is not None:
                    break
                time.sleep(1)
            else:
                proc.kill()
        finally:
            shutil.rmtree(tmp_root, ignore_errors=True)

    commit = args.hermes_commit or subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    provider_contract = hashlib.sha256((root / "agent/memory_provider.py").read_bytes()).hexdigest()
    passed = [r for r in results if r["correct"]]
    per_class = {}
    for r in results:
        per_class.setdefault(r["class"], []).append(r)
    aggregate = {
        "total": len(results),
        "passed": len(passed),
        "failed": len(results) - len(passed),
        "by_class": {k: {"total": len(v), "passed": sum(1 for r in v if r["correct"])} for k, v in per_class.items()},
        "total_input_tokens": sum(r.get("input_tokens") or 0 for r in results),
        "total_output_tokens": sum(r.get("output_tokens") or 0 for r in results),
        "total_api_calls": sum(r.get("api_calls") or 0 for r in results),
        "mean_latency_sec": round(sum(r["latency_sec"] for r in results) / max(1, len(results)), 2),
        "abstention_correct": sum(1 for r in results if r["class"] == "abstention" and r["correct"]),
        "hallucinations": sum(1 for r in results if r.get("hallucinated")),
    }
    report = {
        "hermes_tag": "v2026.9.14", "hermes_commit": commit,
        "provider_contract_sha256": provider_contract,
        "model": results[0]["model"] if results else None,
        "scenario_results": results, "aggregate": aggregate,
        "scope": ("Real `hermes -z` oneshot turns against the ephemeral Hermes profile + "
                  "deployed personal-memory provider + host llama.cpp model. Store seeded via "
                  "direct /v1/ingest; canonical reset between scenarios. Production store "
                  "and archive untouched."),
        "relevance_floor_used": 0.35,
        "relevance_floor_shipped_default": 0.5,
        "calibration_finding": ("Step 2 measured MiniLM multilingual similarity 0.4894 on a "
                                 "zero-lexical-overlap paraphrase pair; the shipped default "
                                 "0.5 floor would reject it. This baseline therefore uses "
                                 "semantic_minimum=0.35 in the ephemeral profile and flags "
                                 "the shipped default as an open calibration question."),
        "real_manager_loop_tested": True,
        "live_llm_tested": True,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    md = [
        "# Phase 1 Hermes-loop answer-quality baseline",
        "",
        f"- hermes: `v2026.9.14` @ `{commit[:12]}`",
        f"- provider contract sha256: `{provider_contract[:16]}…`",
        f"- model: `{report['model']}`",
        f"- total scenarios: {aggregate['total']}  | passed: {aggregate['passed']}  | failed: {aggregate['failed']}",
        f"- aggregate tokens: {aggregate['total_input_tokens']} in / {aggregate['total_output_tokens']} out over {aggregate['total_api_calls']} api calls",
        f"- mean turn latency: {aggregate['mean_latency_sec']}s",
        f"- abstention correct: {aggregate['abstention_correct']} / hallucinations: {aggregate['hallucinations']}",
        "",
        "## Per-scenario",
        "",
        "| scenario | class | correct | latency (s) | tokens (in/out) | api_calls | detail |",
        "| --- | --- | :-: | --: | --: | --: | --- |",
    ]
    for r in results:
        md.append(
            f"| {r['scenario_id']} | {r['class']} | {'yes' if r['correct'] else 'NO'} | "
            f"{r['latency_sec']} | {r.get('input_tokens')}/{r.get('output_tokens')} | "
            f"{r.get('api_calls')} | {r['detail']} |"
        )
    md += ["", "## Answers (truncated)", ""]
    for r in results:
        md += [f"### {r['scenario_id']}", "", "```", r["answer"], "```", ""]
    md += [
        "## Calibration note",
        "",
        report["calibration_finding"],
        "",
        "## Scope statement",
        "",
        report["scope"],
        "",
    ]
    Path(args.report).write_text("\n".join(md) + "\n")
    print(json.dumps({"passed": aggregate["passed"], "failed": aggregate["failed"],
                      "by_class": aggregate["by_class"]}, indent=2))
    sys.exit(0 if aggregate["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
