#!/usr/bin/env python3
"""Phase 1 Step 2: deterministic contract proof of correctness Fixes 1-5 through the
REAL Hermes MemoryManager + the deployed personal_memory provider + an ephemeral-port
copied ASGI service. No LLM turn, no production store, no user archive is touched.

Run under the memory-service venv (the only interpreter with uvicorn+hindsight_api+fastembed):

    <svc-venv>/bin/python scripts/verify_correctness_fixes.py \
        --hermes-root ~/hermes-agent --hermes-commit <pin> --output report.json

Each fix maps to one or more named checks. A check passes only when the observed behavior
equals the fix's contract, so this file is the non-flaky correctness proof that the earlier
mock-based unit tests could not provide for the real integration.
"""
import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--hermes-root", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--hermes-commit")
args = parser.parse_args()
root = Path(args.hermes_root).resolve()
project = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(root), str(project)]

from personal_memory.setup import install
from personal_memory.client import Client
from personal_memory.provider import _claim_fingerprint

CHECKS = []
def record(name, ok, detail=""):
    CHECKS.append({"check": name, "passed": bool(ok), "fix": name.split(" ", 1)[0], "detail": str(detail)[:400]})
    print(("PASS " if ok else "FAIL ") + name + (("  :: " + str(detail)[:200]) if detail and not ok else ""))

def drain_recall(provider, timeout=90):
    """Wait until every background automatic recall has settled."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with provider.recall_ready:
            if not provider.inflight:
                return True
            provider.recall_ready.wait(0.05)
    return not provider.inflight

def settled_recall(manager, provider, query, session_id):
    """Return the *settled* automatic-recall text: kick the background lookup, drain, re-read.

    The first call may return a pending placeholder while the (cold) lookup runs; the second
    call reads the now-cached evidence envelope exactly as a later host turn would.
    """
    manager.prefetch_all(query, session_id=session_id)
    drain_recall(provider)
    return manager.prefetch_all(query, session_id=session_id)

# Unique, keyword-matchable episodes so candidate generation is deterministic; the semantics
# under test (suppression, reuse, exposure) are state-based, not ranking-based.
FIX1 = "The harbor bicycle is chained inside the north dockshed."
FIX2 = "The zephyr9 parcel sits behind the gamma crate in the boathouse."
FIX4 = "My favourite colour is cerulean."
FIX5 = "A violet umbrella was left on the evening train platform."

with tempfile.TemporaryDirectory() as tmp:
    home = Path(tmp) / "profile"
    os.environ["HERMES_HOME"] = str(home)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
    install(home, port=port, exclusive=True)
    from plugins.memory import load_memory_provider, list_memory_provider_names
    from agent.memory_manager import MemoryManager
    from personal_memory.common import atomic_json
    assert "personal-memory" in list_memory_provider_names()
    provider = load_memory_provider("personal-memory")
    assert provider is not None and provider.is_available()
    cfg = json.loads((home / "personal-memory" / "settings.json").read_text())
    cfg.setdefault("retrieval", {})["semantic"] = {"enabled": True}
    # Set the ephemeral relevance floor to the operating point of the pinned multilingual MiniLM
    # so the zero-lexical-overlap Fix 4 paraphrase exercises the semantic channel end-to-end; the
    # deterministic 4a sub-check proves the gate logic independently of this value.
    cfg["retrieval"]["relevance"] = {"semantic_minimum": 0.25}
    atomic_json(home / "personal-memory" / "settings.json", cfg)
    public = home / "personal-memory" / "config.json"
    pub = json.loads(public.read_text())
    pub.setdefault("retrieval", {})["semantic"] = {"enabled": True}
    pub["retrieval"]["relevance"] = {"semantic_minimum": 0.25}
    atomic_json(public, pub)

    process = subprocess.Popen([sys.executable, str(home / "personal-memory" / "run_service.py")],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    manager = MemoryManager()
    try:
        client = Client(cfg["url"], cfg["token"], timeout=1)
        deadline = time.monotonic() + 120
        while True:
            try:
                client.call("/v1/health"); break
            except Exception:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("ephemeral ASGI failed to start")
                time.sleep(0.05)
        manager.add_provider(provider)
        manager.initialize_all("session-a", hermes_home=str(home), platform="cli",
                               agent_context="primary", user_id="proof-owner")
        assert provider.client is not None and provider.outbox is not None

        for text in (FIX1, FIX2, FIX4, FIX5):
            manager.sync_all(text, "Recorded.", session_id="session-a",
                             messages=[{"role": "user", "content": text}])
        manager.flush_pending(timeout=8); provider.outbox.flush()

        # Warm the lazy semantic index once so per-check lookups finish well inside the 10s
        # recall-cache freshness window (a cold first build can exceed it). The query matches no
        # target episode, so warming never pre-injects a row the later checks depend on.
        settled_recall(manager, provider, "routine index warmup", "session-a")

        # Instrument /v1/search round-trips issued through the real client (mirrors
        # check_hermes_release's slow_search seam) to observe reuse decisions directly.
        original_call = provider.client.call
        counts = {"search": 0}
        def counting(path, *a, **k):
            if path == "/v1/search":
                counts["search"] += 1
            return original_call(path, *a, **k)
        provider.client.call = counting
        try:
            # ---- Fix 1: capability-gated reuse of the automatic fast/4 prefetch ----
            counts["search"] = 0
            settled_recall(manager, provider, "harbor bicycle north dockshed", "session-a")  # populates fast/4 cache
            cache_built = True
            counts["search"] = 0
            bare = json.loads(manager.handle_tool_call("personal_memory_search",
                        {"query": "harbor bicycle north dockshed"}))
            bare_depth = bare.get("diagnostics", {}).get("depth")
            bare_reused = bare.get("diagnostics", {}).get("reused_automatic_prefetch")
            record("Fix 1 default search is balanced/8 and NOT served by the fast prefetch",
                   bare_depth == "balanced" and not bare_reused and counts["search"] == 1,
                   f"depth={bare_depth} reused={bare_reused} searches={counts['search']}")
            counts["search"] = 0
            fast = json.loads(manager.handle_tool_call("personal_memory_search",
                        {"query": "harbor bicycle north dockshed", "depth": "fast", "limit": 4}))
            fast_reused = fast.get("diagnostics", {}).get("reused_automatic_prefetch")
            record("Fix 1 explicit fast/4 search reuses the automatic prefetch (zero round-trips)",
                   fast_reused is True and counts["search"] == 0,
                   f"reused={fast_reused} searches={counts['search']}")

            # ---- Fix 2: compression-epoch rehydration + still-in-context accuracy ----
            before_epoch = provider._exposure_state("session-a")["epoch"]
            injected = settled_recall(manager, provider, "zephyr9 parcel boathouse", "session-a")
            has_row = provider._retained_rows("session-a") is not None and any(
                k.startswith("e:") for k in provider._retained_rows("session-a")[1])
            suppressed = settled_recall(manager, provider, "gamma crate parcel", "session-a")
            suppressed_empty = suppressed == "" or '"episodes": []' in suppressed
            record("Fix 2 injected evidence is suppressed as already-in-context (still retained)",
                   has_row and suppressed_empty,
                   f"has_row={has_row} suppressed_len={len(suppressed)}")
            cp = [{"role": "user", "content": "Compressing the current turn for the fixture."}]
            ck = manager.on_pre_compress(cp, evidence_messages=cp, require_checkpoint=True)
            provider.outbox.flush()
            after_epoch = provider._exposure_state("session-a")["epoch"]
            rehydrated = settled_recall(manager, provider, "boathouse parcel behind crate", "session-a")
            record("Fix 2 on_pre_compress bumps the injection epoch",
                   "committed locally" in ck and after_epoch > before_epoch,
                   f"{before_epoch}->{after_epoch}")
            record("Fix 2 previously-suppressed evidence re-injects after compression",
                   '"episodes": []' not in rehydrated and "zephyr9" in rehydrated,
                   rehydrated[:200])
            # Claim fingerprint sensitivity (deterministic, exercises the real symbol).
            base = {"status": "current", "valid_from": None, "valid_to": None, "text": "same span"}
            changed = dict(base, valid_to="2026-01-01")
            record("Fix 2 a changed claim fingerprint differs (re-injects without compression)",
                   _claim_fingerprint(base) != _claim_fingerprint(changed),
                   "fingerprint equality")

            # ---- Fix 3: stable host-message identity dedup ----
            sid = "session-a"
            def count_hits(marker):
                r = json.loads(manager.handle_tool_call("personal_memory_search",
                        {"query": marker, "depth": "balanced", "limit": 25}))
                return sum(1 for e in r.get("episodes", []) if marker in e.get("text", ""))
            m = [{"role": "user", "content": "The vault combination is DELTA-7788.", "id": "host-42"}]
            provider.sync_turn("The vault combination is DELTA-7788.", "Ok.", session_id=sid, messages=m)
            provider.outbox.flush()
            captured_ids = provider.outbox.captured_host_ids(sid)
            first_hits = count_hits("DELTA-7788")
            # Same host id returns at higher ordinal positions (retried/reordered transcript):
            # the content+ordinal counter would double-capture, but the stable id must not.
            m2 = [{"role": "user", "content": "The vault combination is DELTA-7788.", "id": "host-42"}] * 3
            provider.sync_turn("The vault combination is DELTA-7788.", "Ok.", session_id=sid, messages=m2)
            provider.on_session_end(m2)
            provider.outbox.flush()
            repeat_hits = count_hits("DELTA-7788")
            record("Fix 3 host id parsed from the stable transcript field",
                   "id:host-42" in captured_ids, captured_ids)
            record("Fix 3 identical host id is captured once despite a colliding ordinal",
                   first_hits == 1 and repeat_hits == 1, f"{first_hits}->{repeat_hits}")
            # No-id fallback: the durable ordinal ledger still dedups a single occurrence replayed
            # across two hooks (sync_turn then on_session_end of the same one-occurrence line).
            noid = [{"role": "user", "content": "Repeatable no identifier line 991."}]
            provider.sync_turn("Repeatable no identifier line 991.", "Ok.", session_id=sid, messages=noid)
            provider.on_session_end(noid)
            provider.outbox.flush()
            noid_hits = count_hits("no identifier line 991")
            record("Fix 3 no host id: durable ordinal ledger still prevents a duplicate capture",
                   noid_hits == 1, f"hits={noid_hits}")
            with provider.outbox.connect() as db:
                has_tbl = db.execute("SELECT 1 FROM sqlite_master WHERE name='message_capture_ids'").fetchone()
            record("Fix 3 durable message_capture_ids table is present", bool(has_tbl), has_tbl)

            # ---- Fix 5: browse/timeline attribute lineage but never suppress injection ----
            rows_before = provider._retained_rows("session-a")
            before_map = dict(rows_before[1]) if rows_before else {}
            browsed = json.loads(manager.handle_tool_call("personal_memory_browse", {"limit": 20}))
            rows_after = provider._retained_rows("session-a")
            after_map = dict(rows_after[1]) if rows_after else {}
            no_error = "error" not in browsed
            record("Fix 5 a browse result adds no injected-exposure rows (provenance only)",
                   no_error and before_map == after_map,
                   f"added={set(after_map) - set(before_map)} err={browsed.get('error')}")
            # A non-mutating search of an injected-eligible record must not pre-suppress recall.
            provider.exposure.pop("session-a", None)
            counts["search"] = 0
            seen = json.loads(manager.handle_tool_call("personal_memory_search",
                        {"query": "violet umbrella train platform"}))
            prefetch_after = settled_recall(manager, provider, "violet umbrella train platform", "session-a")
            record("Fix 5 an explicit tool search does not starve the next automatic injection",
                   "violet" in prefetch_after and '"episodes": []' not in prefetch_after,
                   prefetch_after[:200])
            # Session-switch scoping: other sessions keep their own state (no global clear()).
            provider.exposure["session-c"] = {"epoch": 3, "rows": {"e:keep": {"epoch": 3, "fp": "zz"}}}
            provider.current_input_record_ids["session-c"] = ["rec_keep"]
            provider.started_inputs[("session-c", "d")] = 1
            provider.on_session_switch("session-b")
            keep_c = provider.exposure.get("session-c", {}).get("rows")
            keep_input = provider.current_input_record_ids.get("session-c")
            provider.on_session_switch("session-a")
            record("Fix 5 on_session_switch is scoped; a third session's state survives",
                   keep_c == {"e:keep": {"epoch": 3, "fp": "zz"}} and keep_input == ["rec_keep"],
                   f"rows={keep_c} inputs={keep_input}")
        finally:
            provider.client.call = original_call

        # ---- Fix 4: the score-less relevance path is real (semantic can stand alone) ----
        # 4a (deterministic, the exact shipped symbol): a candidate with no lexical anchor is
        # admitted purely by the semantic floor, and denied below it.
        from personal_memory.relevance import RelevanceGate
        gate = RelevanceGate()
        g_accept = gate.assess(["zzqxtripline"], "the target document shares nothing", similarity=0.71)
        g_reject = gate.assess(["zzqxtripline"], "the target document shares nothing", similarity=0.11)
        record("Fix 4 RelevanceGate admits a lexical-overlap-free candidate solely from the semantic floor",
               g_accept["accepted"] and not g_accept["lexical_anchors"] and not g_reject["accepted"],
               f"accept={g_accept['accepted']}/lex={g_accept['lexical_anchors']} reject={g_reject['accepted']}")
        # 4b (end-to-end in the real manager): a paraphrase sharing NO content word with the
        # stored record is surfaced by the local semantic channel and accepted, proven by the
        # per-episode relevance object showing the semantic channel (not lexical anchors) drove it.
        paraphrase = json.loads(manager.handle_tool_call("personal_memory_search",
                    {"query": "What shade of paint do I love the most?"}))
        episodes = paraphrase.get("episodes", [])
        target = [e for e in episodes if "cerulean" in e.get("text", "")]
        sem_cand = paraphrase.get("diagnostics", {}).get("candidates", {}).get("semantic", 0)
        rel = target[0].get("relevance", {}) if target else {}
        sem_driven = (bool(target) and rel.get("lexical_anchors") is False and
                      isinstance(rel.get("semantic_similarity"), (int, float)))
        record("Fix 4 zero-lexical-overlap paraphrase accepted end-to-end in the real manager via semantic",
               sem_cand >= 1 and sem_driven,
               f"episodes={len(episodes)} sem_candidates={sem_cand} target={len(target)} rel={rel}")

    finally:
        try:
            manager.shutdown_all()
        finally:
            process.terminate()
            try: process.wait(timeout=15)
            except subprocess.TimeoutExpired: process.kill(); process.wait(); raise

commit = args.hermes_commit or subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
passed = sum(1 for c in CHECKS if c["passed"])
report = {
    "hermes_tag": "v2026.9.14", "hermes_commit": commit,
    "provider_contract_sha256": hashlib.sha256((root / "agent/memory_provider.py").read_bytes()).hexdigest(),
    "checks": CHECKS, "passed": passed, "failed": len(CHECKS) - passed,
    "scope": ("Real Hermes MemoryManager + deployed personal_memory provider + ephemeral-port "
              "copied ASGI service. Deterministic contract proof of Fixes 1-5. No LLM answer turn; "
              "no production store or archive."),
    "real_manager_loop_tested": True, "live_llm_tested": False,
}
Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps({"passed": passed, "failed": report["failed"], "total": len(CHECKS)}, indent=2))
sys.exit(1 if report["failed"] else 0)
