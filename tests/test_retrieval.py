"""Correctness tests use deterministic vectors, not a semantic quality benchmark."""
import json
import os
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from personal_memory import importers
from personal_memory.hindsight import Hindsight
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store


def record(i,text="vehicle repair",source="whatsapp",when="2024-01-01T12:00:00Z",participants=None):
    return {"source":source,"source_id":str(i),"text":text,"occurred_at":when,
            "metadata":{"participants":participants or []}}


def _env_restore(case, key, previous):
    """Return one environment variable to its exact prior state (value or absence) after the
    test, so kill-switch settings never leak between model-loading behaviours in one process."""
    def restore():
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous
    case.addCleanup(restore)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.store=Store(self.root/"memory.db")
    def put(self,*records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]
    def hybrid(self,**kw):
        # Isolate the deterministic fusion/recency/gate path: the external learned re-ranker and
        # the (default-on, lazy) local embedding index are exercised separately with injected
        # fakes, so no real model is ever loaded here. An explicitly injected semantic engine still
        # wins because Hybrid treats a non-None semantic as enabled regardless of this config.
        config=dict(kw.pop("config",None) or {})
        rerank=dict(config.get("rerank") or {}); rerank.setdefault("enabled",False); config["rerank"]=rerank
        semantic=dict(config.get("semantic") or {}); semantic.setdefault("enabled",False); config["semantic"]=semantic
        h=Hybrid(self.store,config,start=False,**kw); self.addCleanup(h.close); return h


class IdentityTests(Fixture):
    def test_account_resolution_aliases_and_distinct_people(self):
        self.put(record(1,participants=[{"namespace":"phone","address":"+44 7700 900123","label":"Unknown"}]))
        self.put(record(2,participants=[{"namespace":"phone","address":"+447700900123","label":"Amit"}]))
        self.assertEqual(self.store.status()["entities"],1)
        self.assertEqual(len(self.store.entities("Amit")["entities"]),1)
        self.assertEqual(len(self.store.entities("+447700900123")["entities"]),1)
        a=self.store.entity("person","Amit"); b=self.store.entity("person","Amit")
        self.assertNotEqual(a["id"],b["id"])

    def test_provisional_link_does_not_expand_confirmed_dates_do_and_revoke_undoes(self):
        participant=[{"namespace":"phone","address":"+447700900123"}]
        early,late=self.put(record(1,participants=participant),record(2,when="2025-01-01T12:00:00Z",participants=participant))
        account=self.store.entities("+447700900123")["entities"][0]["id"]
        person=self.store.entity("person","Amit")["id"]
        self.store.identity(account,person,late)
        h=self.hybrid()
        self.assertEqual(h.search("vehicle",entity_id=person)["episodes"],[])
        link=self.store.identity(account,person,late,status="confirmed",valid_from="2025-01-01T00:00:00Z")
        self.assertEqual([r["id"] for r in h.search("vehicle",entity_id=person)["episodes"]],[late])
        other=self.store.entity("person","Other")["id"]
        with self.assertRaises(ValueError): self.store.identity(account,other,late,status="confirmed")
        self.store.identity_revoke(link["id"])
        self.assertEqual(self.store.related_ids(person),set())

    def test_forgetting_identity_evidence_revokes_link(self):
        rid=self.put(record(1,participants=[{"namespace":"email","address":"alex@example.org"}]))[0]
        person=self.store.entity("person","Alex")["id"]
        account=self.store.entities("alex@example.org")["entities"][0]["id"]
        self.store.identity(account,person,rid,status="confirmed")
        self.store.forget(rid)
        self.assertEqual(self.store.related_ids(person),set())
        self.assertEqual(self.store.connections(person)["connections"],[])

    def test_paginated_history_over_old_limit(self):
        ids=self.put(*(record(i) for i in range(75)))
        found=[]; cursor=None
        while True:
            page=self.store.browse(limit=20,cursor=cursor,source="whatsapp")
            found += [r["id"] for r in page["episodes"]]
            cursor=page["next_cursor"]
            if not cursor: break
        self.assertEqual(len(found),75); self.assertEqual(set(found),set(ids))


class DeterministicEmbedder:
    key="fixture-vectors-not-a-language-model"
    def chunks(self,text):
        for i in range(0,len(text),40): yield i,min(i+40,len(text)),text[i:i+40]
    def documents(self,texts):
        return [[1.,0.,0.] if "garage" in t else [0.,1.,0.] for t in texts]
    def query(self,text): return [1.,0.,0.]


class RetrievalTests(Fixture):
    def test_variants_filters_and_claim_corrections(self):
        a,b=self.put(record(1,"car garage"),record(2,"bicycle shop",source="email"))
        old=self.store.claim("old garage address",a,predicate="address")
        self.store.claim("new bicycle shop address",b,predicate="address",supersedes=old["id"])
        h=self.hybrid()
        result=h.search("unmatched",queries=["garage","bicycle"],depth="deep",source="email")
        self.assertEqual([r["id"] for r in result["episodes"]],[b])
        self.assertTrue(all(c["status"]=="active" for c in result["claims"]))

    def test_recency_consolidation_is_enabled_by_default(self):
        import datetime
        now=datetime.datetime.now(datetime.timezone.utc)
        iso=lambda days:(now-datetime.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        stale,current=self.put(
            record(1,"office office office on Elm Street",when=iso(3000)),
            record(2,"my office is on Oak Street",when=iso(2)))
        self.assertEqual(self.hybrid().temporal_weight,1.0)  # enabled by default; no opt-in required
        got=[r["id"] for r in self.hybrid().search("office",expand_entities=False)["episodes"]]
        self.assertEqual(got[0],current)  # bounded recency bonus surfaces current evidence without demoting relevance
        self.assertIn(stale,got)  # stale evidence stays fully retrievable, never suppressed
        off=self.hybrid(config={"temporal":{"weight":0.0}})
        naive=[r["id"] for r in off.search("office",expand_entities=False)["episodes"]]
        self.assertEqual(naive[0],stale)  # explicit opt-out restores the pure rank-fusion (stale-first) ordering
        window=[r["id"] for r in self.hybrid().search("office",expand_entities=False,after=iso(3010),before=iso(1000))["episodes"]]
        self.assertEqual(window,[stale])  # an explicit time filter still overrides recency

    def test_rerank_is_enabled_by_default_and_lazy(self):
        import os
        saved=os.environ.pop("PERSONAL_MEMORY_DISABLE_RERANK",None)
        try:
            h=Hybrid(self.store,start=False); self.addCleanup(h.close)
            self.assertTrue(h._rerank_enabled)  # production standard: on, no opt-in required
            self.assertIsNone(h.reranker)  # but the model is never loaded at construction (startup stays cheap/offline)
            self.assertEqual(h.rerank_window,24)
        finally:
            if saved is not None: os.environ["PERSONAL_MEMORY_DISABLE_RERANK"]=saved

    def test_rerank_env_killswitch_forces_it_off(self):
        saved=os.environ.get("PERSONAL_MEMORY_DISABLE_RERANK")
        _env_restore(self,"PERSONAL_MEMORY_DISABLE_RERANK",saved)
        os.environ["PERSONAL_MEMORY_DISABLE_RERANK"]="1"
        h=Hybrid(self.store,start=False); self.addCleanup(h.close)
        self.assertFalse(h._rerank_enabled)  # operator/test override without any config change

    def test_semantic_is_enabled_by_default_and_lazy(self):
        import os
        saved=os.environ.pop("PERSONAL_MEMORY_DISABLE_SEMANTIC",None)
        try:
            h=Hybrid(self.store,start=False); self.addCleanup(h.close)
            self.assertTrue(h._semantic_enabled)  # production standard: on, no opt-in required
            self.assertIsNone(h.semantic)  # but the index is never built at construction (startup stays cheap/offline)
            status=h.status()
            self.assertIn("semantic",status["capabilities"])
            self.assertTrue(status["semantic"]["lazy"])  # reported as built-on-first-search
        finally:
            if saved is not None: os.environ["PERSONAL_MEMORY_DISABLE_SEMANTIC"]=saved

    def test_semantic_env_killswitch_forces_it_off(self):
        saved=os.environ.get("PERSONAL_MEMORY_DISABLE_SEMANTIC")
        _env_restore(self,"PERSONAL_MEMORY_DISABLE_SEMANTIC",saved)
        os.environ["PERSONAL_MEMORY_DISABLE_SEMANTIC"]="1"
        h=Hybrid(self.store,start=False); self.addCleanup(h.close)
        self.assertFalse(h._semantic_enabled)  # operator/test override without any config change
        self.assertNotIn("semantic",h.status()["capabilities"])

    def test_concurrent_semantic_initialization_constructs_one_engine(self):
        from concurrent.futures import ThreadPoolExecutor
        from unittest.mock import patch
        calls=[]
        class FakeSemantic:
            def __init__(self,*args,**kwargs):
                calls.append(1);time.sleep(.05)
        h=Hybrid(self.store,start=False);self.addCleanup(h.close)
        with patch('personal_memory.semantic.SemanticIndex',FakeSemantic):
            with ThreadPoolExecutor(max_workers=8) as pool:
                engines=list(pool.map(lambda _:h._ensure_semantic(),range(8)))
        self.assertEqual(len(calls),1)
        self.assertTrue(all(engine is engines[0] for engine in engines))

    def test_rerank_noops_when_explicitly_disabled(self):
        a,b=self.put(record(1,"alpha answer here"),record(2,"beta answer here"))
        h=self.hybrid(config={"rerank":{"enabled":False}})
        self.assertFalse(h._rerank_enabled)
        ordered,scores=h._apply_rerank("query",[a,b],{a:0.03,b:0.02})
        self.assertEqual(ordered,[a,b])  # explicit opt-out returns the fusion order untouched
        self.assertIsNone(h.reranker)  # and never builds a model

    def test_rerank_reorders_only_the_window_and_preserves_the_tail(self):
        a,b,c=self.put(record(1,"alpha"),record(2,"beta"),record(3,"gamma"))
        h=self.hybrid(); h.rerank_window=2
        class Fake:
            def predict(self,pairs,**kw):
                return [1.0,0.0]  # first window item judged more relevant
        h.reranker=Fake()
        ordered,scores=h._apply_rerank("query",[a,b,c],{a:0.03,b:0.02,c:0.01})
        self.assertEqual(ordered,[a,b,c])  # a>b within the window; c (outside window) keeps its place
        self.assertAlmostEqual(scores[a],2.0); self.assertAlmostEqual(scores[b],1.0)
        self.assertEqual(scores[c],0.01)  # out-of-window candidates are never rescored

    def test_rerank_degrades_to_fusion_order_when_scoring_fails(self):
        a,b=self.put(record(1,"alpha"),record(2,"beta"))
        h=self.hybrid()
        class Boom:
            def predict(self,*args,**kwargs): raise RuntimeError("model exploded")
        h.reranker=Boom()
        ordered,scores=h._apply_rerank("query",[a,b],{a:0.03,b:0.02})
        self.assertEqual(ordered,[a,b])  # failure keeps the fusion order, never aborts retrieval
        self.assertIn("rerank",h.errors)

    def test_vector_chunk_recall_persistence_filters_and_tombstones(self):
        try: from personal_memory.semantic import SemanticIndex
        except ImportError: self.skipTest("numpy unavailable")
        a,b=self.put(record(1,"x"*120+"garage fixed the engine"),record(2,"flowers",source="email"))
        engine=SemanticIndex(self.store,embedder=DeterministicEmbedder()); engine.sync()
        h=self.hybrid(semantic=engine)
        hit=h.search("mechanic",expand_entities=False)["episodes"][0]
        self.assertEqual(hit["id"],a); self.assertIn("garage",hit["text"])
        self.assertGreater(hit["span_start"],0)
        self.assertEqual(h.search("mechanic",source="email")["episodes"],[]) # Orthogonal flowers are rejected.
        reopened=SemanticIndex(self.store,embedder=DeterministicEmbedder())
        self.assertEqual(reopened.status()["pending_records"],0)
        self.store.forget(a)
        self.assertNotIn(a,[r["id"] for r in h.search("mechanic")["episodes"]])
        engine.sync()
        with self.store.connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM vector_chunks WHERE record_id=?",(a,)).fetchone()[0],0)

    def test_untrusted_remote_ids_and_failed_engine_do_not_become_facts(self):
        rid=self.put(record(1,"car garage"))[0]
        class Remote:
            def candidates(self,*args): return [{"id":"invented"},{"id":rid}]
            def status(self): return {"enabled":True}
        h=self.hybrid(hindsight=Remote())
        self.assertEqual(h.search("mechanic")["episodes"],[]) # Remote rank alone is insufficient.
        self.assertEqual([r["id"] for r in h.search("garage")["episodes"]],[rid])
        self.store.forget(rid)
        self.assertEqual(h.search("mechanic")["episodes"],[])
        class Broken(Remote):
            def candidates(self,*args): raise TimeoutError()
        self.put(record(2,"garage"))
        result=self.hybrid(hindsight=Broken()).search("garage")
        self.assertTrue(result["episodes"]); self.assertIn("hindsight",result["diagnostics"]["failures"])

    def test_hindsight_semantic_score_can_pass_the_canonical_gate(self):
        rid=self.put(record(1,"the violet folder is in the bedroom cupboard"))[0]
        class Remote:
            def candidates(self,*args):return [{"id":rid,"similarity":0.8}]
            def status(self):return {"enabled":True}
        result=self.hybrid(hindsight=Remote()).search("where is my passport",expand_entities=False)
        self.assertEqual(result["episodes"][0]["id"],rid)
        self.assertEqual(result["episodes"][0]["relevance"]["semantic_similarity"],0.8)

    def test_scoreless_hindsight_candidates_pass_the_gate_via_local_semantic(self):
        try: from personal_memory.semantic import SemanticIndex
        except ImportError: self.skipTest("numpy unavailable")
        # A pinned runtime that returns candidates WITHOUT similarity scores: relevance must not
        # depend on the external engine scoring. The local semantic index carries the paraphrase.
        rid=self.put(record(1,"the garage fixed the engine"))[0]
        class Remote:
            def candidates(self,*args):return [{"id":rid}]  # no "similarity" key at all
            def status(self):return {"enabled":True}
        engine=SemanticIndex(self.store,embedder=DeterministicEmbedder()); engine.sync()
        result=self.hybrid(hindsight=Remote(),semantic=engine).search("mechanic",expand_entities=False)
        self.assertEqual([r["id"] for r in result["episodes"]],[rid])
        relevance=result["episodes"][0]["relevance"]
        self.assertFalse(relevance["lexical_anchors"])  # no shared token; accepted semantically
        self.assertGreaterEqual(relevance["semantic_similarity"],0.5)

    def test_current_input_can_be_excluded_without_hiding_older_evidence(self):
        old,current=self.put(record(1,"passport is in the violet folder"),record(2,"where is my passport"))
        result=self.hybrid().search("passport",exclude_record_ids=[current],expand_entities=False)
        self.assertEqual([row["id"] for row in result["episodes"]],[old])


class GraphActivationTests(Fixture):
    """HippoRAG-style multi-hop propagation is deterministic and dependency-free, so it is
    tested directly against the entity graph rather than through a learned model."""
    def chain(self):
        # seed S --entity X--> bridge M --entity Y--> target T. S and T share no entity, so T
        # is reachable only through the intermediate bridge (a genuine second hop).
        s, m, t = self.put(
            record(1, "the launch event ran smoothly"),
            record(2, "an unrelated bridge note"),
            record(3, "a distant conclusion far from the query"))
        x = self.store.entity("topic", "EventX", record_id=s)["id"]
        self.store.entity("topic", "EventX", entity_id=x, record_id=m)
        y = self.store.entity("topic", "ThingY", record_id=m)["id"]
        self.store.entity("topic", "ThingY", entity_id=y, record_id=t)
        return s, m, t

    def test_graph_multihop_is_enabled_by_default(self):
        h = Hybrid(self.store, start=False); self.addCleanup(h.close)
        self.assertTrue(h.graph_enabled)  # production standard: on, no opt-in required
        self.assertEqual(h.graph_hops, 2)
        self.assertIn("graph_multihop", h.status()["capabilities"])

    def test_second_hop_bridge_is_reached_only_by_multi_hop(self):
        s, m, t = self.chain()
        two = {item["id"] for item in self.hybrid()._propagate_graph({s: 1.0}, None)}
        self.assertIn(m, two)  # direct entity neighbour (one hop)
        self.assertIn(t, two)  # reached only by diffusing through the bridge (two hops)
        one = {item["id"] for item in self.hybrid(config={"graph": {"hops": 1}})._propagate_graph({s: 1.0}, None)}
        self.assertIn(m, one)
        self.assertNotIn(t, one)  # the previous one-hop behaviour never saw the far target

    def test_propagation_respects_allowed_filter_and_degrades_when_disabled(self):
        s, m, t = self.chain()
        restricted = {item["id"] for item in self.hybrid()._propagate_graph({s: 1.0}, {s, m})}
        self.assertNotIn(t, restricted)  # an entity_id/source filter still bounds the walk
        off = self.hybrid(config={"graph": {"enabled": False}})
        self.assertFalse(off.graph_enabled)
        self.assertEqual(off._propagate_graph({s: 1.0}, None), [])  # graceful: no graph, no error

    def test_hub_entity_degree_is_bounded(self):
        seed = self.put(record(1, "central seed"))[0]
        hub = self.store.entity("topic", "Hub", record_id=seed)["id"]
        for i in range(2, 40):
            self.store.entity("topic", "Hub", entity_id=hub, record_id=self.put(record(i, "fanout %d" % i))[0])
        h = self.hybrid(); h.graph_entity_degree = 5
        self.assertLessEqual(len(h._propagate_graph({seed: 1.0}, None)), 5)  # a hub cannot flood results


class ImporterTests(Fixture):
    def test_whatsapp_multiline_android_ios_and_reimport(self):
        p=self.root/"chat.txt"
        p.write_text("01/02/2024, 13:05 - +44 7700 900123: Hello\nsecond line\n[02/02/2024, 9:06:02 PM] +447700900123: Namaste\n")
        rows=list(importers.whatsapp(p,"group-1","DMY","Europe/London"))
        self.assertEqual(len(rows),2); self.assertIn("second line",rows[0]["text"])
        self.assertTrue(rows[0]["occurred_at"].startswith("2024-02-01"))
        self.store.ingest(rows)
        self.assertTrue(all(r["duplicate"] for r in self.store.ingest(rows)["records"]))
        self.assertEqual(self.store.status()["entities"],1)
        bad=self.root/"bad.txt"; bad.write_text("not an export")
        with self.assertRaises(ValueError): list(importers.whatsapp(bad,"group","DMY","UTC"))

    def test_email_mime_identity_and_health_units(self):
        p=self.root/"mail.eml"
        p.write_text("From: Alex <alex@EXAMPLE.org>\nTo: Me <me@example.org>\nDate: Thu, 1 Feb 2024 10:00:00 +0000\nMessage-ID: <fixture@example.org>\nSubject: Repair\nMIME-Version: 1.0\nContent-Type: text/html; charset=utf-8\n\n<p>Garage appointment</p><script>ignore this</script>")
        rows=list(importers.emails(p)); self.assertIn("Garage appointment",rows[0]["text"])
        self.assertNotIn("ignore this",rows[0]["text"])
        self.store.ingest(rows)
        self.assertEqual(self.store.entities("alex@example.org")["entities"][0]["address"],"alex@example.org")
        csv=self.root/"health.csv"; csv.write_text("timestamp,metric,value,unit\n2024-01-01T12:00:00Z,heart_rate,72,bpm\n")
        self.assertEqual(list(importers.health_csv(csv))[0]["metadata"]["value_decimal"],"72")
        csv.write_text("timestamp,metric,value,unit\n2024-01-01T12:00:00Z,heart_rate,NaN,bpm\n")
        with self.assertRaises(ValueError): list(importers.health_csv(csv))


class HindsightContractTests(Fixture):
    def test_http_retain_recall_delete_and_durable_sync(self):
        # A protocol fixture verifies request/response handling, not live LLM extraction.
        observed=[]; remote={}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def answer(self,data):
                raw=json.dumps(data).encode(); self.send_response(200)
                self.send_header("Content-Length",str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def do_POST(self):
                data=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                observed.append((self.path,data))
                if self.path.endswith("/memories/recall"):
                    self.answer({"results":[{"document_id":rid,"text":"generated statement",
                                             "scores":{"semantic":0.83}} for rid in remote]})
                else:
                    for item in data["items"]: remote[item["document_id"]]=item
                    self.answer({"success":True,"async":False,"items_count":len(data["items"])})
            def do_DELETE(self):
                remote.pop(self.path.rsplit("/",1)[1],None); self.answer({"success":True})
        server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        cfg={"url":f"http://127.0.0.1:{server.server_port}","bank_id":"fixture","sources":["whatsapp"]}
        adapter=Hindsight(self.store,cfg)
        a,b=self.put(record(1),record(2,source="health"))
        self.assertEqual(adapter.sync(),1); self.assertNotIn(b,remote)
        self.assertEqual(Hindsight(self.store,cfg).sync(),0)
        candidate=adapter.candidates("mechanic","deep")[0]
        self.assertEqual(candidate["id"],a);self.assertAlmostEqual(candidate["similarity"],0.83)
        self.assertEqual(observed[-1][1]["budget"],"high")
        self.assertEqual(remote[a]["update_mode"],"replace")
        self.assertEqual(remote[a]["metadata"]["canonical_record_id"],a)
        self.store.forget(a); adapter.sync(); self.assertFalse(remote)


if __name__=="__main__": unittest.main()
