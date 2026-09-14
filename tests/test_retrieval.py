"""Correctness tests use deterministic vectors, not a semantic quality benchmark."""
import json
import tempfile
import threading
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


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.store=Store(self.root/"memory.db")
    def put(self,*records):
        return [r["id"] for r in self.store.ingest(list(records))["records"]]
    def hybrid(self,**kw):
        h=Hybrid(self.store,start=False,**kw); self.addCleanup(h.close); return h


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
                    self.answer({"results":[{"document_id":rid,"text":"generated statement"} for rid in remote]})
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
        self.assertEqual(adapter.candidates("mechanic","deep")[0]["id"],a)
        self.assertEqual(observed[-1][1]["budget"],"high")
        self.assertEqual(remote[a]["update_mode"],"replace")
        self.assertEqual(remote[a]["metadata"]["canonical_record_id"],a)
        self.store.forget(a); adapter.sync(); self.assertFalse(remote)


if __name__=="__main__": unittest.main()
