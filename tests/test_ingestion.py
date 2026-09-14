import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError

from personal_memory.ingestion import ContractError,ConnectorSpec,IngestionConnector,validate_record,RECORD_SCHEMA
from personal_memory.store import Store
from personal_memory.retrieval import Hybrid
from personal_memory.server import create_server


def item(source_id="one"):
    return {"schema_version":"1.0","source":"custom-notes","source_id":source_id,"revision":"1",
            "kind":"document","occurred_at":None,"observed_at":"2026-09-06T12:00:00Z",
            "text":"A source note about a bicycle repair.","participants":[],
            "provenance":{"connector_id":"example.notes","connector_version":"1.0.0",
                          "source_locator":"notes://"+source_id,"origin":"source","parent_record_ids":[]},
            "extensions":{"example.notes":{"version":"1.0","data":{"folder":"repairs","flags":["open"],"custom":{"source":"This cannot override core source"}}}}}


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/"memory.db")

    def test_every_core_field_required_and_unknown_core_rejected(self):
        for key in RECORD_SCHEMA["required"]:
            with self.subTest(key=key):
                record=item(); del record[key]
                with self.assertRaises(ContractError) as error: validate_record(record)
                self.assertIn(key,error.exception.path)
        record=item();record["surprise_field"]=123
        with self.assertRaises(ContractError):validate_record(record)

    def test_dynamic_extensions_round_trip_without_mutating_core(self):
        record=item();record["extensions"]["future.sensor"]={"version":"9.4","data":{"reading":1.5,"unit":"widgets","optional":None}}
        original=copy.deepcopy(record)
        rid=self.store.ingest_contract([record])["records"][0]["id"]
        evidence=self.store.evidence(rid)
        self.assertEqual(evidence["source"],"custom-notes")
        self.assertEqual(evidence["ingestion_record"]["extensions"],original["extensions"])
        self.assertEqual(record,original)

    def test_extension_namespaces_versions_and_json_are_enforced(self):
        bad=[{"not_namespaced":{"version":"1","data":{}}},
             {"example.notes":{"data":{}}},
             {"example.notes":{"version":"1","data":{"x":float('nan')}}},
             {"example.notes":{"version":"1","data":[]}}]
        for extension in bad:
            with self.subTest(extension=str(extension)):
                record=item();record["extensions"]=extension
                with self.assertRaises(ContractError):validate_record(record)

    def test_unknown_event_time_is_preserved_and_excluded_from_date_filter(self):
        rid=self.store.ingest_contract([item()])["records"][0]["id"]
        self.assertIsNone(self.store.evidence(rid)["occurred_at"])
        self.assertIsNone(self.store.search("bicycle")["episodes"][0]["occurred_at"])
        self.assertFalse(self.store.browse(before="2027-01-01T00:00:00Z")["episodes"])
        engine=Hybrid(self.store,start=False);self.addCleanup(engine.close)
        self.assertFalse(engine.search("bicycle",before="2027-01-01T00:00:00Z")["episodes"])

    def test_new_observation_is_not_new_source_revision(self):
        first=item();rid=self.store.ingest_contract([first])["records"][0]["id"]
        second=item();second["observed_at"]="2026-09-07T12:00:00Z"
        second["provenance"]["connector_version"]="1.0.1"
        result=self.store.ingest_contract([second])["records"][0]
        self.assertTrue(result["duplicate"]);self.assertEqual(result["id"],rid)
        self.assertEqual(self.store.evidence(rid)["ingestion_record"]["provenance"]["connector_version"],"1.0.1")
        second["extensions"]["example.notes"]["data"]["folder"]="changed"
        with self.assertRaises(ValueError):self.store.ingest_contract([second])
        second["revision"]="2"
        self.assertNotEqual(self.store.ingest_contract([second])["records"][0]["id"],rid)

    def test_bad_batch_and_nonexistent_evidence_roll_back(self):
        bad=item("two");del bad["text"]
        with self.assertRaises(ContractError):self.store.ingest_contract([item(),bad])
        self.assertEqual(self.store.status()["records"],0)
        bad=item("two");bad["provenance"].update(origin="derived",parent_record_ids=["missing"])
        with self.assertRaises(ValueError):self.store.ingest_contract([item(),bad])
        self.assertEqual(self.store.status()["records"],0)

    def test_derived_evidence_and_receipts_retract_transitively(self):
        parent=self.store.ingest_contract([item()])["records"][0]["id"]
        child=item("two");child["provenance"].update(origin="derived",parent_record_ids=[parent])
        child_id=self.store.ingest_contract([child])["records"][0]["id"]
        result=self.store.forget(parent)
        self.assertEqual(set(result["affected_records"]),{parent,child_id})
        with self.store.connect() as db:self.assertEqual(db.execute("SELECT count(*) FROM ingestion_receipts").fetchone()[0],0)

    def test_incompatible_version_and_incomplete_provenance_rejected(self):
        record=item();record["schema_version"]="2.0"
        with self.assertRaises(ContractError):validate_record(record)
        record=item();record["provenance"]["origin"]="derived"
        with self.assertRaises(ContractError):validate_record(record)
        record=item();record["observed_at"]="2026-09-06"
        with self.assertRaises(ContractError):validate_record(record)

    def test_python_connector_must_implement_and_match_declaration(self):
        with self.assertRaises(TypeError):IngestionConnector()
        class Notes(IngestionConnector):
            spec=ConnectorSpec("example.notes","1.0.0","custom-notes")
            def read(self,checkpoint=None):yield item()
        self.assertEqual(len(list(Notes().records())),1)
        class Impostor(Notes):spec=ConnectorSpec("wrong.connector","1.0.0","custom-notes")
        with self.assertRaises(ContractError):list(Impostor().records())

    def test_direct_http_cannot_bypass_contract(self):
        token="t"*40
        server=create_server(Path(self.tmp.name)/"http",token,port=0)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        incomplete=item();del incomplete["provenance"]
        request=Request(f"http://127.0.0.1:{server.server_port}/v1/ingest",
                        data=json.dumps({"items":[incomplete]}).encode(),
                        headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
        with self.assertRaises(HTTPError) as error:urlopen(request)
        self.assertEqual(error.exception.code,422)
        payload=json.loads(error.exception.read())
        self.assertEqual(payload["path"],"$.items[0].provenance")
        self.assertEqual(server.store.status()["records"],0)


if __name__=="__main__":unittest.main()
