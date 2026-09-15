import tempfile
import unittest
from pathlib import Path
from personal_memory.relevance import RelevanceGate
from personal_memory.store import Store
from personal_memory.retrieval import Hybrid
from personal_memory.adaptive import AdaptiveRecall

class RelevanceTests(unittest.TestCase):
    def test_common_words_and_bare_numbers_do_not_support_query(self):
        gate=RelevanceGate()
        for q in ['What is my blood type?','What is my submarine registration number?','What is my private helicopter tail number?','What is it?']:
            self.assertFalse(gate.assess([q],'My office is here. The phone number changed in 2023.')['accepted'])
        self.assertTrue(gate.assess(['Where was my office in 2023?'],'My office is River Road.')['accepted'])
    def test_semantic_and_lexical_paths(self):
        gate=RelevanceGate()
        self.assertTrue(gate.assess(['passport'],'My passport is in a cupboard.')['accepted'])
        self.assertTrue(gate.assess(['Where is my passport?'],'मेरा पासपोर्ट अलमारी में है।',0.8)['accepted'])
        self.assertFalse(gate.assess(['blood type'],'Office address',0.2)['accepted'])
    def test_invalid_threshold(self):
        for x in [True,float('nan'),-1,2,'0.4']:
            with self.assertRaises(ValueError):RelevanceGate({'semantic_minimum':x})
    def test_search_and_adaptive_abstain(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db');store.ingest([{'source':'test','source_id':'a','occurred_at':'2026-01-01T00:00:00Z','text':'My passport is in the cupboard.'}])
            backend=Hybrid(store,{"semantic":{"enabled":False}},start=False)
            try:
                for engine in [backend,AdaptiveRecall(store,backend)]:
                    result=engine.search('What is my blood type?')
                    self.assertEqual(result['episodes'],[]);self.assertEqual(result['claims'],[])
                    self.assertEqual(result['retrieval_status'],'no_relevant_evidence')
                    self.assertEqual(result['evidence_sufficiency'],'not_established')
                    self.assertTrue(engine.search('Where is my passport?')['episodes'])
            finally:backend.close()
    def test_rejected_source_cannot_leak_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db')
            rid=store.ingest([{'source':'test','source_id':'a','occurred_at':'2026-01-01T00:00:00Z','text':'My office is here.'}])['records'][0]['id']
            store.claim('My office is here.',rid)
            backend=Hybrid(store,{"semantic":{"enabled":False}},start=False)
            try:self.assertEqual(backend.search('What is my blood type?')['claims'],[])
            finally:backend.close()
    def test_incomplete_engine_does_not_prove_absence(self):
        class Broken:
            def candidates(self,*a):raise TimeoutError()
            def status(self):return {'enabled':True,'ready':False}
        with tempfile.TemporaryDirectory() as tmp:
            backend=Hybrid(Store(Path(tmp)/'memory.db'),semantic=Broken(),start=False)
            try:
                self.assertEqual(backend.search('passport')['retrieval_status'],'retrieval_incomplete')
                self.assertEqual(AdaptiveRecall(backend.store,backend).search('passport')['retrieval_status'],'retrieval_incomplete')
            finally:backend.close()
