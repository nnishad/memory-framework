import threading
import tempfile
import unittest
from pathlib import Path
from personal_memory.store import Store
from personal_memory.retrieval import Hybrid
from personal_memory.intelligence import Intelligence
from personal_memory.investigate import Investigation
from personal_memory.service import MemoryService, AccessDenied

class InvestigationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.store=Store(self.root/'memory.db');self.i=Intelligence(self.store)
        self.backend=Hybrid(self.store,{"semantic":{"enabled":False}},start=False);self.addCleanup(self.backend.close)
        self.engine=Investigation(self.store,self.backend,self.i);self.addCleanup(self.engine.close)
    def put(self,sid,text,source='email',date='2025-01-01T00:00:00Z'):
        return self.store.ingest([{'source':source,'source_id':sid,'text':text,'occurred_at':date}])['records'][0]['id']
    def branch(self,bid,query,**filters):return {'id':bid,'intent':bid,'queries':[query],'filters':filters}
    def test_real_concurrent_search_and_fair_merge(self):
        self.put('a','Office at River Road');self.put('b','Passport in cupboard')
        barrier=threading.Barrier(2);base=self.backend.search
        def search(**args):barrier.wait(timeout=3);return base(**args)
        self.backend.search=search
        result=self.engine.search(goal='Office and passport',branches=[self.branch('office','office'),self.branch('passport','passport')],limit=2)
        self.assertEqual(len(result['episodes']),2);self.assertEqual(result['unresolved_requirements'],[])
        self.assertTrue(all(not r['answer_verified'] for r in result['requirements']))
    def test_identical_search_runs_once_without_losing_intents(self):
        self.put('a','Office at River Road');calls=[];base=self.backend.search
        def search(**args):calls.append(args);return base(**args)
        self.backend.search=search
        r=self.engine.search(goal='Office',branches=[self.branch('where','office'),self.branch('address','office')])
        self.assertEqual(len(calls),1);self.assertEqual(set(r['episodes'][0]['branch_ids']),{'where','address'})
    def test_filters_and_identical_text_keep_provenance(self):
        a=self.put('a','Office address','email');b=self.put('b','Office address','whatsapp')
        r=self.engine.search(goal='Both sources',branches=[self.branch('e','office',source='email'),self.branch('w','office',source='whatsapp')])
        self.assertEqual(r['requirements'][0]['record_ids'],[a]);self.assertEqual(r['requirements'][1]['record_ids'],[b])
    def test_dates_and_deleted_rehydration(self):
        a=self.put('a','Office old',date='2023-01-01T00:00:00Z');b=self.put('b','Office new')
        base=self.backend.search
        def search(**args):r=base(**args);self.store.forget(a);return r
        self.backend.search=search
        r=self.engine.search(goal='History',branches=[self.branch('old','office',before='2024-01-01T00:00:00Z')])
        self.assertEqual(r['episodes'],[])
    def test_failure_preserves_success_and_marks_requirement(self):
        self.put('a','Passport in cupboard');base=self.backend.search
        def search(**args):
            if args['query']=='broken':raise TimeoutError()
            return base(**args)
        self.backend.search=search
        r=self.engine.search(goal='Partial',branches=[self.branch('ok','passport'),self.branch('bad','broken')])
        self.assertEqual(r['unresolved_requirements'],['bad']);self.assertTrue(r['episodes'])
    def test_deadline_does_not_publish_late_result(self):
        release=threading.Event();self.addCleanup(release.set)
        def search(**args):release.wait(3);return {'episodes':[]}
        self.backend.search=search
        try:r=self.engine.search(goal='Deadline',branches=[self.branch('slow','passport')],timeout=1)
        finally:release.set()
        self.assertEqual(r['diagnostics']['timed_out'],1);self.assertEqual(r['requirements'][0]['status'],'retrieval_incomplete')
    def test_budget_and_unknown(self):
        self.put('a','passport '+'x'*4000)
        r=self.engine.search(goal='Lookup',branches=[self.branch('a','passport'),self.branch('b','submarine')],text_budget=1000)
        self.assertLessEqual(r['diagnostics']['text_characters'],1000);self.assertIn('b',r['unresolved_requirements'])
    def test_validation_and_overload(self):
        with self.assertRaises(ValueError):self.engine.search(goal='x',branches=[self.branch('a','x')]*2)
        with self.assertRaises(ValueError):self.engine.search(goal='x',branches=[{'id':'a','intent':'x','queries':['x'],'filters':{'actor':'admin'}}])
        for _ in range(12):self.engine.capacity.acquire()
        try:
            with self.assertRaises(ValueError):self.engine.search(goal='x',branches=[self.branch('a','x')])
        finally:
            for _ in range(12):self.engine.capacity.release()
    def test_reported_graph_and_scoped_non_expansion(self):
        rid=self.put('a','Mira recommended Theo for bicycle repair')
        mira=self.store.entity('person','Mira')['id'];theo=self.store.entity('person','Theo')['id']
        self.i.relation(key='r',subject_id=mira,predicate='recommended',object_id=theo,evidence=[{'record_id':rid,'quote':'Mira recommended Theo'}],actor='agent')
        r=self.engine.search(goal='Connections',branches=[self.branch('repair','bicycle')],graph_hops=2)
        self.assertEqual(len(r['connections']),1);self.assertEqual(r['connections'][0]['verification'],'reported_relation')
        r=self.engine.search(goal='Scoped',branches=[self.branch('repair','bicycle',source='email')],graph_hops=2)
        self.assertEqual(r['connections'],[])
    def test_service_authorization(self):
        service=MemoryService(self.root/'service','a'*40,principals=[{'token':'r'*40,'role':'reader'},{'token':'s'*40,'role':'scheduler'}]);self.addCleanup(service.close)
        args={'goal':'Lookup','branches':[self.branch('a','passport')]}
        r=service.dispatch('/v1/investigate',args,service.authenticate('Bearer '+'r'*40));self.assertEqual(r['episodes'],[])
        with self.assertRaises(AccessDenied):service.dispatch('/v1/investigate',args,service.authenticate('Bearer '+'s'*40))

    def test_query_variant_drift_does_not_turn_blood_group_into_chat_group(self):
        self.put('chat','Our community group discussed the office')
        r=self.engine.search(goal='Blood type',branches=[{'id':'blood','intent':'Explicit blood type','queries':['blood type','ABO blood group']}])
        self.assertEqual(r['episodes'],[])
        self.assertEqual(r['verification_required'],['blood'])
        self.assertGreater(r['diagnostics']['variant_drift_rejected'],0)

    def test_model_guidance_names_the_investigate_tool_callable(self):
        # The release gate proves the copied provider's system prompt advertises the
        # investigate tool under the exact name the host registers in the tool surface.
        from personal_memory.tools import GUIDANCE
        self.assertIn('personal_memory_investigate',GUIDANCE)
