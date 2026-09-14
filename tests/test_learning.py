import tempfile
import unittest
from pathlib import Path
from personal_memory.service import MemoryService,AccessDenied
from personal_memory.learning import Learning
from personal_memory.store import Store
from personal_memory.adaptive import AdaptiveRecall
from test_ingestion import item

class LearningTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.service=MemoryService(self.root,'a'*40,principals=[{'token':'g'*40,'role':'agent'},{'token':'e'*40,'role':'evaluator'},{'token':'r'*40,'role':'reader'}])
        self.addCleanup(self.service.close);self.store=self.service.store
        self.roles={r:self.service.authenticate('Bearer '+t*40) for r,t in [('admin','a'),('agent','g'),('evaluator','e'),('reader','r')]}
        self.rid=self.store.ingest_contract([item()])['records'][0]['id']
        self.other=self.store.ingest_contract([item('evaluation')])['records'][0]['id']
    def call(self,action,args,role='agent'):
        return self.service.dispatch('/v1/learning/'+action,args,self.roles[role])
    def outcome(self,key='task'):
        return self.call('outcome',dict(key=key,goal='Repair bicycle',action='Check brakes',result='Brake test passed',outcome='success',evidence_ids=[self.rid]))
    def proposal(self,revision=1,key=None):
        outcome=self.outcome()
        return self.call('propose',dict(key=key or f'lesson-{revision}',family='bicycle-check',revision=revision,category='procedural',lesson='Check brakes before riding',scope='bicycle maintenance',prerequisites=['Bicycle available'],exceptions=['No permission to alter brakes'],outcome_ids=[outcome['id']],evidence_ids=[]))
    def evaluation(self,candidate,passed=True,key='eval'):
        return self.call('evaluate',dict(key=key,candidate_id=candidate['id'],evidence_ids=[self.other],cases=[{'id':k,'kind':k,'baseline_pass':True,'candidate_pass':passed} for k in ['target','regression','non_applicable']]),'evaluator')
    def promote(self,candidate,evaluation,expected=None):
        return self.call('promote',dict(candidate_id=candidate['id'],evaluation_id=evaluation['id'],expected_active_id=expected),'admin')
    def test_outcome_replay_and_changed_key_rejected(self):
        first=self.outcome();self.assertEqual(first,self.outcome())
        with self.assertRaises(ValueError):self.call('outcome',dict(key='task',goal='Changed',action='Check',result='ok',outcome='success',evidence_ids=[self.rid]))
    def test_agent_cannot_evaluate_promote_or_forge_actor(self):
        candidate=self.proposal()
        for action in ['evaluate','promote','retract']:
            with self.assertRaises(AccessDenied):self.call(action,{})
        with self.assertRaises(ValueError):self.call('outcome',{'actor':'admin'})
        with self.assertRaises(AccessDenied):self.call('outcome',{},'reader')
        with self.assertRaises(AccessDenied):self.call('propose',{},'evaluator')
    def test_pending_is_not_active_and_failed_evaluation_blocks_promotion(self):
        candidate=self.proposal();self.assertEqual(self.call('browse',{})['items'],[])
        evaluation=self.evaluation(candidate,False)
        with self.assertRaises(ValueError):self.promote(candidate,evaluation)
        self.assertEqual(self.call('browse',{})['items'],[])
    def test_revision_compare_and_swap_prevents_stale_promotion(self):
        first=self.proposal();ev=self.evaluation(first);self.promote(first,ev)
        second=self.proposal(2);ev2=self.evaluation(second,key='eval2')
        with self.assertRaises(ValueError):self.promote(second,ev2)
        self.promote(second,ev2,first['id'])
        self.assertEqual([r['id'] for r in self.call('browse',{})['items']],[second['id']])
        with self.assertRaises(ValueError):self.proposal(2,key='different-key')
    def test_deletion_of_evaluation_evidence_invalidates_active_lesson(self):
        candidate=self.proposal();ev=self.evaluation(candidate);self.promote(candidate,ev)
        self.store.forget(self.other)
        self.assertEqual(self.call('browse',{})['items'],[])
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT payload FROM learning_objects WHERE id=?',(candidate['id'],)).fetchone()[0],'{}')
        self.assertEqual(len(self.call('browse',{'kind':'outcome','state':'recorded'})['items']),1)
    def test_deletion_of_source_invalidates_whole_learning_chain_after_restart(self):
        candidate=self.proposal();ev=self.evaluation(candidate);self.promote(candidate,ev)
        self.store.forget(self.rid);reopened=Learning(Store(self.root/'memory.db'))
        self.assertEqual(reopened.browse()['items'],[])
        self.assertEqual(reopened.browse(kind='outcome',state='recorded')['items'],[])
        with self.assertRaises(ValueError):self.outcome()
    def test_self_evaluation_and_self_promotion_are_rejected(self):
        candidate=self.proposal();actor=candidate['actor']
        with self.assertRaises(ValueError):self.service.learning.evaluate(key='self',candidate_id=candidate['id'],cases=[{'id':k,'kind':k,'baseline_pass':True,'candidate_pass':True} for k in ['target','regression','non_applicable']],evidence_ids=[self.other],actor=actor)
        ev=self.evaluation(candidate)
        with self.assertRaises(ValueError):self.service.learning.promote(candidate_id=candidate['id'],evaluation_id=ev['id'],expected_active_id=None,actor=actor)
    def test_mismatched_evaluation_and_missing_test_category_rejected(self):
        first=self.proposal();second=self.proposal(2);ev=self.evaluation(first)
        with self.assertRaises(ValueError):self.promote(second,ev)
        with self.assertRaises(ValueError):self.call('evaluate',dict(key='bad',candidate_id=first['id'],evidence_ids=[self.other],cases=[]),'evaluator')
    def test_retraction_is_durable_and_cannot_be_replayed_as_new(self):
        candidate=self.proposal();ev=self.evaluation(candidate);self.promote(candidate,ev)
        self.call('retract',{'candidate_id':candidate['id']},'admin')
        self.assertEqual(self.call('browse',{})['items'],[])
        with self.assertRaises(ValueError):self.proposal()
    def test_missing_evidence_leaves_no_partial_outcome(self):
        with self.assertRaises(ValueError):self.call('outcome',dict(key='missing',goal='g',action='a',result='r',outcome='unknown',evidence_ids=['missing']))
        self.assertEqual(self.call('browse',{'kind':'outcome','state':'recorded'})['items'],[])
    def test_promotion_retry_is_idempotent(self):
        candidate=self.proposal();evaluation=self.evaluation(candidate)
        self.assertEqual(self.promote(candidate,evaluation),self.promote(candidate,evaluation))
    def test_old_snapshot_applies_retraction_ledger(self):
        import sqlite3
        candidate=self.proposal();evaluation=self.evaluation(candidate);self.promote(candidate,evaluation)
        old=self.root/'old.db'
        with self.store.connect() as src,sqlite3.connect(old) as dst:src.backup(dst)
        self.call('retract',{'candidate_id':candidate['id']},'admin')
        from personal_memory.deletions import DeletionLedger
        DeletionLedger(self.root/'old.deletions.db').merge(self.store.deletions)
        self.assertEqual(Learning(Store(old)).browse()['items'],[])
    def test_concurrent_promotions_preserve_one_active_revision(self):
        from concurrent.futures import ThreadPoolExecutor
        first=self.proposal();second=self.proposal(2)
        evaluations=[self.evaluation(first,key='e1'),self.evaluation(second,key='e2')]
        def attempt(pair):
            try:self.promote(*pair);return True
            except ValueError:return False
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(attempt,zip([first,second],evaluations)))
        self.assertEqual(sum(results),1)
        self.assertEqual(len(self.call('browse',{})['items']),1)

    def test_http_learning_routes(self):
        import threading
        from personal_memory.server import create_server
        from personal_memory.client import Client,ServiceError
        # Exercise the public service through the same HTTP transport used by connector tests.
        server=create_server(self.root/'http','h'*40,port=0)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        client=Client(f'http://127.0.0.1:{server.server_port}','h'*40)
        rid=client.call('/v1/ingest',{'items':[item('http')]})['records'][0]['id']
        result=client.call('/v1/learning/outcome',dict(key='http',goal='g',action='a',result='r',outcome='unknown',evidence_ids=[rid]))
        self.assertEqual(result['kind'],'outcome')
        self.assertTrue(client.call('/v1/recall',{'query':'bicycle'})['episodes'])

    def test_progressive_recall_budget_filters_and_source_deletion(self):
        seen=[];store=self.store;rid=self.rid
        class Backend:
            def search(self,**args):
                seen.append(args)
                if len(seen)==2:store.forget(rid)
                return {'episodes':[{'id':rid,'text':'stale','span_start':0}], 'claims':[], 'coverage':[]}
        result=AdaptiveRecall(store,Backend()).search('bicycle',subqueries=['brakes'],source='custom-notes',max_calls=3,text_budget=1000)
        self.assertEqual(result['episodes'],[])
        self.assertEqual(len(seen),2)
        self.assertTrue(all(x['source']=='custom-notes' for x in seen))
        self.assertEqual(seen[1]['queries'],['brakes'])
        self.assertEqual(result['evidence_sufficiency'],'not_established')
    def test_progressive_recall_real_backend_and_validation(self):
        result=self.service.dispatch('/v1/recall',{'query':'bicycle','text_budget':1000},self.roles['reader'])
        self.assertTrue(result['episodes']);self.assertLessEqual(result['diagnostics']['text_characters'],1000)
        with self.assertRaises(ValueError):self.service.adaptive.search('bicycle',max_calls=100)


    def test_native_skill_export_requires_active_lesson_and_checks_retraction(self):
        from personal_memory.skill_export import export_skill,verify
        candidate=self.proposal()
        service=self.service;roles=self.roles
        class Client:
            def call(self,path,data):return service.dispatch(path,data,roles['admin'])
        client=Client();home=self.root/'hermes'
        with self.assertRaises(ValueError):export_skill(home,client,candidate['id'],'bicycle-check')
        self.promote(candidate,self.evaluation(candidate))
        result=export_skill(home,client,candidate['id'],'bicycle-check')
        path=Path(result['path']);self.assertTrue(verify(home,client,path,path.read_text()))
        path.write_text(path.read_text()+'\nLocal edit\n')
        self.assertFalse(verify(home,client,path,path.read_text()))
        with self.assertRaises(ValueError):export_skill(home,client,candidate['id'],'bicycle-check')
        self.call('retract',{'candidate_id':candidate['id']},'admin')
        with self.assertRaises(ValueError):export_skill(home,client,candidate['id'],'another-skill')

if __name__=='__main__':unittest.main()
