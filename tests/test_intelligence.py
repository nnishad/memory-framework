import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from personal_memory.store import Store
from personal_memory.intelligence import Intelligence
from personal_memory.workflows import Workflows
from personal_memory.learning import Learning
from personal_memory.service import MemoryService,AccessDenied
from test_ingestion import item

class IntelligenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
        self.store=Store(self.root/'memory.db');self.i=Intelligence(self.store);self.l=Learning(self.store)
        self.rid=self.store.ingest_contract([item()])['records'][0]['id'];self.refs=[self.rid]
        self.evidence=[{'record_id':self.rid,'quote':'bicycle repair'}]
        self.subject=self.store.entity('person','Synthetic owner')['id'];self.other=self.store.entity('organization','Workshop')['id']
        self.actor='agent'
    def belief(self,key='b1',value='yes',**extra):
        return self.i.belief(key=key,subject_id=self.subject,predicate='repair',value=value,evidence=self.evidence,actor=self.actor,**extra)
    def task(self,key='t1',**extra):return self.i.task(key=key,title=key,evidence_ids=self.refs,actor=self.actor,**extra)
    def candidate(self):
        outcome=self.l.outcome(key='out',goal='Repair',action='Check',result='Checked',outcome='success',evidence_ids=self.refs,actor='agent')
        return self.l.propose(key='cand',family='check',revision=1,category='procedural',lesson='Check brakes',scope='bicycle',prerequisites=[],exceptions=[],outcome_ids=[outcome['id']],evidence_ids=[],actor='agent')
    def worker(self):
        w=Workflows(self.store,{'timeout':5,'adapters':{'evaluate':{'entrypoint':'personal_memory.adapters:policy_fixture','config':{}}},'capabilities':{'echo':{'entrypoint':'personal_memory.adapters:echo_capability','config':{}}}})
        self.addCleanup(w.close);return w
    def evaluated(self):
        w=self.worker();c=self.candidate()
        suite=w.suite(key='suite',cases=[{'id':'t','kind':'target','input':{'scope':'bicycle'},'expected':'Check brakes'},
            {'id':'r','kind':'regression','input':{'scope':'other','fallback':'unchanged'},'expected':'unchanged'},
            {'id':'n','kind':'non_applicable','input':{'scope':'none','fallback':'none'},'expected':'none'}],evidence_ids=self.refs,actor='admin')
        job=w.enqueue(key='eval',type='evaluate',candidate_id=c['id'],suite_id=suite['id'],actor='evaluator')
        self.assertTrue(w.tick());result=w.get(job_id=job['id']);self.assertEqual(result['state'],'completed',result)
        return w,c,result['result']
    def test_snapshot_immutable_replay_and_source_deletion(self):
        first=self.i.snapshot(key='s',record_ids=self.refs,actor='agent')
        self.assertEqual(first,self.i.snapshot(key='s',record_ids=self.refs,actor='agent'))
        self.store.forget(self.rid)
        with self.assertRaises(ValueError):self.i.snapshot(key='s',record_ids=self.refs,actor='agent')
    def test_conflicts_temporal_validity_and_preferences(self):
        self.belief(value='yes',origin='explicit_preference',context={'activity':'cycling'})
        self.belief('b2','no');self.belief('b3','old',valid_to='2020-01-01T00:00:00Z')
        answer=self.i.beliefs(subject_id=self.subject,context={'activity':'cycling'})
        self.assertEqual(answer['beliefs'][0]['payload']['origin'],'explicit_preference');self.assertTrue(answer['conflicts'])
        self.assertEqual(len(self.i.beliefs(subject_id=self.subject)['beliefs']),1)
    def test_quotes_and_entity_ids_are_validated(self):
        with self.assertRaises(ValueError):self.i.belief(key='bad',subject_id=self.subject,predicate='x',value='x',evidence=[{'record_id':self.rid,'quote':'invented'}],actor='agent')
        with self.assertRaises(ValueError):self.i.relation(key='bad',subject_id=self.subject,predicate='knows',object_id='unknown',evidence=self.evidence,actor='agent')
    def test_correction_scope(self):
        a=self.belief();b=self.belief('b2','no')
        self.assertTrue(self.store.search('bicycle',entity_id=self.subject)['episodes'])
        self.i.resolve_belief(belief_id=b['id'],supersedes=[a['id']],actor='admin')
        self.assertFalse(self.i.beliefs(subject_id=self.subject)['conflicts'])
    def test_graph_preserves_paths_without_identity_merge(self):
        self.i.relation(key='r',subject_id=self.subject,predicate='visited',object_id=self.other,evidence=self.evidence,actor='agent')
        result=self.i.graph(entity_id=self.subject);self.assertEqual(len(result['edges']),1)
        self.store.forget(self.rid);self.assertEqual(self.i.graph(entity_id=self.subject)['edges'],[])
    def test_measurements_convert_units_and_aggregate_only_matching_scope(self):
        for key,value,unit in [('a',1000,'g'),('b',2,'kg')]:
            self.i.measurement(key=key,subject_id=self.subject,metric='mass',value=value,unit=unit,measured_at='2026-01-01T12:00:00Z',evidence=self.evidence,actor='agent')
        result=self.i.aggregate(subject_id=self.subject,metric='mass',unit='kg',after='2026-01-01T00:00:00Z',before='2026-01-02T00:00:00Z')
        self.assertEqual(result['mean'],1.5);self.assertEqual(result['count'],2)
        with self.assertRaises(ValueError):self.i.measurement(key='nan',subject_id=self.subject,metric='m',value=float('nan'),unit='kg',measured_at='2026-01-01T00:00:00Z',evidence=self.evidence,actor='agent')
    def test_task_dependencies_version_conflicts_and_terminal_restore(self):
        first=self.task();second=self.task('t2',depends_on=[first['id']])
        with self.assertRaises(ValueError):self.i.transition(task_id=second['id'],expected_version=1,state='in_progress',evidence_ids=self.refs,actor='agent')
        self.i.transition(task_id=first['id'],expected_version=1,state='in_progress',evidence_ids=self.refs,actor='agent')
        self.i.transition(task_id=first['id'],expected_version=2,state='completed',evidence_ids=self.refs,actor='agent')
        reopened=Intelligence(Store(self.store.path))
        self.assertEqual(reopened.transition(task_id=second['id'],expected_version=1,state='in_progress',evidence_ids=self.refs,actor='agent')['version'],2)
    def test_events_lease_ack_and_cancellation(self):
        task=self.task(due_at='2020-01-01T00:00:00Z');event=self.i.claim_event(actor='scheduler')['event']
        self.assertIsNotNone(event);self.assertIsNone(self.i.claim_event(actor='scheduler')['event'])
        self.i.transition(task_id=task['id'],expected_version=1,state='cancelled',evidence_ids=self.refs,actor='agent')
        with self.assertRaises(ValueError):self.i.ack_event(event_id=event['id'],lease=event['lease'],actor='scheduler')
        self.assertIsNone(self.i.claim_event(actor='scheduler')['event'])
    def test_event_ack_replay(self):
        self.task(due_at='2020-01-01T00:00:00Z');e=self.i.claim_event(actor='scheduler')['event']
        self.assertEqual(self.i.ack_event(event_id=e['id'],lease=e['lease'],actor='scheduler'),self.i.ack_event(event_id=e['id'],lease=e['lease'],actor='scheduler'))
    def test_consolidation_subprocess_and_pending_output(self):
        w=self.worker();snapshot=self.i.snapshot(key='s',record_ids=self.refs,actor='agent')
        job=w.enqueue(key='c',type='consolidate',snapshot_id=snapshot['id'],actor='agent');self.assertTrue(w.tick())
        result=w.get(job_id=job['id']);self.assertEqual(result['state'],'completed',result)
        self.assertEqual(result['result']['state'],'candidate')
        self.assertEqual(w.enqueue(key='c',type='consolidate',snapshot_id=snapshot['id'],actor='agent')['id'],job['id'])
    def test_executed_suite_and_capability_validation(self):
        w,c,e=self.evaluated();self.assertEqual(e['payload']['verification'],'executed_suite');self.assertTrue(e['payload']['passed'])
        self.l.promote(candidate_id=c['id'],evaluation_id=e['id'],expected_active_id=None,actor='admin')
        p=w.procedure(key='p',candidate_id=c['id'],capability='echo',arguments={'value':1},expected={'value':1},actor='admin')
        job=w.execute(key='run',procedure_id=p['id'],actor='executor');w.tick();result=w.get(job_id=job['id'])
        self.assertTrue(result['result']['payload']['validation_passed']);self.assertEqual(w.execute(key='run',procedure_id=p['id'],actor='executor')['state'],'completed')
    def test_evaluation_source_deletion_invalidates_procedure(self):
        w,c,e=self.evaluated();self.l.promote(candidate_id=c['id'],evaluation_id=e['id'],expected_active_id=None,actor='admin')
        p=w.procedure(key='p',candidate_id=c['id'],capability='echo',arguments={},expected={},actor='admin')
        self.store.forget(self.rid)
        with self.assertRaises(ValueError):w.execute(key='blocked',procedure_id=p['id'],actor='executor')
    def test_failed_adapter_is_retryable_and_quarantined(self):
        w=Workflows(self.store,{'adapters':{'consolidate':{'entrypoint':'personal_memory.adapters:missing','config':{}}}})
        s=self.i.snapshot(key='s',record_ids=self.refs,actor='agent');j=w.enqueue(key='bad',type='consolidate',snapshot_id=s['id'],actor='agent')
        for _ in range(3):
            w.tick()
            with self.store.connect() as db:db.execute('UPDATE workflow_jobs SET retry_at=0')
        self.assertEqual(w.get(job_id=j['id'])['state'],'quarantined')
        self.assertEqual(w.retry(job_id=j['id'],actor='admin')['state'],'pending')
    def test_auto_cursor_replay(self):
        w=Workflows(self.store,{'auto_consolidate':True});self.assertEqual(w.scan(),1);self.assertEqual(w.scan(),0)
        self.assertTrue(w.tick())
    def test_feedback_idempotency_and_forgetting(self):
        self.i.feedback(key='f',record_id=self.rid,result='stale',actor='agent');self.i.feedback(key='f',record_id=self.rid,result='stale',actor='agent')
        self.assertEqual(self.i.quality()['feedback'][0]['count'],1)
        self.store.forget(self.rid);self.assertEqual(self.i.quality()['feedback'],[])
    def test_terminal_task_survives_old_database_restore(self):
        task=self.task(due_at='2020-01-01T00:00:00Z');old=self.root/'old.db'
        with self.store.connect() as source,sqlite3.connect(old) as target:source.backup(target)
        self.i.transition(task_id=task['id'],expected_version=1,state='cancelled',evidence_ids=self.refs,actor='agent')
        from personal_memory.deletions import DeletionLedger
        DeletionLedger(self.root/'old.deletions.db').merge(self.store.deletions)
        self.assertIsNone(Intelligence(Store(old)).claim_event(actor='scheduler')['event'])
    def test_hub_cannot_escalate_roles(self):
        service=MemoryService(self.root/'service','a'*40,principals=[{'token':'g'*40,'role':'agent'},{'token':'s'*40,'role':'scheduler'}]);self.addCleanup(service.close)
        agent=service.authenticate('Bearer '+'g'*40);scheduler=service.authenticate('Bearer '+'s'*40)
        with self.assertRaises(ValueError):service.dispatch('/v1/intelligence/write',{'operation':'promote','arguments':{}},agent)
        with self.assertRaises(AccessDenied):service.dispatch('/v1/suite',{},agent)
        with self.assertRaises(AccessDenied):service.dispatch('/v1/search',{'query':'private'},scheduler)
        with self.assertRaises(AccessDenied):service.dispatch('/v1/workflow/enqueue',{'type':'evaluate'},agent)
        self.assertIn('learning_states',service.dispatch('/v1/intelligence/read',{'operation':'quality','arguments':{}},agent))

    def test_hub_read_contract_is_self_correcting(self):
        from personal_memory.ingestion import ContractError
        service=MemoryService(self.root/'hub-service','a'*40,principals=[{'token':'g'*40,'role':'agent'}]);self.addCleanup(service.close)
        agent=service.authenticate('Bearer '+'g'*40)
        # A read may omit arguments entirely; the shape check fills the empty object.
        self.assertIn('learning_states',service.dispatch('/v1/intelligence/read',{'operation':'quality'},agent))
        # An empty body names the exact contract through a typed detail, not a bare rejection.
        with self.assertRaises(ContractError) as missing:service.dispatch('/v1/intelligence/read',{},agent)
        self.assertEqual(missing.exception.path,'$')
        # An unknown operation points the caller at the schema listing so a small model can recover.
        with self.assertRaises(ContractError) as unknown:service.dispatch('/v1/intelligence/read',{'operation':'nope','arguments':{}},agent)
        self.assertEqual(unknown.exception.path,'$.operation')

    def test_domain_coverage_gates_quantified_beliefs(self):
        self.i.domain(domain='repairs',required_sources=['custom-notes'],scope='all repair notes',actor='admin')
        with self.assertRaises(ValueError):self.belief(quantifier='none',domain='repairs')
        self.store.coverage('custom-notes','complete',through_at='2030-01-01T00:00:00Z',note='Synthetic complete repair scope')
        self.assertTrue(self.i.coverage(domain='repairs')['complete'])
        self.assertEqual(self.belief(quantifier='none',domain='repairs')['payload']['quantifier'],'none')
    def test_suite_immutability(self):
        w,c,e=self.evaluated()
        with self.assertRaises(ValueError):w.suite(key='suite',cases=[{'id':k,'kind':k,'input':{},'expected':'changed'} for k in ['target','regression','non_applicable']],evidence_ids=self.refs,actor='admin')
    def test_auto_learning_policy_promotion_and_recovery(self):
        w,c,e=self.evaluated()
        w.config['learning_policies']={'bicycle':{'suite_id':e['payload']['suite_id'],'promote':True}}
        w.reconcile_promotions()
        self.assertEqual(self.l.browse()['items'][0]['id'],c['id'])
    def test_event_delivery_fixture_has_no_external_effect(self):
        self.task(due_at='2020-01-01T00:00:00Z')
        w=Workflows(self.store,{'event_handler':{'entrypoint':'personal_memory.adapters:event_fixture','config':{}}})
        self.assertTrue(w.deliver_event());self.assertFalse(w.deliver_event())
    def test_consolidation_proposals_remain_unverified_after_review(self):
        from unittest.mock import patch
        w=self.worker();snapshot=self.i.snapshot(key='s',record_ids=self.refs,actor='agent')
        job=w.enqueue(key='c',type='consolidate',snapshot_id=snapshot['id'],actor='agent')
        result={'summary':'Repair note','quotes':self.evidence,'proposals':[{'subject_id':self.subject,'predicate':'repair','value':'needed','evidence':self.evidence}]}
        with patch('personal_memory.workflows.run_adapter',return_value=result):w.tick()
        completed=w.get(job_id=job['id']);self.assertEqual(completed['state'],'completed',completed)
        self.assertEqual(self.i.beliefs(subject_id=self.subject)['beliefs'],[])
        accepted=w.accept_consolidation(result_id=completed['result_id'],actor='admin')
        self.assertEqual(len(accepted['belief_ids']),1)
        self.assertEqual(len(self.i.summaries(query='Repair')['summaries']),1)
        self.assertEqual(self.i.beliefs(subject_id=self.subject)['beliefs'][0]['payload']['truth'],'unverified')
        self.store.forget(self.rid);self.assertEqual(self.i.beliefs(subject_id=self.subject)['beliefs'],[])
        self.assertEqual(self.i.summaries(query='Repair')['summaries'],[])
    def test_abandoned_job_lease_is_reclaimed(self):
        w=self.worker();snapshot=self.i.snapshot(key='s',record_ids=self.refs,actor='agent')
        job=w.enqueue(key='c',type='consolidate',snapshot_id=snapshot['id'],actor='agent')
        with self.store.connect() as db:db.execute("UPDATE workflow_jobs SET state='running',attempts=1,lease='dead-process',lease_until=0 WHERE id=?",(job['id'],))
        self.assertTrue(w.tick());self.assertEqual(w.get(job_id=job['id'])['state'],'completed')
    def test_bad_adapter_config_fails_before_worker_start(self):
        with self.assertRaises(ValueError):Workflows(self.store,{'auto_consolidate':'false'})
        with self.assertRaises(ValueError):Workflows(self.store,{'unknown':True})
    def test_planner_reranker_and_duplicate_context(self):
        from unittest.mock import patch
        from personal_memory.adaptive import AdaptiveRecall
        from personal_memory.retrieval import Hybrid
        second=self.store.ingest_contract([item('copy')])['records'][0]['id']
        backend=Hybrid(self.store,start=False);self.addCleanup(backend.close)
        adaptive=AdaptiveRecall(self.store,backend,{'planner':{'entrypoint':'test:planner'},'reranker':{'entrypoint':'test:reranker'}})
        def adapter(entry,config,request,timeout):
            return {'queries':['repair']} if entry.endswith('planner') else {'ordered_ids':[r['id'] for r in reversed(request['documents'])]}
        with patch('personal_memory.workflows.run_adapter',side_effect=adapter):result=adaptive.search('bicycle')
        self.assertEqual(result['diagnostics']['planner'],'operator_planner')
        self.assertEqual(len(result['episodes']),1);self.assertEqual(len(result['episodes'][0]['duplicate_record_ids']),1)
    def test_executor_capability_scope(self):
        w,c,e=self.evaluated();self.l.promote(candidate_id=c['id'],evaluation_id=e['id'],expected_active_id=None,actor='admin')
        procedure=w.procedure(key='p',candidate_id=c['id'],capability='echo',arguments={},expected={},actor='admin')
        service=MemoryService(self.root,'a'*40,principals=[{'token':'g'*40,'role':'agent'}]);self.addCleanup(service.close)
        with self.assertRaises(AccessDenied):service.dispatch('/v1/procedure/execute',{'key':'bad','procedure_id':procedure['id']},service.authenticate('Bearer '+'g'*40))

    def test_delivered_events_do_not_starve_later_tasks(self):
        self.task('old',due_at='2020-01-01T00:00:00Z')
        event=self.i.claim_event(actor='scheduler')['event'];self.i.ack_event(event_id=event['id'],lease=event['lease'],actor='scheduler')
        later=self.task('later',due_at='2021-01-01T00:00:00Z')
        self.assertEqual(self.i.claim_event(actor='scheduler')['event']['task_id'],later['id'])
    def test_event_retry_limit_and_administrator_replay(self):
        self.task(due_at='2020-01-01T00:00:00Z')
        for _ in range(3):
            event=self.i.claim_event(actor='scheduler')['event'];self.assertIsNotNone(event)
            with self.store.connect() as db:db.execute('UPDATE task_events SET lease_until=0')
        self.assertIsNone(self.i.claim_event(actor='scheduler')['event'])
        self.i.retry_event(event_id=event['id'],actor='admin')
        self.assertIsNotNone(self.i.claim_event(actor='scheduler')['event'])
    def test_typed_metric_registry_rejects_incompatible_units(self):
        with self.assertRaises(ValueError):self.i.measurement(key='bad-unit',subject_id=self.subject,metric='heart_rate',value=60,unit='kg',measured_at='2026-01-01T00:00:00Z',evidence=self.evidence,actor='agent')
        self.i.metric(metric='custom.distance.v1',canonical_unit='m',conversions={'m':[1,0],'km':[1000,0]},actor='admin')
        value=self.i.measurement(key='distance',subject_id=self.subject,metric='custom.distance.v1',value=2,unit='km',measured_at='2026-01-01T00:00:00Z',evidence=self.evidence,actor='agent')
        self.assertEqual(value['payload']['value'],2000)
        with self.assertRaises(ValueError):self.i.metric(metric='custom.distance.v1',canonical_unit='m',conversions={'m':[1,0],'km':[1,0]},actor='admin')
    def test_encrypted_intelligence_restore_applies_current_deletions(self):
        from personal_memory.recovery import backup,restore,create_key
        from personal_memory.common import atomic_json
        w,c,e=self.evaluated();self.l.promote(candidate_id=c['id'],evaluation_id=e['id'],expected_active_id=None,actor='admin')
        task=self.task(due_at='2020-01-01T00:00:00Z')
        home=self.root/'profile';atomic_json(home/'personal-memory/settings.json',{'data_dir':str(self.root)})
        key=self.root/'key';create_key(key);archive=self.root/'backup.enc';backup(home,archive,key)
        self.i.transition(task_id=task['id'],expected_version=1,state='cancelled',evidence_ids=self.refs,actor='agent')
        self.store.forget(self.rid)
        destination=self.root/'restored';restore(archive,key,destination,deletion_ledger=self.store.deletions.path)
        restored=Store(destination/'memory.db')
        self.assertEqual(Learning(restored).browse()['items'],[])
        self.assertIsNone(Intelligence(restored).claim_event(actor='scheduler')['event'])

    def test_automatic_consolidation_partitions_entire_long_source(self):
        source=item('long');source['text']='start '+'x'*4500+' final-evidence'
        rid=self.store.ingest_contract([source])['records'][0]['id']
        worker=Workflows(self.store,{'auto_consolidate':True});worker.scan()
        with self.store.connect() as db:
            payloads=[json.loads(r[0]) for r in db.execute("SELECT payload FROM learning_objects WHERE kind='job'")]
        segments=[segment for payload in payloads for segment in (payload.get('segments') or []) if segment['record_id']==rid]
        self.assertEqual(min(s['start'] for s in segments),0)
        self.assertEqual(max(s['end'] for s in segments),len(source['text']))
        for left,right in zip(sorted(segments,key=lambda s:s['start']),sorted(segments,key=lambda s:s['start'])[1:]):self.assertLessEqual(right['start'],left['end'])

    def test_automatic_policy_enqueues_and_promotes_new_candidate(self):
        w,c,e=self.evaluated()
        second=self.l.propose(key='cand2',family='second-family',revision=1,category='procedural',lesson='Check brakes',scope='bicycle',prerequisites=[],exceptions=[],outcome_ids=[self.l.outcome(key='out',goal='Repair',action='Check',result='Checked',outcome='success',evidence_ids=self.refs,actor='agent')['id']],evidence_ids=[],actor='agent')
        w.config['learning_policies']={'bicycle':{'suite_id':e['payload']['suite_id'],'promote':True}}
        w.autolearn()
        for _ in range(4):w.tick()
        self.assertIn(second['id'],[c['id'] for c in self.l.browse()['items']])
    def test_capability_scope_cannot_be_a_string(self):
        with self.assertRaises(ValueError):MemoryService(self.root/'bad-service','a'*40,principals=[{'token':'g'*40,'role':'agent','capabilities':'echo'}])
    def test_model_adapters_against_local_http_protocol_fixture(self):
        import threading
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        from personal_memory.adapters import openai_consolidate,openai_plan,openai_rerank,openai_evaluate
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                request=json.loads(body['messages'][-1]['content'])
                if 'evidence' in request:
                    result={'summary':'fixture','quotes':[{'record_id':request['evidence'][0]['record_id'],'quote':'bicycle repair'}]}
                elif 'documents' in request:result={'ordered_ids':[r['id'] for r in request['documents']]}
                elif 'input' in request:result={'answer':'fixture'}
                else:result={'queries':['bicycle repair']}
                raw=json.dumps({'choices':[{'message':{'content':json.dumps(result)}}]}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        cfg={'url':f'http://127.0.0.1:{server.server_port}/v1','model':'protocol-fixture'}
        self.assertEqual(openai_plan(cfg,{'query':'repair'})['queries'],['bicycle repair'])
        self.assertEqual(openai_rerank(cfg,{'query':'q','documents':[{'id':self.rid,'text':'bicycle'}]})['ordered_ids'],self.refs)
        self.assertEqual(openai_consolidate(cfg,{'evidence':[{'record_id':self.rid,'text':'bicycle repair'}]})['summary'],'fixture')
        self.assertEqual(openai_evaluate(cfg,{'input':{},'candidate':None}),{'answer':'fixture'})

if __name__=='__main__':unittest.main()
