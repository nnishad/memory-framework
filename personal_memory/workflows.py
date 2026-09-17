"""Leased durable jobs, immutable suites and bounded operator-installed adapters."""
import json
import builtins
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from .common import digest, now, required_text
from .learning import Learning, strings, document

SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_jobs(
 id TEXT PRIMARY KEY REFERENCES learning_objects(id), type TEXT NOT NULL, state TEXT NOT NULL,
 attempts INTEGER NOT NULL, lease TEXT, lease_until REAL, retry_at REAL NOT NULL,
 result_id TEXT REFERENCES learning_objects(id), error TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS workflow_pending ON workflow_jobs(state,retry_at);
CREATE TABLE IF NOT EXISTS workflow_cursor(name TEXT PRIMARY KEY, watermark INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS evaluation_suites(
 id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL);
"""


def run_adapter(entrypoint,configuration,request,timeout=30):
    if not isinstance(entrypoint,str) or ':' not in entrypoint:raise ValueError('Operator adapter requires module:function')
    # The process receives only explicitly supplied configuration and evidence.
    # This is timeout isolation for trusted plugins, not an OS security sandbox.
    env={k:v for k,v in os.environ.items() if k in {'PATH','SYSTEMROOT','LANG','LC_ALL','HOME','TMPDIR'}}
    env['PYTHONPATH']=str(Path(__file__).resolve().parent.parent)
    payload=document({'config':configuration,'request':request})
    with tempfile.TemporaryDirectory(prefix='memory-adapter-') as tmp:
        # Disk-bounded by the trusted adapter contract; stdout protocol itself is capped below.
        with tempfile.TemporaryFile() as output,tempfile.TemporaryFile() as errors:
            process=subprocess.Popen([sys.executable,'-m','personal_memory.runner',entrypoint],stdin=subprocess.PIPE,
                                     stdout=output,stderr=errors,env=env,cwd=tmp,start_new_session=(os.name=='posix'))
            try:
                process.communicate(payload.encode(),timeout=timeout)
                if process.returncode:raise subprocess.CalledProcessError(process.returncode,entrypoint)
            except BaseException:
                if process.poll() is None:
                    if os.name=='posix':
                        import signal
                        os.killpg(process.pid,signal.SIGKILL)
                    else:process.kill()
                    process.wait()
                raise
            output.seek(0);raw=output.read(65537)
            if len(raw)>65536:raise ValueError('Adapter output exceeds 64 KiB')
    from .asgi import strict_json
    result=strict_json(raw)
    document(result)
    return result


def validate_intelligence_config(config=None):
    """Pure operator-configuration check, reusable before any worker is started."""
    config={} if config is None else config
    if not isinstance(config,dict) or set(config)-{'timeout','adapters','capabilities','auto_consolidate','learning_policies','event_handler','recall'}:raise ValueError('Unknown intelligence configuration')
    if type(config.get('auto_consolidate',False)) is not bool:raise ValueError('auto_consolidate must be boolean')
    if not isinstance(config.get('learning_policies',{}),dict):raise ValueError('Invalid learning policies')
    for scope,policy in config.get('learning_policies',{}).items():
        required_text(scope,'scope',12000)
        if not isinstance(policy,dict) or set(policy)!={'suite_id','promote'} or type(policy['promote']) is not bool:raise ValueError('Invalid learning policy')
        required_text(policy['suite_id'],'suite_id',200)
    for name in ['capabilities','adapters']:
        if not isinstance(config.get(name,{}),dict):raise ValueError('Invalid adapter registry')
        for key,adapter in config.get(name,{}).items():
            required_text(key,'adapter name',200)
            if not isinstance(adapter,dict) or set(adapter)-{'entrypoint','config'} or ':' not in adapter.get('entrypoint',''):raise ValueError('Invalid adapter declaration')
    if config.get('event_handler'):
        adapter=config['event_handler']
        if not isinstance(adapter,dict) or set(adapter)-{'entrypoint','config'} or ':' not in adapter.get('entrypoint',''):raise ValueError('Invalid event adapter')
    timeout=config.get('timeout',20)
    if type(timeout) is not int or not 1<=timeout<=60:raise ValueError('Worker timeout must be 1..60')
    adapters={'consolidate':{'entrypoint':'personal_memory.adapters:extractive','config':{}},**config.get('adapters',{})}
    for kind,value in adapters.items():
        if kind not in {'consolidate','evaluate','procedure'} or not isinstance(value,dict) or set(value)-{'entrypoint','config'}:raise ValueError('Invalid operator adapter configuration')
        required_text(value.get('entrypoint'),'entrypoint',300)
    return config


class Workflows:
    def __init__(self,store,config=None):
        self.store=store;self.learning=Learning(store);self.config=validate_intelligence_config(config or {})

        self.stop=threading.Event();self.thread=None;self.last_error=None
        self.timeout=self.config.get('timeout',20)
        self.adapters={'consolidate':{'entrypoint':'personal_memory.adapters:extractive','config':{}},**self.config.get('adapters',{})}

    def suite(self,*,key,cases,evidence_ids,actor):
        required_text(key,'key',200)
        if not isinstance(cases,list) or not 3<=len(cases)<=50:raise ValueError('Suite needs 3..50 cases')
        seen=set()
        for case in cases:
            if not isinstance(case,dict) or set(case)!={'id','kind','input','expected'}:raise ValueError('Invalid suite case')
            cid=required_text(case['id'],'case ID',200)
            if cid in seen or case['kind'] not in {'target','regression','non_applicable'}:raise ValueError('Invalid case identity or kind')
            seen.add(cid)
        if {c['kind'] for c in cases}!={'target','regression','non_applicable'}:raise ValueError('All evaluation categories required')
        payload={'cases':cases};document(payload)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            obj=self.learning._put(db,'suite',key,payload,actor,strings(evidence_ids,'evidence_ids'))
        return {'id':obj['id'],'digest':digest(payload),'case_count':len(cases)}

    def enqueue(self,*,key,type,snapshot_id=None,candidate_id=None,suite_id=None,segments=None,actor):
        if type not in {'consolidate','evaluate'}:raise ValueError('Unsupported job type')
        if type not in self.adapters:raise ValueError('Operator must configure this adapter')
        adapter=self.adapters[type]
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if type=='consolidate':
                snapshot=self.learning._get(db,snapshot_id,'snapshot');parents=[snapshot_id]
                if segments is None:
                    segments=[]
                    for rid in snapshot['payload']['record_ids']:
                        row=db.execute('SELECT length(text) FROM records WHERE id=? AND deleted=0',(rid,)).fetchone()
                        if not row:raise ValueError('Snapshot source unavailable')
                        segments.append({'record_id':rid,'start':0,'end':row[0]})
                if not isinstance(segments,list) or not 1<=len(segments)<=32:raise ValueError('Partition snapshot into 1..32 segments per job')
                size=0
                for segment in segments:
                    if not isinstance(segment,dict) or set(segment)!={'record_id','start','end'}:raise ValueError('Invalid evidence segment')
                    if segment['record_id'] not in snapshot['payload']['record_ids'] or builtins.type(segment['start']) is not int or builtins.type(segment['end']) is not int or not 0<=segment['start']<segment['end']:raise ValueError('Invalid segment span')
                    text=db.execute('SELECT text FROM records WHERE id=? AND deleted=0',(segment['record_id'],)).fetchone()
                    if not text or segment['end']>len(text[0]):raise ValueError('Segment exceeds source bounds')
                    size+=len(text[0][segment['start']:segment['end']].encode())
                if size>24000:raise ValueError('Partition snapshot: at most 24000 evidence bytes per job')
                if candidate_id is not None or suite_id is not None:raise ValueError('Unexpected consolidation arguments')
            else:
                candidate=self.learning._get(db,candidate_id,'candidate')
                if candidate['state']!='candidate':raise ValueError('Evaluation requires pending candidate')
                if snapshot_id is not None or segments is not None:raise ValueError('Unexpected evaluation snapshot')
                self.learning._get(db,suite_id,'suite')
                parents=[candidate_id,suite_id]
            payload={'type':type,'snapshot_id':snapshot_id,'candidate_id':candidate_id,'suite_id':suite_id,'segments':segments,'adapter_digest':digest(adapter)}
            obj=self.learning._put(db,'job',key,payload,actor,[],parents)
            db.execute("INSERT OR IGNORE INTO workflow_jobs(id,type,state,attempts,retry_at) VALUES(?,?,'pending',0,0)",(obj['id'],type))
            return {'id':obj['id'],'state':db.execute('SELECT state FROM workflow_jobs WHERE id=?',(obj['id'],)).fetchone()[0]}

    def procedure(self,*,key,candidate_id,capability,arguments,expected,actor):
        capabilities=self.config.get('capabilities',{})
        if capability not in capabilities:raise ValueError('Capability is not operator-installed')
        if not isinstance(arguments,dict):raise ValueError('Procedure arguments must be an object')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');candidate=self.learning._get(db,candidate_id,'candidate')
            if candidate['state']!='active' or candidate['payload']['category']!='procedural':raise ValueError('Procedure requires an active evaluated procedural lesson')
            payload={'candidate_id':candidate_id,'capability':capability,'arguments':arguments,'expected':expected,'adapter_digest':digest(capabilities[capability])}
            return self.learning._put(db,'procedure',key,payload,actor,[],[candidate_id])

    def execute(self,*,key,procedure_id,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');procedure=self.learning._get(db,procedure_id,'procedure')
            payload={'type':'procedure','procedure_id':procedure_id,'adapter_digest':procedure['payload']['adapter_digest']}
            job=self.learning._put(db,'job',key,payload,actor,[],[procedure_id])
            db.execute("INSERT OR IGNORE INTO workflow_jobs(id,type,state,attempts,retry_at) VALUES(?,'procedure','pending',0,0)",(job['id'],))
            return {'id':job['id'],'state':db.execute('SELECT state FROM workflow_jobs WHERE id=?',(job['id'],)).fetchone()[0]}

    def _procedure(self,job):
        with self.store.connect() as db:
            procedure=self.learning._get(db,job['payload']['procedure_id'],'procedure')
            candidate=self.learning._get(db,procedure['payload']['candidate_id'],'candidate')
            if candidate['state']!='active':raise ValueError('Procedure lesson is no longer active')
        payload=procedure['payload'];adapter=self.config.get('capabilities',{}).get(payload['capability'])
        if not adapter or digest(adapter)!=payload['adapter_digest']:raise ValueError('Capability version changed or was disabled')
        output=run_adapter(adapter['entrypoint'],adapter.get('config',{}),{'arguments':payload['arguments'],'idempotency_key':job['id']},self.timeout)
        return {'kind':'execution','payload':{'procedure_id':procedure['id'],'output':output,'validation_passed':document(output)==document(payload['expected']),'adapter_digest':digest(adapter)},'parents':[procedure['id']]},adapter

    def get(self,*,job_id):
        with self.store.connect() as db:
            self.learning._get(db,job_id,'job')
            row=db.execute('SELECT id,type,state,attempts,result_id,error FROM workflow_jobs WHERE id=?',(job_id,)).fetchone()
            result=dict(row)
            if row['result_id']:
                result['result']=self.learning._get(db,row['result_id'])
            return result

    def retry(self,*,job_id,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self.learning._get(db,job_id,'job')
            cursor=db.execute("UPDATE workflow_jobs SET state='pending',attempts=0,retry_at=0,error='' WHERE id=? AND state='quarantined'",(job_id,))
            if not cursor.rowcount:raise ValueError('Only quarantined jobs can be retried')
            self.store.audit(db,'job_retry',job_id,{'actor':actor})
            return {'id':job_id,'state':'pending'}

    def tick(self):
        if self.stop.is_set():return False
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');clock=time.time()
            db.execute("UPDATE workflow_jobs SET state='quarantined',lease=NULL,lease_until=NULL,error='LeaseExpired' WHERE state='running' AND lease_until<? AND attempts>=3",(clock,))
            row=db.execute("""SELECT j.* FROM workflow_jobs j JOIN learning_objects o ON o.id=j.id WHERE o.state='recorded'
                AND ((j.state='pending' AND j.retry_at<=?) OR (j.state='running' AND j.lease_until<?)) ORDER BY o.created_at,j.id LIMIT 1""",(clock,clock)).fetchone()
            if row is None:return False
            lease=uuid.uuid4().hex
            # Evaluation can run baseline + candidate for every suite case.
            lease_seconds=self.timeout+30
            db.execute("UPDATE workflow_jobs SET state='running',attempts=attempts+1,lease=?,lease_until=? WHERE id=?",(lease,clock+lease_seconds,row['id']))
            job=self.learning._get(db,row['id'],'job');job['_lease']=lease
        try:
            if job['payload']['type']=='procedure':result,adapter=self._procedure(job)
            else:
                adapter=self.adapters[job['payload']['type']]
                if digest(adapter)!=job['payload']['adapter_digest']:raise ValueError('Adapter configuration changed; submit a new job')
                if job['payload']['type']=='consolidate':result=self._consolidate(job,adapter)
                else:result=self._evaluate(job,adapter)
            with self.store.lock,self.store.connect() as db:
                db.execute('BEGIN IMMEDIATE');self.learning._get(db,job['id'],'job')
                current=db.execute('SELECT lease FROM workflow_jobs WHERE id=?',(job['id'],)).fetchone()[0]
                if current!=lease:raise ValueError('Job lease superseded')
                artifact=self.learning._put(db,result['kind'],job['id']+'/result',result['payload'],'worker:'+digest(adapter),[],[job['id']]+result.get('parents',[]))
                # Consolidated model output is pending evidence, never active truth.
                if result['kind']=='consolidation':db.execute("UPDATE learning_objects SET state='candidate' WHERE id=?",(artifact['id'],))
                db.execute("UPDATE workflow_jobs SET state='completed',result_id=?,lease=NULL,lease_until=NULL,error='' WHERE id=?",(artifact['id'],job['id']))
            if result['kind']=='evaluation':
                try:self._auto_promote(artifact)
                except ValueError:
                    with self.store.connect() as db:self.store.audit(db,'auto_promotion_blocked',artifact['id'])
        except Exception as error:
            with self.store.lock,self.store.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("""UPDATE workflow_jobs SET state=CASE WHEN attempts>=3 THEN 'quarantined' ELSE 'pending' END,
                    retry_at=?,lease=NULL,lease_until=NULL,error=? WHERE id=? AND lease=?""",
                    (time.time()+min(60,2**(row['attempts']+1)),type(error).__name__,job['id'],lease))
        return True

    def _consolidate(self,job,adapter):
        with self.store.connect() as db:
            snapshot=self.learning._get(db,job['payload']['snapshot_id'],'snapshot')
            evidence=[]
            for segment in job['payload']['segments']:
                rid=segment['record_id']
                r=db.execute('SELECT id,text,fingerprint FROM records WHERE id=? AND deleted=0',(rid,)).fetchone()
                if not r or r['fingerprint']!=snapshot['payload']['fingerprints'][rid]:raise ValueError('Snapshot evidence unavailable')
                evidence.append({'record_id':rid,'text':r['text'][segment['start']:segment['end']],'start':segment['start'],'end':segment['end']})
            entities=[]
            for record in evidence:
                entities.extend(dict(e) for e in db.execute('SELECT e.id,e.kind,e.label FROM entities e JOIN entity_links l ON l.entity_id=e.id WHERE l.record_id=? LIMIT 8',(record['record_id'],)))
            entities=list({e['id']:e for e in entities}.values())[:64]
        result=run_adapter(adapter['entrypoint'],adapter.get('config',{}),{'evidence':evidence,'known_entities':entities},self.timeout)
        if not isinstance(result,dict) or set(result)-{'summary','quotes','proposals'} or not {'summary','quotes'}<=set(result):raise ValueError('Consolidator must return summary, quotes and optional proposals')
        required_text(result['summary'],'summary',12000)
        supplied={}
        for segment in evidence:supplied.setdefault(segment['record_id'],[]).append(segment['text'])
        def validate_supplied_quotes(quotes):
            if not isinstance(quotes,list):raise ValueError('Evidence quotes must be an array')
            for quote in quotes:
                if (not isinstance(quote,dict) or set(quote)!={'record_id','quote'} or
                        not isinstance(quote['record_id'],str) or
                        not isinstance(quote['quote'],str) or not quote['quote'] or
                        not any(quote['quote'] in text for text in supplied.get(quote['record_id'],[]))):
                    raise ValueError('Quote must occur within supplied evidence spans')
        validate_supplied_quotes(result['quotes'])
        from .intelligence import Intelligence
        with self.store.connect() as db:
            intelligence=Intelligence(self.store)
            snapshot_records=set(snapshot['payload']['record_ids'])
            refs=intelligence._quote(db,result['quotes'])
            if set(refs)-snapshot_records:raise ValueError('Consolidator referenced evidence outside snapshot')
            proposals=result.get('proposals',[])
            if not isinstance(proposals,list) or len(proposals)>32:raise ValueError('At most 32 structured proposals')
            from .intelligence import scalar
            accepted=[]
            for proposal in proposals:
                try:
                    if not isinstance(proposal,dict) or set(proposal)!={'subject_id','predicate','value','evidence'}:raise ValueError('Invalid structured proposal')
                    validate_supplied_quotes(proposal['evidence'])
                    intelligence._entity(db,proposal['subject_id']);required_text(proposal['predicate'],'predicate',200);scalar(proposal['value'])
                    if set(intelligence._quote(db,proposal['evidence']))-snapshot_records:raise ValueError('Proposal evidence outside snapshot')
                except (ValueError,KeyError,TypeError):
                    continue
                accepted.append(proposal)
        return {'kind':'consolidation','payload':{**result,'proposals':accepted,'rejected_proposals':len(proposals)-len(accepted),'snapshot_id':snapshot['id'],'adapter_digest':digest(adapter),'status':'unverified_summary','segments':job['payload']['segments'],'coverage':'Only the explicit source spans in this job; canonical originals remain complete'},'parents':[snapshot['id']]}

    def accept_consolidation(self,*,result_id,actor):
        # Independent administrative review publishes attributed inferred beliefs.
        # It does not label their contents verified or grant permissions.
        from . import lifecycle
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');result=self.learning._get(db,result_id,'consolidation')
            if result['state'] not in {'candidate','recorded'}:raise ValueError('Consolidation unavailable')
            # Targeted validation: this review may only publish from evidence that is
            # still live and visible. The archive-wide repair is a startup migration.
            cited=sorted({quote['record_id'] for quote in result['payload'].get('quotes',[]) if isinstance(quote,dict) and 'record_id' in quote})
            lifecycle.require_live_evidence(db,cited,message='Consolidation evidence was retired; submit a new job')
            created=[]
            for index,proposal in enumerate(result['payload'].get('proposals',[])):
                payload={**proposal,'origin':'inferred','context':{},'valid_from':None,'valid_to':None,'truth':'unverified','quantifier':'value','domain':None}
                belief=self.learning._put(db,'belief',result_id+'/'+str(index),payload,actor,[],[result_id])
                created.append(belief['id'])
                db.execute('INSERT OR IGNORE INTO entity_links SELECT ?,record_id,? FROM learning_evidence WHERE object_id=?',(proposal['subject_id'],proposal['predicate'],belief['id']))
            db.execute("UPDATE learning_objects SET state='recorded' WHERE id=?",(result_id,))
            db.execute('DELETE FROM knowledge_fts WHERE id=?',(result_id,))
            db.execute('INSERT INTO knowledge_fts VALUES(?,?)',(result_id,result['payload']['summary']))
            self.store.audit(db,'consolidation_accept',result_id,{'actor':actor})
            return {'result_id':result_id,'belief_ids':created,'truth':'unverified'}

    def _evaluate(self,job,adapter):
        with self.store.connect() as db:
            candidate=self.learning._get(db,job['payload']['candidate_id'],'candidate')
            if candidate['state']!='candidate':raise ValueError('Candidate is not pending')
            suite=self.learning._get(db,job['payload']['suite_id'],'suite')['payload']
            baseline_row=db.execute("SELECT id,payload FROM learning_objects WHERE kind='candidate' AND state='active' AND json_extract(payload,'$.family')=?",(candidate['payload']['family'],)).fetchone()
            baseline=json.loads(baseline_row['payload']) if baseline_row else None
            baseline_id=baseline_row['id'] if baseline_row else None
        cases=[]
        for case in suite['cases']:
            if self.stop.is_set():raise RuntimeError('Worker stopping')
            outputs=[]
            for policy in [baseline,candidate['payload']]:
                if self.stop.is_set():raise RuntimeError('Worker stopping')
                with self.store.lock,self.store.connect() as db:
                    db.execute('BEGIN IMMEDIATE');self.learning._get(db,job['id'],'job')
                    renewed=db.execute("UPDATE workflow_jobs SET lease_until=? WHERE id=? AND lease=? AND state='running'",(time.time()+self.timeout+30,job['id'],job['_lease']))
                    if not renewed.rowcount:raise ValueError('Evaluation lease expired')
                outputs.append(run_adapter(adapter['entrypoint'],adapter.get('config',{}),{'input':case['input'],'candidate':policy},self.timeout))
            cases.append({'id':case['id'],'kind':case['kind'],'baseline_pass':document(outputs[0])==document(case['expected']),'candidate_pass':document(outputs[1])==document(case['expected'])})
        return {'kind':'evaluation','payload':{'candidate_id':candidate['id'],'candidate_digest':digest(candidate['payload']),
                    'baseline_id':baseline_id,'suite_id':job['payload']['suite_id'],'suite_digest':digest(suite),'adapter_digest':digest(adapter),'cases':cases,
                    'passed':all(c['candidate_pass'] for c in cases),'verification':'executed_suite'},'parents':[candidate['id']]}

    def scan(self):
        """Crash-replayable source cursor; one extractive episode per live canonical record.

        Retired evidence never wedges the cursor: records hidden after selection
        (or whose auto snapshot was invalidated by a retirement) are skipped and
        the cursor advances past them, while genuine failures leave the cursor in
        place so the next pass retries.
        """
        if not self.config.get('auto_consolidate',False):return 0
        from . import lifecycle
        from .intelligence import Intelligence
        with self.store.connect() as db:
            row=db.execute("SELECT watermark FROM workflow_cursor WHERE name='ingest'").fetchone();watermark=row[0] if row else 0
            rows=db.execute("""SELECT a.id,a.object_id FROM audit a JOIN records r ON r.id=a.object_id WHERE a.action='ingest' AND a.id>? AND r.deleted=0
                AND NOT EXISTS(SELECT 1 FROM record_visibility v WHERE v.record_id=r.id AND v.hidden=1) ORDER BY a.id LIMIT 10""",(watermark,)).fetchall()
        for row in rows:
            if self.stop.is_set():return 0
            try:
                with self.store.connect() as db:
                    # Recheck visibility after selection: retirement may race with this pass.
                    if not lifecycle.live_and_visible(db,row['object_id']):raise ValueError('Evidence retired after selection')
                snapshot=Intelligence(self.store).snapshot(key='auto/'+row['object_id'],record_ids=[row['object_id']],actor='auto-consolidator')
                with self.store.connect() as db:length=db.execute('SELECT length(text) FROM records WHERE id=?',(row['object_id'],)).fetchone()[0]
                for start in range(0,length,1800):
                    if self.stop.is_set():return 0
                    self.enqueue(key='auto/'+row['object_id']+'/'+str(start),type='consolidate',snapshot_id=snapshot['id'],
                                 segments=[{'record_id':row['object_id'],'start':start,'end':min(length,start+2000)}],actor='auto-consolidator')
            except ValueError:
                # Only retirement races skip; anything else must retry from this cursor.
                if not self._retired_in_flight(row['object_id']):raise
            self._advance_cursor(row['id'])
        return len(rows)

    def _retired_in_flight(self,record_id):
        from . import lifecycle
        with self.store.connect() as db:
            if not lifecycle.live_and_visible(db,record_id):return True
            # A snapshot invalidated by an earlier retirement can never be recreated
            # under the same key; its record must not fail every subsequent scan.
            return db.execute("SELECT 1 FROM learning_objects WHERE kind='snapshot' AND logical_key=? AND state IN ('invalidated','retracted')",
                              ('auto/'+record_id,)).fetchone() is not None

    def _advance_cursor(self,watermark):
        with self.store.lock,self.store.connect() as db:
            db.execute("INSERT INTO workflow_cursor VALUES('ingest',?) ON CONFLICT(name) DO UPDATE SET watermark=max(watermark,excluded.watermark)",(watermark,))

    def autolearn(self):
        policies=self.config.get('learning_policies',{})
        for scope,policy in policies.items():
            key_prefix='auto-eval/'+digest(policy)+'/'
            with self.store.connect() as db:
                rows=db.execute("""SELECT c.id FROM learning_objects c WHERE c.kind='candidate' AND c.state='candidate'
                  AND json_extract(c.payload,'$.scope')=? AND NOT EXISTS(
                    SELECT 1 FROM learning_objects j WHERE j.kind='job' AND j.logical_key=?||c.id) ORDER BY c.id LIMIT 10""",(scope,key_prefix)).fetchall()
            for row in rows:
                self.enqueue(key=key_prefix+row['id'],type='evaluate',candidate_id=row['id'],suite_id=policy['suite_id'],actor='automatic-evaluator')

    def _auto_promote(self,evaluation):
        result=evaluation['payload']
        if not result.get('passed') or result.get('verification')!='executed_suite':return
        with self.store.connect() as db:
            candidate=self.learning._get(db,result['candidate_id'],'candidate')
        policy=self.config.get('learning_policies',{}).get(candidate['payload']['scope'])
        if not policy or policy.get('promote') is not True or policy.get('suite_id')!=result['suite_id']:return
        self.learning.promote(candidate_id=candidate['id'],evaluation_id=evaluation['id'],expected_active_id=result['baseline_id'],actor='promotion-policy:'+digest(policy))

    def reconcile_promotions(self):
        for scope,policy in self.config.get('learning_policies',{}).items():
            if not policy.get('promote'):continue
            with self.store.connect() as db:
                rows=db.execute("""SELECT e.id FROM learning_objects e JOIN learning_objects c ON c.id=json_extract(e.payload,'$.candidate_id')
                 WHERE e.kind='evaluation' AND e.state='recorded' AND c.state='candidate' AND json_extract(c.payload,'$.scope')=?
                 AND json_extract(e.payload,'$.suite_id')=? AND NOT EXISTS(SELECT 1 FROM audit a WHERE a.object_id=e.id AND a.action='auto_promotion_blocked') LIMIT 10""",(scope,policy['suite_id'])).fetchall()
                evaluations=[self.learning._get(db,r['id'],'evaluation') for r in rows]
            for evaluation in evaluations:
                try:self._auto_promote(evaluation)
                except ValueError:
                    with self.store.connect() as db:self.store.audit(db,'auto_promotion_blocked',evaluation['id'])

    def deliver_event(self):
        handler=self.config.get('event_handler')
        if self.stop.is_set():return False
        if not handler:return False
        from .intelligence import Intelligence
        intelligence=Intelligence(self.store)
        event=intelligence.claim_event(actor='event-bridge')['event']
        if event is None:return False
        with self.store.connect() as db:self.learning._get(db,event['task_id'],'task')
        result=run_adapter(handler['entrypoint'],handler.get('config',{}),{'event':event,'idempotency_key':event['id']},min(self.timeout,20))
        if result!={'delivered':True}:raise ValueError('Delivery adapter did not acknowledge success')
        intelligence.ack_event(event_id=event['id'],lease=event['lease'],actor='event-bridge')
        return True

    def start(self):
        if self.thread:return
        def loop():
            while not self.stop.is_set():
                # Scan/queue maintenance failures must never starve queued jobs.
                iteration_error=None
                try:self.scan();self.autolearn();self.reconcile_promotions()
                except Exception as error:self.last_error=iteration_error=type(error).__name__
                try:busy=self.tick();self.deliver_event()
                except Exception as error:busy=False;iteration_error=type(error).__name__
                self.last_error=iteration_error
                self.stop.wait(.05 if busy else 1)
        self.thread=threading.Thread(target=loop,name='memory-workflows',daemon=True);self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:self.thread.join(timeout=self.timeout+5)

    def status(self):
        with self.store.connect() as db:
            return {'enabled_adapters':sorted(self.adapters),'automatic_consolidation':bool(self.config.get('auto_consolidate',False)),
                    'jobs':[dict(r) for r in db.execute("SELECT j.state,count(*) AS count FROM workflow_jobs j JOIN learning_objects o ON o.id=j.id WHERE o.state='recorded' GROUP BY j.state")],
                    'last_error':self.last_error,'worker_alive':bool(self.thread and self.thread.is_alive())}
