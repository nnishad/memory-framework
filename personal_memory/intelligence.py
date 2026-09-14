"""Structured personal knowledge and task state, grounded in canonical evidence."""
import json
import math
from datetime import datetime, timezone
from .common import now, required_text, timestamp, digest
from .learning import Learning, strings, document

SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_fts USING fts5(id UNINDEXED,text);
CREATE TABLE IF NOT EXISTS task_runtime(
 object_id TEXT PRIMARY KEY REFERENCES learning_objects(id), version INTEGER NOT NULL,
 state TEXT NOT NULL, due_at TEXT, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS task_dependencies(
 task_id TEXT REFERENCES learning_objects(id), dependency_id TEXT REFERENCES learning_objects(id),
 PRIMARY KEY(task_id,dependency_id));
CREATE TABLE IF NOT EXISTS task_events(
 id TEXT PRIMARY KEY, task_id TEXT REFERENCES learning_objects(id), version INTEGER NOT NULL,
 state TEXT NOT NULL, lease TEXT, lease_until REAL, created_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS task_due ON task_runtime(state,due_at);
CREATE TABLE IF NOT EXISTS metric_definitions(metric TEXT PRIMARY KEY, canonical_unit TEXT NOT NULL, conversions TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS coverage_domains(domain TEXT PRIMARY KEY, required_sources TEXT NOT NULL, scope TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS memory_feedback(
 key TEXT PRIMARY KEY, record_id TEXT REFERENCES records(id), result TEXT NOT NULL, created_at TEXT NOT NULL);
"""


def bounds(start,end):
    start=timestamp(start) if start is not None else None
    end=timestamp(end) if end is not None else None
    if start and end and end<=start:raise ValueError('Invalid validity interval')
    return start,end


def valid(payload,at):
    return (payload.get('valid_from') is None or payload['valid_from']<=at) and (payload.get('valid_to') is None or at<payload['valid_to'])


def scalar(value):
    if type(value) not in {str,int,float,bool} or (type(value) is float and not math.isfinite(value)):
        raise ValueError('Expected a finite scalar')
    if len(document(value))>4096:raise ValueError('Scalar too large')
    return value


class Intelligence:
    def __init__(self,store):self.store=store;self.learning=Learning(store)

    def _quote(self,db,evidence):
        if not isinstance(evidence,list) or not 1<=len(evidence)<=32:raise ValueError('Evidence must contain 1..32 quotes')
        ids=[]
        for e in evidence:
            if not isinstance(e,dict) or set(e)!={'record_id','quote'}:raise ValueError('Expected record_id and quote')
            quote=required_text(e['quote'],'quote',12000)
            row=db.execute('SELECT text FROM records WHERE id=? AND deleted=0',(e['record_id'],)).fetchone()
            if not row or quote not in row[0]:raise ValueError('Quote must occur in live source evidence')
            ids.append(e['record_id'])
        return sorted(set(ids))

    def _entity(self,db,eid):
        if not db.execute('SELECT 1 FROM entities WHERE id=?',(eid,)).fetchone():raise ValueError('Unknown entity')

    def snapshot(self,*,key,record_ids,actor):
        record_ids=strings(record_ids,'record_ids')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Snapshot identity is an idempotency key; never silently refresh its cutoff.
            oid='lrn_'+digest(['snapshot',key])[:32]
            old=db.execute('SELECT id FROM learning_objects WHERE id=?',(oid,)).fetchone()
            if old:
                obj=self.learning._get(db,oid,'snapshot')
                if obj['actor']!=actor or sorted(obj['payload']['record_ids'])!=sorted(record_ids):raise ValueError('Snapshot key conflict')
                return obj
            hashes={}
            for rid in record_ids:
                row=db.execute('SELECT fingerprint FROM records WHERE id=? AND deleted=0',(rid,)).fetchone()
                if not row:raise ValueError('Missing snapshot evidence')
                hashes[rid]=row[0]
            coverage=[dict(r) for r in db.execute('SELECT source,state,through_at,updated_at FROM sources ORDER BY source')]
            return self.learning._put(db,'snapshot',key,{'record_ids':record_ids,'fingerprints':hashes,'coverage':coverage,'cutoff':now()},actor,record_ids)

    def belief(self,*,key,subject_id,predicate,value,evidence,origin='reported',context=None,valid_from=None,valid_to=None,quantifier='value',domain=None,actor):
        if quantifier not in {'value','none','all'}:raise ValueError('Invalid quantifier')
        if origin not in {'reported','observed','inferred','explicit_preference'}:raise ValueError('Invalid belief origin')
        if not isinstance(context or {},dict) or any(not isinstance(k,str) or not isinstance(v,str) for k,v in (context or {}).items()):raise ValueError('Context must map strings to strings')
        required_text(predicate,'predicate',200);scalar(value);start,end=bounds(valid_from,valid_to)
        payload=dict(subject_id=subject_id,predicate=predicate,value=value,evidence=evidence,origin=origin,context=context or {},valid_from=start,valid_to=end,truth='unverified',quantifier=quantifier,domain=domain)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._entity(db,subject_id);refs=self._quote(db,evidence)
            if quantifier!='value':
                coverage=self._coverage(db,domain,now())
                if not coverage['complete']:raise ValueError('Quantified belief requires complete configured domain coverage')
                payload['coverage_snapshot']=coverage
            obj=self.learning._put(db,'belief',key,payload,actor,refs)
            db.executemany('INSERT OR IGNORE INTO entity_links VALUES(?,?,?)',[(subject_id,r,predicate) for r in refs])
            return obj

    def resolve_belief(self,*,belief_id,supersedes,actor):
        supersedes=strings(supersedes,'supersedes')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');chosen=self.learning._get(db,belief_id,'belief')
            if chosen['state']!='recorded':raise ValueError('Belief is not current')
            for oid in supersedes:
                previous=self.learning._get(db,oid,'belief')
                if oid==belief_id or any(previous['payload'][k]!=chosen['payload'][k] for k in ['subject_id','predicate','context']):raise ValueError('Correction must address the same proposition scope')
                db.execute("UPDATE learning_objects SET state='superseded' WHERE id=?",(oid,))
            self.store.audit(db,'belief_resolve',belief_id,{'supersedes':supersedes,'actor':actor})
            return chosen

    def beliefs(self,*,subject_id,predicate=None,context=None,at=None,limit=100):
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('Invalid limit')
        at=timestamp(at) if at is not None else now();context=context or {}
        if not isinstance(context,dict):raise ValueError('Invalid context')
        args=[subject_id];clause="kind='belief' AND state='recorded' AND json_extract(payload,'$.subject_id')=?"
        if predicate is not None:clause+=" AND json_extract(payload,'$.predicate')=?";args.append(predicate)
        with self.store.connect() as db:
            rows=db.execute('SELECT * FROM learning_objects WHERE '+clause+' ORDER BY created_at DESC,id LIMIT 1001',args).fetchall()
            matched=[{**dict(r),'payload':json.loads(r['payload'])} for r in rows[:1000]]
            matched=[r for r in matched if valid(r['payload'],at) and all(context.get(k)==v for k,v in r['payload']['context'].items())]
            matched.sort(key=lambda r:(r['payload']['origin']=='explicit_preference',len(r['payload']['context']),r['created_at']),reverse=True)
            groups={}
            for r in matched:groups.setdefault(r['payload']['predicate'],[]).append(r)
            conflicts=[{'predicate':k,'belief_ids':[r['id'] for r in rs]} for k,rs in groups.items() if len({document(r['payload']['value']) for r in rs})>1]
            return {'beliefs':matched[:limit],'conflicts':conflicts,'truncated':len(rows)>1000 or len(matched)>limit,
                    'warning':'Conflicts are unresolved alternatives. Explicit/contextual preferences rank first; quotes prove attribution, not truth.'}

    def relation(self,*,key,subject_id,predicate,object_id,evidence,valid_from=None,valid_to=None,actor):
        required_text(predicate,'predicate',200);start,end=bounds(valid_from,valid_to)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._entity(db,subject_id);self._entity(db,object_id)
            refs=self._quote(db,evidence)
            obj=self.learning._put(db,'relation',key,dict(subject_id=subject_id,predicate=predicate,object_id=object_id,evidence=evidence,valid_from=start,valid_to=end,verification='reported_relation'),actor,refs)
            db.executemany('INSERT OR IGNORE INTO entity_links VALUES(?,?,?)',[(entity,r,predicate) for entity in (subject_id,object_id) for r in refs])
            return obj

    def graph(self,*,entity_id,hops=2,at=None,max_edges=100):
        if type(hops) is not int or not 1<=hops<=3:raise ValueError('hops must be 1..3')
        if type(max_edges) is not int or not 1<=max_edges<=200:raise ValueError('max_edges must be 1..200')
        at=timestamp(at) if at is not None else now();frontier={entity_id};visited=set();edges={};truncated=False
        with self.store.connect() as db:
            self._entity(db,entity_id)
            for _ in range(hops):
                next_frontier=set()
                for eid in sorted(frontier-visited):
                    visited.add(eid)
                    rows=db.execute("SELECT * FROM learning_objects WHERE kind='relation' AND state='recorded' AND (json_extract(payload,'$.subject_id')=? OR json_extract(payload,'$.object_id')=?) ORDER BY id LIMIT ?",(eid,eid,max_edges+1)).fetchall()
                    if len(rows)>max_edges:truncated=True
                    for row in rows[:max_edges]:
                        payload=json.loads(row['payload'])
                        if not valid(payload,at):continue
                        if len(edges)>=max_edges:truncated=True;break
                        edges[row['id']]={**dict(row),'payload':payload};next_frontier.update([payload['subject_id'],payload['object_id']])
                frontier=next_frontier
                if truncated:break
        return {'edges':list(edges.values()),'truncated':truncated,'warning':'Paths are attributed associations, not causal proof or identity merges.'}

    def measurement(self,*,key,subject_id,metric,value,unit,measured_at,evidence,actor):
        if type(value) not in {int,float} or not math.isfinite(value):raise ValueError('Measurement must be finite numeric')
        required_text(metric,'metric',200);required_text(unit,'unit',40);measured_at=timestamp(measured_at)
        defaults={
            'mass':('kg',{'kg':[1,0],'g':[.001,0],'lb':[.45359237,0]}),
            'weight':('kg',{'kg':[1,0],'g':[.001,0],'lb':[.45359237,0]}),
            'body_mass':('kg',{'kg':[1,0],'g':[.001,0],'lb':[.45359237,0]}),
            'temperature':('C',{'C':[1,0],'F':[5/9,-32*5/9]}),
            'body_temperature':('C',{'C':[1,0],'F':[5/9,-32*5/9]}),
            'heart_rate':('bpm',{'bpm':[1,0]}),'hrv':('ms',{'ms':[1,0]}),
            'systolic_bp':('mmHg',{'mmHg':[1,0]}),'diastolic_bp':('mmHg',{'mmHg':[1,0]})}
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._entity(db,subject_id)
            registered=db.execute('SELECT * FROM metric_definitions WHERE metric=?',(metric,)).fetchone()
            definition=(registered['canonical_unit'],json.loads(registered['conversions'])) if registered else defaults.get(metric)
            if not definition or unit not in definition[1]:raise ValueError('Register the metric and unit before typed ingestion')
            normalized,conversions=definition;factor,offset=conversions[unit];number=value*factor+offset
            if not math.isfinite(number):raise ValueError('Normalized value overflow')
            return self.learning._put(db,'measurement',key,dict(subject_id=subject_id,metric=metric,original_value=value,original_unit=unit,value=number,unit=normalized,measured_at=measured_at,evidence=evidence),actor,self._quote(db,evidence))

    def aggregate(self,*,subject_id,metric,unit,after,before):
        start,end=bounds(after,before)
        if start is None or end is None:raise ValueError('Explicit time window required')
        with self.store.connect() as db:
            row=db.execute("""SELECT count(*),min(json_extract(payload,'$.value')),max(json_extract(payload,'$.value')),avg(json_extract(payload,'$.value'))
             FROM learning_objects WHERE kind='measurement' AND state='recorded' AND json_extract(payload,'$.subject_id')=?
             AND json_extract(payload,'$.metric')=? AND json_extract(payload,'$.unit')=?
             AND json_extract(payload,'$.measured_at')>=? AND json_extract(payload,'$.measured_at')<?""",(subject_id,metric,unit,start,end)).fetchone()
            return dict(count=row[0],minimum=row[1],maximum=row[2],mean=row[3],unit=unit,after=start,before=end,
                        warning='Stored measurements only; unweighted sample mean, not clinical interpretation or complete sensor coverage.')

    def task(self,*,key,title,evidence_ids,due_at=None,depends_on=None,actor):
        required_text(title,'title',4000);due_at=timestamp(due_at) if due_at is not None else None
        refs=strings(evidence_ids,'evidence_ids');deps=strings(depends_on or [],'depends_on',empty=True)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for oid in deps:self.learning._get(db,oid,'task')
            obj=self.learning._put(db,'task',key,dict(title=title,due_at=due_at,depends_on=deps),actor,refs,deps)
            terminal='completed' if self.store.deletions.contains('done_'+obj['id'][4:]) else ('cancelled' if self.store.deletions.contains('cancel_'+obj['id'][4:]) else 'open')
            db.execute("INSERT OR IGNORE INTO task_runtime VALUES(?,1,?,?,?)",(obj['id'],terminal,due_at,now()))
            db.executemany('INSERT OR IGNORE INTO task_dependencies VALUES(?,?)',[(obj['id'],d) for d in deps])
            return {**obj,'runtime':dict(db.execute('SELECT * FROM task_runtime WHERE object_id=?',(obj['id'],)).fetchone())}

    def transition(self,*,task_id,expected_version,state,evidence_ids,actor):
        transitions={'open':{'in_progress','blocked','cancelled'},'in_progress':{'blocked','completed','cancelled'},'blocked':{'open','cancelled'}}
        refs=strings(evidence_ids,'evidence_ids')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self.learning._get(db,task_id,'task')
            row=db.execute('SELECT * FROM task_runtime WHERE object_id=?',(task_id,)).fetchone()
            if type(expected_version) is not int or row['version']!=expected_version:raise ValueError('Task version conflict')
            if state not in transitions.get(row['state'],set()):raise ValueError('Invalid task transition')
            if state in {'in_progress','completed'}:
                blocked=db.execute("SELECT 1 FROM task_dependencies d JOIN task_runtime t ON t.object_id=d.dependency_id JOIN learning_objects o ON o.id=t.object_id WHERE d.task_id=? AND (t.state!='completed' OR o.state!='recorded')",(task_id,)).fetchone()
                if blocked:raise ValueError('Task dependencies are incomplete')
            for rid in refs:
                if not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(rid,)).fetchone():raise ValueError('Live transition evidence required')
            db.executemany('INSERT OR IGNORE INTO learning_evidence VALUES(?,?)',[(task_id,r) for r in refs])
            if state in {'completed','cancelled'}:self.store.deletions.append([('done_' if state=='completed' else 'cancel_')+task_id[4:]])
            db.execute('UPDATE task_runtime SET version=version+1,state=?,updated_at=? WHERE object_id=?',(state,now(),task_id))
            db.execute("UPDATE task_events SET state='cancelled',lease=NULL,lease_until=NULL WHERE task_id=? AND state!='delivered'",(task_id,))
            self.store.audit(db,'task_transition',task_id,{'version':expected_version+1,'state':state,'actor':actor})
            return dict(db.execute('SELECT * FROM task_runtime WHERE object_id=?',(task_id,)).fetchone())

    def tasks(self,*,state=None,after='',limit=50):
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('Invalid limit')
        args=[after];clause="o.state='recorded' AND o.id>?"
        if state is not None:clause+=' AND t.state=?';args.append(state)
        with self.store.connect() as db:
            rows=db.execute('SELECT o.id,o.payload,t.version,t.state,t.due_at FROM learning_objects o JOIN task_runtime t ON t.object_id=o.id WHERE '+clause+' ORDER BY o.id LIMIT ?',args+[limit+1]).fetchall()
            return {'tasks':[{**dict(r),'payload':json.loads(r['payload'])} for r in rows[:limit]],'next_cursor':rows[limit-1]['id'] if len(rows)>limit else None}

    def due_events(self,*,limit=20,actor):
        # Transactionally create reminder events. Delivery is a host responsibility.
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('Invalid limit')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows=db.execute("SELECT t.* FROM task_runtime t JOIN learning_objects o ON o.id=t.object_id WHERE o.state='recorded' AND t.state IN ('open','in_progress') AND t.due_at<=? AND NOT EXISTS(SELECT 1 FROM task_events e WHERE e.task_id=t.object_id AND e.version=t.version) ORDER BY t.due_at LIMIT ?",(now(),limit)).fetchall()
            for r in rows:
                eid='evt_'+digest([r['object_id'],r['version']])[:32]
                db.execute("INSERT OR IGNORE INTO task_events(id,task_id,version,state,lease,lease_until,created_at) VALUES(?,?,?,'pending',NULL,NULL,?)",(eid,r['object_id'],r['version'],now()))
            return {'created_or_existing':len(rows)}

    def feedback(self,*,key,record_id,result,actor):
        if result not in {'useful','irrelevant','incorrect','stale'}:raise ValueError('Invalid feedback')
        required_text(key,'key',200)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(record_id,)).fetchone():raise ValueError('Missing feedback evidence')
            old=db.execute('SELECT record_id,result FROM memory_feedback WHERE key=?',(key,)).fetchone()
            if old and tuple(old)!=(record_id,result):raise ValueError('Feedback key conflict')
            db.execute('INSERT OR IGNORE INTO memory_feedback VALUES(?,?,?,?)',(key,record_id,result,now()))
            return {'record_id':record_id,'result':result}

    def quality(self):
        with self.store.connect() as db:
            return {'learning_states':[dict(r) for r in db.execute('SELECT kind,state,count(*) AS count FROM learning_objects GROUP BY kind,state')],
                    'feedback':[dict(r) for r in db.execute('SELECT result,count(*) AS count FROM memory_feedback f JOIN records r ON r.id=f.record_id WHERE r.deleted=0 GROUP BY result')],
                    'pending_tasks':db.execute("SELECT count(*) FROM task_runtime t JOIN learning_objects o ON o.id=t.object_id WHERE o.state='recorded' AND t.state NOT IN ('completed','cancelled')").fetchone()[0],
                    'policy':'Feedback is diagnostic. It does not overwrite evidence or prove truth.'}

    def claim_event(self,*,actor):
        import time,uuid
        self.due_events(actor=actor)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');clock=time.time()
            db.execute("UPDATE task_events SET state='quarantined',lease=NULL,lease_until=NULL WHERE state='leased' AND lease_until<? AND attempts>=3",(clock,))
            row=db.execute("""SELECT e.* FROM task_events e JOIN task_runtime t ON t.object_id=e.task_id JOIN learning_objects o ON o.id=e.task_id
                WHERE o.state='recorded' AND t.state IN ('open','in_progress') AND t.version=e.version
                AND (e.state='pending' OR (e.state='leased' AND e.lease_until<?)) ORDER BY e.created_at,e.id LIMIT 1""",(clock,)).fetchone()
            if not row:return {'event':None}
            lease=uuid.uuid4().hex
            db.execute("UPDATE task_events SET state='leased',lease=?,lease_until=?,attempts=attempts+1 WHERE id=?",(lease,clock+60,row['id']))
            task=self.learning._get(db,row['task_id'],'task')
            return {'event':{'id':row['id'],'task_id':row['task_id'],'version':row['version'],'lease':lease,'payload':task['payload']},
                    'delivery_contract':'At least once. Deduplicate by event ID. Revalidate the task immediately before any authorized delivery.'}

    def ack_event(self,*,event_id,lease,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM task_events WHERE id=?',(event_id,)).fetchone()
            if not row or row['lease']!=lease:raise ValueError('Stale event lease')
            self.learning._get(db,row['task_id'],'task')
            runtime=db.execute('SELECT * FROM task_runtime WHERE object_id=?',(row['task_id'],)).fetchone()
            if runtime['version']!=row['version'] or runtime['state'] not in {'open','in_progress'}:raise ValueError('Task no longer deliverable')
            if row['state'] not in {'leased','delivered'}:raise ValueError('Event is not leased')
            db.execute("UPDATE task_events SET state='delivered' WHERE id=?",(event_id,))
            self.store.audit(db,'task_event_ack',event_id,{'actor':actor})
            return {'id':event_id,'state':'delivered'}

    def domain(self,*,domain,required_sources,scope,actor):
        required_text(domain,'domain',200);required_text(scope,'scope',2000);sources=strings(required_sources,'required_sources')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO coverage_domains VALUES(?,?,?) ON CONFLICT(domain) DO UPDATE SET required_sources=excluded.required_sources,scope=excluded.scope',(domain,document(sources),scope))
            self.store.audit(db,'domain_config',domain,{'actor':actor})
        return {'domain':domain,'required_sources':sources,'scope':scope}

    def _coverage(self,db,domain,through_at):
        configured=db.execute('SELECT * FROM coverage_domains WHERE domain=?',(domain,)).fetchone()
        if not configured:return {'domain':domain,'complete':False,'reason':'Domain not configured'}
        statuses=[]
        for source in json.loads(configured['required_sources']):
            row=db.execute('SELECT source,state,through_at FROM sources WHERE source=?',(source,)).fetchone()
            statuses.append(dict(row) if row else {'source':source,'state':'missing','through_at':None})
        complete=all(s['state']=='complete' and s['through_at'] is not None and s['through_at']>=through_at for s in statuses)
        return {'domain':domain,'scope':configured['scope'],'through_at':through_at,'complete':complete,'sources':statuses,
                'warning':'Coverage is an operator assertion about source scope, not proof that an arbitrary proposition is true.'}

    def coverage(self,*,domain,through_at=None):
        cutoff=timestamp(through_at) if through_at is not None else now()
        with self.store.connect() as db:return self._coverage(db,domain,cutoff)

    def procedures(self,*,after='',limit=20):
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('Invalid limit')
        with self.store.connect() as db:
            rows=db.execute("""SELECT p.* FROM learning_objects p JOIN learning_objects c ON c.id=json_extract(p.payload,'$.candidate_id')
             WHERE p.kind='procedure' AND p.state='recorded' AND c.state='active' AND p.id>? ORDER BY p.id LIMIT ?""",(after,limit+1)).fetchall()
            return {'procedures':[{**dict(r),'payload':json.loads(r['payload'])} for r in rows[:limit]],'next_cursor':rows[limit-1]['id'] if len(rows)>limit else None,
                    'warning':'Execution additionally requires the calling credential to allow the installed capability.'}

    def retry_event(self,*,event_id,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM task_events WHERE id=?',(event_id,)).fetchone()
            if not row:raise ValueError('Unknown event')
            self.learning._get(db,row['task_id'],'task')
            runtime=db.execute('SELECT version,state FROM task_runtime WHERE object_id=?',(row['task_id'],)).fetchone()
            if runtime['version']!=row['version'] or runtime['state'] not in {'open','in_progress'}:raise ValueError('Task no longer deliverable')
            cursor=db.execute("UPDATE task_events SET state='pending',attempts=0 WHERE id=? AND state='quarantined'",(event_id,))
            if not cursor.rowcount:raise ValueError('Only quarantined events can be retried')
            self.store.audit(db,'event_retry',event_id,{'actor':actor})
            return {'id':event_id,'state':'pending'}

    def summaries(self,*,query,limit=10):
        required_text(query,'query',4000)
        if type(limit) is not int or not 1<=limit<=30:raise ValueError('Invalid limit')
        terms=self.store.query_terms(query)
        if not terms:return {'summaries':[]}
        with self.store.connect() as db:
            rows=db.execute("SELECT o.* FROM knowledge_fts f JOIN learning_objects o ON o.id=f.id WHERE knowledge_fts MATCH ? AND o.kind='consolidation' AND o.state='recorded' ORDER BY rank LIMIT ?",(terms,limit)).fetchall()
            return {'summaries':[{**dict(r),'payload':json.loads(r['payload'])} for r in rows],
                    'warning':'Reviewed summaries are derived, unverified interpretations. Inspect their exact quotes and canonical evidence.'}

    def metric(self,*,metric,canonical_unit,conversions,actor):
        required_text(metric,'metric',200);required_text(canonical_unit,'canonical unit',40)
        if not isinstance(conversions,dict) or not 1<=len(conversions)<=32:raise ValueError('Invalid conversions')
        for unit,pair in conversions.items():
            required_text(unit,'unit',40)
            if not isinstance(pair,list) or len(pair)!=2 or any(type(x) not in {int,float} or not math.isfinite(x) for x in pair) or pair[0]<=0:raise ValueError('Conversions need positive finite scale and finite offset')
        if conversions.get(canonical_unit)!=[1,0]:raise ValueError('Canonical unit must have identity conversion')
        raw=document(conversions)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');old=db.execute('SELECT canonical_unit,conversions FROM metric_definitions WHERE metric=?',(metric,)).fetchone()
            if old and tuple(old)!=(canonical_unit,raw):raise ValueError('Metric definition is immutable; use a versioned metric name')
            if not old and db.execute("SELECT 1 FROM learning_objects WHERE kind='measurement' AND json_extract(payload,'$.metric')=? LIMIT 1",(metric,)).fetchone():raise ValueError('Cannot redefine an already ingested metric; use a versioned name')
            db.execute('INSERT OR IGNORE INTO metric_definitions VALUES(?,?,?)',(metric,canonical_unit,raw))
            self.store.audit(db,'metric_register',metric,{'actor':actor})
        return {'metric':metric,'canonical_unit':canonical_unit}
