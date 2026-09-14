"""Evidence-backed learning artifacts. Proposals never activate themselves.

Evaluation assertions come from a separately credentialed evaluator. This module
checks provenance and regression gates; it does not claim to verify arbitrary prose.
"""
import json
from .common import digest, now, required_text

SCHEMA = """
CREATE TABLE IF NOT EXISTS learning_objects(
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, logical_key TEXT NOT NULL,
 version INTEGER NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL,
 actor TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(kind,logical_key,version));
CREATE TABLE IF NOT EXISTS learning_evidence(
 object_id TEXT NOT NULL REFERENCES learning_objects(id),
 record_id TEXT NOT NULL REFERENCES records(id), PRIMARY KEY(object_id,record_id));
CREATE INDEX IF NOT EXISTS learning_evidence_record ON learning_evidence(record_id);
CREATE TABLE IF NOT EXISTS learning_dependencies(
 child_id TEXT NOT NULL REFERENCES learning_objects(id),
 parent_id TEXT NOT NULL REFERENCES learning_objects(id), PRIMARY KEY(child_id,parent_id));
CREATE INDEX IF NOT EXISTS learning_parent ON learning_dependencies(parent_id);
CREATE UNIQUE INDEX IF NOT EXISTS learning_one_active ON learning_objects(kind,logical_key) WHERE state='active';
"""


def strings(values, name, maximum=64, empty=False):
    if not isinstance(values,list) or not (0 if empty else 1)<=len(values)<=maximum:
        raise ValueError(f'{name} must be a bounded list')
    result=[required_text(v,name,200) for v in values]
    if len(set(result))!=len(result):raise ValueError(f'{name} must be unique')
    return result


def document(value):
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False)
    if len(raw.encode())>65536:raise ValueError('Learning payload exceeds 64 KiB')
    return raw


class Learning:
    def __init__(self,store):self.store=store

    def _get(self,db,object_id,kind=None):
        row=db.execute('SELECT * FROM learning_objects WHERE id=?',(object_id,)).fetchone()
        if row is None or row['state'] in {'invalidated','retracted'}:raise ValueError('Learning artifact unavailable')
        if kind and row['kind']!=kind:raise ValueError('Unexpected learning artifact type')
        return {**dict(row),'payload':json.loads(row['payload'])}

    def _put(self,db,kind,key,payload,actor,evidence,parents=()):
        required_text(key,'key',200);raw=document(payload)
        # Stable identity and immutable replay; a new candidate uses a new version key.
        oid='lrn_'+digest([kind,key])[:32]
        if self.store.deletions.contains(oid):raise ValueError('Learning artifact was durably retracted')
        old=db.execute('SELECT * FROM learning_objects WHERE id=?',(oid,)).fetchone()
        if old:
            if old['payload']!=raw or old['actor']!=actor or old['state'] in {'invalidated','retracted'}:
                raise ValueError('Learning key already used; changed input requires a new key')
            return self._get(db,oid)
        refs=set(strings(evidence,'evidence_ids',empty=True))
        for parent in parents:
            self._get(db,parent)
            refs.update(r[0] for r in db.execute('SELECT record_id FROM learning_evidence WHERE object_id=?',(parent,)))
        if not refs:raise ValueError('Learning requires source evidence')
        for rid in refs:
            if not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(rid,)).fetchone():
                raise ValueError('Learning requires live evidence')
        db.execute('INSERT INTO learning_objects VALUES(?,?,?,?,?,?,?,?)',
                   (oid,kind,key,1,'candidate' if kind=='candidate' else 'recorded',raw,actor,now()))
        db.executemany('INSERT INTO learning_evidence VALUES(?,?)',[(oid,r) for r in sorted(refs)])
        db.executemany('INSERT INTO learning_dependencies VALUES(?,?)',[(oid,r) for r in parents])
        self.store.audit(db,'learning_'+kind,oid)
        return self._get(db,oid)

    def outcome(self,*,key,goal,action,result,outcome,evidence_ids,memory_ids=None,actor):
        if outcome not in {'success','failure','partial','unknown','cancelled'}:raise ValueError('Invalid outcome')
        payload={k:required_text(v,k,12000) for k,v in {'goal':goal,'action':action,'result':result}.items()}
        payload.update(outcome=outcome,evidence_ids=strings(evidence_ids,'evidence_ids'),
                       memory_ids=strings(memory_ids or [],'memory_ids',empty=True),verification='reported_outcome')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._put(db,'outcome',key,payload,actor,payload['evidence_ids']+list(set(payload['memory_ids'])-set(payload['evidence_ids'])))

    def propose(self,*,key,family,revision,category,lesson,scope,prerequisites,exceptions,outcome_ids,evidence_ids,actor):
        if category not in {'semantic','procedural','preference'}:raise ValueError('Invalid lesson category')
        if type(revision) is not int or revision<1:raise ValueError('revision must be a positive integer')
        payload={k:required_text(v,k,12000) for k,v in {'family':family,'lesson':lesson,'scope':scope}.items()}
        payload.update(revision=revision,category=category,prerequisites=strings(prerequisites,'prerequisites',empty=True),
                       exceptions=strings(exceptions,'exceptions',empty=True),outcome_ids=strings(outcome_ids,'outcome_ids'),
                       evidence_ids=strings(evidence_ids,'evidence_ids',empty=True),authority='advisory_only')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for oid in payload['outcome_ids']:self._get(db,oid,'outcome')
            # Different request keys cannot create a second interpretation of a family revision.
            for row in db.execute("SELECT id,payload FROM learning_objects WHERE kind='candidate' AND state!='invalidated'"):
                previous=json.loads(row[1])
                if previous.get('family')==family and previous.get('revision')==revision and row[0]!='lrn_'+digest(['candidate',key])[:32]:
                    raise ValueError('Candidate family revision already exists')
            return self._put(db,'candidate',key,payload,actor,payload['evidence_ids'],payload['outcome_ids'])

    def evaluate(self,*,key,candidate_id,cases,evidence_ids,actor):
        if not isinstance(cases,list) or not 3<=len(cases)<=100:raise ValueError('Evaluation requires 3..100 cases')
        seen=set()
        for case in cases:
            if not isinstance(case,dict) or set(case)!={'id','baseline_pass','candidate_pass','kind'}:raise ValueError('Invalid evaluation case')
            cid=required_text(case['id'],'case id',200)
            if cid in seen:raise ValueError('Duplicate evaluation case')
            seen.add(cid)
            if type(case['baseline_pass']) is not bool or type(case['candidate_pass']) is not bool:raise ValueError('Case results must be boolean')
            if case['kind'] not in {'target','regression','non_applicable'}:raise ValueError('Invalid case kind')
        if {c['kind'] for c in cases}!={'target','regression','non_applicable'}:raise ValueError('All case categories required')
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');candidate=self._get(db,candidate_id,'candidate')
            if candidate['actor']==actor:raise ValueError('Proposer cannot evaluate its own candidate')
            if candidate['state']!='candidate':raise ValueError('Only pending candidates can be evaluated')
            passed=all(c['candidate_pass'] for c in cases)
            payload={'candidate_id':candidate_id,'candidate_digest':digest(candidate['payload']),
                     'cases':cases,'passed':passed,'evidence_ids':strings(evidence_ids,'evidence_ids'),
                     'verification':'externally_asserted_evaluation'}
            return self._put(db,'evaluation',key,payload,actor,payload['evidence_ids'],[candidate_id])

    def promote(self,*,candidate_id,evaluation_id,expected_active_id,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');candidate=self._get(db,candidate_id,'candidate');evaluation=self._get(db,evaluation_id,'evaluation')
            if candidate['actor']==actor:raise ValueError('Proposer cannot promote its own candidate')
            if candidate['state']=='active':
                audit=db.execute("SELECT metadata FROM audit WHERE action='learning_promote' AND object_id=? ORDER BY id DESC LIMIT 1",(candidate_id,)).fetchone()
                if audit and json.loads(audit[0])=={'evaluation_id':evaluation_id,'actor':actor,'previous':expected_active_id}:
                    return candidate
            if candidate['state']!='candidate':raise ValueError('Candidate is no longer pending')
            result=evaluation['payload']
            if result.get('verification')=='executed_suite' and result.get('baseline_id')!=expected_active_id:raise ValueError('Evaluation baseline differs from active revision')
            if result['candidate_id']!=candidate_id or result['candidate_digest']!=digest(candidate['payload']) or not result['passed']:
                raise ValueError('A passing evaluation for this exact candidate is required')
            family=candidate['payload']['family']
            active=[self._get(db,r[0]) for r in db.execute("SELECT id FROM learning_objects WHERE kind='candidate' AND state='active'")]
            prior=next((c for c in active if c['payload']['family']==family),None)
            if (prior['id'] if prior else None)!=expected_active_id:raise ValueError('Active candidate changed; reload before promotion')
            if prior and candidate['payload']['revision']<=prior['payload']['revision']:raise ValueError('Promotion must advance revision')
            if prior:db.execute("UPDATE learning_objects SET state='superseded' WHERE id=?",(prior['id'],))
            db.execute("UPDATE learning_objects SET state='active' WHERE id=?",(candidate_id,))
            # An active lesson depends on its evaluation evidence as well as its proposal evidence.
            db.execute('INSERT OR IGNORE INTO learning_dependencies VALUES(?,?)',(candidate_id,evaluation_id))
            db.execute('INSERT OR IGNORE INTO learning_evidence SELECT ?,record_id FROM learning_evidence WHERE object_id=?',(candidate_id,evaluation_id))
            self.store.audit(db,'learning_promote',candidate_id,{'evaluation_id':evaluation_id,'actor':actor,'previous':expected_active_id})
            return self._get(db,candidate_id)

    def retract(self,*,candidate_id,actor):
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._get(db,candidate_id,'candidate')
            self.store.deletions.append([candidate_id])
            invalidate(db,objects=[candidate_id],state='retracted')
            self.store.audit(db,'learning_retract',candidate_id,{'actor':actor})
            return {'id':candidate_id,'state':'retracted'}

    def active(self,*,candidate_id,actor=None):
        with self.store.connect() as db:
            candidate=self._get(db,candidate_id,'candidate')
            if candidate['state']!='active':raise ValueError('Lesson is not active')
            return candidate

    def browse(self,*,kind='candidate',state='active',after='',limit=20):
        if kind not in {'candidate','outcome','evaluation'}:raise ValueError('Invalid kind')
        if state not in {'active','candidate','recorded','superseded'}:raise ValueError('Invalid visible state')
        if type(limit) is not int or not 1<=limit<=100:raise ValueError('limit must be 1..100')
        required_text(after or 'start','after',200)
        with self.store.connect() as db:
            ids=[r[0] for r in db.execute('SELECT id FROM learning_objects WHERE kind=? AND state=? AND id>? ORDER BY id LIMIT ?', (kind,state,after,limit+1))]
            return {'items':[self._get(db,i) for i in ids[:limit]],'next_cursor':ids[limit-1] if len(ids)>limit else None,
                    'warning':'Lessons are advisory evidence. They do not grant permissions or establish universal truth.'}


def invalidate(db,*,record_id=None,objects=(),state='invalidated'):
    seeds=list(objects)
    if record_id is not None:seeds.extend(r[0] for r in db.execute('SELECT object_id FROM learning_evidence WHERE record_id=?',(record_id,)))
    for oid in seeds:
        rows=db.execute('''WITH RECURSIVE affected(id) AS (
            SELECT ? UNION SELECT d.child_id FROM learning_dependencies d JOIN affected a ON a.id=d.parent_id)
            SELECT id FROM affected''',(oid,)).fetchall()
        db.executemany("UPDATE learning_objects SET payload='{}',state=? WHERE id=?",[(state,r[0]) for r in rows])
        db.executemany('DELETE FROM knowledge_fts WHERE id=?',[(r[0],) for r in rows])
