"""Hermes-authored parallel retrieval plans. Evidence leads, never answer verification."""
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from .common import required_text, timestamp
from .relevance import terms

class Investigation:
    def __init__(self,store,backend,intelligence):
        self.store,self.backend,self.intelligence=store,backend,intelligence
        self.pool=ThreadPoolExecutor(max_workers=3,thread_name_prefix='memory-plan')
        self.capacity=threading.BoundedSemaphore(12)

    def close(self):
        self.pool.shutdown(wait=True,cancel_futures=True)

    def search(self,*,goal,branches,limit=18,text_budget=16000,timeout=10,graph_hops=0):
        required_text(goal,'goal',4000)
        if not isinstance(branches,list) or not 1<=len(branches)<=6:raise ValueError('Provide 1..6 branches')
        for value,low,high,label in [(limit,1,30,'limit'),(text_budget,1000,48000,'text_budget'),(timeout,1,25,'timeout'),(graph_hops,0,2,'graph_hops')]:
            if type(value) is not int or not low<=value<=high:raise ValueError(f'{label} must be {low}..{high}')
        parsed=[];seen=set();jobs={}
        for branch in branches:
            if not isinstance(branch,dict) or set(branch)-{'id','intent','queries','filters'}:raise ValueError('Invalid branch fields')
            bid=required_text(branch.get('id'),'branch id',100);intent=required_text(branch.get('intent'),'intent',1000)
            if bid in seen:raise ValueError('Branch IDs must be unique')
            seen.add(bid);queries=branch.get('queries')
            if not isinstance(queries,list) or not 1<=len(queries)<=3:raise ValueError('Provide 1..3 query variants per branch')
            queries=list(dict.fromkeys(required_text(q,'query',1000) for q in queries))
            filters=branch.get('filters',{})
            if not isinstance(filters,dict) or set(filters)-{'entity_id','source','after','before'}:raise ValueError('Invalid branch filters')
            filters=dict(filters)
            for k,v in filters.items():filters[k]=timestamp(v) if k in {'after','before'} else required_text(v,k,500)
            if filters.get('after') and filters.get('before') and filters['before']<=filters['after']:raise ValueError('Invalid time interval')
            args=dict(query=queries[0],queries=queries[1:],depth='balanced',limit=8,expand_entities=False,**filters)
            key=json.dumps(args,sort_keys=True);jobs[key]=args
            parsed.append(dict(id=bid,intent=intent,queries=queries,filters=filters,key=key))
        # Reserve the whole request before submitting. Overload cannot grow an
        # unbounded executor queue; identical branches share one retrieval job.
        reserved=0
        for _ in jobs:
            if not self.capacity.acquire(blocking=False):
                for _ in range(reserved):self.capacity.release()
                raise ValueError('Investigation busy; retry after running searches finish')
            reserved+=1
        started=time.monotonic();futures={}
        try:
            for key,args in jobs.items():
                future=self.pool.submit(self.backend.search,**args)
                futures[key]=future
                future.add_done_callback(lambda _:self.capacity.release())
        except BaseException:
            for _ in range(reserved-len(futures)):self.capacity.release()
            for f in futures.values():f.cancel()
            raise
        done,_=wait(futures.values(),timeout=timeout)
        results={};errors={}
        for key,f in futures.items():
            if f not in done:f.cancel();errors[key]='deadline';continue
            try:results[key]=f.result()
            except Exception as e:errors[key]=type(e).__name__
        # Fair round-robin selection keeps a noisy branch from consuming all
        # context. Re-read under the writer lock after every parallel operation.
        episodes=[];requirements=[];used=0;variant_rejected=0;selected={};aliases={};branch_ids={b['id']:[] for b in parsed};connections=[];graph_truncated=False
        with self.store.lock,self.store.connect() as db:
            allowed={b['id']:self.store.related_ids(b['filters']['entity_id']) if b['filters'].get('entity_id') else None for b in parsed}
            queues={b['id']:list(results.get(b['key'],{}).get('episodes',[])) for b in parsed}
            while any(queues.values()) and len(episodes)<limit and used<text_budget:
                for b in parsed:
                    queue=queues[b['id']]
                    if not queue or len(episodes)>=limit or used>=text_budget:continue
                    hit=queue.pop(0);rid=hit['id']
                    row=db.execute("SELECT id,source,source_id,occurred_at,text FROM records WHERE id=? AND deleted=0",(rid,)).fetchone()
                    if not row:continue
                    f=b['filters']
                    if allowed[b['id']] is not None and rid not in allowed[b['id']]:continue
                    if f.get('source') and row['source']!=f['source']:continue
                    if f.get('after') and (not row['occurred_at'] or row['occurred_at']<f['after']):continue
                    if f.get('before') and (not row['occurred_at'] or row['occurred_at']>=f['before']):continue
                    # Primary terms define the intent anchor. Broader variants
                    # need two content matches, or the backend's semantic floor,
                    # so 'blood type' -> 'ABO blood group' cannot match only 'group'.
                    content=terms(row['text']);queries=b['queries']
                    primary=bool(terms(queries[0]) & content)
                    alternate=any(bool(terms(q)) and len(terms(q)&content)>=min(2,len(terms(q))) for q in queries[1:])
                    relevance=hit.get('relevance') or {}
                    score=relevance.get('semantic_similarity')
                    semantic=isinstance(score,(int,float)) and score>=relevance.get('semantic_minimum',1)
                    if not (primary or alternate or semantic):
                        variant_rejected+=1
                        continue
                    normalized=rid  # Preserve separate provenance and branch scopes for identical text.
                    canonical=aliases.get(normalized)
                    if canonical:
                        item=selected[canonical]
                        if rid!=canonical and rid not in item['duplicate_record_ids']:item['duplicate_record_ids'].append(rid)
                    else:
                        start=max(0,min(hit.get('span_start',0),len(row['text'])))
                        end=min(len(row['text']),start+2400,start+text_budget-used)
                        if end<=start:continue
                        item={**dict(row),'text':row['text'][start:end],'span_start':start,'span_end':end,'truncated':start>0 or end<len(row['text']),
                              'branch_ids':[],'duplicate_record_ids':[],'relevance':hit.get('relevance')}
                        selected[rid]=item;episodes.append(item);aliases[normalized]=rid;used+=end-start;canonical=rid
                    if b['id'] not in item['branch_ids']:item['branch_ids'].append(b['id'])
                    if canonical not in branch_ids[b['id']]:branch_ids[b['id']].append(canonical)
            # Reported relationship traversal is a separate evidence operation.
            # Never widen a source/time/entity-filtered branch via the graph.
            if graph_hops:
                seeds=set()
                for item in episodes:
                    if not any(not b['filters'] and b['id'] in item['branch_ids'] for b in parsed):continue
                    for row in db.execute("SELECT e.id FROM entity_links l JOIN entities e ON e.id=l.entity_id WHERE l.record_id=? AND e.kind!='account' ORDER BY e.id LIMIT 3",(item['id'],)):seeds.add(row[0])
                edge_ids=set();graph_truncated=len(seeds)>3
                for seed in sorted(seeds)[:3]:
                    graph=self.intelligence.graph(entity_id=seed,hops=graph_hops,max_edges=12)
                    graph_truncated=graph_truncated or graph['truncated']
                    for edge in graph['edges']:
                        if edge['id'] in edge_ids:continue
                        payload=edge['payload'];evidence=payload['evidence']
                        if not all(db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(e['record_id'],)).fetchone() for e in evidence):continue
                        connection={'id':edge['id'],'from':payload['subject_id'],'predicate':payload['predicate'],'to':payload['object_id'],
                                    'evidence':evidence,'valid_from':payload.get('valid_from'),'valid_to':payload.get('valid_to'),'seed_id':seed,'verification':'reported_relation'}
                        size=len(json.dumps(connection,ensure_ascii=False))
                        if used+size>text_budget:
                            graph_truncated=True
                            continue
                        connections.append(connection);edge_ids.add(edge['id']);used+=size
            for b in parsed:
                result=results.get(b['key'],{});error=errors.get(b['key'])
                incomplete=bool(error or result.get('diagnostics',{}).get('failures') or result.get('retrieval_status')=='retrieval_incomplete')
                requirements.append({'id':b['id'],'intent':b['intent'],'queries':b['queries'],'filters':b['filters'],'record_ids':branch_ids[b['id']],
                    'status':'candidates_found' if branch_ids[b['id']] else ('retrieval_incomplete' if incomplete else 'no_evidence_in_context'),
                    'incomplete':incomplete,'error':error,'answer_verified':False,'remaining_candidates':len(queues[b['id']])})
        return {'retrieval':'parallel_investigation','goal':goal,'episodes':episodes,'requirements':requirements,'connections':connections,
                'evidence_sufficiency':'not_established','verification_required':[b['id'] for b in parsed],'unresolved_requirements':[r['id'] for r in requirements if not r['record_ids'] or r['incomplete']],
                'diagnostics':{'variant_drift_rejected':variant_rejected,'branches':len(parsed),'unique_searches':len(jobs),'max_concurrency':3,'elapsed_ms':round((time.monotonic()-started)*1000,2),
                               'text_characters':used,'text_budget':text_budget,'graph_truncated':graph_truncated,'timed_out':sum(e=='deadline' for e in errors.values()),'graph_scope':'unfiltered branches only; at current time; at most 3 seeds and 12 edges per seed'},
                'next_steps':['Read cited evidence and check it answers each intent.','Resolve entities only from explicit source evidence.','Refine unresolved branches with discovered names, aliases or dates; use structured knowledge tools for beliefs and numeric aggregation.'],
                'warning':'Hermes authored this plan. Query variants and relation paths are leads, not identity merges or proof. Candidate availability does not establish answerability. Running calls may finish after the response deadline; global worker/queue bounds remain enforced.'}
