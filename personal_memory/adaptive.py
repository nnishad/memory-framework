"""Bounded progressive recall. Stopping is operational, never a proof of sufficiency."""
import time
import re
import math
from .common import required_text

class AdaptiveRecall:
    def __init__(self,store,backend,config=None):
        self.store,self.backend,self.config=store,backend,config or {}
        if not isinstance(self.config,dict) or set(self.config)-{'planner','reranker'}:raise ValueError('Invalid recall configuration')
        for adapter in self.config.values():
            if not isinstance(adapter,dict) or not isinstance(adapter.get('entrypoint'),str) or ':' not in adapter['entrypoint'] or set(adapter)-{'entrypoint','config'}:raise ValueError('Invalid recall adapter')

    def search(self,query,subqueries=None,limit=12,max_calls=3,text_budget=16000,**filters):
        required_text(query,'query',4000)
        if type(max_calls) is not int or not 1<=max_calls<=3:raise ValueError('max_calls must be 1..3')
        if type(limit) is not int or not 1<=limit<=30:raise ValueError('limit must be 1..30')
        if type(text_budget) is not int or not 1000<=text_budget<=48000:raise ValueError('text_budget must be 1000..48000 characters')
        overall_started=time.monotonic()
        planning='caller_supplied';planner_failure=None
        if subqueries is None:
            planner=self.config.get('planner')
            if planner:
                try:
                    from .workflows import run_adapter
                    plan=run_adapter(planner['entrypoint'],planner.get('config',{}),{'query':query},timeout=3)
                    if not isinstance(plan,dict) or set(plan)!={'queries'}:raise ValueError('Invalid query plan')
                    planned=plan['queries']
                    if not isinstance(planned,list) or len(planned)>4:raise ValueError('Invalid planner queries')
                    subqueries=[required_text(q,'planned query',4000) for q in planned];planning='operator_planner'
                except Exception as error:subqueries=[];planner_failure=type(error).__name__;planning='fallback'
            else:
                subqueries=[s.strip() for s in re.split(r'[?;\n]+',query) if s.strip() and s.strip()!=query][:4]
                planning='deterministic_clause_split'

        if not isinstance(subqueries,list) or len(subqueries)>4:raise ValueError('At most four subqueries')
        subqueries=list(dict.fromkeys(required_text(q,'subquery',4000) for q in subqueries))
        if set(filters)-{'entity_id','source','after','before','include_history'}:raise ValueError('Unsupported recall filters')
        stages=[('fast',[]),('balanced',subqueries),('deep',subqueries)]
        pool={};claims={};trace=[];coverage=[];started=overall_started;stop='call_budget'
        for index,(depth,variants) in enumerate(stages[:max_calls]):
            if index and time.monotonic()-started>=2:
                stop='round_launch_deadline';break
            result=self.backend.search(query=query,queries=variants,depth=depth,limit=limit,**filters)
            added=0
            for row in result.get('episodes',[]):
                if row['id'] not in pool:added+=1
                pool[row['id']]=row
            # The latest, deeper ranking leads; retain earlier unique evidence as fallback.
            latest={r['id']:pool[r['id']] for r in result.get('episodes',[])}
            pool={**latest,**{rid:row for rid,row in pool.items() if rid not in latest}}
            for row in result.get('claims',[]):claims[row['id']]=row
            coverage=result.get('coverage',[])
            failures=result.get('diagnostics',{}).get('failures',{})
            trace.append({'depth':depth,'new_records':added,'query_count':1+len(variants),'failures':failures,'retrieval_status':result.get('retrieval_status')})
            if failures:stop='backend_incomplete';break
            # Always attempt supplied subquestions before considering convergence.
            if index>=1 and not added and not failures:stop='no_new_candidates';break
        reranker_failure=None
        reranker=self.config.get('reranker')
        if reranker and pool and time.monotonic()-started<25:
            try:
                from .workflows import run_adapter
                request={'query':query,'documents':[{'id':r['id'],'text':r.get('text','')[:400]} for r in pool.values()]}
                ranked=run_adapter(reranker['entrypoint'],reranker.get('config',{}),request,timeout=3)
                ids=ranked['ordered_ids']
                if not isinstance(ids,list) or len(ids)!=len(pool) or set(ids)!=set(pool):raise ValueError('Invalid reranker permutation')
                pool={rid:pool[rid] for rid in ids}
            except Exception as error:reranker_failure=type(error).__name__
        episodes=[];selected_claims=[];used=0;duplicates={}
        related=self.store.related_ids(filters['entity_id']) if filters.get('entity_id') else None
        with self.store.connect() as db:
            # Rehydrate all candidate IDs after the final search. Never return
            # stale/deleted text retained from an earlier round.
            for rid,row in pool.items():
                current=db.execute('SELECT text,source,occurred_at FROM records WHERE id=? AND deleted=0',(rid,)).fetchone()
                if not current:continue
                if related is not None and rid not in related:continue
                if filters.get('source') and current['source']!=filters['source']:continue
                if filters.get('after') and (not current['occurred_at'] or current['occurred_at']<filters['after']):continue
                if filters.get('before') and (not current['occurred_at'] or current['occurred_at']>=filters['before']):continue
                normalized=' '.join(current[0].split())
                if normalized in duplicates:
                    duplicates[normalized]['duplicate_record_ids'].append(rid);continue
                if len(episodes)>=limit or used>=text_budget:break
                start=min(row.get('span_start',0),len(current[0]));end=min(len(current[0]),start+2400,start+text_budget-used)
                text=current[0][start:end]
                if not text:continue
                item={**row,'text':text,'span_start':start,'span_end':end,'truncated':start>0 or end<len(current[0]),'duplicate_record_ids':[]}
                episodes.append(item);duplicates[normalized]=item;used+=len(text)
            selected={r['id'] for r in episodes}
            for cid in claims:
                row=db.execute("SELECT c.* FROM claims c JOIN records r ON c.record_id=r.id WHERE c.id=? AND r.deleted=0 AND c.status!='retracted'",(cid,)).fetchone()
                if row and row['record_id'] in selected and (filters.get('include_history') or row['status']=='active'):
                    item=dict(row)
                    size=len(item['text'])+len(item.get('evidence_quote') or '')
                    if used+size<=text_budget:selected_claims.append(item);used+=size
        return {'episodes':episodes,'claims':selected_claims,'coverage':coverage,'retrieval':'bounded_progressive',
                'diagnostics':{'planner':planning,'planner_failure':planner_failure,'reranker_failure':reranker_failure,'stages':trace,'stop_reason':stop,'text_characters':used,'text_budget':text_budget,'elapsed_ms':round((time.monotonic()-started)*1000,2)},
                'retrieval_status':'candidates_found' if episodes else ('retrieval_incomplete' if any(t['failures'] or t.get('retrieval_status')=='retrieval_incomplete' for t in trace) else 'no_relevant_evidence'),
                'evidence_sufficiency':'not_established','warning':'Bounded search does not prove completeness, truth or absence. Planning and reranking are relevance aids; inspect source evidence and contradictions.'}
