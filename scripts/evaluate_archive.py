"""Read-only evaluation against a running service. Gold cases stay on the operator host."""
import argparse
import json
import statistics
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.__main__ import settings
from personal_memory.client import Client
p=argparse.ArgumentParser();p.add_argument('--hermes-home',required=True);p.add_argument('--cases',required=True);p.add_argument('--output',required=True)
a=p.parse_args();cfg=settings(a.hermes_home);client=Client(cfg['url'],cfg.get('agent_token',cfg['token']))
rows=[]
for line in Path(a.cases).read_text().splitlines():
    if not line.strip():continue
    case=json.loads(line);start=time.monotonic()
    result=client.call('/v1/search',{'query':case['query'],'limit':10,**case.get('filters',{})})
    found=[r['id'] for r in result['episodes']];expected=set(case['expected_record_ids'])
    rows.append({'case_id':case['case_id'],'retrieval_recall_at_10':len(expected.intersection(found))/len(expected) if expected else None,
                 'candidate_count':len(found),'elapsed_ms':round((time.monotonic()-start)*1000,2),
                 'failures':result['diagnostics']['failures'],
                 'requires_answer_abstention_review':not expected})
if not rows:raise ValueError('No evaluation cases')
scored=[r['retrieval_recall_at_10'] for r in rows if r['retrieval_recall_at_10'] is not None]
report={'cases':rows,'mean_recall_at_10':statistics.mean(scored) if scored else None,
        'production_certified':False,'note':'Retrieval evaluation only. Separately review actual Hermes answers/actions, negative cases, identity precision and sensitive-source permissions.'}
Path(a.output).write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'cases':len(rows),'mean_recall_at_10':report['mean_recall_at_10']}))
