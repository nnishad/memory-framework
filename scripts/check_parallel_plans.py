"""Curated Hermes-style plans against fictional archive; no real LLM planning test."""
import json
import sys
import tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from evaluate_synthetic_personal import corpus
from personal_memory.store import Store
from personal_memory.retrieval import Hybrid
from personal_memory.intelligence import Intelligence
from personal_memory.investigate import Investigation

CASES=[
 {'goal':'Who fixed my automobile?', 'branches':[{'id':'repair','intent':'Find car mechanic recommendation','queries':['car engine repair','garage mechanic']}], 'expected':['garage']},
 {'goal':'Where is my blue bicycle key?', 'branches':[{'id':'key','intent':'Locate key using English and Hindi','queries':['blue bicycle key','नीली साइकिल चाबी']}], 'expected':['hindi']},
 {'goal':'Where was my office in 2023 and where is it now?', 'branches':[{'id':'past','intent':'Office in 2023','queries':['office address'],'filters':{'after':'2023-01-01T00:00:00Z','before':'2024-01-01T00:00:00Z'}},{'id':'current','intent':'Office after move','queries':['office address correction'],'filters':{'after':'2025-01-01T00:00:00Z'}}], 'expected':['old-address','new-address']},
 {'goal':'Who offered bicycle repairs in the group and who recommended them?', 'branches':[{'id':'group','intent':'Group repair offer','queries':['bicycle brakes Cedar Cycles']},{'id':'recommendation','intent':'Personal recommendation','queries':['Mira Theo recommended']}], 'expected':['group-unknown','recommend']},
 {'goal':'Where is my travel document and how do I restore home internet?', 'branches':[{'id':'document','intent':'Passport location','queries':['passport folder cupboard']},{'id':'internet','intent':'Router recovery instructions','queries':['router wireless power']}], 'expected':['passport','wifi']},
 {'goal':'What is my blood type?', 'branches':[{'id':'blood','intent':'Explicit blood group record','queries':['blood type','ABO blood group']}], 'expected':[]},
]

def main():
    rows=[]
    with tempfile.TemporaryDirectory() as tmp:
        store=Store(Path(tmp)/'memory.db');data=corpus()
        for start in range(0,len(data),100):store.ingest_contract(data[start:start+100])
        backend=Hybrid(store,start=False);engine=Investigation(store,backend,Intelligence(store))
        try:
            for case in CASES:
                result=engine.search(goal=case['goal'],branches=case['branches'])
                found=[r['source_id'] for r in result['episodes']]
                passed=all(s in found for s in case['expected']) if case['expected'] else not found
                rows.append({**case,'found':found,'passed':passed,'result':result})
        finally:engine.close();backend.close()
    report={'scope':'1,226 fictional records; keyword-only backend; six explicitly authored plan fixtures. No real Hermes model or automatic plan-quality evaluation.','passed':sum(r['passed'] for r in rows),'total':len(rows),'cases':rows}
    Path('docs/PARALLEL_PLAN_CHECK.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='cases'},indent=2))
    return 0 if all(r['passed'] for r in rows) else 1
if __name__=='__main__':sys.exit(main())
