"""Small synthetic language-recall benchmark; does not qualify a personal archive."""
import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.semantic import SemanticIndex
from personal_memory.retrieval import Hybrid
from personal_memory.store import Store

CORPUS=[
 ('garage','whatsapp','2024-01-01','Ravi recommended Lotus Garage. The mechanic there repaired my car engine.'),
 ('allergy','email','2024-02-01','My clinic record reports an allergy to penicillin. Discuss medications with my clinician.'),
 ('passport','email','2024-03-01','My passport is stored in the blue folder in the bedroom cupboard.'),
 ('food','whatsapp','2024-04-01','I prefer vegetarian meals and dislike mushrooms.'),
 ('wifi','email','2024-05-01','To restore the home wireless connection, unplug the router, wait thirty seconds, then reconnect its power.'),
 ('school','whatsapp','2024-06-01','Ananya and I met at Greenwood School in 2012. She now works as an architect.'),
 ('flight','email','2024-07-01','My flight to Jaipur departs at 7 AM on July 19.'),
 ('oldaddress','email','2022-08-01','My office address is 14 Market Street.'),
 ('newaddress','email','2025-08-01','My office has moved to 91 River Road.'),
 ('sleep','health','2024-09-01','Last night I slept for six hours and twenty minutes.'),
 ('cycle','whatsapp','2024-10-01','Kabir lent me his bicycle for the weekend.'),
 ('pet','whatsapp','2024-11-01','Our cat is named Pepper and likes sleeping on the sofa.'),
 ('tax','email','2024-12-01','Keep the invoices in the annual tax folder before sending them to the accountant.'),
 ('garden','whatsapp','2025-01-01','Water the basil plant every morning before breakfast.'),
 ('music','whatsapp','2025-02-01','I enjoy listening to jazz while cooking dinner.'),
 ('bank','email','2025-03-01','The bank branch closes at four in the afternoon.'),
 ('dentist','email','2025-04-01','My dental cleaning appointment is on April 16 at 10 AM.'),
 ('book','whatsapp','2025-05-01','Leena borrowed my astronomy book last Friday.'),
 ('train','email','2025-06-01','The train ticket reservation is in coach B2, seat 36.'),
 ('gym','health','2025-07-01','I completed three sets of squats during yesterday\'s workout.')]
CASES=[
 ('en','Who fixed my automobile?','garage',{}),
 ('hi','मेरी गाड़ी का इंजन किसने ठीक किया था?','garage',{}),
 ('hinglish','Gaadi repair karane ke liye Ravi ne kaunsi jagah batayi thi?','garage',{}),
 ('en','Where did I put my travel identity document?','passport',{}),
 ('hi','मेरा पासपोर्ट कहाँ रखा है?','passport',{}),
 ('en','How should I restart the internet equipment at home?','wifi',{}),
 ('en','Which antibiotic am I allergic to?','allergy',{}),
 ('en','Who designs buildings and attended school with me?','school',{}),
 ('hi','मुझे खाने में क्या पसंद नहीं है?','food',{}),
 ('en','How much rest did I get overnight?','sleep',{'source':'health'}),
 ('en','Where is my office?','newaddress',{'after':'2025-01-01T00:00:00Z'}),
 ('en','Where was my workplace in 2022?','oldaddress',{'before':'2023-01-01T00:00:00Z'})]

def main():
    p=argparse.ArgumentParser();p.add_argument('--model-path',required=True);p.add_argument('--output',required=True)
    a=p.parse_args();results=[]
    with tempfile.TemporaryDirectory() as tmp:
        store=Store(Path(tmp)/'memory.db')
        store.ingest([{'source':source,'source_id':sid,'text':text,'occurred_at':date+'T12:00:00Z'} for sid,source,date,text in CORPUS])
        started=time.monotonic();engine=SemanticIndex(store,{'model_path':a.model_path})
        while engine.status()['pending_records']:
            engine.sync()
            if engine.status()['failed_records']:raise RuntimeError('Indexing failed')
        indexing_seconds=time.monotonic()-started
        hybrid=Hybrid(store,semantic=engine,start=False)
        try:
            for lang,query,want,filters in CASES:
                start=time.monotonic();answer=hybrid.search(query,limit=3,expand_entities=False,**filters)
                ids=[r['source_id'] for r in answer['episodes']]
                rank=ids.index(want)+1 if want in ids else None
                results.append({'language':lang,'query':query,'expected':want,'retrieved':ids,'rank':rank,'elapsed_ms':round(1000*(time.monotonic()-start),2)})
            negative=hybrid.search('What is my submarine registration number?',limit=3,expand_entities=False)
        finally:hybrid.close()
        report={'scope':'20 synthetic records; 12 curated questions; no user archive or LLM answer evaluation',
                'model':engine.embedder.model.model_name,'model_key':engine.key,'index':engine.status()['index'],
                'python':platform.python_version(),'records':len(CORPUS),'queries':len(results),'indexing_seconds':round(indexing_seconds,2),
                'recall_at_1':sum(r['rank']==1 for r in results)/len(results),'recall_at_3':sum(r['rank'] is not None for r in results)/len(results),
                'mrr_at_3':sum(1/r['rank'] if r['rank'] else 0 for r in results)/len(results),
                'latency_median_ms':statistics.median(r['elapsed_ms'] for r in results),'latency_max_ms':max(r['elapsed_ms'] for r in results),
                'negative_query_returned_candidates':len(negative['episodes']),
                'abstention':'Candidates are not answers. No calibrated absence threshold; Hermes must inspect evidence and say unknown when unsupported.',
                'cases':results}
        Path(a.output).write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');print(json.dumps({k:v for k,v in report.items() if k!='cases'},indent=2))
if __name__=='__main__':main()
