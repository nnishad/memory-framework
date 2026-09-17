"""Opt-in bounded Gmail integration test. Persists private evidence outside the repo.

Does not enable indefinite ingestion or call an LLM. Uses the real HTTP boundary,
Gmail source runtime, canonical storage and keyword retrieval. Production model
readiness and a live Hermes host are separate deployment qualifications.
"""
import argparse
import json
import secrets
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.client import Client
from personal_memory.common import atomic_json
from personal_memory.server import create_server


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--credentials-file',required=True,type=Path)
    parser.add_argument('--data-dir',required=True,type=Path)
    parser.add_argument('--pages',type=int,default=2)
    parser.add_argument('--polls',type=int,default=2)
    parser.add_argument('--poll-delay',type=int,default=20)
    args=parser.parse_args()
    if not 1<=args.pages<=10 or not 1<=args.polls<=5 or not 0<=args.poll_delay<=60:
        parser.error('Bounded test: 1..10 pages, 1..5 polls, 0..60 seconds between polls')
    token=secrets.token_urlsafe(32)
    server=create_server(args.data_dir,token,port=0,source_config={'enabled':False},
                         retrieval_config={'semantic':{'enabled':False},'rerank':{'enabled':False}})
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    client=Client(f'http://127.0.0.1:{server.server_port}',token,timeout=120)
    report={'historical_passes':[],'incremental_passes':[],'model_processing_tested':False}
    cid=None
    try:
        connected=client.call('/v1/sources/gmail/connect',{'credentials':json.loads(args.credentials_file.read_text())})
        cid=connected['connection_id'];report['mailbox_messages']=connected['messages_total']
        print(json.dumps({'stage':'connected','mailbox_messages':connected['messages_total']}),flush=True)
        for i in range(args.pages):
            server.service.sources.tick()
            status=client.call('/v1/sources/status',{'connection_id':cid})['connections'][0]
            errors=[s['error'] for s in status['schedule'] if s['error']]
            if errors:raise RuntimeError('Source pass failed: '+', '.join(errors))
            report['historical_passes'].append({'pass':i+1,'stored_messages':status['stored_messages']})
            print(json.dumps({'stage':'historical_pass',**report['historical_passes'][-1]}),flush=True)
        for i in range(args.polls):
            if i:time.sleep(args.poll_delay)
            result=server.service.sources.worker.run_once(cid,stream='messages',role='incremental',ttl=900)
            if result['status'] not in ('committed','complete'):raise RuntimeError('Incremental sync failed')
            report['incremental_passes'].append(result)
            print(json.dumps({'stage':'incremental_pass','pass':i+1,**result}),flush=True)
        # Verify private content internally; print only validation booleans/counts.
        with server.store.connect() as db:
            row=db.execute('SELECT id,text FROM records WHERE source=? AND deleted=0 LIMIT 1',(connected['source'],)).fetchone()
        if not row:raise RuntimeError('No historical evidence was imported')
        evidence=client.call('/v1/evidence',{'record_id':row['id']})
        import re
        words=[word for word in re.findall(r'[^\W_]+',row['text']) if len(word)>=4]
        found={}
        for query in words[:10]:
            found=client.call('/v1/search',{'query':query,'source':connected['source'],'limit':30})
            if any(item['id']==row['id'] for item in found.get('episodes',[])):
                break
        report['evidence_roundtrip']=evidence['text']==row['text']
        report['retrieval_returned_source_evidence']=any(item['id']==row['id'] for item in found.get('episodes',[]))
        if not report['evidence_roundtrip'] or not report['retrieval_returned_source_evidence']:
            raise RuntimeError('Evidence retrieval verification failed')
        report['stored_messages']=client.call('/v1/sources/status',{'connection_id':cid})['connections'][0]['stored_messages']
        report['result']='passed'
    finally:
        if cid:
            client.call('/v1/sources/control',{'connection_id':cid,'action':'pause'})
            report['connection_paused']=True
        atomic_json(args.data_dir/'live-test-report.json',report)
        server.shutdown();server.server_close();thread.join(timeout=5)
    print(json.dumps(report,indent=2),flush=True)


if __name__=='__main__':main()
