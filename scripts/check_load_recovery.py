"""Synthetic lexical API load and hard-restart check; no full-archive/model SLO claim."""
import argparse,json,os,socket,statistics,subprocess,sys,tempfile,time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.setup import install
from personal_memory.client import Client
from personal_memory.common import now
from personal_memory.recovery import backup,restore,create_key
p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--records',type=int,default=10000);a=p.parse_args()
if a.records<100:raise ValueError('At least 100 records')
with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp);home=root/'home'
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    install(home,port=port,exclusive=True);cfg=json.loads((home/'personal-memory/settings.json').read_text())
    client=Client(cfg['url'],cfg['token'],timeout=30);process=None
    def start():
        proc=subprocess.Popen([sys.executable,str(home/'personal-memory/run_service.py')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        deadline=time.monotonic()+90
        while True:
            try:client.call('/v1/health');return proc
            except Exception:
                if proc.poll() is not None or time.monotonic()>deadline:proc.kill();proc.wait();raise RuntimeError('Startup failed')
                time.sleep(.03)
    process=start()
    try:
        observed=now();started=time.monotonic()
        for offset in range(0,a.records,100):
            records=[{'schema_version':'1.0','source':'synthetic-load','source_id':str(i),'revision':'1','kind':'note',
                      'occurred_at':'2025-01-01T12:00:00Z','observed_at':observed,'text':f'The unique asset asset{i} belongs in storage location shelf{i}.',
                      'participants':[],'provenance':{'connector_id':'tests.load','connector_version':'1','source_locator':f'fixture://{i}','origin':'source','parent_record_ids':[]},'extensions':{}}
                     for i in range(offset,min(offset+100,a.records))]
            client.call('/v1/ingest',{'items':records})
        ingest_seconds=time.monotonic()-started
        def query(i):
            begin=time.monotonic();r=client.call('/v1/search',{'query':f'asset{i}','depth':'fast','expand_entities':False})
            if not r['episodes'] or r['episodes'][0]['source_id']!=str(i):raise AssertionError('Incorrect exact-identifier retrieval')
            return (time.monotonic()-begin)*1000
        cases=[i*(a.records//100) for i in range(100)]
        with ThreadPoolExecutor(max_workers=8) as pool:latencies=list(pool.map(query,cases))
        process.kill();process.wait();process=start()
        assert client.call('/v1/status')['records']==a.records
        query(a.records-1)
        key=root/'key';create_key(key);archive=root/'backup.enc';recovery_start=time.monotonic()
        backup(home,archive,key);r=restore(archive,key,root/'restored');assert r['verified']
        recovery_seconds=time.monotonic()-recovery_start
        report={'records':a.records,'concurrent_clients':8,'queries':100,'retrieval':'keyword only; synthetic exact-identifier questions',
                'ingest_seconds':round(ingest_seconds,2),'query_p50_ms':round(statistics.median(latencies),2),
                'query_p95_ms':round(sorted(latencies)[94],2),'query_max_ms':round(max(latencies),2),
                'acknowledged_records_survived_SIGKILL':True,'encrypted_backup_and_isolated_restore_seconds':round(recovery_seconds,2),
                'archive_bytes':archive.stat().st_size,'production_certified':False,
                'scope':'Ephemeral test host. Does not measure full-archive semantic indexing, personal-data recall, deployment hardware or host-loss recovery.'}
        Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
    finally:
        if process:process.terminate();process.wait(timeout=15)
