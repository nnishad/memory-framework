"""Copied-provider launcher and CLI integration smoke test with synthetic data."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from personal_memory.setup import install,rollback
from personal_memory.client import Client

with tempfile.TemporaryDirectory() as tmp:
    home=Path(tmp)/'hermes'
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    install(home,port=port,exclusive=True)
    cfg=json.loads((home/'personal-memory/settings.json').read_text())
    process=subprocess.Popen([sys.executable,str(home/'personal-memory/run_service.py')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        client=Client(cfg['url'],cfg['token'],timeout=1);deadline=time.monotonic()+90
        while True:
            try:client.call('/v1/health');break
            except Exception:
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Copied service did not start')
                time.sleep(.05)
        def cli(*args):
            run=subprocess.run([sys.executable,'-m','personal_memory',*args],capture_output=True,text=True,check=True)
            return json.loads(run.stdout)
        source=Path(__file__).resolve().parents[1]/'examples/encounters.jsonl'
        first=cli('import-jsonl','--hermes-home',str(home),str(source))
        second=cli('import-jsonl','--hermes-home',str(home),str(source))
        assert second['duplicates']==first['processed']
        deadline=time.monotonic()+90
        while True:
            try:
                if client.call('/v1/ready')['ready']:break
            except Exception:
                pass
            if process.poll() is not None or time.monotonic()>deadline:
                raise RuntimeError('Copied service readiness did not converge after import')
            time.sleep(.05)
        checked=subprocess.run([sys.executable,'-m','personal_memory','doctor','--hermes-home',str(home)],capture_output=True,text=True)
        diagnosis=json.loads(checked.stdout)
        assert checked.returncode==1
        assert [c['check'] for c in diagnosis['checks'] if not c['passed']]==['pinned_host_patch']
        request_body=Path(tmp)/'request.json';request_body.write_text('{}')
        capabilities=cli('request','/v1/intelligence-schema',str(request_body),'--hermes-home',str(home),'--credential-role','agent')
        assert '/v1/procedure' in capabilities['endpoints']

        key=Path(tmp)/'backup.key';archive=Path(tmp)/'snapshot.enc'
        cli('backup-keygen',str(key))
        cli('backup','--hermes-home',str(home),'--key-file',str(key),'--destination',str(archive))
        assert cli('restore',str(archive),'--key-file',str(key),'--destination',str(Path(tmp)/'restore'))['verified']
        assert rollback(home)['restored']
        print(json.dumps({'passed':True,'checks':['copied ASGI launcher','contract JSONL import','idempotent replay','doctor rejects missing host attestation','role-aware request CLI and runtime schema','encrypted backup CLI','isolated restore CLI','provider selection rollback'],'live_hermes':False}))
    finally:
        process.terminate()
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait();raise
