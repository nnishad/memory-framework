"""Full AIAgent loop against a local deterministic OpenAI protocol fixture.
This proves runtime wiring, not LLM quality; the fixture is not a language model.
"""
import argparse,json,os,socket,subprocess,sys,tempfile,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args()
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
requests=[]
class Model(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_POST(self):
        payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])));requests.append(payload)
        tool_results=[m for m in payload.get('messages',[]) if m.get('role')=='tool']
        if not tool_results:
            msg={'role':'assistant','content':None,'tool_calls':[{'id':'call_memory_1','type':'function','function':{'name':'personal_memory_investigate','arguments':json.dumps({'goal':'Find bicycle and missing passport location','branches':[{'id':'bicycle','intent':'Bicycle location','queries':['bicycle','west shed']},{'id':'passport','intent':'Passport location','queries':['passport']}]})}}]};finish='tool_calls'
        else:
            msg={'role':'assistant','content':'The bicycle is in the west shed. The memory search did not establish a passport location.'};finish='stop'
        envelope={'id':'fixture-completion','object':'chat.completion','created':int(time.time()),'model':'memory-fixture','choices':[{'index':0,'message':msg,'finish_reason':finish}],'usage':{'prompt_tokens':200,'completion_tokens':30,'total_tokens':230}}
        if payload.get('stream'):
            delta=dict(msg)
            if delta.get('tool_calls'):
                delta['tool_calls']=[{'index':0,**delta['tool_calls'][0]}]
            chunk={'id':'fixture-completion','object':'chat.completion.chunk','created':int(time.time()),'model':'memory-fixture','choices':[{'index':0,'delta':delta,'finish_reason':None}]}
            end={**chunk,'choices':[{'index':0,'delta':{},'finish_reason':finish}],'usage':envelope['usage']}
            raw=('data: '+json.dumps(chunk)+'\n\ndata: '+json.dumps(end)+'\n\ndata: [DONE]\n\n').encode();ctype='text/event-stream'
        else:raw=json.dumps(envelope).encode();ctype='application/json'
        self.send_response(200);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        raw=json.dumps({'object':'list','data':[{'id':'memory-fixture','object':'model','owned_by':'local'}]}).encode()
        self.send_response(200);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
with tempfile.TemporaryDirectory() as tmp:
    home=Path(tmp)/'hermes';os.environ['HERMES_HOME']=str(home)
    from personal_memory.setup import install
    from personal_memory.client import Client
    from personal_memory.ingestion import adapt_existing
    from personal_memory.common import now
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    install(home,port=port,exclusive=True)
    cfg=json.loads((home/'personal-memory/settings.json').read_text())
    process=subprocess.Popen([sys.executable,str(home/'personal-memory/run_service.py')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    model=ThreadingHTTPServer(('127.0.0.1',0),Model);thread=threading.Thread(target=model.serve_forever,daemon=True);thread.start()
    agent=None
    try:
        client=Client(cfg['url'],cfg['token'],timeout=1);deadline=time.monotonic()+90
        while True:
            try:client.call('/v1/health');break
            except Exception:
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Service startup failed')
                time.sleep(.03)
        # The poll budget above is deliberately short. Managed Hindsight retains synchronously, so real
        # calls on this path wait for model extraction and must not inherit the 1 second probe timeout.
        client=Client(cfg['url'],cfg['token'],timeout=600)
        wire=adapt_existing({'source':'fixture','source_id':'bicycle','text':'The bicycle is in the west shed.','occurred_at':now()},connector_id='tests.runtime',connector_version='1',source_locator='fixture://bicycle',observed_at=now())
        client.call('/v1/ingest',{'items':[wire]})
        from run_agent import AIAgent
        agent=AIAgent(base_url=f'http://127.0.0.1:{model.server_port}/v1',api_key='local-protocol-fixture',provider='custom',api_mode='chat_completions',model='memory-fixture',
            enabled_toolsets=['memory'],max_iterations=4,quiet_mode=True,skip_context_files=True,skip_background_review=True,session_id='runtime-session',platform='cli')
        assert agent._memory_manager is not None
        assert 'personal_memory_investigate' in agent.valid_tool_names
        result=agent.run_conversation('Where is my bicycle and do you have evidence for my passport location?')
        assert requests and len(requests)>=2
        assert any('west shed' in json.dumps(m) for r in requests for m in r.get('messages',[]) if m.get('role')=='tool')
        assert any(any(marker in json.dumps(r.get('messages')) for marker in ('Untrusted personal memory evidence', 'Personal memory recall is pending')) for r in requests)
        assert 'west shed' in json.dumps(result)
        tool_messages=[m for r in requests for m in r.get('messages',[]) if m.get('role')=='tool']
        assert any('parallel_investigation' in json.dumps(m) and 'verification_required' in json.dumps(m) for m in tool_messages)
        assert any('personal_memory_investigate' in json.dumps(r.get('tools',[])) for r in requests)
        assert agent._memory_manager.flush_pending(timeout=5)
        provider=agent._memory_manager.get_provider('personal-memory');provider.outbox.flush()
        assert client.call('/v1/search',{'query':'bicycle','source':'hermes'})['episodes']
        report={'passed':True,'actual_AIAgent':True,'model_endpoint':'local deterministic OpenAI protocol fixture','live_language_model':False,
                'requests':len(requests),'checks':['provider initialization in real AIAgent','memory tools in model request','automatic memory evidence or bounded pending status in model context','parallel investigation model tool request dispatched to memory service','requirement verification metadata returned to model','memory result returned to model','final response completed','completed turn durably captured']}
        Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
    finally:
        if agent:agent.close()
        process.terminate()
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        model.shutdown();model.server_close();thread.join()
