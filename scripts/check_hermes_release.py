"""Exercise the actual Hermes loader/MemoryManager against a copied plugin and live ASGI service.
No fake agent modules, LLM calls, credentials or user archives are required.
"""
import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

p=argparse.ArgumentParser();p.add_argument('--hermes-root',required=True);p.add_argument('--output',required=True);p.add_argument('--hermes-commit')
a=p.parse_args();root=Path(a.hermes_root).resolve();project=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(root),str(project)]
from personal_memory.setup import install
from personal_memory.client import Client
checks=[]
def passed(name):checks.append({'check':name,'passed':True})
with tempfile.TemporaryDirectory() as tmp:
    home=Path(tmp)/'profile';os.environ['HERMES_HOME']=str(home)
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    install(home,port=port,exclusive=True)
    from plugins.memory import find_provider_dir,load_memory_provider,list_memory_provider_names
    from agent.memory_manager import MemoryManager,inject_memory_provider_tools,build_memory_context_block
    from tools.memory_tool import get_builtin_memory_store_flags
    import yaml
    assert 'personal-memory' in list_memory_provider_names()
    assert find_provider_dir('personal-memory')==home/'plugins/personal-memory'
    provider=load_memory_provider('personal-memory');assert provider is not None and provider.is_available()
    assert str(home) in sys.modules[provider.__class__.__module__].__file__
    passed('real user-plugin discovery and copied-module loading')
    from plugins.memory.config_schema import get_provider_config_schema
    schema=get_provider_config_schema('personal-memory')
    assert schema is not None and any(f.key=='session_access' for f in schema.fields)
    provider.save_config({'port':str(port),'prefetch_wait_ms':'200'},str(home))
    from personal_memory.configuration import load_settings
    assert load_settings(home)['port']==port
    passed('native CLI string-valued setup and declarative dashboard configuration')
    assert get_builtin_memory_store_flags(yaml.safe_load((home/'config.yaml').read_text()))==(True,True)
    assert yaml.safe_load((home/'config.yaml').read_text())['memory']['store']=='provider'
    passed('exclusive setup keeps native memory surfaces enabled while selecting the canonical provider store')
    cfg=json.loads((home/'personal-memory/settings.json').read_text())
    cfg['intelligence']={'adapters':{'evaluate':{'entrypoint':'personal_memory.adapters:policy_fixture','config':{}}},'capabilities':{'echo':{'entrypoint':'personal_memory.adapters:echo_capability','config':{}}}}
    for principal in cfg['principals']:
        if principal['role']=='agent':principal['capabilities']=['echo']
    from personal_memory.common import atomic_json
    atomic_json(home/'personal-memory/settings.json',cfg)

    process=subprocess.Popen([sys.executable,str(home/'personal-memory/run_service.py')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    manager=MemoryManager()
    try:
        client=Client(cfg['url'],cfg['token'],timeout=1);deadline=time.monotonic()+90
        while True:
            try:client.call('/v1/health');break
            except Exception:
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('ASGI launcher failed')
                time.sleep(.02)
        manager.add_provider(provider)
        manager.initialize_all('session-a',hermes_home=str(home),platform='cli',agent_context='primary',user_id='synthetic-owner')
        assert provider.client is not None
        agent=SimpleNamespace(_memory_manager=manager,tools=[],enabled_toolsets=['memory'],disabled_toolsets=[])
        expected={s['name'] for s in provider.get_tool_schemas()}
        assert inject_memory_provider_tools(agent)==len(expected)
        assert agent.valid_tool_names==expected and inject_memory_provider_tools(agent)==0
        gated=SimpleNamespace(_memory_manager=manager,tools=[],enabled_toolsets=['memory'],disabled_toolsets=['memory'])
        assert inject_memory_provider_tools(gated)==0
        passed('actual host tool injection, duplicate suppression and disabled-toolset gate')
        prompt=manager.build_system_prompt()
        manager.sync_all('My bicycle is stored in the west shed.','Recorded.',session_id='session-a',messages=[{'role':'user','content':'My bicycle is stored in the west shed.'}])
        assert manager.flush_pending(timeout=5);provider.outbox.flush()
        result=json.loads(manager.handle_tool_call('personal_memory_search',{'query':'bicycle'}))
        assert result['episodes'] and 'west shed' in result['episodes'][0]['text']
        passed('actual manager background sync and tool dispatch to authenticated live service')
        status=json.loads(manager.handle_tool_call('personal_memory_status',{}))
        assert 'parallel_investigation' in status['capabilities']
        assert status['framework_version']==__import__('personal_memory').__version__
        assert 'personal_memory_investigate' in prompt
        plan={'goal':'Locate bicycle and check missing passport evidence','branches':[
            {'id':'bicycle','intent':'Bicycle location','queries':['bicycle','west shed']},
            {'id':'passport','intent':'Passport location','queries':['passport']}]}
        investigation=json.loads(manager.handle_tool_call('personal_memory_investigate',plan))
        assert investigation['episodes'] and investigation['unresolved_requirements']==['passport'],investigation
        assert investigation['verification_required']==['bicycle','passport']
        assert investigation['diagnostics']['unique_searches']==2
        from personal_memory.operations import doctor
        atomic_json(home/'personal-memory/host-runtime.json', {
            'root': str(root), 'api': 2, 'release': 'v2026.9.14',
            'commit': a.hermes_commit or '345cd2b057a452236de401d3534b8502a7465e8d'})
        # The managed Hindsight worker may still be committing the turn above.
        # Readiness deliberately stays false until its durable queue converges.
        deadline=time.monotonic()+90
        while True:
            try:
                if client.call('/v1/ready')['ready']:break
            except Exception:
                pass
            if process.poll() is not None or time.monotonic()>deadline:
                raise RuntimeError('Service readiness did not converge after host sync')
            time.sleep(.05)
        diagnosis=doctor(home)
        assert diagnosis['checks_passed'],diagnosis
        passed('native investigation schema, planning guidance, two-branch tool dispatch, capability discovery and upgrade doctor')
        progressive=json.loads(manager.handle_tool_call('personal_memory_recall',{'query':'bicycle','subqueries':['west shed']}))
        assert progressive['episodes'] and progressive['evidence_sufficiency']=='not_established'
        outcome=json.loads(manager.handle_tool_call('personal_memory_outcome',{'key':'bridge-outcome','goal':'Locate bicycle','action':'Read source','result':'Source names west shed','outcome':'success','evidence_ids':[result['episodes'][0]['id']]}))
        assert outcome['kind']=='outcome'
        proposal=json.loads(manager.handle_tool_call('personal_memory_propose',{'key':'bridge-lesson','family':'storage-lookup','revision':1,'category':'procedural','lesson':'Read dated storage evidence','scope':'Locating stored objects','prerequisites':[],'exceptions':[],'outcome_ids':[outcome['id']],'evidence_ids':[]}))
        assert proposal['state']=='candidate'
        assert not json.loads(manager.handle_tool_call('personal_memory_lessons',{}))['items']
        provider.agent_context='secondary'
        assert 'error' in json.loads(manager.handle_tool_call('personal_memory_outcome',{'key':'blocked'}))
        provider.agent_context='primary'
        passed('progressive recall and evidence-backed learning tools through copied ASGI; inactive proposals and secondary write guard')
        def memory_tool(name,arguments):
            response=json.loads(manager.handle_tool_call(name,arguments))
            assert not response.get('error'),response
            return response
        def manage(operation,arguments):return memory_tool('personal_memory_manage',{'operation':operation,'arguments':arguments})
        def knowledge(operation,arguments):return memory_tool('personal_memory_knowledge',{'operation':operation,'arguments':arguments})
        rid=result['episodes'][0]['id']
        entity=memory_tool('personal_memory_entity',{'kind':'person','label':'Fixture owner','record_id':rid})['id']
        manage('belief',{'key':'bridge-belief','subject_id':entity,'predicate':'storage','value':'west shed','evidence':[{'record_id':rid,'quote':'west shed'}]})
        assert knowledge('beliefs',{'subject_id':entity})['beliefs']
        task=manage('task',{'key':'bridge-task','title':'Check bicycle location','evidence_ids':[rid]})
        assert manage('transition',{'task_id':task['id'],'expected_version':1,'state':'in_progress','evidence_ids':[rid]})['state']=='in_progress'
        snapshot=manage('snapshot',{'key':'bridge-snapshot','record_ids':[rid]})
        consolidation=manage('consolidate',{'key':'bridge-consolidation','type':'consolidate','snapshot_id':snapshot['id']})
        def completed(job):
            deadline=time.monotonic()+12
            while True:
                status=knowledge('workflow',{'job_id':job['id']})
                if status['state']=='completed':return status
                assert status['state']!='quarantined' and time.monotonic()<deadline,status
                time.sleep(.03)
        assert completed(consolidation)['result']['state']=='candidate'
        passed('structured beliefs, task transitions and durable consolidation through real Hermes tools')
        suite=client.call('/v1/suite',{'key':'bridge-suite','evidence_ids':[rid],'cases':[
            {'id':'target','kind':'target','input':{'scope':'Locating stored objects'},'expected':'Read dated storage evidence'},
            {'id':'regression','kind':'regression','input':{'scope':'other','fallback':'same'},'expected':'same'},
            {'id':'negative','kind':'non_applicable','input':{'scope':'different'},'expected':''}]})
        evaluation=client.call('/v1/workflow/enqueue',{'key':'bridge-evaluation','type':'evaluate','candidate_id':proposal['id'],'suite_id':suite['id']})
        evaluated=completed(evaluation)
        assert evaluated['result']['payload']['passed']
        client.call('/v1/learning/promote',{'candidate_id':proposal['id'],'evaluation_id':evaluated['result_id'],'expected_active_id':None})
        procedure=client.call('/v1/procedure',{'key':'bridge-procedure','candidate_id':proposal['id'],'capability':'echo','arguments':{'echo':'ok'},'expected':{'echo':'ok'}})
        execution=memory_tool('personal_memory_execute',{'key':'bridge-execute','procedure_id':procedure['id']})
        assert completed(execution)['result']['payload']['validation_passed']
        assert knowledge('procedures',{})['procedures']
        passed('executed evaluation suite, administrator promotion and capability-scoped procedure via actual Hermes tool')


        # Host calls prefetch once for a new turn. Success must occur on this call.
        context=manager.prefetch_all('Where is my bicycle stored?',session_id='session-a')
        assert 'west shed' in context,context
        assert manager.describe_recall()
        assert 'NOT new user input' in build_memory_context_block(context)
        assert manager.build_system_prompt()==prompt
        passed('first-call recall injection, host indicator and stable system prompt')
        original_call=provider.client.call
        def slow_search(path,*args,**kwargs):
            if path=='/v1/search':time.sleep(.6)
            return original_call(path,*args,**kwargs)
        with patch.object(provider.client,'call',side_effect=slow_search):
            started=time.monotonic()
            pending=manager.prefetch_all('A fresh slow lookup of bicycle storage',session_id='session-a')
            assert 'pending' in pending and time.monotonic()-started<.5
            assert not manager.describe_recall()
            with provider.recall_ready:
                assert provider.recall_ready.wait_for(lambda:not provider.inflight,timeout=3)
        passed('slow recall stays bounded and does not report a stale recall indicator')
        assert manager.supports_pre_compress_checkpoint()
        direct=[{'role':'user','content':'Checkpoint direct evidence: blue suitcase.'}]
        raw=[{'role':'user','content':'DERIVATIVE_SHOULD_NOT_BE_CAPTURED','_compressed_summary':True}]+direct
        assert 'committed locally' in manager.on_pre_compress(raw,evidence_messages=direct,require_checkpoint=True)
        provider.outbox.flush()
        found=json.loads(manager.handle_tool_call('personal_memory_search',{'query':'suitcase'}))
        assert found['episodes']
        assert not json.loads(manager.handle_tool_call('personal_memory_search',{'query':'DERIVATIVE_SHOULD_NOT_BE_CAPTURED'}))['episodes']
        with patch.object(provider.outbox,'enqueue',side_effect=OSError('synthetic disk failure')):
            try:manager.on_pre_compress(direct,evidence_messages=direct,require_checkpoint=True)
            except OSError:pass
            else:raise AssertionError('Required checkpoint failure was swallowed')
        passed('normalized checkpoint-v2 evidence and host fail-closed propagation')
        manager.commit_session_boundary_async(direct,new_session_id='session-b',parent_session_id='session-a')
        manager.sync_all('The telescope is in the attic.','Recorded.',session_id='session-b')
        assert manager.flush_pending(timeout=5);provider.outbox.flush()
        answer=json.loads(manager.handle_tool_call('personal_memory_search',{'query':'telescope'}))
        assert provider.session_id=='session-b'
        assert all(r['source_id'].startswith('session-b/') for r in answer['episodes'])
        passed('serialized session boundary and new-session source attribution')
        transcript=[{'role':'assistant','tool_calls':[{'id':'call-1','function':{'name':'read_file','arguments':'{}'}},
                    {'id':'call-memory','function':{'name':'personal_memory_search','arguments':'{}'}}]},
                    {'role':'tool','tool_call_id':'call-1','content':'The workshop door code is synthetic-512.'},
                    {'role':'tool','tool_call_id':'call-memory','content':json.dumps({'episodes':[], 'warning':'DO_NOT_DUPLICATE_MEMORY_RESULTS'})}]
        manager.sync_all('Read workshop instructions','Read.',session_id='session-b',messages=transcript)
        assert manager.flush_pending(timeout=5);provider.outbox.flush()
        tool_hits=json.loads(manager.handle_tool_call('personal_memory_search',{'query':'synthetic-512','source':'hermes-tools'}))['episodes']
        assert tool_hits
        assert not json.loads(manager.handle_tool_call('personal_memory_search',{'query':'DO_NOT_DUPLICATE_MEMORY_RESULTS'}))['episodes']
        before=client.call('/v1/status')['records']
        manager.on_session_end(transcript);provider.outbox.flush()
        assert client.call('/v1/status')['records']==before
        manager.on_delegation('Inspect build logs','The fixture build failed with SYNTHETIC_BUILD_ERROR.',child_session_id='child-1')
        provider.outbox.flush()
        assert json.loads(manager.handle_tool_call('personal_memory_search',{'query':'SYNTHETIC_BUILD_ERROR','source':'hermes-delegation'}))['episodes']
        passed('tool outputs and delegated results captured; memory results excluded; replay deduplicated')
        for context in [dict(platform='telegram',user_id='stranger',chat_type='private'),dict(platform='telegram',user_id='owner',chat_type='group')]:
            blocked=load_memory_provider('personal-memory')
            blocked.initialize('blocked',hermes_home=str(home),**context)
            assert blocked.get_tool_schemas()==[] and blocked.prefetch('bicycle')==''
            assert 'error' in json.loads(blocked.handle_tool_call('personal_memory_search',{'query':'bicycle'}))
            assert blocked.outbox is None
            blocked.shutdown()
        passed('real loaded provider withholds tools, recall and capture from unauthorized/shared sessions')
        from hermes_cli.backup import _write_full_zip_backup
        import zipfile,sqlite3
        backup_file=Path(tmp)/'native-backup.zip'
        assert _write_full_zip_backup(backup_file,home)==backup_file
        with zipfile.ZipFile(backup_file) as z:
            raw=z.read('personal-memory/data/memory.db')
            restored=Path(tmp)/'native-restored.db';restored.write_bytes(raw)
            with sqlite3.connect(restored) as db:
                assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
                assert db.execute('SELECT count(*) FROM records WHERE deleted=0').fetchone()[0]==client.call('/v1/status')['records']
            assert 'personal-memory/outbox.db' in z.namelist()
            assert not any(n.endswith(('.db-wal','.db-shm')) for n in z.namelist())
        passed('actual Hermes native backup contains independently readable SQLite snapshots')

        manager.shutdown_all();assert manager.shutdown_drain_state['status']=='drained'
        assert not provider.outbox.thread.is_alive() and not provider.recall_threads
        passed('manager shutdown drains writes and closes adapter workers')
    finally:
        manager.shutdown_all();process.terminate()
        try:process.wait(timeout=15)
        except subprocess.TimeoutExpired:process.kill();process.wait();raise
report={'hermes_tag':'v2026.9.14','hermes_commit':a.hermes_commit or subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip(),
        'provider_contract_sha256':hashlib.sha256((root/'agent/memory_provider.py').read_bytes()).hexdigest(),
        'checks':checks,'passed':len(checks),'failed':0,
        'scope':'Actual release loader, MemoryManager, tool gate, context fence and live copied ASGI service. Agent tool-surface container is a SimpleNamespace; no AIAgent/LLM/desktop/gateway end-to-end run.',
        'live_llm_tested':False,'provider_recipient_gate_tested':True,'native_backup_sqlite_snapshot_tested':True,'full_native_import_cycle_tested':False}
Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
