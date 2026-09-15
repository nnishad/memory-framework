"""Patched pinned host integration against a real provider and HTTP service."""
import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[1] / 'tests')]
import test_memory as fixtures
from personal_memory.setup import install
from personal_memory.native_history import HermesHistoryConnector, sync_history

checks = []
with tempfile.TemporaryDirectory(prefix='hermes-host-bridges-') as temp:
    home = Path(temp) / 'profile'
    os.environ['HERMES_HOME'] = str(home)
    case = fixtures.HTTPFixture()
    case.setUp()
    install(home, exclusive=True)
    settings_file = home / 'personal-memory/settings.json'
    cfg = json.loads(settings_file.read_text())
    cfg.update(url=case.client.url, token=case.token, agent_token=case.token)
    settings_file.write_text(json.dumps(cfg))
    public = home / 'personal-memory/config.json'
    values = json.loads(public.read_text()); values.pop('port', None); public.write_text(json.dumps(values))
    from plugins.memory import load_memory_provider
    from agent.memory_bridge import MemoryHostContext, gui_context, delegation_packet, review_completed, check_cron_delivery
    from agent.memory_manager import MemoryManager
    from tui_gateway.transport import StdioTransport, bind_transport, reset_transport
    from tui_gateway.ws import WSTransport
    from hermes_state import SessionDB
    from run_agent import AIAgent
    native = SessionDB(home / 'state.db')
    agents = []
    providers = []
    def provider(platform='cli', context=None):
        p = load_memory_provider('personal-memory')
        p.initialize('owner-session', hermes_home=str(home), platform=platform, host_context=context)
        providers.append(p)
        return p
    try:
        stdio = StdioTransport(lambda: sys.stdout, threading.Lock())
        local = gui_context(stdio, home)
        p = provider('tui', local)
        assert p.access_allowed
        assert provider('desktop', {'kind':'local_owner','home':str(home)}).access_allowed is False
        assert provider('desktop', MemoryHostContext(str(home / 'other'), 'local_owner')).access_allowed is False
        assert provider('desktop').access_allowed is False
        loop = asyncio.new_event_loop()
        ws = WSTransport(None, loop, memory_owner=True)
        assert provider('desktop', gui_context(ws, home)).access_allowed
        assert gui_context(WSTransport(None, loop), home) is None
        loop.close()
        checks.append('typed local transport identity accepted; JSON claims, unstamped desktop and cross-profile contexts denied')

        agent = AIAgent(provider='custom', base_url='http://127.0.0.1:9/v1', api_key='fixture-only', model='fixture',
                        enabled_toolsets=['memory'], quiet_mode=True, skip_context_files=True, skip_background_review=True,
                        session_id='gui-fixture', platform='desktop', memory_host_context=local)
        agents.append(agent)
        assert agent._memory_manager.get_provider('personal-memory').access_allowed
        assert 'personal_memory_search' in agent.valid_tool_names
        checks.append('actual AIAgent threads typed GUI identity through agent_init to copied provider')
        assert agent._memory_store is not None and agent._memory_store.framework_backed
        frozen_before = agent._memory_store.format_for_system_prompt('memory')
        from tools.memory_tool import memory_tool
        native_write = json.loads(memory_tool(
            action='add', target='memory', content='Fictional native store marker amber-cedar-204.',
            store=agent._memory_store))
        assert native_write['success'] and native_write['done']
        curated_state = case.client.call('/v1/curated/read')
        assert any('amber-cedar-204' in entry['text']
                   for entry in curated_state['stores']['memory']['entries'])
        assert agent._memory_store.format_for_system_prompt('memory') == frozen_before
        assert any('amber-cedar-204' in entry for entry in agent._memory_store.memory_entries)
        checks.append('actual native memory handler writes canonical state while its prompt snapshot remains frozen')

        from agent.memory_bridge import native_scope
        read_scope=native_scope(home,True,p);read_scope.__enter__()
        native.create_session('history', 'cli')
        mid = native.append_message('history', 'user', 'Fictional secret maple-quartz-748.')
        native.append_message('history', 'assistant', 'An untracked echo maple-quartz-748.')
        native.append_message('history', 'user', 'Summary containing maple-quartz-748.', _compressed_summary=True)
        independent_mid = native.append_message('history', 'user', 'Independent user note cedar-harbour-596.')
        connector = HermesHistoryConnector(home / 'state.db', 'live-host', ['history'])
        sync_history(case.client, connector, home / 'personal-memory/outbox.db')
        hit = case.client.call('/v1/search', {'query':'maple-quartz-748', 'source':'hermes-history'})['episodes'][0]
        assert native.get_messages('history')
        case.client.call('/v1/forget-source', {'source':hit['source'], 'source_id':hit['source_id']})
        assert 'maple-quartz-748' not in json.dumps(native.get_messages('history'))
        assert 'maple-quartz-748' not in json.dumps(native.get_messages_as_conversation('history'))
        assert 'maple-quartz-748' not in json.dumps(native.get_resume_conversations('history')[0])
        assert 'maple-quartz-748' not in json.dumps(native.get_messages_around('history', mid)['window'])
        assert not native.search_messages('maple-quartz-748')
        assert 'cedar-harbour-596' in json.dumps(native.get_messages('history'))
        assert 'maple-quartz-748' not in json.dumps(native.search_messages('cedar-harbour-596'))
        # A new native exact copy must also be withheld before another sync.
        with sqlite3.connect(home/'state.db') as db:
            db.execute('INSERT INTO messages(session_id,role,content,timestamp,active,compacted,_compressed_summary) SELECT session_id,role,content,timestamp,1,0,0 FROM messages WHERE id=?',(mid,))
        assert 'maple-quartz-748' not in json.dumps(native.get_messages('history'))
        checks.append('actual native search, context windows and both resume paths suppress forgotten sources, summaries, unknown echoes and unsynced exact copies')
        original_settings = settings_file.read_text()
        unavailable = json.loads(original_settings); unavailable['url'] = 'http://127.0.0.1:1'
        settings_file.write_text(json.dumps(unavailable))
        try:
            try: native.get_messages('history')
            except Exception: pass
            else: raise AssertionError('Native history silently bypassed an unavailable memory service')
        finally: settings_file.write_text(original_settings)
        checks.append('native read guard fails closed on unavailable service and preserves independent user notes')

        fixture = fixtures.wire_record()
        fixture.update(source_id='delegation-source', text='Fictional bicycle stands in the cedar shelter.')
        rid = case.client.call('/v1/ingest', {'items':[fixture]})['records'][0]['id']
        manager = MemoryManager(); manager.add_provider(p)
        from types import SimpleNamespace
        parent = SimpleNamespace(_memory_manager=manager)
        packet = delegation_packet(parent, 'Find the bicycle shelter')
        assert 'cedar shelter' in packet and rid in packet
        assert rid in p.lineage.parents(p.session_id)
        checks.append('parent delegation packet contains scoped live evidence IDs and records exposure without child memory credentials')

        review = [{'role':'user','content':'harnesscanaryxy72953'},
                  {'role':'assistant','tool_calls':[{'id':'review-1','function':{'name':'skill_manage','arguments':json.dumps({'name':'fictional-skill','action':'patch'})}}]},
                  {'role':'tool','tool_call_id':'review-1','content':json.dumps({'success':True,'message':'Patched fictional-skill'})}]
        review_completed(parent, review, [])
        p.outbox.flush()
        events = case.client.call('/v1/search', {'query':'fictional-skill','source':'hermes-host-events'})['episodes']
        assert events
        assert not case.client.call('/v1/search', {'query':'harnesscanaryxy72953'})['episodes']
        with sqlite3.connect(Path(case.tmp.name)/'data/memory.db') as db:
            row = db.execute("SELECT payload,state FROM learning_objects WHERE kind='outcome' AND json_extract(payload,'$.action')='review_change'").fetchone()
            assert row and json.loads(row[0])['outcome']=='unknown'
        review_completed(parent, review, review)
        p.outbox.flush()
        checks.append('review change enters reported-outcome learning without harness prompts, replay duplicates or automatic promotion')

        targets = (('telegram','owner-chat',''),)
        cron = MemoryHostContext(str(home.resolve()), 'cron', job_id='job-a',run_id='run-a',targets=targets)
        from dataclasses import replace
        assert provider('cron', replace(cron,run_id='denied-initial')).access_allowed is False
        values['session_access']={'owners':{}, 'cron_recipients':[{'platform':'telegram','chat_id':'owner-chat','thread_id':'','chat_type':'private'}]}
        public.write_text(json.dumps(values))
        cp = provider('cron', cron)
        assert cp.access_allowed
        cp.register_native_artifact('job-a','scope-only-fixture','delivery')
        check_cron_delivery(home, {'id':'job-a','_memory_run_id':'run-a'}, [{'platform':'telegram','chat_id':'owner-chat'}], 'scope-only-fixture')
        try: check_cron_delivery(home, {'id':'job-a','_memory_run_id':'run-a'}, [{'platform':'telegram','chat_id':'other-chat'}])
        except PermissionError: pass
        else: raise AssertionError('Changed recipient was allowed')
        cp.on_host_event('cron_completed', {'job_id':'job-a','run_id':'run-a','success':True,'result':'Fictional scheduled observation cedar-clock-657'})
        cp.outbox.flush()
        assert case.client.call('/v1/search', {'query':'cedar-clock-657','source':'hermes-host-events'})['episodes']
        checks.append('cron recipient allowlist, changed-destination denial and durable run outcome')
        from personal_memory.host_bridge import check_delivery, continuity
        other=(('telegram','other-chat',''),)
        assert provider('cron',replace(cron,run_id='run-denied',targets=other)).access_allowed is False
        assert check_delivery(home,'job-a',targets,'run-a','scope-only-fixture')
        assert not check_delivery(home,'job-a',targets,'run-a')
        assert not check_delivery(home,'job-a',other,'run-a')
        assert not check_delivery(home,'job-a',other,'run-denied')
        assert not check_delivery(home,'job-a',targets,'unknown-run')
        try: provider('cron',replace(cron,targets=other))
        except ValueError: pass
        else: raise AssertionError('Run scope was mutable')
        checks.append('immutable run-specific delivery scope survives later denied runs; missing scope denied')
        artifact='Fictional continuity artifact cedarbridge92573'
        receipt=cp.register_native_artifact('job-a',artifact,'output')
        next_run=replace(cron,run_id='next-run')
        assert continuity(home,next_run,artifact,'output') == artifact
        assert continuity(home,next_run,'Untracked private legacy output','output') == ''
        assert continuity(home,replace(next_run,run_id='other-run',targets=other),artifact,'output') == ''
        assert continuity(home,replace(next_run,job_id='job-b',run_id='cross-job'),artifact,'output','job-a')==artifact
        delivery=cp.register_native_artifact('job-a','Fictional outgoing proof','delivery')
        assert check_delivery(home,'job-a',targets,'run-a','Fictional outgoing proof')
        assert not check_delivery(home,'job-a',targets,'run-a','Changed outgoing text')
        assert not check_delivery(home,'job-a',targets,'next-run','Fictional outgoing proof')
        case.client.call('/v1/forget',{'record_id':delivery['record_id']})
        assert not check_delivery(home,'job-a',targets,'run-a','Fictional outgoing proof')
        long_output='Long fictional artifact. '*6000
        long_receipt=cp.register_native_artifact('job-a',long_output,'output')
        assert continuity(home,next_run,long_output.strip(),'output')==long_output.strip()
        checks.append('delivery binds exact text to live canonical evidence and run; cross-job and long-output continuity validated')
        case.client.call('/v1/forget',{'record_id':receipt['record_id']})
        assert continuity(home,next_run,artifact,'output') == ''
        checks.append('canonical continuity requires exact recipient scope and live evidence; legacy output withheld')
        for selected_home,allowed in [(home,False),(home/'other',True)]:
            with native_scope(selected_home,allowed):
                try: native.get_messages('history')
                except PermissionError: pass
                else: raise AssertionError('Unauthorized native history read')
        checks.append('native history denied for unauthorized and cross-profile caller scopes')
        native.set_session_title('history','Fictional private title canary')
        meta=native.get_session('history')
        assert meta['title'] is None and meta['system_prompt'] is None
        assert native.get_session_title('history') is None
        checks.append('native generated session title and cached prompt are redacted without field-level provenance')
        from agent.memory_bridge import authorize_memory_delivery
        with native_scope(home,True,cp):
            authorize_memory_delivery('telegram','owner-chat','','Fictional tool-delivery proof',[])
            for target,media in [('other-chat',[]),('owner-chat',['untracked.png'])]:
                try:authorize_memory_delivery('telegram',target,'','fictional',media)
                except PermissionError:pass
                else:raise AssertionError('Cron tool delivery bypassed scope')
        checks.append('tool-driven cron delivery validates recipients and rejects untracked media without any real send')

        from cron import notepad
        with native_scope(home,True,cp):
            notepad.set_note('job-a','fixture','notepadcanary75932')
            assert notepad.get_note('job-a','fixture')=='notepadcanary75932'
        future_job={'id':'job-a','_memory_run_id':'notepad-future','deliver':'telegram:owner-chat'}
        assert 'notepadcanary75932' in notepad.render_notepad_section('job-a',memory_job=future_job)
        note_records=case.client.call('/v1/search',{'query':'notepadcanary75932','source':'hermes-artifact'})['episodes']
        assert note_records
        case.client.call('/v1/forget',{'record_id':note_records[0]['id']})
        with native_scope(home,True,cp):
            assert notepad.get_note('job-a','fixture') is None
            try: notepad.set_note('job-a','other','new value')
            except PermissionError: pass
            else: raise AssertionError('Forgotten notes were relabelled as fresh evidence')
        notepad.clear_notepad('job-a')
        with native_scope(home,True,cp):
            notepad.set_note('job-a','new','replacement note')
            assert notepad.delete_note('job-a','new')
            assert notepad.list_notes('job-a')==[]
        notepad.clear_notepad('job-a')
        checks.append('native notepad write/read/continuity carries scoped canonical lineage and suppresses forgotten notes')
        from agent.memory_bridge import native_recall
        from tools.session_search_tool import session_search
        native.create_session('federation','cli')
        native.append_message('federation','user','Fictional federationcanary62841.')
        with native_scope(home,False):
            try: session_search(db=native)
            except PermissionError: pass
            else: raise AssertionError('Metadata-only native browse bypassed authorization')
        result=native_recall(lambda:session_search(query='federationcanary62841',db=native))
        proof=json.loads(result)['_memory_read']
        assert proof['tracked'] and proof['record_ids']
        p.sync_turn('User asks about the fixture','federationcanary62841',messages=[
            {'role':'assistant','tool_calls':[{'id':'native-recall','function':{'name':'session_search','arguments':'{}'}}]},
            {'role':'tool','tool_call_id':'native-recall','content':result}])
        p.outbox.flush()
        assert p.capture_warning is None
        case.client.call('/v1/forget',{'record_id':proof['record_ids'][0]})
        assert not case.client.call('/v1/search',{'query':'federationcanary62841','source':'hermes'})['episodes']
        checks.append('native metadata browse enforces identity; canonical native recall supports source-dependent answer capture')
        from personal_memory.skill_export import export_skill
        from personal_memory.learning import Learning
        from test_ingestion import item
        learning=Learning(case.server.store)
        ev1=case.client.call('/v1/ingest',{'items':[item('skill-source')]})['records'][0]['id']
        ev2=case.client.call('/v1/ingest',{'items':[item('skill-eval')]})['records'][0]['id']
        outcome=learning.outcome(key='host-skill',goal='Check fictional bicycle',action='Check brakes',result='Fixture passed',outcome='success',evidence_ids=[ev1],actor='agent')
        lesson=learning.propose(key='host-lesson',family='host-bicycle',revision=1,category='procedural',lesson='Check bicycle brakes before riding',scope='Fictional bicycle',prerequisites=[],exceptions=[],outcome_ids=[outcome['id']],evidence_ids=[],actor='agent')
        evaluation=learning.evaluate(key='host-eval',candidate_id=lesson['id'],cases=[{'id':k,'kind':k,'baseline_pass':True,'candidate_pass':True} for k in ['target','regression','non_applicable']],evidence_ids=[ev2],actor='evaluator')
        learning.promote(candidate_id=lesson['id'],evaluation_id=evaluation['id'],expected_active_id=None,actor='admin')
        export_skill(home,case.client,lesson['id'],'memory-bicycle')
        from tools.skills_tool import skill_view
        loaded=json.loads(skill_view('memory-bicycle',preprocess=False));assert loaded['success'],loaded
        learning.retract(candidate_id=lesson['id'],actor='admin')
        revoked=json.loads(skill_view('memory-bicycle',preprocess=False));assert not revoked['success'],revoked
        checks.append('actual native skill loader accepts active evaluated export and rejects it after retraction')
        cp.lineage.block(cp.session_id,'fictional untracked recall')
        ack=cp.on_host_event('cron_completed',{'job_id':'job-a','run_id':'blocked','result':'fictional'})
        assert ack['state']=='withheld'
        from agent.memory_bridge import emit
        with __import__('contextlib').nullcontext():
            try: emit(SimpleNamespace(_memory_manager=SimpleNamespace(providers=[cp])), 'cron_completed', {'result':'fictional'})
            except RuntimeError: pass
            else: raise AssertionError('Withheld host event silently acknowledged')
        checks.append('withheld host events have durable typed receipts and propagate to the native caller')

        from personal_memory.host_bridge import authorize_native, local_process_caller
        # The interactive CLI never binds a transport, so an unscoped native read there is the owner's
        # own session (compression and continuity lookups); denying it disabled compaction entirely.
        assert local_process_caller() and authorize_native(home, None) is True
        # The same missing identity while a transport IS bound is a remote caller and must stay denied.
        with patch('tui_gateway.transport.current_transport', return_value=object()):
            assert not local_process_caller() and authorize_native(home, None) is False
        checks.append('unscoped local-process native read is authorized as the owner while a bound transport with no caller identity stays denied')

    finally:
        for a in agents: a.close()
        for p in providers: p.shutdown()
        if 'read_scope' in locals(): read_scope.__exit__(None,None,None)
        native.close()
        case.doCleanups()
report = {'passed':True,'checks':checks,'actual_AIAgent':True,'actual_SessionDB':True,
          'live_model':False,'full_desktop_ui':False,'real_cron_delivery':False}
Path('docs/HOST_BRIDGE_RUNTIME.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
