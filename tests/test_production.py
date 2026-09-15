import copy
import hashlib
import importlib.util
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from personal_memory.client import Client,ServiceError
from personal_memory.outbox import Outbox
from personal_memory.service import MemoryService,AccessDenied
from personal_memory.store import Store
from personal_memory.recovery import backup,restore,create_key,inspect_database
from test_ingestion import item


class ProductionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)

    def service(self,**kwargs):
        service=MemoryService(self.root/'data','a'*40,**kwargs)
        self.addCleanup(service.close);return service

    def test_reader_and_ingest_credentials_have_distinct_scopes(self):
        service=self.service(principals=[{'token':'r'*40,'role':'reader'},
            {'token':'i'*40,'role':'ingest','sources':['custom-notes'],'connector_id':'example.notes'}])
        reader=service.authenticate('Bearer '+'r'*40);ingester=service.authenticate('Bearer '+'i'*40)
        with self.assertRaises(AccessDenied):service.dispatch('/v1/ingest',{'items':[item()]},reader)
        self.assertTrue(service.dispatch('/v1/ingest',{'items':[item()]},ingester)['records'])
        with self.assertRaises(AccessDenied):service.dispatch('/v1/search',{'query':'bicycle'},ingester)
        wrong=item('wrong');wrong['source']='another-source'
        with self.assertRaises(AccessDenied):service.dispatch('/v1/ingest',{'items':[wrong]},ingester)
        self.assertEqual(service.store.status()['records'],1)

    def test_checkpoint_atomicity_compare_and_swap_and_replay(self):
        store=Store(self.root/'memory.db')
        cp={'connector_id':'example.notes','source':'custom-notes','expected_cursor':None,'cursor':{'offset':1}}
        first=store.ingest_contract([item()],cp)
        self.assertTrue(first['checkpoint_committed'])
        self.assertTrue(store.ingest_contract([item()],cp)['records'][0]['duplicate'])
        with self.assertRaises(ValueError):store.ingest_contract([item('other')],cp)
        self.assertEqual(store.status()['records'],1)
        next_cp={**cp,'expected_cursor':{'offset':1},'cursor':{'offset':2}}
        bad=item('bad');bad['provenance'].update(origin='derived',parent_record_ids=['missing'])
        with self.assertRaises(ValueError):store.ingest_contract([bad],next_cp)
        self.assertEqual(store.checkpoint('example.notes','custom-notes')['cursor'],{'offset':1})

    def test_exact_quotes_required_for_reported_claims(self):
        store=Store(self.root/'memory.db');rid=store.ingest_contract([item()])['records'][0]['id']
        with self.assertRaises(ValueError):store.claim('The bicycle needs repair',rid,evidence_kind='reported')
        with self.assertRaises(ValueError):store.claim('The bicycle needs repair',rid,evidence_kind='reported',evidence_quote='invented quote')
        claim=store.claim('The note mentions repair',rid,evidence_kind='reported',evidence_quote='bicycle repair')
        self.assertEqual(claim['evidence_quote'],'bicycle repair')
        store.forget(rid)
        with store.connect() as db:self.assertIsNone(db.execute('SELECT evidence_quote FROM claims WHERE id=?',(claim['id'],)).fetchone()[0])

    def test_poison_capture_is_quarantined_without_blocking_following_records(self):
        received=[]
        class Endpoint:
            def call(self,path,payload):
                sid=payload['items'][0]['source_id']
                if sid=='bad':raise ServiceError(422)
                received.append(sid);return {'records':[{'id':sid}]}
        box=Outbox(self.root/'outbox.db',Endpoint());self.addCleanup(box.close)
        box.enqueue([item('bad')]);box.enqueue([item('good')]);box.flush()
        self.assertIn('good',received);self.assertEqual(box.health()['dead_letters'],1)
        self.assertEqual(box.pending(),0)

    def test_outbox_rejects_same_identity_with_changed_content(self):
        class Offline:
            def call(self,*args):raise ConnectionError()
        box=Outbox(self.root/'outbox.db',Offline());self.addCleanup(box.close)
        box.enqueue([item()]);changed=item();changed['text']='Different source content'
        with self.assertRaises(ValueError):box.enqueue([changed])
        changed=item();changed['occurred_at']='2025-01-01T00:00:00Z'
        with self.assertRaises(ValueError):box.enqueue([changed])

    def test_outbox_can_flush_only_the_current_capture(self):
        from unittest.mock import patch
        received=[]
        class Endpoint:
            def call(self,path,payload):received.extend(i['source_id'] for i in payload['items']);return {'records':[]}
        with patch('personal_memory.outbox.threading.Thread.start'):
            box=Outbox(self.root/'outbox.db',Endpoint())
        first=box.enqueue([item('old')]);second=box.enqueue([item('current')])
        box.flush(keys=[second])
        self.assertEqual(received,['current']);self.assertEqual(box.pending(),1)
        box.flush();self.assertEqual(received,['current','old'])

    @unittest.skipUnless(importlib.util.find_spec('jsonschema'),'jsonschema production extra required')
    def test_registered_extension_schema_is_enforced_at_server_boundary(self):
        from personal_memory.ingestion import ContractError
        service=self.service(extension_schemas={'example.notes':{'1.0':{
            'type':'object','properties':{'folder':{'const':'repairs'}},'required':['folder']}}})
        admin=service.authenticate('Bearer '+'a'*40)
        bad=item();bad['extensions']['example.notes']['data']['folder']='wrong'
        with self.assertRaises(ContractError):service.dispatch('/v1/ingest',{'items':[bad]},admin)
        self.assertEqual(service.store.status()['records'],0)

    @unittest.skipUnless(importlib.util.find_spec('cryptography'),'cryptography production extra required')
    def test_backup_restore_integrity_tampering_and_no_overwrite(self):
        home=self.root/'home';state=home/'personal-memory';state.mkdir(parents=True)
        data=state/'data';store=Store(data/'memory.db')
        rid=store.ingest_contract([item()])['records'][0]['id']
        (state/'settings.json').write_text(json.dumps({'data_dir':str(data)}))
        key=self.root/'backup.key';create_key(key)
        target=self.root/'backup.enc';backup(home,target,key)
        destination=self.root/'restored';result=restore(target,key,destination)
        self.assertTrue(result['verified']);inspect_database(destination/'memory.db')
        self.assertEqual(Store(destination/'memory.db').evidence(rid)['text'],item()['text'])
        with self.assertRaises(ValueError):restore(target,key,destination)
        damaged=bytearray(target.read_bytes());damaged[len(damaged)//2]^=1
        corrupted=self.root/'corrupt.enc';corrupted.write_bytes(damaged)
        with self.assertRaises(Exception):restore(corrupted,key,self.root/'bad-restore')
        self.assertFalse((self.root/'bad-restore').exists())

    def test_only_one_production_process_can_own_data_directory(self):
        from personal_memory.asgi import ProcessLease
        first=ProcessLease(self.root/'service.lock')
        try:
            with self.assertRaises(RuntimeError):ProcessLease(self.root/'service.lock')
        finally:first.close()
        second=ProcessLease(self.root/'service.lock');second.close()


@unittest.skipUnless(importlib.util.find_spec('uvicorn'),'uvicorn production extra required')
class ASGIIntegrationTests(unittest.TestCase):
    def test_real_uvicorn_auth_ingestion_retrieval_and_shutdown(self):
        import uvicorn
        from personal_memory.asgi import Application
        with tempfile.TemporaryDirectory() as tmp:
            app=Application({'data_dir':tmp,'token':'a'*40,'principals':[{'token':'g'*40,'role':'agent'}]})
            server=uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=0,log_level='error',access_log=False))
            thread=threading.Thread(target=server.run,daemon=True);thread.start()
            try:
                deadline=time.monotonic()+5
                while not server.started and time.monotonic()<deadline:time.sleep(.02)
                self.assertTrue(server.started)
                port=server.servers[0].sockets[0].getsockname()[1]
                client=Client(f'http://127.0.0.1:{port}','g'*40)
                rid=client.call('/v1/ingest',{'items':[item()]})['records'][0]['id']
                self.assertEqual(client.call('/v1/search',{'query':'bicycle'})['episodes'][0]['id'],rid)
                with self.assertRaises(ServiceError) as error:client.call('/v1/forget',{'record_id':rid})
                self.assertEqual(error.exception.status,403)
                self.assertTrue(client.call('/v1/ready')['ready'])
            finally:
                server.should_exit=True;thread.join(timeout=10)
            self.assertFalse(thread.is_alive())



class IndexFailureTests(unittest.TestCase):
    def test_hindsight_retains_records_in_one_batch_and_skips_lineage_nodes(self):
        from personal_memory.hindsight import Hindsight
        from test_retrieval import record
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db')
            store.ingest([record('first'),record('second'),record('lineage',source='hermes-lineage')])
            engine=Hindsight(store,{'url':'http://127.0.0.1:8767','bank_id':'test','sources':['*']})
            requests=[]
            class Remote:
                def call(self,path,payload=None,**kwargs):
                    requests.append(payload);return {'success':True,'async':False,'items_count':len(payload['items'])}
            engine.write_client=Remote()
            self.assertEqual(engine.sync(batch=8),2)
            self.assertEqual(len(requests),1)
            self.assertEqual(len(requests[0]['items']),2)
            self.assertEqual(engine.status()['pending_records'],0)

    def test_hindsight_isolates_contract_failure_but_backs_off_service_outage(self):
        from personal_memory.hindsight import Hindsight
        from test_retrieval import record
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db');ids=store.ingest([record('bad'),record('good')])['records']
            engine=Hindsight(store,{'url':'http://127.0.0.1:8767','bank_id':'test','sources':['*']})
            calls=[]
            class ContractFailure:
                def call(self,path,payload=None,**kwargs):
                    calls.append(len(payload['items']))
                    if len(payload['items'])>1 or payload['items'][0]['document_id']==ids[0]['id']:
                        raise ServiceError(422)
                    return {'success':True,'async':False}
            engine.write_client=ContractFailure();engine.sync(batch=8)
            self.assertEqual(calls,[2,1,1])
            self.assertEqual(engine.status()['synced_records'],1)
            self.assertEqual(engine.status()['pending_records'],1)

            other=Store(Path(tmp)/'other.db');other.ingest([record('one'),record('two')])
            outage=Hindsight(other,{'url':'http://127.0.0.1:8767','bank_id':'other','sources':['*']})
            outage_calls=[]
            class Outage:
                def call(self,path,payload=None,**kwargs):
                    outage_calls.append(len(payload['items']));raise ServiceError(503)
            outage.write_client=Outage();outage.sync(batch=8)
            self.assertEqual(outage_calls,[2])
            self.assertEqual(outage.status()['pending_records'],2)

    def test_bad_dimension_does_not_poison_restart_or_later_records(self):
        from personal_memory.semantic import SemanticIndex
        from test_retrieval import DeterministicEmbedder,record
        class Changing(DeterministicEmbedder):
            def documents(self,texts):
                return [[1.,0.] if t=='bad' else [1.,0.,0.] for t in texts]
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db')
            store.ingest([record('first','garage')]);engine=SemanticIndex(store,embedder=Changing());engine.sync()
            ids=store.ingest([record('bad','bad'),record('later','garage')])['records']
            engine.sync();self.assertEqual(engine.status()['failed_records'],1)
            reopened=SemanticIndex(store,embedder=Changing())
            self.assertIn(ids[1]['id'],[r['id'] for r in reopened.candidates('garage',10)])
            with store.connect() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM vector_chunks WHERE record_id=?',(ids[0]['id'],)).fetchone()[0],0)

    def test_lost_remote_ack_is_retried_and_forgotten(self):
        from personal_memory.hindsight import Hindsight
        from test_retrieval import record
        with tempfile.TemporaryDirectory() as tmp:
            store=Store(Path(tmp)/'memory.db');ids=store.ingest([record('bad'),record('good')])['records']
            engine=Hindsight(store,{'url':'http://127.0.0.1:8767','bank_id':'test','sources':['whatsapp']})
            retained=set();deleted=[]
            class Remote:
                def call(self,path,payload=None,**kwargs):
                    if kwargs.get('method')=='DELETE':deleted.append(path.rsplit('/',1)[1]);return {}
                    rid=payload['items'][0]['document_id'];retained.add(rid)
                    if rid==ids[0]['id']:raise TimeoutError('lost acknowledgement')
                    return {'success':True,'async':False}
            engine.write_client=Remote();engine.sync()
            self.assertEqual(engine.status()['pending_records'],1)
            self.assertEqual(engine.status()['synced_records'],1)
            store.forget(ids[0]['id'])
            self.assertEqual(engine.status()['pending_deletions'],1)
            engine.sync();self.assertIn(ids[0]['id'],deleted)
            self.assertEqual(engine.status()['pending_deletions'],0)



class SessionAccessTests(unittest.TestCase):
    def test_remote_requires_exact_owner_and_private_chat(self):
        from personal_memory.access import session_allowed
        policy={'owners':{'telegram':['owner-1']}}
        self.assertTrue(session_allowed(policy,{'platform':'telegram','user_id':'owner-1','chat_type':'private'}))
        for context in [
            {'platform':'telegram','user_id':'owner-1','chat_type':'group'},
            {'platform':'telegram','user_id':'stranger','chat_type':'private'},
            {'platform':'telegram','user_id':'owner-1'},
            {'platform':'api','user_id':'owner-1','chat_type':'private'}]:
            self.assertFalse(session_allowed(policy,context))
        with self.assertRaises(ValueError):session_allowed({'owners':{'telegram':'owner-1'}},{})

class DeletionRecoveryTests(unittest.TestCase):
    def test_durable_intent_replays_after_canonical_transaction_failure(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'memory.db';store=Store(path)
            rid=store.ingest_contract([item()])['records'][0]['id']
            with patch.object(store,'audit',side_effect=OSError('synthetic commit interruption')):
                with self.assertRaises(OSError):store.forget(rid)
            self.assertTrue(store.deletions.contains(rid))
            reopened=Store(path)
            with self.assertRaises(ValueError):reopened.evidence(rid)
            with self.assertRaises(ValueError):reopened.ingest_contract([item()])

    def test_older_backup_reconciles_later_deletions_and_blocks_reimport(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);home=root/'home';state=home/'personal-memory';state.mkdir(parents=True)
            data=state/'data';store=Store(data/'memory.db')
            rid=store.ingest_contract([item()])['records'][0]['id']
            (state/'settings.json').write_text(json.dumps({'data_dir':str(data)}))
            key=root/'key';create_key(key);archive=root/'before-delete.enc';backup(home,archive,key)
            store.forget(rid)
            late=store.ingest_contract([item('late')])['records'][0]['id'];store.forget(late)
            destination=root/'restore';r=restore(archive,key,destination,data/'memory.deletions.db')
            self.assertTrue(r['current_deletion_ledger_applied'])
            recovered=Store(destination/'memory.db')
            with self.assertRaises(ValueError):recovered.evidence(rid)
            with self.assertRaises(ValueError):recovered.ingest_contract([item()])
            with self.assertRaises(ValueError):recovered.ingest_contract([item('late')])

class HostPatchBundleTests(unittest.TestCase):
    """The patch, its manifest and the packaged contract are a single pin and must move together."""

    project=Path(__file__).resolve().parents[1]

    def manifest(self):
        return json.loads((self.project/'host-patch'/'manifest.json').read_text(encoding='utf-8'))

    def test_pinned_artifacts_hash_to_each_other(self):
        manifest=self.manifest()
        patch=self.project/'host-patch'/('hermes-'+manifest['release']+'.patch')
        self.assertTrue(patch.is_file(),'the manifest names a release whose patch is not in the bundle')
        self.assertEqual(hashlib.sha256(patch.read_bytes()).hexdigest(),manifest['patch_sha256'])
        contract=json.loads((self.project/'personal_memory'/'host_contract.json').read_text(encoding='utf-8'))
        self.assertEqual(contract,manifest,'doctor verifies a contract that the bundle no longer matches')

    def test_attestation_hunk_is_self_contained(self):
        # The hunk that supplies the host attestation once called Path() without importing pathlib. It sits
        # inside `with suppress(Exception)`, so the NameError was swallowed, the provider never recorded the
        # runtime it runs under and doctor could not certify any installation. Without an upstream checkout
        # a hunk cannot be re-verified against its file, so its own added lines are checked here.
        lines=(self.project/'host-patch'/('hermes-'+self.manifest()['release']+'.patch')).read_text(encoding='utf-8').splitlines()
        hunk=None;run=[]
        for line in lines+[None]:
            if line is not None and line.startswith('+') and not line.startswith('+++'):
                run.append(line[1:]);continue
            if hunk is None and any('host_memory_root' in added for added in run): hunk=run
            run=[]
        self.assertIsNotNone(hunk,'the pinned patch no longer passes the host attestation kwargs')
        joined='\n'.join(hunk)
        self.assertIn('host_memory_api',joined)
        if 'Path(' in joined:self.assertIn('from pathlib import Path',joined,'the hunk must bind every name it uses')

class HostEntryPointTests(unittest.TestCase):
    """`hermes doctor` probes only <root>/venv and <root>/.venv for the command, so an agent installed
    into a venv outside its clone reports a missing entry point and asks for a second installation."""

    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('manage_hermes_host_patch',
            Path(__file__).resolve().parents[1]/'scripts'/'manage_hermes_host_patch.py')
        cls.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.module)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)/'hermes-agent';self.root.mkdir()
        self.venv=Path(self.tmp.name)/'venvs'/'hermes';(self.venv/'bin').mkdir(parents=True)
        (self.venv/'bin'/'hermes').write_text('#!/bin/sh\n',encoding='utf-8')

    def require_symlink(self):
        # Windows without developer mode cannot create symlinks; the behaviour under test is POSIX-only.
        probe=self.root/'probe'
        try:
            probe.symlink_to(self.venv,target_is_directory=True)
        except OSError as exc:
            self.skipTest('platform cannot create directory symlinks: %s'%exc)
        probe.unlink()

    def test_existing_entry_point_is_left_alone(self):
        (self.root/'venv'/'bin').mkdir(parents=True)
        shipped=self.root/'venv'/'bin'/'hermes';shipped.write_text('#!/bin/sh\n',encoding='utf-8')
        result=self.module.ensure_entry_point(self.root,self.venv)
        self.assertEqual(result['state'],'present')
        self.assertEqual(Path(result['entry_point']),shipped)
        self.assertFalse((self.root/'.venv').exists(),'an install.sh layout must not gain a competing link')

    def test_links_an_external_venv_and_repeats_harmlessly(self):
        self.require_symlink()
        self.assertIsNone(self.module.find_entry_point(self.root),'doctor would warn about this layout')
        created=self.module.ensure_entry_point(self.root,self.venv)
        self.assertEqual(created['state'],'created')
        self.assertEqual(Path(created['entry_point']),self.root/'.venv'/'bin'/'hermes',
            'only .venv is gitignored by Hermes; linking to venv would dirty the patched tree')
        again=self.module.ensure_entry_point(self.root,self.venv)
        self.assertEqual(again['state'],'linked','a second run must report the link, never rewrite it')
        self.assertEqual({k:v for k,v in again.items() if k!='state'},{k:v for k,v in created.items() if k!='state'},
            'only the state may differ between runs')
        self.assertTrue((self.root/'.venv'/'bin'/'hermes').is_file())

    def test_refuses_a_bad_venv_even_when_an_entry_point_exists(self):
        # A wrong --agent-venv must never be masked by an entry point that happens to be there; that
        # is how a typo gets baked into a deployment.
        (self.root/'venv'/'bin').mkdir(parents=True)
        (self.root/'venv'/'bin'/'hermes').write_text('#!/bin/sh\n',encoding='utf-8')
        (self.venv/'bin'/'hermes').unlink()
        with self.assertRaises(SystemExit):
            self.module.ensure_entry_point(self.root,self.venv)

    def test_refuses_to_replace_anything(self):
        self.require_symlink()
        (self.root/'.venv').mkdir()
        with self.assertRaises(SystemExit):
            self.module.ensure_entry_point(self.root,self.venv)
        (self.root/'.venv').rmdir()
        other=Path(self.tmp.name)/'venvs'/'other';(other/'bin').mkdir(parents=True)
        (other/'bin'/'hermes').write_text('#!/bin/sh\n',encoding='utf-8')
        (self.root/'.venv').symlink_to(other,target_is_directory=True)
        # The misplaced link does satisfy doctor, so refusing to move it is the only way the operator
        # learns the venv they named is not the one in use.
        with self.assertRaises(SystemExit):
            self.module.ensure_entry_point(self.root,self.venv)
        self.assertEqual(Path(self.root/'.venv').resolve(),other)

    def test_refuses_a_venv_without_the_command(self):
        (self.venv/'bin'/'hermes').unlink()
        with self.assertRaises(SystemExit):
            self.module.ensure_entry_point(self.root,self.venv)
        self.assertFalse((self.root/'.venv').exists())


if __name__=="__main__":unittest.main()
