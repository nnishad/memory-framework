import base64
import copy
import json
import tempfile
import unittest
from pathlib import Path

from personal_memory.gmail import GmailAdapter
from personal_memory.source_sdk import AdapterError, read_state, source_operation, source_page, validate_page
from personal_memory.source_runtime import SourceRuntime
from personal_memory.store import Store
from personal_memory.service import MemoryService, AccessDenied
from tests.test_source_sync import FixtureAdapter, note_record
from personal_memory.source_sync import SourceSync, SyncWorker


def message(mid, text='The project deadline is Monday.', version='100', labels=None):
    return {'id':mid,'threadId':'thread-1','historyId':version,'internalDate':'1700000000000',
            'labelIds':labels or ['INBOX'], 'payload':{'mimeType':'text/plain','headers':[
                {'name':'From','value':'Ada <ada@example.com>'}, {'name':'Subject','value':'Project deadline'}],
                'body':{'data':base64.urlsafe_b64encode(text.encode()).decode()}}}


class Mailbox:
    def __init__(self,count=23):
        self.messages={f'm{i}':message(f'm{i}') for i in range(count)}
        self.history=[];self.version=100;self.calls=[];self.expired=False
    def __call__(self,context,path,params):
        self.calls.append((path,params))
        if path=='profile':return {'emailAddress':'owner@example.com','historyId':str(self.version),'messagesTotal':len(self.messages)}
        if path=='messages':
            offset=int(params.get('pageToken',0));ids=sorted(self.messages)
            return {'messages':[{'id':i} for i in ids[offset:offset+20]],
                    **({'nextPageToken':str(offset+20)} if offset+20<len(ids) else {})}
        if path=='history':
            if self.expired:
                self.expired=False;raise AdapterError('cursor','History expired')
            return {'history':[h for h in self.history if int(h['id'])>int(params['startHistoryId'])], 'historyId':str(self.version)}
        if '/attachments/' in path:return {'data':base64.urlsafe_b64encode(b'attachment text').decode()}
        mid=path.split('/')[1]
        if mid not in self.messages:
            error=AdapterError('permanent','Not found');error.status=404;raise error
        return copy.deepcopy(self.messages[mid])
    def change(self,mid,text='A new message',remove=False):
        self.version+=1
        if remove:self.messages.pop(mid,None)
        else:self.messages[mid]=message(mid,text,str(self.version))
        self.history.append({'id':str(self.version), 'messagesDeleted' if remove else 'messagesAdded':[{'message':{'id':mid}}]})


class GmailTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'memory.db');self.mailbox=Mailbox()
        self.adapter=GmailAdapter(self.mailbox)
        self.runtime=SourceRuntime(self.store,self.tmp.name,{'enabled':False},self.adapter)
        self.addCleanup(self.runtime.close)
        self.connection=self.runtime.connect_gmail(credentials={'client_id':'fake','client_secret':'fake','refresh_token':'fake'})
        self.cid=self.connection['connection_id']
    def force_live(self):self.runtime._schedule(self.cid,'incremental',0)
    def test_history_addition_is_live_and_backfill_is_historical(self):
        from personal_memory import changes
        changes.configure(self.store,{"journal":{"enabled":True}})
        self.runtime.tick()
        self.mailbox.change('fresh-mail','New mail after authorization')
        self.force_live();self.runtime.tick()
        rows=changes.read(self.store,limit=100)['events']
        old=next(row for row in rows if row['source_item_id']=='m0')
        fresh=next(row for row in rows if row['source_item_id']=='fresh-mail')
        self.assertEqual(old['novelty'],'historical')
        self.assertEqual(fresh['novelty'],'live')
        self.assertEqual(fresh['conversation_key'],'thread-1')

    def test_backfill_and_live_share_identity_and_resume(self):
        self.runtime.tick()
        self.mailbox.change('new','A new live email')
        self.force_live();self.runtime.tick()
        status=self.runtime.status(self.cid)['connections'][0]
        self.assertEqual(status['stored_messages'],24)
        self.assertTrue(self.runtime.sync.stream_state(self.cid,'messages',role='backfill')['cursor']['done'])
        restarted=SourceRuntime(Store(Path(self.tmp.name)/'memory.db'),self.tmp.name,{'enabled':False},self.adapter)
        self.addCleanup(restarted.close);restarted.tick()
        self.assertEqual(restarted.status(self.cid)['connections'][0]['stored_messages'],24)
        self.assertEqual(len(self.store.search('live')['episodes']),1)
    def test_empty_poll_then_later_changes_do_not_conflict_receipts(self):
        self.runtime.tick();self.force_live();self.runtime.tick()
        self.mailbox.change('fresh','Fresh fact after an empty poll')
        self.force_live();self.runtime.tick()
        self.assertEqual(len(self.store.search('Fresh')['episodes']),1)
        self.assertFalse(any(r['error'] for r in self.runtime.status(self.cid)['connections'][0]['schedule']))
    def test_large_history_page_resumes_without_large_cursor(self):
        for i in range(75):self.mailbox.change('extra'+str(i))
        context=self.runtime.sync.context(self.runtime.sync.connection(self.cid));state=read_state(mode='incremental')
        seen=[]
        for _ in range(4):
            page=self.adapter.read_page(context,state);validate_page(page)
            seen.extend(o['source_id'] for o in page['operations']);state=page['next_state']
            self.assertLess(len(json.dumps(state)),1024)
        self.assertEqual(len(set(seen)),75)
        self.assertEqual(state['cursor']['history'],str(self.mailbox.version))
    def test_history_boundary_survives_provider_pagination(self):
        calls=[]
        def transport(context,path,params):
            calls.append((path,params))
            if path=='history':
                if not params.get('pageToken'):
                    return {'history':[{'id':'101','messagesDeleted':[{'message':{'id':'old'}}]}],
                            'historyId':'103','nextPageToken':'second'}
                return {'history':[{'id':'102','messagesDeleted':[{'message':{'id':'middle'}}]}],
                        'historyId':'104'}
            raise AssertionError(path)
        adapter=GmailAdapter(transport)
        context=self.runtime.sync.context(self.runtime.sync.connection(self.cid))
        first=adapter.read_page(context,read_state(mode='incremental'))
        self.assertEqual(first['next_state']['cursor']['boundary'],'103')
        second=adapter.read_page(context,first['next_state'])
        self.assertEqual(second['next_state']['cursor']['history'],'103')
        self.assertEqual([o['source_id'] for o in second['operations']],['middle'])
    def test_label_change_reuses_evidence(self):
        first=self.adapter.normalize({'source':'test','message':message('m')})
        second=self.adapter.normalize({'source':'test','message':message('m',version='101',labels=['SENT'])})
        self.assertEqual(first['records'][0]['revision'],second['records'][0]['revision'])
    def test_mail_changes_remove_stale_recall(self):
        self.runtime.tick();self.mailbox.change('m0','The unique deadline is Friday.')
        self.force_live();self.runtime.tick()
        self.assertEqual(len(self.store.search('Friday')['episodes']),1)
        with self.store.connect() as db:
            ids=[r[0] for r in db.execute('SELECT id FROM records WHERE source_id=?',('m0',))]
        visible={r['id'] for r in self.store.search('deadline',limit=100)['episodes']}
        self.assertEqual(len(visible.intersection(ids)),1)
    def test_archive_retains_removed_mail(self):
        self.runtime.tick();self.mailbox.change('m0',remove=True)
        self.force_live();self.runtime.tick()
        head=self.runtime.sync.head(self.connection['source'],'m0')
        self.assertEqual(head['state'],'removed')
        self.assertIn(head['record_id'],{r['id'] for r in self.store.search('deadline',limit=100)['episodes']})
    def test_expired_history_restarts_with_new_anchor(self):
        self.runtime.tick();self.mailbox.expired=True;self.force_live();self.runtime.tick()
        self.assertEqual(self.runtime.sync.connection(self.cid)['scope']['initial_history'],str(self.mailbox.version))
        self.assertTrue(any(c['state']=='gap' for c in self.runtime.sync.coverage(self.cid,'messages')))
    def test_attachments_are_durable_and_retryable(self):
        email=self.mailbox.messages['m0']; email['payload']={'mimeType':'multipart/mixed','headers':[],
            'parts':[{'mimeType':'text/plain','body':{'data':'SGVsbG8='}},
                     {'mimeType':'application/pdf','filename':'file.pdf','partId':'1','body':{'attachmentId':'a','size':15}}]}
        self.runtime.tick()
        with self.store.connect() as db:
            blob=db.execute('SELECT state,size FROM memory_blobs').fetchone()
        self.assertEqual(tuple(blob),('complete',15))
    def test_inline_attachment_is_fetched_without_persisting_base64_in_page(self):
        email=self.mailbox.messages['m0'];email['payload']={'mimeType':'multipart/mixed','headers':[],
            'parts':[{'mimeType':'text/plain','partId':'0','body':{'data':'SGVsbG8='}},
                     {'mimeType':'application/octet-stream','filename':'inline.bin','partId':'1',
                      'body':{'data':base64.urlsafe_b64encode(b'inline bytes').decode(),'size':12}}]}
        context=self.runtime.sync.context(self.runtime.sync.connection(self.cid))
        page=self.adapter.read_page(context,read_state(mode='backfill'))
        attachment=page['operations'][0]['attachments'][0]
        self.assertNotIn('inline_data',attachment)
        self.assertEqual(self.adapter.attachment(context,attachment),b'inline bytes')
    def test_auth_check_rejects_wrong_account(self):
        context=self.runtime.sync.context(self.runtime.sync.connection(self.cid));context['scope']['account_id']='different@example.com'
        with self.assertRaises(AdapterError):self.adapter.check(context)
    def test_long_email_is_chunked_without_loss(self):
        text='abcdef '*30000
        item=self.adapter.normalize({'source':'test','message':message('m',text)})
        self.assertGreater(len(item['records']),1)
        self.assertTrue(''.join(r['text'] for r in item['records']).endswith(text))
        self.assertTrue(all(len(r['text'])<=90000 for r in item['records']))
    def test_hidden_revision_is_erased_from_hindsight(self):
        from personal_memory.hindsight import Hindsight
        rid=self.store.ingest_contract([note_record('h1',source='hindsight-test')])['records'][0]['id']
        engine=Hindsight(self.store,{'sources':['hindsight-test'],'url':'http://127.0.0.1:1','bank_id':'test'})
        calls=[]
        class Client:
            def call(self,path,body=None,**kwargs):
                calls.append((path,kwargs.get('method')))
                return {'success':True,'async':False,'items_count':len(body['items'])} if body else {}
        engine.write_client=Client()
        engine.sync(batch=1)
        self.assertEqual(engine.status()['synced_records'],1)
        with self.store.connect() as db:
            db.execute('INSERT INTO record_visibility VALUES(?,1,NULL)',(rid,))
        engine.sync(batch=1)
        self.assertEqual(engine.status()['synced_records'],0)
        self.assertTrue(any(method=='DELETE' for _,method in calls))


class SourceRegressionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=Store(Path(self.tmp.name)/'memory.db');self.sync=SourceSync(self.store,{'fixture.gmail':FixtureAdapter()})
        self.cid=self.sync.configure(adapter_id='fixture.gmail',source='gmail-acct1',scope={})['connection_id']
    def lease(self,role='backfill'):return self.sync.claim(self.cid,stream='messages',role=role,owner='worker')
    def page(self,pid,ops,cursor):return source_page(page_id=pid,operations=ops,next_state=read_state(cursor=cursor))
    def op(self,version,text):return source_operation('upsert','m',records=[note_record('m',text=text,revision=str(version))],source_version=version)
    def test_old_evidence_is_history_only(self):
        self.sync.commit_page(self.lease('incremental'),op_id='new',page=self.page('new',[self.op(50,'Monday')],50))
        self.sync.commit_page(self.lease(),op_id='old',page=self.page('old',[self.op(40,'Friday')],40))
        self.assertFalse(self.store.search('Friday')['episodes'])
        self.assertTrue(self.store.search('Friday',include_history=True)['episodes'])
    def test_remove_before_backfill_leaves_tombstone(self):
        self.sync.commit_page(self.lease('incremental'),op_id='delete',page=self.page('delete',[source_operation('remove','m',source_version=50)],50))
        self.sync.commit_page(self.lease(),op_id='old',page=self.page('old',[self.op(40,'Friday')],40))
        self.assertFalse(self.store.search('Friday')['episodes'])
    def test_stale_delete_and_cursor_are_rejected(self):
        lease=self.lease();stale=dict(lease)
        self.sync.commit_page(lease,op_id='first',page=self.page('first',[self.op(50,'Monday')],50))
        with self.assertRaises(ValueError):self.sync.commit_page(stale,op_id='stale',page=self.page('stale',[],40))
        self.sync.commit_page(lease,op_id='old-remove',page=self.page('old-remove',[source_operation('remove','m',source_version=40)],60))
        self.assertEqual(self.sync.head('gmail-acct1','m')['state'],'live')
    def test_digest_checks_progress_and_validation_rechecks_records(self):
        lease=self.lease();page=self.page('p',[],1)
        self.sync.commit_page(lease,op_id='same',page=page)
        changed=copy.deepcopy(page);changed['next_state']['cursor']=2
        with self.assertRaises(ValueError):self.sync.commit_page(lease,op_id='same',page=changed)
        invalid=self.page('bad',[self.op(1,'bad')],2);invalid['operations'][0]['records'][0]['schema_version']='INVALID'
        with self.assertRaises(ValueError):self.sync.commit_page(lease,op_id='bad',page=invalid)
    def test_reset_pauses_sources_and_cancels_jobs(self):
        from personal_memory.reset import reset
        with self.store.connect() as db:self.sync._enqueue_job(db,self.cid,'attachment','job',{'record_id':'r'})
        reset(self.store,'canonical')
        self.assertEqual(self.sync.connection(self.cid)['state'],'paused')
        self.assertIsNone(self.sync.claim_job('worker'))
        with self.assertRaises(ValueError):self.lease()
    def test_scope_update_preserves_connection_identity(self):
        updated=self.sync.configure(adapter_id='fixture.gmail',source='gmail-acct1',scope={'after':'2020-01-01'})
        self.assertEqual(updated['connection_id'],self.cid)
    def test_schema_version_does_not_increment_with_progress(self):
        class Strict(FixtureAdapter):
            def read_page(self,context,state):
                if state['state_version']!=1:raise AdapterError('cursor','Wrong schema version')
                return source_page(page_id='p',operations=[],next_state=read_state(cursor={'done':True}))
        self.sync.registry['fixture.gmail']=Strict();worker=SyncWorker(self.sync)
        self.assertEqual(worker.run_once(self.cid,stream='messages')['status'],'complete')
        self.assertEqual(worker.run_once(self.cid,stream='messages')['status'],'complete')


class SourceAuthorizationTests(unittest.TestCase):
    def test_source_management_is_admin_only(self):
        with tempfile.TemporaryDirectory() as root:
            service=MemoryService(root,'a'*32,retrieval_config={'semantic':{'enabled':False}},source_config={'enabled':False})
            try:
                self.assertEqual(service.dispatch('/v1/sources/status',{}, {'role':'agent'})['connections'],[])
                with self.assertRaises(AccessDenied):service.dispatch('/v1/sources/gmail/connect',{}, {'role':'agent'})
                result=service.dispatch('/v1/intelligence/read',{'operation':'sources','arguments':{}},{'role':'agent'})
                self.assertEqual(result['connections'],[])
            finally:service.close()


if __name__=='__main__':unittest.main()
