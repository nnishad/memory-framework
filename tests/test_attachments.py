import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from personal_memory import blobs
from personal_memory.service import MemoryService,AccessDenied
from personal_memory.store import Store
from personal_memory.recovery import backup,restore,create_key
from test_ingestion import item

class AttachmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.home=self.root/'home';self.data=self.home/'personal-memory/data'
        self.service=MemoryService(self.data,'a'*40,principals=[{'token':'g'*40,'role':'agent'},
          {'token':'i'*40,'role':'ingest','sources':['custom-notes'],'connector_id':'example.notes'},
          {'token':'x'*40,'role':'ingest','sources':['custom-notes'],'connector_id':'other.notes'}])
        self.addCleanup(self.service.close);self.store=self.service.store
        self.rid=self.store.ingest_contract([item()])['records'][0]['id']
        self.roles={r:self.service.authenticate('Bearer '+r*40) for r in ['a','g','i','x']}
    def call(self,path,data,role='a'):return self.service.dispatch('/v1/blob/'+path,data,self.roles[role])
    def begin(self,data,role='a'):
        return self.call('begin',dict(record_id=self.rid,filename='fictional.bin',mime='application/octet-stream',size=len(data),sha256=hashlib.sha256(data).hexdigest()),role)
    def put(self,blob,index,data,role='a'):return self.call('put',dict(blob_id=blob['id'],index=index,data=base64.b64encode(data).decode()),role)
    def test_resume_integrity_scope_and_forgetting(self):
        data=b'A'*blobs.CHUNK_SIZE+b'fictional-tail';blob=self.begin(data,'i')
        with self.assertRaises(AccessDenied):self.begin(data,'g')
        with self.assertRaises(AccessDenied):self.put(blob,1,b'fictional-tail','x')
        self.put(blob,1,b'fictional-tail','i')
        self.assertEqual(self.begin(data,'i')['received_chunks'],[1])
        with self.assertRaises(ValueError):self.call('complete',{'blob_id':blob['id']})
        self.put(blob,0,data[:blobs.CHUNK_SIZE],'i');self.put(blob,0,data[:blobs.CHUNK_SIZE],'i')
        with self.assertRaises(ValueError):self.put(blob,0,b'B'*blobs.CHUNK_SIZE,'i')
        self.call('complete',{'blob_id':blob['id']},'i')
        read=self.call('read',{'blob_id':blob['id'],'index':1},'g')
        self.assertEqual(base64.b64decode(read['data']),b'fictional-tail')
        self.assertEqual(len(self.call('list',{'record_id':self.rid},'g')['attachments']),1)
        self.store.forget(self.rid)
        self.assertEqual(self.call('list',{'record_id':self.rid},'g')['attachments'],[])
        with self.assertRaises(ValueError):self.call('read',{'blob_id':blob['id']},'g')
        with self.store.connect() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM memory_blob_chunks').fetchone()[0],0)
    def test_backup_replays_attachment_deletion(self):
        blob=self.begin(b'fictional');self.put(blob,0,b'fictional');self.call('complete',{'blob_id':blob['id']})
        settings=self.home/'personal-memory/settings.json';settings.write_text(json.dumps({'data_dir':str(self.data)}))
        registry={'skills/fixture/SKILL.md':{'candidate_id':'fictional'}}
        (self.home/'personal-memory/skill-exports.json').write_text(json.dumps(registry))
        key=self.root/'key';create_key(key);archive=self.root/'backup.enc';backup(self.home,archive,key)
        restored=self.root/'restored';restore(archive,key,restored)
        self.assertEqual(json.loads((restored/'skill-exports.json').read_text()),registry)
        self.assertEqual(base64.b64decode(blobs.read(Store(restored/'memory.db'),blob_id=blob['id'])['data']),b'fictional')
        self.store.forget(self.rid);destination=self.root/'after-delete'
        restore(archive,key,destination,self.data/'memory.deletions.db')
        with self.assertRaises(ValueError):blobs.read(Store(destination/'memory.db'),blob_id=blob['id'])
    def test_hash_mismatch_and_empty_attachment(self):
        blob=self.begin(b'correct');self.put(blob,0,b'incorec')
        with self.assertRaises(ValueError):self.call('complete',{'blob_id':blob['id']})
        empty=self.begin(b'');self.call('complete',{'blob_id':empty['id']})
        self.assertEqual(self.call('read',{'blob_id':empty['id']})['data'],'')

    def test_old_backup_replays_global_reset_and_does_not_reset_new_data_twice(self):
        from personal_memory import reset
        with self.store.connect() as db:db.execute("UPDATE entities SET label='fictional identity secret'")
        (self.home/'personal-memory/settings.json').write_text(json.dumps({'data_dir':str(self.data)}))
        key=self.root/'key';create_key(key);archive=self.root/'before-reset.enc';backup(self.home,archive,key)
        reset.reset(self.store,'canonical')
        new_id=self.store.ingest_contract([item('after-reset')])['records'][0]['id']
        self.assertEqual(Store(self.store.path).evidence(new_id)['text'],item('after-reset')['text'])
        destination=self.root/'reset-restored';restore(archive,key,destination,self.data/'memory.deletions.db')
        restored=Store(destination/'memory.db')
        with self.assertRaises(ValueError):restored.evidence(self.rid)
        with restored.connect() as db:self.assertFalse(db.execute("SELECT 1 FROM entities WHERE label='fictional identity secret'").fetchone())
        self.assertGreater(reset.epoch(restored),0)
