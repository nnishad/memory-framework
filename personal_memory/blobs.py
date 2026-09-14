"""Resumable attachments inside the canonical SQLite backup/deletion boundary."""
import base64
import hashlib
import math
from .common import digest,required_text

CHUNK_SIZE=512*1024
SCHEMA='''CREATE TABLE IF NOT EXISTS memory_blobs(id TEXT PRIMARY KEY,record_id TEXT NOT NULL,filename TEXT,mime TEXT,size INTEGER,sha256 TEXT,state TEXT);
CREATE TABLE IF NOT EXISTS memory_blob_chunks(blob_id TEXT,chunk_index INTEGER,data BLOB,PRIMARY KEY(blob_id,chunk_index));'''


def _get(db,blob_id):
    row=db.execute('SELECT b.* FROM memory_blobs b JOIN records r ON r.id=b.record_id WHERE b.id=? AND b.state!=\'forgotten\' AND r.deleted=0',(blob_id,)).fetchone()
    if not row:raise ValueError('Attachment or live parent not found')
    return dict(row)


def begin(store,*,record_id,filename,mime,size,sha256):
    required_text(filename,'filename',255);required_text(mime,'mime',200)
    if type(size) is not int or not 0<=size<=1024**3:raise ValueError('Attachment size must be 0..1 GiB')
    if not isinstance(sha256,str) or len(sha256)!=64 or any(c not in '0123456789abcdef' for c in sha256):raise ValueError('Expected lowercase SHA256')
    blob_id='blob_'+digest([record_id,filename,mime,size,sha256])[:32]
    with store.lock,store.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        if not db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0',(record_id,)).fetchone():raise ValueError('Live parent evidence required')
        db.execute("INSERT OR IGNORE INTO memory_blobs VALUES(?,?,?,?,?,?,'pending')",(blob_id,record_id,filename,mime,size,sha256))
        result=_get(db,blob_id)
        result['received_chunks']=[r[0] for r in db.execute('SELECT chunk_index FROM memory_blob_chunks WHERE blob_id=? ORDER BY chunk_index',(blob_id,))]
    result['chunk_size']=CHUNK_SIZE;return result


def put(store,*,blob_id,index,data):
    if type(index) is not int or index<0 or not isinstance(data,str) or len(data)>4*((CHUNK_SIZE+2)//3):raise ValueError('Invalid attachment chunk')
    raw=base64.b64decode(data,validate=True)
    with store.lock,store.connect() as db:
        db.execute('BEGIN IMMEDIATE');blob=_get(db,blob_id)
        if blob['state']!='pending':raise ValueError('Attachment is already complete')
        count=math.ceil(blob['size']/CHUNK_SIZE)
        if index>=count or len(raw)!=min(CHUNK_SIZE,blob['size']-index*CHUNK_SIZE):raise ValueError('Chunk offset or length mismatch')
        old=db.execute('SELECT data FROM memory_blob_chunks WHERE blob_id=? AND chunk_index=?',(blob_id,index)).fetchone()
        if old and old[0]!=raw:raise ValueError('Chunk replay changed bytes')
        db.execute('INSERT OR IGNORE INTO memory_blob_chunks VALUES(?,?,?)',(blob_id,index,raw))
    return {'blob_id':blob_id,'index':index,'stored':True}


def complete(store,*,blob_id):
    with store.lock,store.connect() as db:
        db.execute('BEGIN IMMEDIATE');blob=_get(db,blob_id)
        hasher=hashlib.sha256();size=count=0
        for index,data in db.execute('SELECT chunk_index,data FROM memory_blob_chunks WHERE blob_id=? ORDER BY chunk_index',(blob_id,)):
            if index!=count:raise ValueError('Missing attachment chunk')
            hasher.update(data);size+=len(data);count+=1
        if size!=blob['size'] or hasher.hexdigest()!=blob['sha256']:raise ValueError('Incomplete or mismatched attachment')
        db.execute("UPDATE memory_blobs SET state='complete' WHERE id=?",(blob_id,))
    return {'blob_id':blob_id,'state':'complete','sha256':blob['sha256'],'size':size}


def read(store,*,blob_id,index=0):
    with store.connect() as db:
        blob=_get(db,blob_id)
        if blob['state']!='complete':raise ValueError('Attachment is not complete')
        if type(index) is not int or index<0 or index>=max(1,math.ceil(blob['size']/CHUNK_SIZE)):raise ValueError('Invalid attachment chunk index')
        row=db.execute('SELECT data FROM memory_blob_chunks WHERE blob_id=? AND chunk_index=?',(blob_id,index)).fetchone()
        blob.update(index=index,data=base64.b64encode(row[0] if row else b'').decode(),next_index=index+1 if (index+1)*CHUNK_SIZE<blob['size'] else None)
        return blob


def listing(store,*,record_id):
    with store.connect() as db:
        rows=db.execute("SELECT b.id,b.filename,b.mime,b.size,b.sha256,b.state FROM memory_blobs b JOIN records r ON r.id=b.record_id WHERE b.record_id=? AND b.state='complete' AND r.deleted=0 ORDER BY b.id",(record_id,)).fetchall()
    return {'record_id':record_id,'attachments':[dict(row) for row in rows],'content_is_untrusted':True}


def upload(client,path,record_id,mime):
    from pathlib import Path
    path=Path(path);before=path.stat();hasher=hashlib.sha256()
    with path.open('rb') as src:
        while data:=src.read(CHUNK_SIZE):hasher.update(data)
    blob=client.call('/v1/blob/begin',{'record_id':record_id,'filename':path.name,'mime':mime,'size':before.st_size,'sha256':hasher.hexdigest()})
    if blob['state']=='complete':return blob
    received=set(blob['received_chunks'])
    with path.open('rb') as src:
        index=0
        while data:=src.read(CHUNK_SIZE):
            if index not in received:client.call('/v1/blob/put',{'blob_id':blob['id'],'index':index,'data':base64.b64encode(data).decode()})
            index+=1
    if (path.stat().st_size,path.stat().st_mtime_ns)!=(before.st_size,before.st_mtime_ns):raise ValueError('Source file changed; upload not committed')
    return client.call('/v1/blob/complete',{'blob_id':blob['id']})
