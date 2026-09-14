"""Profile-contained native file snapshots with chunk lineage and retraction."""
import hashlib
import json
from pathlib import Path
from .common import digest,now
from .ingestion import adapt_existing
from .native_history import sync_state


def _sync_files(home,client,*,kinds=('skill','builtin_memory'),names=None,parents=()):
    home=Path(home).resolve();state=home/'personal-memory/outbox.db'
    if set(kinds)-{'skill','builtin_memory'}:raise ValueError('Invalid native file kind')
    if names is not None and (not isinstance(names,list) or not all(isinstance(n,str) and n for n in names)):raise ValueError('Invalid selected names')
    if len(parents)>100:raise ValueError('Compact file provenance before sync')
    files={}
    if 'builtin_memory' in kinds:
        files.update({('builtin_memory',key):home/'memories'/filename for key,filename in [('memory','MEMORY.md'),('user','USER.md')]})
    root=home/'skills'
    if 'skill' in kinds and root.exists():
        for path in root.rglob('SKILL.md'):files[('skill',str(path.parent.relative_to(root)))]=path
    with sync_state(state) as db:
        db.execute('CREATE TABLE IF NOT EXISTS native_file_heads(kind TEXT,object_key TEXT,record_id TEXT,revision TEXT,PRIMARY KEY(kind,object_key))')
        db.execute('CREATE TABLE IF NOT EXISTS native_file_retirements(record_id TEXT PRIMARY KEY,replacement_id TEXT)')
        if 'replacement_id' not in {r[1] for r in db.execute('PRAGMA table_info(native_file_retirements)')}:db.execute('ALTER TABLE native_file_retirements ADD COLUMN replacement_id TEXT')
        old={(r[0],r[1]):(r[2],r[3]) for r in db.execute('SELECT kind,object_key,record_id,revision FROM native_file_heads') if r[0] in kinds}
    def selected(key):return names is None or key[1] in names or Path(key[1]).name in names
    for name in names or []:
        matches={key for key in set(files)|set(old) if key[1]==name or Path(key[1]).name==name}
        if len(matches)>1:raise ValueError('Ambiguous native file name: '+name)
    keys=sorted(key for key in set(files)|set(old) if selected(key))
    report={'snapshots':0,'unchanged':0,'removed':0,'retired':0,'files':[]}
    def retire():
        with sync_state(state) as db:pending=list(db.execute('SELECT record_id,replacement_id FROM native_file_retirements'))
        for rid,replacement in pending:
            status=client.call('/v1/lineage/status',{'record_ids':[rid]})['states'][rid]
            if status=='live':client.call('/v1/supersede',{'record_id':rid,'replacement_id':replacement})
            if status=='unknown':raise ValueError('Native retirement source is missing; reconcile restore first')
            with sync_state(state) as db:db.execute('DELETE FROM native_file_retirements WHERE record_id=?',(rid,))
            report['retired']+=1
    retire()
    for key in keys:
        path=files.get(key);previous=old.get(key)
        if path is None or not path.exists():
            if previous:
                with sync_state(state) as db:
                    db.execute('INSERT OR IGNORE INTO native_file_retirements VALUES(?,NULL)',(previous[0],))
                    db.execute('DELETE FROM native_file_heads WHERE kind=? AND object_key=?',key)
                report['removed']+=1
            continue
        real=path.resolve()
        if not real.is_relative_to(home) or path.is_symlink():raise ValueError('Native file resolves outside its selected profile or is a link')
        before=path.stat()
        if before.st_size>1024*1024:raise ValueError('Native file exceeds 1 MiB snapshot budget')
        raw=path.read_bytes();after=path.stat()
        if (before.st_mtime_ns,before.st_size)!=(after.st_mtime_ns,after.st_size):raise RuntimeError('Native file changed during snapshot; retry')
        content=raw.decode('utf-8');sha=hashlib.sha256(raw).hexdigest()
        revision=digest([sha,before.st_mtime_ns]);source_id=key[0]+'/'+key[1]
        if previous and previous[1]==revision:
            status=client.call('/v1/lineage/status',{'record_ids':[previous[0]]})['states'][previous[0]]
            if status=='live':report['unchanged']+=1;continue
            # A forgotten unchanged file must not become fresh evidence.
            report['files'].append({'object_key':key[1],'state':'forgotten'});continue
        root_id='rec_'+digest(['hermes-native-file-root',source_id,revision])[:32]
        spans=[content[i:i+60000] for i in range(0,len(content),60000)]
        parts=['rec_'+digest(['hermes-native-file-part',source_id+'/'+str(i),revision])[:32] for i in range(len(spans))]
        document={'path':str(path.relative_to(home)),'sha256':sha,'state':'present','part_record_ids':parts,'authority':'observation_only'}
        item=adapt_existing({'source':'hermes-native-file-root','source_id':source_id,'revision':revision,'occurred_at':None,
            'text':json.dumps(document),'metadata':{'native_kind':key[0],'native_object':key[1],'chunk_offset':0,'authority':'observation_only'}},
            connector_id='hermes.native.files',connector_version='1',source_locator=path.as_uri(),observed_at=now())
        item['provenance'].update(origin='derived' if parents else 'assistant',parent_record_ids=list(parents))
        receipt=client.call('/v1/ingest',{'items':[item]})['records'][0]
        assert receipt['id']==root_id
        for i,text in enumerate(spans):
            part=adapt_existing({'source':'hermes-native-file-part','source_id':source_id+'/'+str(i),'revision':revision,'occurred_at':None,
                                'text':text,'metadata':{'part':i,'path':str(path.relative_to(home))}},
                connector_id='hermes.native.files',connector_version='1',source_locator=path.as_uri()+'#'+str(i),observed_at=now())
            part['provenance'].update(origin='derived',parent_record_ids=[root_id])
            client.call('/v1/ingest',{'items':[part]})
        complete=dict(item);complete['source']='hermes-native-state'
        complete['extensions']=json.loads(json.dumps(item['extensions']))
        complete['extensions']['personal_memory.legacy']['data']['native_root']=root_id
        complete['provenance']={**item['provenance'],'origin':'derived','parent_record_ids':[root_id]}
        client.call('/v1/ingest',{'items':[complete]})
        with sync_state(state) as db:
            db.execute('INSERT OR REPLACE INTO native_file_heads VALUES(?,?,?,?)',(*key,root_id,revision))
            if previous and previous[0]!=root_id:db.execute('INSERT OR IGNORE INTO native_file_retirements VALUES(?,?)',(previous[0],root_id))
        report['snapshots']+=1;report['files'].append({'kind':key[0],'object_key':key[1],'record_id':root_id,'parts':len(parts)})
    retire()
    return report



def sync_files(home,client,*,kinds=('skill','builtin_memory'),names=None,parents=()):
    from contextlib import closing
    from .asgi import ProcessLease
    with closing(ProcessLease(Path(home)/'personal-memory/native-file-sync.lock')):
        return _sync_files(home,client,kinds=kinds,names=names,parents=parents)
