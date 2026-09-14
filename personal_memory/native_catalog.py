"""Read model for source-backed native state; never scheduler/execution authority."""
import json
from .common import required_text


def initialize(store):
    with store.connect() as db:
        db.execute('CREATE TABLE IF NOT EXISTS native_state_heads(kind TEXT,object_key TEXT,record_id TEXT,PRIMARY KEY(kind,object_key))')


def observe(store, record, receipt):
    metadata=record.get('extensions',{}).get('personal_memory.legacy',{}).get('data',{})
    kind=metadata.get('native_kind');key=metadata.get('native_object')
    if record['source']!='hermes-native-state' or kind not in {'skill','todo','builtin_memory','notepad'}:return
    required_text(key,'native_object',1000)
    # A replay of an older observation must not replace the current pointer.
    if metadata.get('chunk_offset',0)!=0:return
    with store.lock,store.connect() as db:
        record_id=metadata.get('native_root',receipt['id'])
        if record_id!=receipt['id']:
            parent=db.execute('SELECT 1 FROM record_dependencies WHERE child_id=? AND parent_id=?',(receipt['id'],record_id)).fetchone()
            if not parent:raise ValueError('Native state root must be a direct canonical dependency')
        existing=db.execute('SELECT r.rowid FROM native_state_heads h JOIN records r ON r.id=h.record_id WHERE h.kind=? AND h.object_key=?',(kind,key)).fetchone()
        candidate=db.execute('SELECT rowid FROM records WHERE id=?',(record_id,)).fetchone()[0]
        if existing and existing[0]>=candidate:return
        db.execute('INSERT OR REPLACE INTO native_state_heads VALUES(?,?,?)',(kind,key,record_id))


def read(store,kind=None,after='',limit=50):
    if kind is not None and kind not in {'skill','todo','builtin_memory','notepad'}:raise ValueError('Invalid native kind')
    if not isinstance(after,str) or type(limit) is not int or not 1<=limit<=100:raise ValueError('Invalid native catalog page')
    with store.connect() as db:
        rows=db.execute("SELECT h.kind,h.object_key,h.record_id,(r.deleted OR EXISTS(SELECT 1 FROM record_visibility v WHERE v.record_id=r.id AND v.hidden=1)),r.text FROM native_state_heads h JOIN records r ON r.id=h.record_id WHERE h.kind||'/'||h.object_key>? AND (? IS NULL OR h.kind=?) ORDER BY h.kind,h.object_key LIMIT ?",(after,kind,kind,limit+1)).fetchall()
    output=[]
    for row in rows[:limit]:
        try: observation=None if row[3] else json.loads(row[4])
        except (ValueError,TypeError): observation={'fragment':row[4],'truncated':True}
        output.append({'kind':row[0],'object_key':row[1],'record_id':row[2],
                       'state':'unknown_forgotten' if row[3] else 'observed',
                       'observation':observation,'authority':'observation_only'})
    return {'objects':output,'next_cursor':(rows[limit-1][0]+'/'+rows[limit-1][1]) if len(rows)>limit else None,
            'coverage':'observed host events only; not an exhaustive filesystem inventory'}
