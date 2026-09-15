"""Resumable logical reset of canonical memory with a write epoch fence."""
from .common import now


def initialize(store):
    with store.connect() as db:
        db.executescript('''CREATE TABLE IF NOT EXISTS memory_epoch(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL);
        INSERT OR IGNORE INTO memory_epoch VALUES(1,0);
        CREATE TABLE IF NOT EXISTS memory_resets(epoch INTEGER PRIMARY KEY,state TEXT,requested_at TEXT);
        CREATE TABLE IF NOT EXISTS memory_reset_tokens(token TEXT PRIMARY KEY,epoch INTEGER);
        CREATE TABLE IF NOT EXISTS memory_reset_sources(epoch INTEGER,source TEXT,source_id TEXT,done INTEGER DEFAULT 0,PRIMARY KEY(epoch,source,source_id));''')
    resume(store)
    for token in sorted(rid for rid in store.deletions.ids() if rid.startswith('reset_')):
        with store.connect() as db:known=db.execute('SELECT 1 FROM memory_reset_tokens WHERE token=?',(token,)).fetchone()
        if not known:_reset(store,token)


def pending(store):
    with store.connect() as db:return bool(db.execute("SELECT 1 FROM memory_resets WHERE state='pending' LIMIT 1").fetchone())


def epoch(store):
    with store.connect() as db:return db.execute('SELECT value FROM memory_epoch WHERE id=1').fetchone()[0]


def resume(store):
    with store.lock:
        with store.connect() as db:pending=[r[0] for r in db.execute("SELECT epoch FROM memory_resets WHERE state='pending' ORDER BY epoch")]
        for value in pending:
            while True:
                with store.connect() as db:rows=db.execute('SELECT source,source_id FROM memory_reset_sources WHERE epoch=? AND done=0 LIMIT 100',(value,)).fetchall()
                if not rows:break
                for source,sid in rows:
                    store.forget_source(source,sid)
                    with store.connect() as db:db.execute('UPDATE memory_reset_sources SET done=1 WHERE epoch=? AND source=? AND source_id=?',(value,source,sid))
            with store.connect() as db:
                db.execute("UPDATE entities SET label='[forgotten]'")
                db.execute('DELETE FROM account_aliases')
                db.execute("UPDATE accounts SET address=entity_id")
                db.execute("UPDATE identity_edges SET status='revoked'")
                db.execute("UPDATE learning_objects SET payload='{}',state='invalidated'")
                db.execute('DELETE FROM knowledge_fts')
                for target in ('memory','user'):
                    head=db.execute('SELECT version FROM curated_heads WHERE target=?',(target,)).fetchone()
                    if head:
                        db.execute('UPDATE curated_entries SET retired_version=? WHERE target=? AND retired_version IS NULL',(head[0]+1,target))
                        db.execute('UPDATE curated_heads SET version=?,updated_at=? WHERE target=?',(head[0]+1,now(),target))
                db.execute("UPDATE sources SET state='unknown',note='',through_at=NULL")
                db.execute("UPDATE memory_resets SET state='completed' WHERE epoch=?",(value,))


def reset(store,scope,backend=None):
    if scope!='canonical':raise ValueError('Reset requires explicit scope=canonical')
    token='reset_'+__import__('uuid').uuid4().hex
    with store.lock:
        store.deletions.append([token])
        result=_reset(store,token)
    # Cascade outside the store lock: clearing the external engine is network I/O and the local
    # SQLite reset is already durable. A fresh start must leave no residual evidence in the
    # managed Hindsight bank, so this runs synchronously and its outcome is reported to the caller.
    result['external_engine']=_clear_external(backend)
    return result


def _clear_external(backend):
    clear=getattr(backend,'clear_external',None)
    if callable(clear):
        try:return clear()
        except Exception as error:return {'cleared':False,'error':type(error).__name__}
    return {'cleared':False,'reason':'no external engine bound'}


def _reset(store,token):
    with store.lock:
        resume(store)
        with store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            value=db.execute('SELECT value FROM memory_epoch WHERE id=1').fetchone()[0]+1
            db.execute('UPDATE memory_epoch SET value=? WHERE id=1',(value,))
            db.execute("INSERT INTO memory_resets VALUES(?,'pending',?)",(value,now()))
            db.execute('INSERT INTO memory_reset_tokens VALUES(?,?)',(token,value))
            db.execute('INSERT INTO memory_reset_sources(epoch,source,source_id) SELECT ?,source,source_id FROM records GROUP BY source,source_id',(value,))
        resume(store)
    return {'scope':'canonical','epoch':value,'state':'completed','restart_sessions':True,
            'physical_erasure':False,'excluded':['native files and transcripts','external sources','backups','already loaded prompts']}
