"""Provider-side implementations of optional pinned Hermes host boundaries."""
import json
import sqlite3
from pathlib import Path

from .client import Client
from .common import digest
from .configuration import load_settings
from .native_history import sync_state


def context_allowed(home, policy, kwargs):
    context = kwargs.get('host_context')
    if context is None: return None
    from agent.memory_bridge import MemoryHostContext
    if not isinstance(context, MemoryHostContext) or Path(context.home).resolve() != Path(home).resolve(): return False
    if str(kwargs.get('chat_type') or '').lower() in {'group','supergroup','channel','guild','public','room'}: return False
    if context.kind == 'local_owner':
        return kwargs.get('platform') in {'tui','desktop'}
    if context.kind == 'authenticated_user':
        key = str(kwargs.get('platform')) + ':' + context.identity_provider
        return context.user_id in policy.get('owners', {}).get(key, [])
    if context.kind == 'cron':
        allowed = context.local_delivery and not context.targets
        configured = policy.get('cron_recipients', [])
        if not isinstance(configured, list): raise ValueError('cron_recipients must be a list')
        targets = set()
        for entry in configured:
            if not isinstance(entry, dict) or set(entry) != {'platform','chat_id','thread_id','chat_type'} or entry['chat_type'] != 'private':
                raise ValueError('Cron memory recipients require explicit platform/chat_id/thread_id and private chat_type')
            targets.add((str(entry['platform']).lower(), str(entry['chat_id']), str(entry['thread_id'] or '')))
        allowed = allowed or (bool(context.targets) and set(context.targets) <= targets)
        if not context.job_id or not context.run_id: return False
        with sync_state(Path(home) / 'personal-memory/outbox.db') as db:
            db.execute('CREATE TABLE IF NOT EXISTS host_run_scopes(job_id TEXT,run_id TEXT,targets TEXT,enabled INTEGER,PRIMARY KEY(job_id,run_id))')
            value = (json.dumps(sorted(context.targets)), int(allowed))
            old = db.execute('SELECT targets,enabled FROM host_run_scopes WHERE job_id=? AND run_id=?',
                             (context.job_id, context.run_id)).fetchone()
            if old and tuple(old) != value:
                raise ValueError('Run memory scope is immutable; create a new run')
            db.execute('INSERT OR IGNORE INTO host_run_scopes VALUES(?,?,?,?)',
                       (context.job_id, context.run_id, *value))
        return allowed
    return False


def check_delivery(home, job_id, targets, run_id='', content=None):
    path = Path(home) / 'personal-memory/outbox.db'
    if not path.exists() or not run_id or not isinstance(content,str) or not content.strip(): return False
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='host_run_scopes'").fetchone(): return False
        row = db.execute('SELECT targets,enabled FROM host_run_scopes WHERE job_id=? AND run_id=?', (job_id, run_id)).fetchone()
    if not row: return False
    if content is not None:
        with sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True) as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='host_delivery_payloads'").fetchone():return False
            proof=db.execute('SELECT record_id FROM host_delivery_payloads WHERE job_id=? AND run_id=? AND fingerprint=?',
                             (job_id,run_id,digest(content.strip()))).fetchone()
        if not proof:return False
        cfg=load_settings(home);client=Client(cfg['url'],cfg.get('agent_token',cfg['token']),timeout=3)
        if client.call('/v1/lineage/status',{'record_ids':[proof[0]]})['states'][proof[0]]!='live':return False
    # A denied run cannot be used as an authorization record for any output.
    if not row[1]: return False
    if sorted(map(tuple, json.loads(row[0]))) != sorted(map(tuple, targets)): return False
    from agent.memory_bridge import MemoryHostContext
    policy = load_settings(home).get('session_access', {})
    configured = {(r['platform'].lower(), r['chat_id'], r['thread_id']) for r in policy.get('cron_recipients', [])}
    return not targets or set(map(tuple, targets)) <= configured


def local_process_caller():
    """True when no host transport is bound to this context, i.e. the caller is this process.

    The interactive CLI never binds a transport: every `bind_transport` call site lives in the
    gateway, desktop and voice turn paths. A remote turn therefore always has one bound before it
    reaches a native-history read, including an unauthenticated websocket, which the bridge also
    reports as a missing caller identity. Asking the transport is the only way to tell the local
    owner apart from that remote caller, so the decision is made here rather than by treating an
    unnamed context as an owner.
    """
    try:
        from tui_gateway.transport import current_transport
    except Exception:
        return False
    return current_transport() is None


def authorize_native(home, context):
    from agent.memory_bridge import MemoryHostContext, MemoryReadScope
    if isinstance(context, MemoryReadScope):
        return context.allowed and Path(context.home).resolve() == Path(home).resolve()
    if context is None and local_process_caller():
        # Unscoped reads inside the local process are the owner's own session: compression and
        # continuity lookups run there and have no transport to be named by. Denying them left the
        # interactive CLI unable to compress context at all.
        context = MemoryHostContext(str(Path(home).resolve()), 'local_owner')
    if isinstance(context, MemoryHostContext):
        return context_allowed(home, load_settings(home).get('session_access', {}),
                               {'host_context':context, 'platform':'desktop'}) is True
    return False


def continuity(home, context, content, kind, source_job_id=None):
    if not content: return ''
    policy = load_settings(home).get('session_access', {})
    if not context.run_id or not context_allowed(home, policy, {'host_context':context, 'platform':'cron'}):
        return ''
    path = Path(home) / 'personal-memory/outbox.db'
    with sync_state(path) as db:
        db.execute('CREATE TABLE IF NOT EXISTS host_artifacts(fingerprint TEXT PRIMARY KEY,kind TEXT,record_id TEXT,targets TEXT)')
        row = db.execute('SELECT record_id,targets FROM host_artifacts WHERE fingerprint=? AND kind=?',
                         (digest([source_job_id or context.job_id,kind,content]),kind)).fetchone()
    # Legacy unlabelled output/notepad text cannot acquire trustworthy lineage
    # just because it resides inside the same OS profile.
    if not row or sorted(map(tuple,json.loads(row[1]))) != sorted(context.targets): return ''
    cfg=load_settings(home); client=Client(cfg['url'],cfg.get('agent_token',cfg['token']),timeout=3)
    if client.call('/v1/lineage/status',{'record_ids':[row[0]]})['states'][row[0]] != 'live': return ''
    if kind=='notepad':
        cursor='';current=False
        while True:
            page=client.call('/v1/native-state',{'kind':'notepad','after':cursor})
            current=current or any(obj['object_key']==(source_job_id or context.job_id) and obj['record_id']==row[0] and obj['state']=='observed' for obj in page['objects'])
            cursor=page.get('next_cursor')
            if not cursor:break
        if not current:return ''
    with sync_state(path) as db:
        db.execute('CREATE TABLE IF NOT EXISTS host_run_inputs(job_id TEXT,run_id TEXT,record_id TEXT,PRIMARY KEY(job_id,run_id,record_id))')
        db.execute('INSERT OR IGNORE INTO host_run_inputs VALUES(?,?,?)',(context.job_id,context.run_id,row[0]))
    return content


def filter_native(home, database, rows):
    """Live read guard, enabled by native archive binding created on sync.

    Logical read redaction, not physical erasure. Unknown generated history in
    an affected session is withheld; independent original user rows survive.
    """
    state = Path(home) / 'personal-memory/outbox.db'
    if not state.is_file(): return rows
    with sqlite3.connect(state.resolve().as_uri()+'?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='native_history_archives'").fetchone(): return rows
        archives = [r[0] for r in db.execute('SELECT archive_id FROM native_history_archives WHERE database_path=?', (database,))]
        if not archives: return rows
        # Native IDs resolve session identity in the native DB. Do not trust a
        # model-supplied session label or a search projection without identity.
        normalized = [dict(r) for r in rows]
        row_ids = sorted({r['id'] for r in normalized if isinstance(r.get('id'), int)})
        sessions_by_id = {}
        native_rows = {}
        with sqlite3.connect(Path(database).as_uri()+'?mode=ro', uri=True) as native:
            for start in range(0, len(row_ids), 100):
                batch = row_ids[start:start+100]
                for row in native.execute('SELECT id,session_id,role,content,timestamp,_compressed_summary FROM messages WHERE id IN (' + ','.join('?' for _ in batch) + ')', batch):
                    sessions_by_id[row[0]] = row[1]
                    native_rows[row[0]] = row
        session_ids = sorted(set(sessions_by_id.values()))
        mappings = []
        for archive in archives:
            for sid in session_ids:
                mappings.extend(dict(r) for r in db.execute('SELECT source_id,session_id,record_id FROM native_history_map WHERE archive_id=? AND session_id=?', (archive, sid)))
        known_ids = {int(r['source_id'].rsplit('/', 1)[1]) for r in mappings}
        from datetime import datetime, timezone
        for mid, row in native_rows.items():
            if mid in known_ids or not row[3]: continue
            for archive in archives:
                fingerprint = digest([archive, row[1], row[2], datetime.fromtimestamp(float(row[4]), timezone.utc).isoformat(), row[3]])
                alias = db.execute('SELECT source_id,record_id FROM native_history_fingerprints WHERE fingerprint=?', (fingerprint,)).fetchone()
                if alias:
                    mappings.append({'source_id': alias['source_id'], 'record_id': alias['record_id'],
                                     'session_id': row[1], 'native_id': mid})
    if not mappings: return []
    cfg = load_settings(home)
    client = Client(cfg['url'], cfg.get('agent_token', cfg['token']), timeout=3)
    states, families = {}, {}
    for start in range(0, len(mappings), 100):
        batch = mappings[start:start+100]
        states.update(client.call('/v1/lineage/status', {'record_ids': list({r['record_id'] for r in batch})})['states'])
        families.update(client.call('/v1/source/status', {'source':'hermes-history', 'source_ids':list({r['source_id'] for r in batch})})['states'])
    by_native_id = {}
    affected = set()
    for mapping in mappings:
        native_id = mapping.get('native_id', int(mapping['source_id'].rsplit('/', 1)[1]))
        live = states[mapping['record_id']] == 'live' and families[mapping['source_id']] != 'forgotten'
        by_native_id.setdefault(native_id, []).append(live)
        if not live: affected.add(mapping['session_id'])
    output = []
    for row in normalized:
        mid = row.get('id')
        sid = sessions_by_id.get(mid)
        if mid not in sessions_by_id: continue
        if mid not in by_native_id and native_rows[mid][2] in {'assistant','tool'}: continue
        if mid in by_native_id and not all(by_native_id[mid]): continue
        if sid in affected:
            if row.get('_compressed_summary') or native_rows[mid][5]: continue
            if native_rows[mid][2] in {'assistant','tool'} and mid not in by_native_id: continue
            for field in ('api_content','reasoning','reasoning_content','reasoning_details','codex_reasoning_items','codex_message_items'):
                if field in row: row[field] = None
        output.append(row)
    return output


def skill_revision(home, name):
    if not isinstance(name, str) or not name or len(name) > 200: return None
    # Never follow a skill link out of the selected profile while observing a
    # revision. The host still owns skill resolution, approval and execution.
    root = (Path(home) / 'skills').resolve()
    matches = []
    if root.exists():
        for path in root.rglob('SKILL.md'):
            if path.parent.name != name and str(path.parent.relative_to(root)) != name: continue
            resolved = path.resolve()
            if resolved.is_relative_to(root) and resolved.stat().st_size <= 1024*1024:
                matches.append({'path':str(resolved.relative_to(root)), 'sha256':__import__('hashlib').sha256(resolved.read_bytes()).hexdigest()})
    return matches[0] if len(matches) == 1 else None


def sync_on_read(home, database, rows):
    """Reconcile touched native sessions before returning their history.

    Cache only an unchanged database/WAL fingerprint for the same session.
    Reconciliation uses host-held administration, never an agent credential.
    """
    from .native_history import HermesHistoryConnector, sync_history
    database=Path(database).resolve()
    if database.parent != Path(home).resolve(): raise PermissionError('Cross-profile native sync denied')
    ids=sorted({r['id'] for r in rows if isinstance(r.get('id'),int)})
    if not ids: return
    def stamp():
        values=[]
        for path in (database,Path(str(database)+'-wal')):
            try:
                stat=path.stat();values.append([stat.st_ino,stat.st_size,stat.st_mtime_ns])
            except FileNotFoundError: values.append(None)
        return digest(values)
    version=stamp();sessions=set()
    with sqlite3.connect(database.as_uri()+'?mode=ro',uri=True) as db:
        for offset in range(0,len(ids),100):
            batch=ids[offset:offset+100]
            sessions.update(r[0] for r in db.execute('SELECT DISTINCT session_id FROM messages WHERE id IN ('+','.join('?' for _ in batch)+')',batch))
    state=Path(home)/'personal-memory/outbox.db'
    with sync_state(state) as db:
        db.execute('CREATE TABLE IF NOT EXISTS native_read_sync(database_path TEXT,session_id TEXT,stamp TEXT,PRIMARY KEY(database_path,session_id))')
        archives=[r[0] for r in db.execute('SELECT archive_id FROM native_history_archives WHERE database_path=?',(str(database),))]
        pending=[sid for sid in sessions if not db.execute('SELECT 1 FROM native_read_sync WHERE database_path=? AND session_id=? AND stamp=?',(str(database),sid,version)).fetchone()]
    if not pending:return
    cfg=load_settings(home);client=Client(cfg['url'],cfg['token'],timeout=30)
    connector=HermesHistoryConnector(database, archives[0] if archives else 'profile-native-history', sorted(pending))
    sync_history(client,connector,state)
    # A concurrent native edit forces another pass next time, not a false
    # watermark claiming that the edit was included in our earlier snapshot.
    if stamp()==version:
        with sync_state(state) as db:
            db.executemany('INSERT OR REPLACE INTO native_read_sync VALUES(?,?,?)',[(str(database),sid,version) for sid in pending])



def native_evidence(home,database,rows):
    wanted={row['id'] for row in rows if isinstance(row.get('id'),int)}
    refs=set();covered=set();sessions={}
    with sqlite3.connect(Path(database).resolve().as_uri()+'?mode=ro',uri=True) as native:
        ids=sorted(wanted)
        for offset in range(0,len(ids),100):
            batch=ids[offset:offset+100]
            sessions.update(native.execute('SELECT id,session_id FROM messages WHERE id IN ('+','.join('?' for _ in batch)+')',batch))
    with sync_state(Path(home)/'personal-memory/outbox.db') as db:
        archives=[r[0] for r in db.execute('SELECT archive_id FROM native_history_archives WHERE database_path=?',(str(Path(database).resolve()),))]
        source_ids={digest([archive,sid])+'/message/'+str(mid):mid for archive in archives for mid,sid in sessions.items()}
        keys=sorted(source_ids)
        for offset in range(0,len(keys),100):
            batch=keys[offset:offset+100]
            for source_id,rid in db.execute('SELECT source_id,record_id FROM native_history_map WHERE source_id IN ('+','.join('?' for _ in batch)+')',batch):
                covered.add(source_ids[source_id]);refs.add(rid)
    return {'complete':covered==wanted,'record_ids':sorted(refs)}
