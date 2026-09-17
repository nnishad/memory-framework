"""Service-owned ingestion loop: fair historical/live passes and persistent retry times."""
import hashlib
import json
import logging
import threading
import time
from pathlib import Path

from .gmail import GmailAdapter
from .source_sdk import AdapterError, connection_context
from .source_secrets import SecretStore
from .source_sync import SourceSync, SyncWorker
from .common import digest, now

LOG=logging.getLogger(__name__)


class SourceRuntime:
    def __init__(self, store, data_dir, config=None, adapter=None, adapters=()):
        self.store=store; self.config=config or {}
        self.secrets=SecretStore(Path(data_dir)/'source-secrets.json')
        self.adapter=adapter or GmailAdapter()
        self.sync=SourceSync(store,{'google.gmail':self.adapter},self.secrets)
        for installed in adapters:
            if installed.spec()['adapter_id']=='google.gmail':
                raise ValueError('Pass a Gmail replacement with adapter=, not adapters=')
            self.sync.register(installed)
        self.worker=SyncWorker(self.sync)
        self.stop=threading.Event();self.sync.cancel_event=self.stop
        self.thread=None;self.start_lock=threading.Lock();self.tick_lock=threading.Lock()
        self.discovered={}

    def start(self):
        if self.config.get('enabled',True) is False:return
        with self.start_lock:
            if self.thread and self.thread.is_alive():return
            self.stop.clear()
            self.thread=threading.Thread(target=self._run,name='memory-source-sync',daemon=True)
            self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:self.thread.join(timeout=25)

    def connect_gmail(self, *, credentials, after=None, retention='archive', poll_seconds=300):
        if not isinstance(credentials,dict) or set(credentials)-{'client_id','client_secret','refresh_token','scope'}:
            raise ValueError('Expected desktop OAuth refresh credentials')
        for field in ('client_id','client_secret','refresh_token'):
            if not isinstance(credentials.get(field),str) or not 1<=len(credentials[field])<=8192:
                raise ValueError('Missing or invalid OAuth credential')
        if type(poll_seconds) is not int or not 60<=poll_seconds<=86400:
            raise ValueError('Polling interval must be 60..86400 seconds')
        if after is not None:
            from datetime import date
            after=date.fromisoformat(after).isoformat()
        raw=json.dumps(credentials)
        context=connection_context(connection_id='oauth-check',source='gmail-check',secrets=lambda ref:raw)
        context['secret_ref']='oauth-check'
        profile=self.adapter.check(context)
        source='gmail-'+digest(profile['account_id'].casefold())[:20]
        ref=self.secrets.put(source,raw)
        scope={'account_id':profile['account_id'],'initial_history':profile['history_id'],
               'after':after,'poll_seconds':poll_seconds}
        with self.store.connect() as db:
            old=db.execute('SELECT scope FROM source_connections WHERE adapter_id=? AND source=?',('google.gmail',source)).fetchone()
            if old and json.loads(old[0]).get('after')==after:
                scope['initial_history']=json.loads(old[0])['initial_history']
        result=self.sync.configure(adapter_id='google.gmail',source=source,scope=scope,
                                   retention=retention,secret_ref=ref)
        self.start()
        return {**result,'messages_total':profile['messages_total'],'historical_scope':after or 'all accessible mail',
                'poll_seconds':poll_seconds,'excluded_labels':['SPAM','TRASH'],'retention':retention}

    def status(self, connection_id=None):
        with self.store.connect() as db:
            ids=[r[0] for r in db.execute('SELECT id FROM source_connections ORDER BY created_at')]
        if connection_id:
            if connection_id not in ids:raise ValueError('Unknown connection')
            ids=[connection_id]
        values=[]
        for cid in ids:
            status=self.sync.status(cid);status.pop('secret_ref',None)
            with self.store.connect() as db:
                status['stored_messages']=db.execute("SELECT count(*) FROM source_heads WHERE connection_id=? AND record_id!=''",(cid,)).fetchone()[0]
                status['jobs']={r[0]:r[1] for r in db.execute('SELECT state,count(*) FROM source_jobs WHERE connection_id=? GROUP BY state',(cid,))}
                status['schedule']=[dict(r) for r in db.execute('SELECT role,next_at,failures,error FROM source_schedule WHERE connection_id=?',(cid,))]
            status['memory_processing']='Canonical evidence is retrievable; semantic indexing, Hindsight, and consolidation follow the host configuration and may lag capture.'
            values.append(status)
        return {'connections':values,'worker_running':bool(self.thread and self.thread.is_alive())}

    def control(self, *, connection_id, action):
        if action not in ('pause','resume','disconnect','retry'):raise ValueError('Unsupported source action')
        if action=='retry':
            with self.store.lock,self.store.connect() as db:
                self.sync._connection(db,connection_id)
                db.execute('DELETE FROM source_schedule WHERE connection_id=?',(connection_id,))
                db.execute("UPDATE source_jobs SET state='pending',available_at=?,attempts=0 WHERE connection_id=? AND state='quarantined'",(now(),connection_id))
            result={'connection_id':connection_id,'retry_queued':True}
        else:result=getattr(self.sync,action)(connection_id)
        if action in ('resume','retry'):
            # A resumed connection must not wait behind the last successful
            # discovery interval before it can make progress again.
            self._schedule(connection_id, 'discovery', 0)
            self.discovered.pop(connection_id, None)
            self.start()
        return result

    def _schedule(self, cid, role, delay, error=None):
        with self.store.lock,self.store.connect() as db:
            if error:
                import random
                previous=db.execute('SELECT failures FROM source_schedule WHERE connection_id=? AND role=?',(cid,role)).fetchone()
                delay=max(delay,min(3600,30*2**min(previous[0] if previous else 0,7)))+random.uniform(0,5)
            db.execute('INSERT INTO source_schedule VALUES(?,?,?,?,?) ON CONFLICT(connection_id,role) DO UPDATE SET next_at=excluded.next_at,failures=CASE WHEN excluded.error IS NULL THEN 0 ELSE source_schedule.failures+1 END,error=excluded.error',
                       (cid,role,time.time()+delay,1 if error else 0,error))

    def _due(self,cid,role):
        with self.store.connect() as db:
            row=db.execute('SELECT next_at FROM source_schedule WHERE connection_id=? AND role=?',(cid,role)).fetchone()
        return not row or row[0]<=time.time()

    def _declarations(self, cid, connection, adapter):
        """Resolve validated adapter declarations exactly once per refresh interval.

        Honors a persisted backoff even when nothing is cached, invalidates the cache
        when the connection configuration or credentials change (generation,
        scope hash or secret reference), and treats a discovery auth failure like a
        read failure by parking the connection.
        """
        fingerprint=(connection.get('generation'),connection.get('scope_hash'),connection.get('secret_ref'))
        cached=self.discovered.get(cid)
        if not self._due(cid,'discovery'):
            if cached is None:return None                 # honor a persisted backoff
            if cached[0]==fingerprint:return cached[1]     # valid cache, not yet due
            # A changed configuration or credential rotates the fingerprint and must
            # force a refresh even while the previous discovery interval still holds.
        try:
            streams=adapter.discover(self.sync.context(connection))
        except AdapterError as error:
            if error.kind=='auth':self.sync._set_state(cid,'needs_auth')
            self._schedule(cid,'discovery',60,error.message)
            return None
        except Exception as error:
            self._schedule(cid,'discovery',60,type(error).__name__+': source discovery failed')
            return None
        self.discovered[cid]=(fingerprint,streams)
        # Discovery may contact an upstream service for dynamic streams or
        # partitions. Keep it independent from the one-second supervisor loop and
        # refresh it at the connection's normal poll cadence.
        discovery_delay=min(max(int(connection['scope'].get('poll_seconds',300)),60),3600)
        self._schedule(cid,'discovery',discovery_delay)
        return streams

    def _recover_cursor(self,cid):
        # Reserve a fresh live anchor before restarting a converging historical scan.
        profile=self.sync.verify(cid)
        with self.store.lock,self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=self.sync._connection(db,cid);scope=json.loads(row['scope'])
            scope['initial_history']=profile['history_id'];raw=json.dumps(scope,sort_keys=True)
            db.execute('UPDATE source_connections SET scope=?,scope_hash=?,generation=generation+1 WHERE id=?',(raw,digest(raw),cid))
            db.execute('UPDATE source_streams SET cursor=NULL,scan=scan+1,state_version=state_version+1,lease_owner=NULL,lease_until=NULL WHERE connection_id=?',(cid,))
            db.execute('DELETE FROM source_schedule WHERE connection_id=?',(cid,))
            db.execute("INSERT INTO source_coverage(connection_id,stream,partition,generation,start,end,state,note,created_at) VALUES(?,'messages','',?,'unknown','unknown','gap','Expired Gmail history; rescan cannot recover permanently deleted mail',?)",(cid,row['generation']+1,now()))

    def tick(self):
        if not self.tick_lock.acquire(blocking=False):return
        try:
            with self.store.connect() as db:
                ids=[r[0] for r in db.execute("SELECT id FROM source_connections WHERE state='active'")]
            for cid in ids:
                connection=self.sync.connection(cid)
                adapter=self.sync.registry.get(connection['adapter_id'])
                if adapter is None:
                    continue
                streams=self._declarations(cid, connection, adapter)
                if streams is None:continue
                passes=[(stream['stream_id'], partition, role)
                        for stream in streams
                        for partition in ([part['id'] for part in stream.get('partitions',[])] or [''])
                        for role in stream['modes']
                        if role in ('incremental','backfill') or
                        (role=='reconcile' and connection['scope'].get('reconcile_seconds',0)>=60)]
                passes.sort(key=lambda item: ({'incremental':0,'backfill':1,'reconcile':2}[item[2]],
                                              item[0], item[1]))
                for stream, partition, role in passes:
                    if self.stop.is_set():return
                    schedule_key=(role if connection['adapter_id']=='google.gmail'
                                  and stream=='messages' and not partition else
                                  json.dumps([stream,partition,role],separators=(',',':')))
                    if not self._due(cid,schedule_key):continue
                    state=self.sync.stream_state(cid,stream,partition=partition,role=role)
                    if role=='backfill' and (state['cursor'] or {}).get('done'):continue
                    if role=='reconcile':
                        historical=self.sync.stream_state(cid,stream,partition=partition,role='backfill')
                        if not (historical['cursor'] or {}).get('done'):continue
                        if (state['cursor'] or {}).get('done'):
                            self.sync.restart_stream(cid,stream=stream,partition=partition,role=role)
                    try:
                        if role=='incremental' and connection['adapter_id']=='google.gmail' and not (state['cursor'] or {}).get('offset') and not (state['cursor'] or {}).get('page'):
                            self.sync.verify(cid)
                        result=self.worker.run_once(cid,stream=stream,partition=partition,role=role,ttl=900,declarations=streams)
                        status=result['status']
                        if status=='resync_required':
                            if connection['adapter_id']=='google.gmail':
                                self._recover_cursor(cid);continue
                            self._schedule(cid,schedule_key,3600,'Source cursor requires explicit rescan')
                            continue
                        if status in ('retry','failed','needs_auth'):
                            self._schedule(cid,schedule_key,max(30,result.get('retry_after') or 60),result.get('reason','Source sync failed'))
                        else:
                            cursor=self.sync.stream_state(cid,stream,partition=partition,role=role)['cursor'] or {}
                            if role=='incremental' and not cursor.get('page') and not cursor.get('offset'):
                                delay=connection['scope'].get('poll_seconds',300)
                            elif role=='reconcile' and cursor.get('done'):
                                delay=connection['scope']['reconcile_seconds']
                            else:delay=0
                            self._schedule(cid,schedule_key,delay)
                    except AdapterError as error:
                        if error.kind=='auth':self.sync._set_state(cid,'needs_auth')
                        self._schedule(cid,schedule_key,60,error.message)
                    except Exception as error:
                        self._schedule(cid,schedule_key,60,type(error).__name__+': source pass failed')
                if not self.stop.is_set():self._attachment(cid)
        finally:self.tick_lock.release()

    def _attachment(self,cid):
        try:job=self.sync.claim_job(self.worker.owner,kinds=('attachment',),connection_id=cid,ttl=300)
        except ValueError:return
        if not job:return
        try:
            from . import blobs
            from .source_sdk import AdapterError
            descriptor=job['payload'];rid=descriptor['record_id']
            with self.store.connect() as db:
                live=db.execute('SELECT 1 FROM records WHERE id=? AND deleted=0 AND NOT EXISTS(SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1)',(rid,rid)).fetchone()
            if not live:
                self.sync.complete_job(job,{'skipped':'evidence retired'});return
            connection=self.sync.connection(cid)
            adapter=self.sync.registry[connection['adapter_id']]
            raw=adapter.attachment(self.sync.context(connection),descriptor)
            if len(raw)!=descriptor.get('size',len(raw)):
                raise ValueError('Attachment size does not match the source descriptor')
            checksum=hashlib.sha256(raw).hexdigest()
            import base64
            with self.store.lock:
                with self.store.connect() as db:
                    self.sync._job_guard(db,job,'leased')
                    if db.execute('SELECT 1 FROM record_visibility WHERE record_id=? AND hidden=1',(rid,)).fetchone():
                        raise ValueError('Evidence was retired during attachment fetch')
                blob=blobs.begin(self.store,record_id=rid,filename=descriptor['filename'][:255],
                                 mime=descriptor['mime'],size=len(raw),sha256=checksum)
                if blob['state']!='complete':
                    for index,start in enumerate(range(0,len(raw),blobs.CHUNK_SIZE)):
                        if self.stop.is_set():raise AdapterError('temporary','Source worker is stopping')
                        if index in blob['received_chunks']:continue
                        blobs.put(self.store,blob_id=blob['id'],index=index,data=base64.b64encode(raw[start:start+blobs.CHUNK_SIZE]).decode())
                    blobs.complete(self.store,blob_id=blob['id'])
                self.sync.complete_job(job,{'blob_id':blob['id'],'stored':True,'text_extraction':'not performed'})
        except Exception as error:
            try:self.sync.fail_job(job,reason=type(error).__name__+': attachment processing failed',retry_after=60,quarantine=job['attempts']>=4)
            except ValueError:pass

    def _run(self):
        while not self.stop.is_set():
            try:self.tick()
            except Exception as error:
                LOG.error('Source worker loop failed: %s',type(error).__name__)
            self.stop.wait(1)
