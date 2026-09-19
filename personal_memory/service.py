"""Shared application boundary for production ASGI and the local test transport."""
import hmac
import logging
import threading
import time
from pathlib import Path

from .backend import load_backend
from .ingestion import RECORD_SCHEMA, ContractError, validate_batch
from .store import Store
from .trace import new_trace, get_trace, bind_trace, log_call, sanitize_trace

LOG = logging.getLogger(__name__)

READ_PATHS={"/v1/search","/v1/evidence","/v1/entity","/v1/entities","/v1/timeline",
            "/v1/browse","/v1/connections","/v1/status","/v1/generation","/v1/health","/v1/ready","/v1/ingestion-schema","/v1/recall","/v1/investigate","/v1/learning/browse"}
READ_PATHS.update({'/v1/beliefs','/v1/graph','/v1/aggregate','/v1/tasks','/v1/workflow','/v1/quality','/v1/domain/coverage','/v1/procedures','/v1/summaries','/v1/intelligence-schema'})
READ_PATHS.update({"/v1/native-state","/v1/epoch"})
READ_PATHS.discard("/v1/entity")
READ_PATHS.add("/v1/lineage/status")
READ_PATHS.add("/v1/source/status")
READ_PATHS.update({"/v1/learning/active","/v1/blob/list","/v1/blob/read"})
READ_PATHS.add("/v1/curated/read")
READ_PATHS.update({"/v1/changes/read", "/v1/awareness/status"})
AGENT_WRITES={"/v1/ingest","/v1/entity","/v1/claim","/v1/identity","/v1/identity-revoke","/v1/learning/outcome","/v1/learning/propose"}
AGENT_WRITES.update({'/v1/snapshot','/v1/belief','/v1/relation','/v1/measurement','/v1/task','/v1/task/transition','/v1/feedback'})
AGENT_WRITES.update({"/v1/awareness/prepare", "/v1/awareness/exposed"})
AWARENESS_QUEUE = {"/v1/awareness/claim", "/v1/awareness/complete", "/v1/awareness/defer"}


def _lease(args):
    """Rebuild a queue lease from call arguments; membership never travels back."""
    return {field: args[field] for field in ("batch_id", "owner", "fence") if field in args}


class AccessDenied(PermissionError): pass


class MemoryService:
    def __init__(self,data_dir,token,retrieval_config=None,backend=None,principals=None,extension_schemas=None,intelligence_config=None,source_config=None,source_adapters=()):
        # Startup is transactional: closable resources are tracked in construction
        # order and, if any later stage fails, unwound in reverse through the normal
        # shutdown path, so a failed service never leaks the indexing lease or a
        # worker against this data directory.
        self._closables=[]
        try:
            self._construct(data_dir,token,retrieval_config=retrieval_config,backend=backend,principals=principals,
                            extension_schemas=extension_schemas,intelligence_config=intelligence_config,
                            source_config=source_config,source_adapters=source_adapters)
        except BaseException:
            self._release_failed_startup()
            raise

    def _release_failed_startup(self):
        # The startup exception is the diagnosis; cleanup must never mask it.
        # Individual close failures are logged but do not replace the cause.
        try:
            self.close()
        except Exception as error:
            LOG.warning("Startup cleanup failed for a resource: %s", error)

    def _construct(self,data_dir,token,retrieval_config=None,backend=None,principals=None,extension_schemas=None,intelligence_config=None,source_config=None,source_adapters=()):
        if not isinstance(token,str) or len(token)<32: raise ValueError("Admin token must contain at least 32 characters")
        self.principals=[{"token":token,"role":"admin"}]+list(principals or [])
        seen=set()
        for principal in self.principals:
            key=principal.get("token")
            if not isinstance(key,str) or len(key)<32 or key in seen: raise ValueError("Credentials must be unique and at least 32 characters")
            seen.add(key)
            capabilities=principal.get('capabilities',[])
            if not isinstance(capabilities,list) or any(not isinstance(c,str) or not c for c in capabilities):raise ValueError('Capabilities must be an explicit list of names')
            if principal.get('role')=='executor' and not capabilities:raise ValueError('Executor requires explicit capabilities')
            if principal.get("role") not in {"admin","agent","reader","ingest","evaluator","executor","scheduler","awareness"}: raise ValueError("Unsupported credential role")
            if principal["role"]=="ingest" and (not isinstance(principal.get("sources"),list) or not principal["sources"] or
                    any(not isinstance(s,str) or not s for s in principal["sources"]) or
                    not isinstance(principal.get("connector_id"),str) or not principal["connector_id"]):
                raise ValueError("Ingest credentials require source scopes and connector_id")
        from .extensions import ExtensionRegistry
        self.extension_registry=ExtensionRegistry(extension_schemas)
        from .storage import database_path
        from .adaptive import AdaptiveRecall, validate_recall_config
        from .workflows import Workflows, validate_intelligence_config
        # Operator configuration is validated before any worker or lease exists, so
        # an invalid config cannot leave a half-started service holding ownership.
        validate_intelligence_config(intelligence_config)
        validate_recall_config((intelligence_config or {}).get("recall",{}))
        self.store=Store(database_path(data_dir,"memory"))
        # The service process is the sole owner of the durable indexing journals; a
        # second writer against the same data directory fails at startup.
        self.retrieval=load_backend(self.store,backend,retrieval_config,index_owner=True)
        self._closables.append(self.retrieval)
        from .learning import Learning
        self.learning=Learning(self.store)
        self.adaptive=AdaptiveRecall(self.store,self.retrieval,(intelligence_config or {}).get("recall",{}))
        from .intelligence import Intelligence
        self.intelligence=Intelligence(self.store)
        from .investigate import Investigation
        self.investigation=Investigation(self.store,self.retrieval,self.intelligence)
        self._closables.append(self.investigation)
        self.workflows=Workflows(self.store,intelligence_config)
        self._closables.append(self.workflows)
        self.learning_routes={"/v1/learning/active":self.learning.active,"/v1/learning/outcome":self.learning.outcome,"/v1/learning/propose":self.learning.propose,
                              "/v1/learning/evaluate":self.learning.evaluate,"/v1/learning/promote":self.learning.promote,
                              "/v1/learning/retract":self.learning.retract}
        self.learning_routes.update({
            '/v1/metric':self.intelligence.metric,'/v1/domain':self.intelligence.domain,'/v1/snapshot':self.intelligence.snapshot,'/v1/belief':self.intelligence.belief,
            '/v1/belief/resolve':self.intelligence.resolve_belief,'/v1/relation':self.intelligence.relation,
            '/v1/measurement':self.intelligence.measurement,'/v1/task':self.intelligence.task,
            '/v1/task/transition':self.intelligence.transition,'/v1/task/event/claim':self.intelligence.claim_event,
            '/v1/task/event/retry':self.intelligence.retry_event,'/v1/task/event/ack':self.intelligence.ack_event,'/v1/feedback':self.intelligence.feedback,
            '/v1/consolidation/accept':self.workflows.accept_consolidation,'/v1/workflow/enqueue':self.workflows.enqueue,'/v1/workflow/retry':self.workflows.retry,
            '/v1/suite':self.workflows.suite,'/v1/procedure':self.workflows.procedure,
            '/v1/procedure/execute':self.workflows.execute})
        self.started=time.monotonic();self.counts={"requests":0,"errors":0};self.lock=threading.Lock()
        self.routes={
            "/v1/checkpoint":lambda a:self.store.checkpoint(**a),
            "/v1/ingest":self.ingest,"/v1/ingestion-schema":lambda a:RECORD_SCHEMA,
            "/v1/search":lambda a:self.retrieval.search(**a),
            "/v1/recall":lambda a:self.adaptive.search(**a),
            "/v1/investigate":lambda a:self.investigation.search(**a),
            "/v1/learning/browse":lambda a:self.learning.browse(**a),
            "/v1/evidence":lambda a:self.store.evidence(**a),
            "/v1/lineage/status":lambda a:self.store.lineage_status(**a),
            "/v1/source/status":lambda a:self.store.source_status(**a),
            "/v1/forget-source":lambda a:self.store.forget_source(**a),
            "/v1/supersede":lambda a:self.store.supersede(**a),
            "/v1/entity":lambda a:self.store.entity(**a),"/v1/entities":lambda a:self.store.entities(**a),
            "/v1/claim":lambda a:self.store.claim(**a),"/v1/timeline":lambda a:self.store.timeline(**a),
            "/v1/browse":lambda a:self.store.browse(**a),"/v1/connections":lambda a:self.store.connections(**a),
            "/v1/identity":lambda a:self.store.identity(**a),"/v1/identity-revoke":lambda a:self.store.identity_revoke(**a),
            "/v1/forget":lambda a:self.store.forget(**a),"/v1/coverage":lambda a:self.store.coverage(**a),
            "/v1/status":lambda a:self.status(),
            "/v1/generation":lambda a:self.store.generation(),
            "/v1/health":lambda a:self.health(),"/v1/ready":lambda a:self.ready()}

        from . import blobs
        self.routes.update({path:(lambda a,fn=fn:fn(self.store,**a)) for path,fn in {
            '/v1/blob/begin':blobs.begin,'/v1/blob/put':blobs.put,'/v1/blob/complete':blobs.complete,
            '/v1/blob/read':blobs.read,'/v1/blob/list':blobs.listing}.items()})
        from . import reset
        from . import curated
        reset.initialize(self.store)
        self.workflows.start()
        self.routes["/v1/reset"]=lambda a:reset.reset(self.store,backend=self.retrieval,**a)
        self.routes["/v1/epoch"]=lambda a:{"epoch":reset.epoch(self.store)}
        self.routes["/v1/curated/read"]=lambda a:curated.read(self.store,**a)
        self.routes["/v1/curated/apply"]=None
        self.routes["/v1/curated/reset"]=None
        from . import native_catalog
        native_catalog.initialize(self.store)
        self.routes['/v1/native-state']=lambda a:native_catalog.read(self.store,**a)
        self.hub_reads={'attachments':'/v1/blob/list','native_state':'/v1/native-state','beliefs':'/v1/beliefs','graph':'/v1/graph','aggregate':'/v1/aggregate','tasks':'/v1/tasks','workflow':'/v1/workflow','quality':'/v1/quality','coverage':'/v1/domain/coverage','procedures':'/v1/procedures','summaries':'/v1/summaries','schema':'/v1/intelligence-schema'}
        self.hub_writes={'snapshot':'/v1/snapshot','belief':'/v1/belief','relation':'/v1/relation','measurement':'/v1/measurement','task':'/v1/task','transition':'/v1/task/transition','feedback':'/v1/feedback','consolidate':'/v1/workflow/enqueue'}
        self.routes.update({'/v1/intelligence/read':None,'/v1/intelligence/write':None})
        self.routes.update({path:None for path in self.learning_routes})
        self.routes.update({'/v1/intelligence-schema':lambda a:self.intelligence_schema(),'/v1/summaries':lambda a:self.intelligence.summaries(**a),'/v1/procedures':lambda a:self.intelligence.procedures(**a),'/v1/domain/coverage':lambda a:self.intelligence.coverage(**a),'/v1/beliefs':lambda a:self.intelligence.beliefs(**a),
            '/v1/graph':lambda a:self.intelligence.graph(**a),'/v1/aggregate':lambda a:self.intelligence.aggregate(**a),
            '/v1/tasks':lambda a:self.intelligence.tasks(**a),'/v1/workflow':lambda a:self.workflows.get(**a),
            '/v1/quality':lambda a:{**self.intelligence.quality(),'workflows':self.workflows.status()}})
        from .source_runtime import SourceRuntime
        self.sources=SourceRuntime(self.store,data_dir,source_config,adapters=source_adapters,
                                   extraction_config=(retrieval_config or {}).get('attachment_extraction'))
        self._closables.append(self.sources)
        self.routes.update({'/v1/sources/status':lambda a:self.sources.status(**a),
                            '/v1/sources/gmail/connect':lambda a:self.sources.connect_gmail(**a),
                            '/v1/sources/control':lambda a:self.sources.control(**a)})
        from . import changes, awareness
        self.routes['/v1/changes/read']=None  # credential-derived principal: handled in _dispatch
        self.routes['/v1/awareness/configure']=None  # admin-only: handled in _dispatch
        self.routes['/v1/awareness/status']=lambda a:awareness.status(self.store)
        self.routes['/v1/awareness/claim']=lambda a:awareness.claim(self.store, **a)
        self.routes['/v1/awareness/complete']=lambda a:awareness.complete(
            self.store, _lease(a), summary=a.get('summary'),
            citations=a.get('citations', ()), proposals=a.get('proposals', ()))
        self.routes['/v1/awareness/defer']=lambda a:awareness.defer(
            self.store, _lease(a), reason=a.get('reason', ''), retry_in=a.get('retry_in', 60))
        self.routes['/v1/awareness/prepare']=lambda a:(awareness.sweep(self.store, a["consumer_id"]),
                                                       awareness.prepare(self.store, **a))[1]
        self.routes['/v1/awareness/exposed']=lambda a:awareness.expose(self.store, **a)
        self.hub_reads['sources']='/v1/sources/status'
        self.hub_reads['changes']='/v1/changes/read'
        self.hub_reads['awareness_status']='/v1/awareness/status'
        with self.store.connect() as db:
            active=db.execute("SELECT 1 FROM source_connections WHERE state='active' LIMIT 1").fetchone()
        if active:self.sources.start()

    def intelligence_schema(self):
        import inspect
        methods={**self.learning_routes,
            '/v1/sources/status':self.sources.status,
            '/v1/beliefs':self.intelligence.beliefs,'/v1/graph':self.intelligence.graph,
            '/v1/aggregate':self.intelligence.aggregate,'/v1/tasks':self.intelligence.tasks,
            '/v1/workflow':self.workflows.get,'/v1/domain/coverage':self.intelligence.coverage,
            '/v1/procedures':self.intelligence.procedures,'/v1/summaries':self.intelligence.summaries}
        from functools import partial
        from .native_catalog import read
        methods['/v1/native-state']=partial(read,self.store)
        from . import blobs
        for name,fn in {'list':blobs.listing,'read':blobs.read,'begin':blobs.begin,'put':blobs.put,'complete':blobs.complete}.items():
            methods['/v1/blob/'+name]=partial(fn,self.store)
        from . import awareness, changes
        methods['/v1/changes/read']=partial(changes.read,self.store)
        methods['/v1/awareness/status']=partial(awareness.status,self.store)
        result={}
        for path,method in methods.items():
            required=[];optional={}
            for name,parameter in inspect.signature(method).parameters.items():
                if name in {'actor','principal'}:continue
                if parameter.default is inspect.Parameter.empty:required.append(name)
                else:optional[name]=parameter.default
            result[path]={'required':required,'optional_defaults':optional,'additional_parameters':False}
        return {'version':'1.0','endpoints':result,'authority':'Credentials and session policy are checked separately; schema discovery grants no write or execution permission.'}

    def authenticate(self,authorization):
        matched=None
        for principal in self.principals:
            if hmac.compare_digest(authorization.encode(),("Bearer "+principal["token"]).encode()):matched=principal
        if matched is None:raise AccessDenied("Authentication required")
        return matched

    def authorize(self,principal,path,args):
        role=principal["role"]
        if role=="admin":return
        if path=='/v1/sources/status' and role in {'agent','reader'}:return
        if role=="agent" and path=="/v1/curated/apply":return
        if role=="evaluator" and path in READ_PATHS|{"/v1/learning/evaluate"}:return
        if role=='scheduler' and path in {'/v1/tasks','/v1/task/event/claim','/v1/task/event/ack'}:return
        if role=='awareness' and path in AWARENESS_QUEUE:return
        if role in {'agent','executor'} and path=='/v1/procedure/execute':
            with self.store.connect() as db:
                procedure=self.learning._get(db,args.get('procedure_id'),'procedure')
                if procedure['payload']['capability'] in principal.get('capabilities',[]):return
            raise AccessDenied('Capability outside executor scope')
        if role in {'agent','evaluator'} and path=='/v1/workflow/enqueue':
            if args.get('type')==('consolidate' if role=='agent' else 'evaluate'):return
            raise AccessDenied('Workflow type outside role scope')
        if role=="reader" and path in READ_PATHS:return
        if role=="agent" and path in READ_PATHS|AGENT_WRITES:return
        if role=="ingest":
            if path in {'/v1/blob/begin','/v1/blob/put','/v1/blob/complete'}:
                rid=args.get('record_id')
                if path!='/v1/blob/begin':
                    with self.store.connect() as db:
                        row=db.execute('SELECT record_id FROM memory_blobs WHERE id=?',(args.get('blob_id'),)).fetchone()
                        if not row:raise AccessDenied('Unknown attachment')
                        rid=row[0]
                evidence=self.store.evidence(rid)
                if (('*' in principal['sources'] or evidence['source'] in principal['sources']) and
                    evidence.get('ingestion_record',{}).get('provenance',{}).get('connector_id')==principal['connector_id']):return
                raise AccessDenied('Attachment outside connector source scope')
            if path=="/v1/ingestion-schema":return
            if path=="/v1/checkpoint":
                if args.get("connector_id")==principal["connector_id"] and ("*" in principal["sources"] or args.get("source") in principal["sources"]):return
                raise AccessDenied("Checkpoint outside credential scope")
            if path=="/v1/ingest":
                records=validate_batch(args.get("items"))
                scopes=principal["sources"]
                for record in records:
                    if ("*" not in scopes and record["source"] not in scopes) or record["provenance"]["connector_id"]!=principal["connector_id"]:
                        raise AccessDenied("Connector/source outside credential scope")
                    for parent in record["provenance"]["parent_record_ids"]:
                        try:source=self.store.evidence(parent)["source"]
                        except ValueError:raise AccessDenied("Evidence outside credential scope") from None
                        if "*" not in scopes and source not in scopes:raise AccessDenied("Evidence outside credential scope")
                return
        raise AccessDenied("Operation outside credential scope")

    def ingest(self,args):
        if "items" not in args or set(args)-{"items","checkpoint","epoch"}:raise ContractError("$","Expected items and optional checkpoint")
        from .reset import epoch
        if 'epoch' in args and (type(args['epoch']) is not int or args['epoch']!=epoch(self.store)):
            raise ValueError('Memory epoch changed; stale capture rejected')
        records=validate_batch(args["items"])
        self.extension_registry.validate(records)
        with self.store.lock:
            if 'epoch' in args and args['epoch']!=epoch(self.store):raise ValueError('Memory epoch changed; stale capture rejected')
            result = self.store.ingest_contract(records, checkpoint=args.get("checkpoint"))
        from .native_catalog import observe
        for item, receipt in zip(records, result['records']):
            observe(self.store,item,receipt)
            metadata = item.get('extensions', {}).get('personal_memory.legacy', {}).get('data', {})
            event = metadata.get('host_event')
            if (item['source'] == 'hermes-host-events' and item['provenance']['connector_id'] == 'hermes.native'
                    and event in {'review_change', 'skill_change', 'cron_completed'} and metadata.get('chunk_offset') == 0):
                self.learning.outcome(key='host-event/' + receipt['id'], goal='Observe native Hermes ' + event,
                    action=event, result=item['text'][:11900] + (' [truncated; inspect event chunks]' if len(item['text']) > 11900 else ''),
                    outcome='unknown', evidence_ids=[receipt['id']], actor='native-host-observer')
        return result

    def dispatch(self, path, args, principal, trace_id=None):
        # The intelligence hub below resolves to a concrete route via a nested dispatch; that
        # inner call sees an already-bound trace and defers to _dispatch, so one external call
        # produces exactly one summary line carrying one trace id.
        if get_trace():
            return self._dispatch(path, args, principal)
        with bind_trace(sanitize_trace(trace_id) or new_trace()):
            started = time.monotonic()
            try:
                result = self._dispatch(path, args, principal)
            except Exception as error:
                log_call(logging.WARNING, path, started, ok=False, role=principal.get("role"),
                         error=type(error).__name__)
                raise
            fields = self._summary(result) if LOG.isEnabledFor(logging.INFO) else {}
            log_call(logging.INFO, path, started, role=principal.get("role"), **fields)
            return result

    @staticmethod
    def _summary(result):
        """Cheap, level-guarded outcome metrics so a single INFO line is enough to triage."""
        if not isinstance(result, dict):
            return {}
        out = {}
        if isinstance(result.get("episodes"), list): out["episodes"] = len(result["episodes"])
        if isinstance(result.get("records"), list): out["records"] = len(result["records"])
        if "retrieval_status" in result: out["status"] = result["retrieval_status"]
        diag = result.get("diagnostics")
        if isinstance(diag, dict):
            cand = diag.get("candidates")
            if isinstance(cand, dict) and cand: out["cand"] = sum(v for v in cand.values() if isinstance(v, int))
            if diag.get("failures"): out["failures"] = len(diag["failures"])
            if isinstance(diag.get("elapsed_ms"), int): out["rt_ms"] = diag["elapsed_ms"]
        return out

    def _dispatch(self,path,args,principal):
        if path in {'/v1/intelligence/read','/v1/intelligence/write'}:
            read=path.endswith('/read')
            if read and 'arguments' not in args: args={**args,'arguments':{}}
            if set(args)!={'operation','arguments'} or not isinstance(args['arguments'],dict):
                raise ContractError('$','Expected exactly operation and arguments'
                                    +('; a read may omit arguments' if read else ''))
            operations=self.hub_reads if read else self.hub_writes
            target=operations.get(args['operation']) if isinstance(args['operation'],str) else None
            if target is None:
                raise ContractError('$.operation','Unknown intelligence operation; the schema operation lists them')
            return self.dispatch(target,args['arguments'],principal)
        if path not in self.routes and path not in self.learning_routes:raise KeyError("Unknown endpoint")
        self.authorize(principal,path,args)
        from .reset import pending
        if path not in {'/v1/reset','/v1/health','/v1/status','/v1/ready','/v1/epoch'} and pending(self.store):
            raise ValueError('Canonical reset is incomplete; retry reset or restart service before using memory')
        if path=='/v1/ingest' and principal['role']=='agent':
            from .reset import epoch
            if epoch(self.store)>0 and 'epoch' not in args:raise AccessDenied('Agent writes require the current memory epoch')
        with self.lock:self.counts["requests"]+=1
        try:
            if path in {"/v1/curated/apply", "/v1/curated/reset"}:
                from . import curated
                if "actor" in args:raise ValueError("Actor is credential-derived")
                from .common import digest
                fn=curated.apply if path.endswith("/apply") else curated.reset
                return fn(self.store, **args, actor=digest(principal["token"]))
            if path=="/v1/changes/read":
                from .common import digest
                if "principal" in args:raise ValueError("Principal is credential-derived")
                from . import changes
                return changes.read(self.store,principal=digest(principal["token"]),**args)
            if path=="/v1/awareness/configure":
                from . import awareness, changes
                if "journal" in args:return changes.configure(self.store,args)
                if "replay" in args:return awareness.replay(self.store,**args["replay"])
                return awareness.configure_consumer(self.store,**args)
            if path in self.learning_routes:
                if 'actor' in args:raise ValueError('Actor is credential-derived')
                from .common import digest
                actor=digest(principal['token'])
                return self.learning_routes[path](**args,actor=actor)
            return self.routes[path](args)
        except Exception:
            with self.lock:self.counts["errors"]+=1
            raise

    def status(self):
        from . import __version__
        result=self.retrieval.status() if hasattr(self.retrieval,"status") else self.store.status()
        result["framework_version"]=__version__
        result["capabilities"]=list(dict.fromkeys(result.get("capabilities",[])+[
            "parallel_investigation", "capture_lineage", "source_revision_forgetting", "native_history_sync",
            "native_state_catalog", "native_file_sync", "run_bound_continuity", "evaluated_skill_export",
            "resumable_attachments", "journaled_reset", "source_adapters", "gmail_history_sync", "gmail_incremental_sync"]))
        result["capabilities"]=list(dict.fromkeys(result["capabilities"]+[
            "canonical_curated_memory", "versioned_native_memory_edits",
            "frozen_curated_prompt_snapshot", "durable_observation_receipts"]))
        from . import changes
        result["capabilities"]=list(dict.fromkeys(result["capabilities"]+[
            "memory_change_journal", "awareness_api"]))
        result["change_journal"]=changes.status(self.store)
        result["change_contract"]={"version":"1.0","read":"/v1/changes/read","configure":"/v1/awareness/configure",
            "cursors":"opaque; scoped to credential, filter digest and memory epoch",
            "acknowledgment":"reads never acknowledge; consumer leases land with awareness consumers"}
        result["native_memory_contract"]={"version":"1.0","targets":["memory","user"],
            "read":"/v1/curated/read","apply":"/v1/curated/apply","reset":"/v1/curated/reset",
            "concurrency":"expected_version","reset_fence":"epoch","batch":"atomic"}
        result["investigation_contract"]={"version":"1.0","max_branches":6,"max_concurrency":3,"model_planner":"Hermes"}
        return result

    def health(self):
        # A cheap live database read, no full archive scan or model call.
        generation=self.store.generation()
        with self.lock:counts=dict(self.counts)
        return {"live":True,"uptime_seconds":round(time.monotonic()-self.started),**generation,**counts}

    def ready(self):
        status=self.retrieval.status() if hasattr(self.retrieval,"status") else self.store.status()
        problems=list(status.get("initialization_errors",{}))
        from .reset import pending
        if pending(self.store):problems.append("canonical reset incomplete")
        for name in ("semantic","hindsight"):
            engine=status.get(name,{})
            if engine.get("error"):problems.append(name+": worker error")
            if engine.get("pending_records",0):problems.append(name+": indexing backlog")
            if engine.get("pending_retirements",0):problems.append(name+": retirement backlog")
            if engine.get("pending_deletions",0):problems.append(name+": deletion backlog")
            # A reset's external cleanup is durable and resumable, but the engine is not
            # finished until the remote bank is confirmed empty.
            if engine.get("pending_bank_clear"):problems.append(name+": external cleanup pending")
        return {"ready":not problems,"problems":problems,"generation":status["generation"],
                "workflows":self.workflows.status(),"meaning":"Configured service is operational; this does not certify source completeness or retrieval quality."}

    def close(self):
        # Reverse construction order. Each resource keeps its own shutdown rules, so
        # indexing ownership stays held until its writers have actually stopped, and
        # one failing close never skips the remaining resources. A resource whose
        # close failed stays tracked so a later close() retries it; safe to call on
        # a partially initialized service and safe to repeat.
        errors=[]
        retained=[]
        while getattr(self,"_closables",None):
            resource=self._closables.pop()
            close_fn=getattr(resource,"close",None)
            try:
                if close_fn is not None: close_fn()
            except Exception as error:
                errors.append(error)
                retained.append(resource)
        if retained: self._closables.extend(reversed(retained))
        if errors: raise errors[0]
