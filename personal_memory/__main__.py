import argparse
import functools
import json
import os
from pathlib import Path

from .client import Client
from .setup import install, rollback


def settings(home):
    from .configuration import load_settings
    return load_settings(home)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Personal Memory service and Hermes setup")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("setup", "serve", "status", "import-jsonl", "import-whatsapp", "import-email", "import-health", "forget", "coverage", "rollback", "doctor", "backup-keygen", "backup", "restore", "request", "sync-hermes-history", "sync-hermes-files", "export-native-skill", "attach", "reset", "prune-hindsight", "gmail-authorize", "gmail-connect", "sources-status", "sources-control", "awareness-run"):
        p = sub.add_parser(name)
        p.add_argument("--hermes-home", default=os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
        if name == 'gmail-authorize':
            p.add_argument('--client-secret',type=Path,required=True)
            p.add_argument('--credentials-file',type=Path,required=True,help='Private output file outside the repository')
        elif name == 'gmail-connect':
            p.add_argument('--credentials-file',type=Path,required=True)
            p.add_argument('--after',help='Optional YYYY-MM-DD cutoff; omitted imports all accessible history')
            p.add_argument('--retention',choices=['archive','mirror'],default='archive')
            p.add_argument('--poll-seconds',type=int,default=300)
        elif name == 'sources-status':
            p.add_argument('--connection-id')
        elif name == 'sources-control':
            p.add_argument('action',choices=['pause','resume','disconnect','retry'])
            p.add_argument('--connection-id',required=True)
        elif name == 'awareness-run':
            p.add_argument('--consumer-id',required=True)
            p.add_argument('--continuous',action='store_true',help='Poll for work and process batches continuously')
            p.add_argument('--poll-seconds',type=int,default=60)
            p.add_argument('--deliver',action='store_true',
                           help='Dispatch queued awareness notifications through the selected Hermes profile')
        elif name == "setup":
            p.add_argument("--port", type=int, default=8766)
            p.add_argument("--exclusive", action="store_true", help="Disable built-in memory files without deleting them")
            p.add_argument("--auto-consolidate",action="store_true",help="Enable local extractive consolidation of imported records")
            p.add_argument("--semantic", action="store_true", help="Compatibility flag; local multilingual embeddings are enabled by default")
        elif name == 'attach':
            p.add_argument('file',type=Path)
            p.add_argument('--record-id',required=True)
            p.add_argument('--mime',required=True)
        elif name == 'export-native-skill':
            p.add_argument('--candidate-id',required=True)
            p.add_argument('--name',required=True)
        elif name == 'sync-hermes-files':
            p.add_argument('--kind',action='append',choices=['skill','builtin_memory'])
            p.add_argument('--name',action='append')
        elif name == "sync-hermes-history":
            p.add_argument("file", type=Path, help="Native Hermes state.db; opened read-only")
            p.add_argument("--archive-id", required=True, help="Stable identity for this native archive")
            p.add_argument("--session-id", action="append", required=True, help="Explicit native session; repeat for additional sessions")
            p.add_argument("--lineage-file", type=Path, help="Trusted JSON: native message ID to canonical parent record IDs")
        elif name == "request":
            p.add_argument("endpoint")
            p.add_argument("file",type=Path,help="JSON request body; credentials are read from private settings")
            p.add_argument("--credential-role",choices=["admin","agent","evaluator","scheduler","executor"],default="admin")
        elif name == "serve":
            p.add_argument("--transport",choices=["asgi","reference"],default="asgi")
            p.add_argument("--backend", help="Trusted Python module:factory implementing search(**query)")
        elif name.startswith("import-"):
            p.add_argument("file", type=Path)
            if name != "import-jsonl": p.add_argument("--source", default={"import-whatsapp":"whatsapp","import-email":"email","import-health":"health"}[name])
            if name == "import-health":
                p.add_argument("--subject-id", help="Known entity ID; also creates typed measurements with unit validation")
            if name == "import-whatsapp":
                p.add_argument("--thread-id", required=True)
                p.add_argument("--date-order", required=True, choices=["DMY","MDY","YMD"])
                p.add_argument("--timezone", required=True)
                p.add_argument("--fold", type=int, choices=[0,1],default=0)
        elif name == "doctor":
            p.add_argument("--offline",action="store_true")
        elif name == "reset":
            p.add_argument("--scope",default="canonical",choices=["canonical"])
            p.add_argument("--confirm",action="store_true",help="Required: a canonical reset redacts all memory and clears the external engine")
        elif name == "prune-hindsight":
            p.add_argument("--apply",action="store_true",help="Remove stale instances; default is a dry-run report")
        elif name == "backup-keygen":
            p.add_argument("file",type=Path)
        elif name in {"backup","restore"}:
            p.add_argument("--key-file",type=Path,required=True)
            p.add_argument("--destination",type=Path,required=True)
            if name=="restore":
                p.add_argument("archive",type=Path)
                p.add_argument("--deletion-ledger",type=Path)
        elif name == "forget":
            p.add_argument("record_id")
            p.add_argument("--all-revisions", action="store_true", help="Forget this source item across existing and future revisions")
        elif name == "coverage":
            p.add_argument("source")
            p.add_argument("state", choices=["unknown", "partial", "complete", "failed"])
            p.add_argument("--through-at")
            p.add_argument("--note", default="")
    args = parser.parse_args(argv)
    if args.command == 'gmail-authorize':
        from .gmail_oauth import authorize
        from .common import atomic_json
        credentials=authorize(args.client_secret)
        atomic_json(args.credentials_file,credentials)
        args.credentials_file.chmod(0o600)
        result={'authorized':True,'credentials_file':str(args.credentials_file),'scope':'Gmail read-only'}
    elif args.command == 'awareness-run':
        from .awareness_worker import process_once, hermes_analyze, deliver_once, hermes_deliver
        from . import awareness
        from .backend import load_backend
        from .store import Store
        from .storage import database_path
        if not 5<=args.poll_seconds<=3600:raise ValueError('Poll interval must be 5..3600 seconds')
        cfg=settings(args.hermes_home)
        store=Store(database_path(cfg['data_dir'],'memory'))
        analyze=functools.partial(hermes_analyze, hermes_home=cfg['_hermes_home'])
        dispatch=functools.partial(hermes_deliver, hermes_home=cfg['_hermes_home'])
        retrieval=None
        def current_retrieval():
            nonlocal retrieval
            if retrieval is None:
                retrieval=load_backend(store,config=cfg.get('retrieval'))
            return retrieval
        if args.continuous:
            import time
            try:
                while True:
                    outcome=process_once(store,consumer_id=args.consumer_id,
                                         analyze=analyze,retrieval=current_retrieval)
                    if args.deliver:
                        awareness.reconcile_deliveries(store)
                        outcome['delivery']=deliver_once(store,dispatch=dispatch)
                    if outcome['state']!='complete' and outcome.get('delivery',{}).get('state') == 'idle':
                        time.sleep(args.poll_seconds)
            except KeyboardInterrupt:
                result={'state':'stopped'}
        else:
            result=process_once(store,consumer_id=args.consumer_id,analyze=analyze,
                                retrieval=current_retrieval)
            if args.deliver:
                awareness.reconcile_deliveries(store)
                result['delivery']=deliver_once(store,dispatch=dispatch)
    elif args.command in {'gmail-connect','sources-status','sources-control'}:
        cfg=settings(args.hermes_home)
        client=Client(cfg['url'],cfg['token'],timeout=120)
        if args.command=='gmail-connect':
            result=client.call('/v1/sources/gmail/connect',{'credentials':json.loads(args.credentials_file.read_text()),
                'after':args.after,'retention':args.retention,'poll_seconds':args.poll_seconds})
        elif args.command=='sources-status':
            result=client.call('/v1/sources/status',{'connection_id':args.connection_id})
        else:
            result=client.call('/v1/sources/control',{'connection_id':args.connection_id,'action':args.action})
    elif args.command == "setup":
        result = install(args.hermes_home, args.port, args.exclusive)
        if args.auto_consolidate:
            from .common import atomic_json
            path=Path(args.hermes_home).expanduser()/"personal-memory/settings.json"
            cfg=json.loads(path.read_text());cfg.setdefault('intelligence',{})['auto_consolidate']=True;atomic_json(path,cfg)
        if args.semantic:
            from .common import atomic_json
            cfg=settings(args.hermes_home)
            cfg.setdefault("retrieval", {})["semantic"]={"enabled":True}
            public_path=Path(args.hermes_home).expanduser()/"personal-memory"/"config.json"
            public=json.loads(public_path.read_text())
            public["retrieval"]=cfg["retrieval"]
            atomic_json(public_path,public)
    elif args.command == 'attach':
        from .blobs import upload
        cfg=settings(args.hermes_home)
        result=upload(Client(cfg['url'],cfg['token']),args.file,args.record_id,args.mime)
    elif args.command == 'export-native-skill':
        from .skill_export import export_skill
        cfg=settings(args.hermes_home)
        result=export_skill(args.hermes_home,Client(cfg['url'],cfg['token']),args.candidate_id,args.name)
    elif args.command == 'sync-hermes-files':
        from .native_files import sync_files
        cfg=settings(args.hermes_home)
        result=sync_files(args.hermes_home,Client(cfg['url'],cfg['token'],timeout=30),kinds=args.kind or ('skill','builtin_memory'),names=args.name)
    elif args.command == "sync-hermes-history":
        from .native_history import HermesHistoryConnector, sync_history
        from .asgi import strict_json
        cfg = settings(args.hermes_home)
        lineage = strict_json(args.lineage_file.read_bytes()) if args.lineage_file else None
        connector = HermesHistoryConnector(args.file, args.archive_id, args.session_id, lineage)
        result = sync_history(Client(cfg['url'], cfg['token'], timeout=30), connector,
                              Path(args.hermes_home).expanduser() / 'personal-memory/outbox.db')
    elif args.command == "request":
        cfg=settings(args.hermes_home)
        if not args.endpoint.startswith('/v1/') or '?' in args.endpoint or '#' in args.endpoint:raise ValueError('Expected a versioned endpoint path')
        if args.credential_role=='admin':token=cfg['token']
        else:
            candidates=[p['token'] for p in cfg.get('principals',[]) if p.get('role')==args.credential_role]
            if len(candidates)!=1:raise ValueError('Role must resolve to exactly one configured principal; use a dedicated client otherwise')
            token=candidates[0]
        from .asgi import strict_json
        payload=strict_json(args.file.read_bytes())
        if not isinstance(payload,dict):raise ValueError('Request body must be an object')
        result=Client(cfg['url'],token,timeout=30).call(args.endpoint,payload)
    elif args.command == "rollback":
        result = rollback(args.hermes_home)
    elif args.command=="doctor":
        from .operations import doctor
        result=doctor(args.hermes_home,args.offline)
    elif args.command=="backup-keygen":
        from .recovery import create_key
        result={"key_file":create_key(args.file)}
    elif args.command=="backup":
        from .recovery import backup
        result=backup(args.hermes_home,args.destination,args.key_file)
    elif args.command=="restore":
        from .recovery import restore
        result=restore(args.archive,args.key_file,args.destination,args.deletion_ledger)
    elif args.command=="reset":
        if not args.confirm:raise ValueError("A canonical reset redacts all memory and clears the external engine; re-run with --confirm")
        cfg=settings(args.hermes_home)
        result=Client(cfg['url'],cfg['token'],timeout=120).call('/v1/reset',{'scope':args.scope})
    elif args.command=="prune-hindsight":
        from .hindsight_runtime import prune_stale_instances
        cfg=settings(args.hermes_home)
        result=prune_stale_instances(cfg['data_dir'],apply=args.apply)
    elif args.command == "serve":
        from .server import create_server
        cfg = settings(args.hermes_home)
        if args.backend:cfg["backend"]=args.backend
        # Fresh database/WAL files should inherit private permissions.
        os.umask(0o077)
        # The service owns its process, so it configures the correlation logger here; one INFO
        # summary line is emitted per dispatch and shared trace ids become greppable.
        from .trace import configure_logging
        configure_logging()
        if args.transport=="asgi":
            import uvicorn
            from .asgi import Application
            uvicorn.run(Application(cfg),host="127.0.0.1",port=cfg["port"],workers=1,
                        limit_concurrency=32,timeout_keep_alive=5,timeout_graceful_shutdown=120,
                        access_log=False,proxy_headers=False,server_header=False)
            return
        from .hindsight_runtime import Runtime
        with Runtime(cfg):
            server = create_server(Path(cfg["data_dir"]), cfg["token"], port=cfg["port"], backend=args.backend,
                                   retrieval_config=cfg.get("retrieval",{}))
            print(json.dumps({"listening": cfg["url"], "provider": "personal-memory", "hindsight":"default"}), flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
        return
    else:
        cfg = settings(args.hermes_home)
        client = Client(cfg["url"], cfg["token"])
        if args.command == "status":
            result = client.call("/v1/status")
        elif args.command == "forget":
            if args.all_revisions:
                evidence = client.call('/v1/evidence', {'record_id': args.record_id})
                result = client.call('/v1/forget-source', {'source': evidence['source'], 'source_id': evidence['source_id']})
            else:
                result = client.call("/v1/forget", {"record_id": args.record_id})
        elif args.command == "coverage":
            result = client.call("/v1/coverage", {"source": args.source, "state": args.state,
                                                  "through_at": args.through_at, "note": args.note})
        else:
            from . import importers
            count = duplicates = 0
            if args.command=="import-whatsapp":
                records=importers.whatsapp(args.file,args.thread_id,args.date_order,args.timezone,args.source,args.fold)
            elif args.command=="import-email": records=importers.emails(args.file,args.source)
            elif args.command=="import-health": records=importers.health_csv(args.file,args.source)
            else: records=importers.jsonl(args.file)
            try:
                for item in records:
                    raw_item=item
                    if args.command!="import-jsonl":
                        from .ingestion import adapt_existing
                        from .common import now
                        item=adapt_existing(item,connector_id="personal_memory."+args.command.replace("-","_"),
                            connector_version="0.7.0rc1",source_locator=str(args.file.resolve())+"#"+item["source_id"],observed_at=now())
                    result=client.call("/v1/ingest",{"items":[item]})
                    if args.command=='import-health' and args.subject_id:
                        importers.typed_health(client,raw_item,result['records'][0]['id'],args.subject_id)
                    count+=1; duplicates+=int(result["records"][0]["duplicate"])
            except Exception as error:
                raise RuntimeError(f"Import stopped after {count} committed records; rerun after correcting input. {error}") from None
            result = {"processed": count, "duplicates": duplicates, "coverage": "unchanged; declare explicitly using coverage command"}
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if args.command=="doctor" and not result["checks_passed"]:raise SystemExit(1)


if __name__ == "__main__":
    main()
