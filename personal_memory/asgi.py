"""Production ASGI application. Serve with one Uvicorn worker on loopback."""
import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .ingestion import ContractError
from .curated import VersionConflict
from .service import MemoryService,AccessDenied
from .trace import LOG, TRACE_RESPONSE_HEADER, new_trace, sanitize_trace

MAX_BODY=2*1024*1024


def strict_json(raw):
    def object_pairs(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise ValueError("Duplicate JSON key")
            result[key]=value
        return result
    def nonfinite(value):raise ValueError("Nonfinite JSON value")
    return json.loads(raw,object_pairs_hook=object_pairs,parse_constant=nonfinite)


class ProcessLease:
    def __init__(self,path):
        import os
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.file=path.open("a+b");path.chmod(0o600)
        try:
            if os.name == "nt":
                import msvcrt
                if not self.file.seek(0,2):
                    self.file.write(b"\0");self.file.flush()
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            self.file.close();raise RuntimeError("Another service owns this data directory") from None
    def close(self):self.file.close()


class Application:
    def __init__(self,settings):
        self.settings=settings;self.service=None;self.lease=None;self.hindsight_runtime=None
        # The HTTP executor is owned by a running lifecycle: it is created at startup
        # and drained at shutdown, so a restart never submits work to a shut-down pool.
        self.executor=None;self.accepting=False
        self.slots=asyncio.Semaphore(16)

    def _release(self,name):
        # Close one owned resource. It stays tracked unless the close succeeded,
        # so an incomplete shutdown is never silently dropped: the next startup
        # or shutdown retries it. Returns True once the resource is released.
        resource=getattr(self,name)
        if resource is None:return True
        try:
            resource.close()
        except Exception as error:
            LOG.warning("Shutdown of %s incomplete; a later close will retry: %s",name,error)
            return False
        setattr(self,name,None)
        return True

    def _teardown(self):
        # One dependency-aware cleanup routine shared by normal shutdown, failed
        # startup and cleanup-before-restart. Reverse construction order, but stop at
        # the first resource that will not close: a service that still owns a live
        # writer or an initialization must keep its Hindsight runtime and process
        # lease held, because releasing the lease while a dependent is still running
        # would hand ownership to a second process. Returns True only when every
        # owned resource has actually been released.
        for name in ("service","hindsight_runtime","lease"):
            if not self._release(name):
                return False
        return True

    async def __call__(self,scope,receive,send):
        if scope["type"]=="lifespan":
            while True:
                message=await receive()
                if message["type"]=="lifespan.startup":
                    # A prior failed startup or unfinished shutdown may still own a
                    # resource. Retry that cleanup first; if ownership has not fully
                    # drained, refuse to claim it a second time rather than overwriting
                    # the retained handles with replacements that would double-open them.
                    if not self._teardown():
                        await send({"type":"lifespan.startup.failed","message":"Prior shutdown is still holding ownership; not starting a second owner"})
                        return
                    try:
                        self.lease=ProcessLease(Path(self.settings["data_dir"])/"service.lock")
                        from .hindsight_runtime import Runtime
                        self.hindsight_runtime=Runtime(self.settings).start()
                        self.service=MemoryService(self.settings["data_dir"],self.settings["token"],
                            retrieval_config=self.settings.get("retrieval",{}),backend=self.settings.get("backend"),
                            principals=self.settings.get("principals",[]),extension_schemas=self.settings.get("extension_schemas",{}),intelligence_config=self.settings.get("intelligence",{}),source_config=self.settings.get('sources',{}))
                        # The HTTP executor exists only for a running lifecycle; create
                        # it once ownership is claimed and begin accepting requests.
                        self.executor=ThreadPoolExecutor(max_workers=8,thread_name_prefix="memory-http")
                        self.accepting=True
                        # Prime the lazy retrieval models off the request path so the first Hermes
                        # turn after a restart is fast instead of paying the ~7.7s cold-start. The
                        # retrieval backend owns the worker: readiness must not block on warm-up,
                        # and service shutdown drains it before reporting completion.
                        prime=getattr(self.service.retrieval,"start_warmup",None)
                        if callable(prime):prime()
                        else:
                            warm=getattr(self.service.retrieval,"warmup",None)
                            if callable(warm):
                                threading.Thread(target=warm,daemon=True,name="memory-warmup").start()
                        await send({"type":"lifespan.startup.complete"})
                    except Exception as error:
                        # Cleanup stops at the first dependent that will not close and
                        # keeps its prerequisites owned; it never masks the startup
                        # exception, which is the actual diagnosis.
                        if not self._teardown():
                            LOG.warning("Startup cleanup incomplete; ownership retained for retry")
                        await send({"type":"lifespan.startup.failed","message":type(error).__name__+": inspect configuration/dependencies"})
                        return
                elif message["type"]=="lifespan.shutdown":
                    # Stop accepting new requests and drain already-accepted handlers
                    # before any service resource is closed, so an in-flight handler can
                    # never call a service that has already been torn down.
                    self.accepting=False
                    if self.executor is not None:
                        try:
                            self.executor.shutdown(wait=True,cancel_futures=True)
                        except Exception as error:
                            LOG.warning("HTTP executor shutdown failed: %s",error)
                        self.executor=None
                    # Release in dependency order; a dependent that stays owned keeps
                    # every prerequisite held and reports the shutdown as incomplete.
                    if self._teardown():
                        await send({"type":"lifespan.shutdown.complete"})
                    else:
                        # Ownership may still be held by an unfinished close; say so
                        # instead of claiming a clean shutdown.
                        await send({"type":"lifespan.shutdown.failed","message":"Some resources stayed open; inspect health and retry"})
                    return
            return
        if scope["type"]!="http":return
        trace = new_trace()
        async def respond(code,data):
            if code>=400 and isinstance(data,dict):data={**data,"trace":trace}
            raw=json.dumps(data,ensure_ascii=False,allow_nan=False).encode()
            await send({"type":"http.response.start","status":code,"headers":[
                (b"content-type",b"application/json"),(b"cache-control",b"no-store"),
                (b"x-content-type-options",b"nosniff"),(b"content-length",str(len(raw)).encode()),
                (TRACE_RESPONSE_HEADER,trace.encode())]})
            await send({"type":"http.response.body","body":raw})
        headers={}
        for key,value in scope["headers"]:
            if key in headers and key in {b"authorization",b"content-length"}:
                await respond(400,{"error":"Duplicate request header"});return
            headers[key]=value
        inbound=sanitize_trace(headers.get(b"x-personal-memory-trace",b""))
        if inbound:trace=inbound
        if not self.accepting or self.service is None:
            # Reject before dispatch: a shutting-down or not-yet-started app must not
            # submit work to an executor it is about to drain or has already drained.
            await respond(503,{"error":"Service starting or stopping"});return
        try:principal=self.service.authenticate(headers.get(b"authorization",b"").decode())
        except (AccessDenied,UnicodeError):await respond(401,{"error":"Unauthorized"});return
        if b"origin" in headers:
            await respond(403,{"error":"Browser-origin requests rejected"});return
        if scope["method"]!="POST":await respond(405,{"error":"POST required"});return
        if headers.get(b"content-type",b"").split(b";")[0]!=b"application/json":
            await respond(415,{"error":"application/json required"});return
        if scope["path"] not in self.service.routes:await respond(404,{"error":"Unknown endpoint"});return
        if self.slots.locked():await respond(503,{"error":"Service busy; retry with backoff"});return
        async with self.slots:
            try:
                async def read_body():
                    body=bytearray()
                    while True:
                        message=await receive()
                        if message["type"]=="http.disconnect":raise ConnectionError()
                        body.extend(message.get("body",b""))
                        if len(body)>MAX_BODY:raise OverflowError()
                        if not message.get("more_body",False):return body
                raw=await asyncio.wait_for(read_body(),timeout=10)
                args=strict_json(raw)
                if not isinstance(args,dict):raise ValueError("JSON body must be an object")
                result=await asyncio.get_running_loop().run_in_executor(self.executor,self.service.dispatch,scope["path"],args,principal,trace)
                code=503 if scope["path"]=="/v1/ready" and not result["ready"] else 200
                await respond(code,result)
            except ContractError as error:await respond(422,{"error":"Ingestion contract rejected","path":error.path,"message":error.message})
            except VersionConflict as error:
                await respond(409,{"error":str(error),"conflict":True})
            except AccessDenied:await respond(403,{"error":"Operation outside credential scope"})
            except OverflowError:await respond(413,{"error":"Request body exceeds 2 MiB"})
            except asyncio.TimeoutError:await respond(408,{"error":"Request body timeout"})
            except ConnectionError:return
            except (ValueError,KeyError,TypeError,RecursionError):await respond(400,{"error":"Invalid request; inspect contract and references"})
            except Exception:await respond(500,{"error":"Memory operation failed; inspect service health"})
