"""Production ASGI application. Serve with one Uvicorn worker on loopback."""
import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .ingestion import ContractError
from .curated import VersionConflict
from .service import MemoryService,AccessDenied
from .trace import TRACE_RESPONSE_HEADER, new_trace, sanitize_trace

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
        import fcntl
        path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.file=path.open("a+");path.chmod(0o600)
        try:fcntl.flock(self.file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            self.file.close();raise RuntimeError("Another service owns this data directory") from None
    def close(self):self.file.close()


class Application:
    def __init__(self,settings):
        self.settings=settings;self.service=None;self.lease=None;self.hindsight_runtime=None
        self.executor=ThreadPoolExecutor(max_workers=8,thread_name_prefix="memory-http")
        self.slots=asyncio.Semaphore(16)

    async def __call__(self,scope,receive,send):
        if scope["type"]=="lifespan":
            while True:
                message=await receive()
                if message["type"]=="lifespan.startup":
                    try:
                        self.lease=ProcessLease(Path(self.settings["data_dir"])/"service.lock")
                        from .hindsight_runtime import Runtime
                        self.hindsight_runtime=Runtime(self.settings).start()
                        self.service=MemoryService(self.settings["data_dir"],self.settings["token"],
                            retrieval_config=self.settings.get("retrieval",{}),backend=self.settings.get("backend"),
                            principals=self.settings.get("principals",[]),extension_schemas=self.settings.get("extension_schemas",{}),intelligence_config=self.settings.get("intelligence",{}))
                        # Prime the lazy retrieval models off the request path so the first Hermes
                        # turn after a restart is fast instead of paying the ~7.7s cold-start. Run
                        # in the background: readiness must not block on model/session warm-up.
                        warm=getattr(self.service.retrieval,"warmup",None)
                        if callable(warm):
                            threading.Thread(target=warm,daemon=True,name="memory-warmup").start()
                        await send({"type":"lifespan.startup.complete"})
                    except Exception as error:
                        if self.hindsight_runtime:self.hindsight_runtime.close()
                        if self.lease:self.lease.close()
                        await send({"type":"lifespan.startup.failed","message":type(error).__name__+": inspect configuration/dependencies"})
                        return
                elif message["type"]=="lifespan.shutdown":
                    if self.service:self.service.close()
                    self.executor.shutdown(wait=True,cancel_futures=True)
                    if self.hindsight_runtime:self.hindsight_runtime.close()
                    if self.lease:self.lease.close()
                    await send({"type":"lifespan.shutdown.complete"});return
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
        if not self.service:
            await respond(503,{"error":"Service starting"});return
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
