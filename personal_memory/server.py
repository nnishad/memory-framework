import hmac
import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .backend import load_backend
from .store import Store
from .ingestion import RECORD_SCHEMA,ContractError
from .curated import VersionConflict
from .trace import TRACE_HEADER, new_trace, sanitize_trace

LOG = logging.getLogger(__name__)
MAX_BODY = 2 * 1024 * 1024


def create_server(data_dir, token, host="127.0.0.1", port=8766, backend=None, retrieval_config=None, source_config=None, source_adapters=()):
    if host not in {"127.0.0.1", "localhost"}:
        raise ValueError("This reference server only binds IPv4 loopback")
    if not isinstance(token, str) or len(token) < 32:
        raise ValueError("An authentication token of at least 32 characters is required")
    from .service import MemoryService, AccessDenied
    from .asgi import strict_json
    service = MemoryService(data_dir,token,retrieval_config,backend,source_config=source_config,
                            source_adapters=source_adapters)
    store, retrieval, routes = service.store, service.retrieval, service.routes

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # No personal payloads, tokens, or URL parameters in access logs.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def respond(self, code, data):
            # Surface the trace on failures (additively) and echo it as a header so a client can
            # correlate a returned error id with the server log line emitted for the same call.
            trace = getattr(self, "_trace", "")
            if code >= 400 and trace and isinstance(data, dict):
                data = {**data, "trace": trace}
            raw = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            if trace:
                self.send_header(TRACE_HEADER, trace)
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):
            self._trace = sanitize_trace(self.headers.get(TRACE_HEADER, "")) or new_trace()
            authorization = self.headers.get("Authorization", "")
            try: principal=service.authenticate(authorization)
            except AccessDenied:
                self.respond(401, {"error": "Unauthorized"}); return
            if self.headers.get("Origin"):
                self.respond(403, {"error": "Browser-origin requests are not accepted"}); return
            if self.path not in routes:
                self.respond(404, {"error": "Unknown endpoint"}); return
            try:
                if self.headers.get("Transfer-Encoding"):
                    raise ValueError("Transfer-Encoding is unsupported")
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= MAX_BODY:
                    self.respond(413, {"error": "Body must be 1..2097152 bytes"}); return
                args = strict_json(self.rfile.read(size))
                if not isinstance(args, dict):
                    raise ValueError("JSON body must be an object")
                result = service.dispatch(self.path,args,principal,self._trace)
                self.respond(200, result)
            except AccessDenied:
                self.respond(403,{"error":"Operation outside credential scope"})
            except ContractError as error:
                self.respond(422,{"error":"Ingestion contract rejected","path":error.path,"message":error.message})
            except VersionConflict as error:
                self.respond(409,{"error":str(error),"conflict":True})
            except (ValueError, TypeError, KeyError) as error:
                self.respond(400, {"error": str(error)})
            except Exception:
                LOG.error("Memory operation failed on %s", self.path)
                self.respond(500, {"error": "Memory operation failed; inspect server health"})

    class Server(ThreadingHTTPServer):
        def server_close(self):
            service.close()
            super().server_close()
    server = Server((host, port), Handler)
    server.store = store
    server.service = service
    server.retrieval = retrieval
    server.daemon_threads = True
    return server
