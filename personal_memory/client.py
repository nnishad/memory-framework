import json
import logging
import time
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler
from urllib.error import HTTPError

from .trace import TRACE_HEADER, new_trace, get_trace, bind_trace, log_call

LOG = logging.getLogger(__name__)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class ServiceError(RuntimeError):
    def __init__(self, status, detail="Request rejected", path=None):
        self.status, self.path = status, path
        super().__init__(f"Memory HTTP {status}: {detail}")


class Client:
    def __init__(self, url, token, timeout=3):
        parsed = urlparse(url)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Invalid service URL")
        if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1"}):
            raise ValueError("Service URL requires HTTPS or local loopback HTTP")
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def call(self, path, data=None, method="POST", missing_ok=False):
        if path=='/v1/ingest' and getattr(self,'write_epoch',None) is not None:
            data=dict(data or {});data.setdefault('epoch',self.write_epoch)
        # Inherit the ambient trace when this call belongs to a larger operation (a provider
        # hook or dispatch); otherwise originate one so a direct call is still traceable.
        inherited = get_trace()
        trace = inherited or new_trace()
        request = Request(self.url + path, data=json.dumps(data or {}, ensure_ascii=False).encode() if method not in {"GET", "DELETE"} else None,
                          headers={"Authorization": "Bearer " + self.token,
                                   "Content-Type": "application/json", TRACE_HEADER: trace}, method=method)
        with bind_trace(trace):
            started = time.monotonic()
            try:
                with self.opener.open(request, timeout=self.timeout) as response:
                    raw = response.read(4 * 1024 * 1024 + 1)
                    if len(raw) > 4 * 1024 * 1024:
                        raise RuntimeError("Memory response exceeds budget")
                # Summarise at INFO only when this call started its own trace; nested calls are
                # covered by their hook/dispatch summary and stay at DEBUG.
                log_call(logging.INFO if not inherited else logging.DEBUG, path, started)
                return json.loads(raw)
            except HTTPError as error:
                if missing_ok and error.code == 404:
                    return {"missing": True}
                # Do not emit request headers or payloads in errors.
                try:
                    payload = json.loads(error.read(4096))
                    detail = payload.get("error", "Request rejected")
                    # A contract rejection carries the caller's own validation detail. Without it a
                    # small model cannot correct the call and retries the same shape until it gives up.
                    message = payload.get("message")
                    if isinstance(message, str) and message:
                        detail = "%s (%s: %s)" % (detail, payload.get("path") or "$", message)
                except Exception:
                    payload = {}
                    detail = "Request rejected"
                log_call(logging.WARNING, path, started, ok=False, status=error.code)
                raise ServiceError(error.code, detail, payload.get("path")) from None
            except Exception as error:
                log_call(logging.WARNING, path, started, ok=False, error=type(error).__name__)
                raise
