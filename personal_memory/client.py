import json
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler
from urllib.error import HTTPError


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
        request = Request(self.url + path, data=json.dumps(data or {}, ensure_ascii=False).encode() if method not in {"GET", "DELETE"} else None,
                          headers={"Authorization": "Bearer " + self.token,
                                   "Content-Type": "application/json"}, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
                if len(raw) > 4 * 1024 * 1024:
                    raise RuntimeError("Memory response exceeds budget")
                return json.loads(raw)
        except HTTPError as error:
            if missing_ok and error.code == 404:
                return {"missing": True}
            # Do not emit request headers or payloads in errors.
            try:
                payload = json.loads(error.read(4096))
                detail = payload.get("error", "Request rejected")
            except Exception:
                payload = {}
                detail = "Request rejected"
            raise ServiceError(error.code, detail, payload.get("path")) from None
