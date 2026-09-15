"""Uniform, low-overhead call tracing for the personal-memory stack.

A single trace id is created per top-level memory call, propagated to the service over the
``X-Personal-Memory-Trace`` header, and bound to a :class:`ContextVar` so any nested code can
attribute a log line to that call without threading parameters through every layer. Both the
Hermes-side provider process and the service process emit lines carrying the same id, so one
``grep trace=<id>`` reconstructs a call end to end.

Performance rules (this runs in the retrieval hot path):
* every helper checks ``isEnabledFor`` before doing any work;
* argument interpolation uses deferred ``%s`` (never f-strings) so disabled levels cost almost
  nothing;
* the trace id is embedded in the message itself, so output is identical whether or not a host
  process installs its own formatting.
"""
import logging
import os
import secrets
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

TRACE_HEADER = "X-Personal-Memory-Trace"
TRACE_RESPONSE_HEADER = b"x-personal-memory-trace"
LOG_NAME = "personal_memory"
_MAX_LEN = 64

_trace_var = ContextVar("personal_memory_trace", default="")
LOG = logging.getLogger(LOG_NAME)


def new_trace():
    """A short, collision-resistant id. 12 hex chars is plenty for correlating one process."""
    return secrets.token_hex(6)


def get_trace():
    return _trace_var.get()


def sanitize_trace(raw):
    """Accept only a bounded, charset-safe id from an untrusted inbound header."""
    if not isinstance(raw, (str, bytes)):
        return ""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("ascii")
        except UnicodeError:
            return ""
    return "".join(ch for ch in raw if ch.isalnum() or ch in "-_")[:_MAX_LEN]


@contextmanager
def bind_trace(trace_id):
    token = _trace_var.set(trace_id)
    try:
        yield trace_id
    finally:
        _trace_var.reset(token)


class TraceFilter(logging.Filter):
    """Adds ``record.trace`` for formatters that want ``%(trace)s``."""

    def filter(self, record):
        record.trace = _trace_var.get() or "-"
        return True


def configure_logging(level=None, *, propagate=False):
    """Attach a stderr handler to the ``personal_memory`` logger. Idempotent; never touches root.

    Safe to call from both the service (owns its process) and the provider (embedded in Hermes):
    ``propagate=False`` keeps our records out of the host's formatter so lines stay greppable and
    are emitted exactly once.
    """
    logger = logging.getLogger(LOG_NAME)
    if getattr(logger, "_personal_memory_configured", False):
        if level:
            logger.setLevel(_resolve_level(level))
        return logger
    handler = logging.StreamHandler()
    handler.addFilter(TraceFilter())
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname).1s %(name)s [%(trace)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(handler)
    logger.setLevel(_resolve_level(level))
    logger.propagate = propagate
    logger._personal_memory_configured = True
    return logger


def _resolve_level(level=None):
    if isinstance(level, int):
        return level
    name = (level or os.environ.get("PERSONAL_MEMORY_LOG_LEVEL") or "INFO").upper()
    value = getattr(logging, name, None)
    return value if isinstance(value, int) else logging.INFO


def _fmt(fields):
    return " ".join("%s=%s" % (key, value) for key, value in fields.items() if value is not None)


def log_call(level, op, started, *, ok=True, **fields):
    """Emit one correlation line, guarded and deferred. Cheap when ``level`` is disabled."""
    if not LOG.isEnabledFor(level):
        return
    base = {"op": op, "ms": round((time.monotonic() - started) * 1000, 1), "ok": int(ok)}
    base.update(fields)
    LOG.log(level, _fmt(base))


def debug_enabled():
    return LOG.isEnabledFor(logging.DEBUG)


def traced(op, detail=None):
    """Decorator for a top-level entry point: bind a fresh trace (unless already bound), time
    the call, and log a single summary line. Nested calls reuse the outer trace and log once."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if _trace_var.get():
                return fn(*args, **kwargs)
            with bind_trace(new_trace()):
                started = time.monotonic()
                try:
                    result = fn(*args, **kwargs)
                except Exception as error:
                    log_call(logging.WARNING, op, started, ok=False, error=type(error).__name__)
                    raise
                extra = detail(result) if detail is not None and LOG.isEnabledFor(logging.INFO) else None
                log_call(logging.INFO, op, started, **(extra or {}))
                return result
        return wrapper
    return decorator
