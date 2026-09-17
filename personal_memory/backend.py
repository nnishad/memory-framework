"""Retrieval extension boundary; canonical storage stays owned by Store."""
import importlib
from typing import Protocol


class RetrievalBackend(Protocol):
    def search(self, **query) -> dict: ...


def load_backend(store, entrypoint=None, config=None, index_owner=True):
    if not entrypoint:
        from .retrieval import Hybrid
        # Starting indexing workers requires ownership: normal startup acquires the
        # durable lease over the data directory automatically, so a second writer
        # fails at startup. index_owner=False is the explicit non-indexing mode for
        # local inspection; application recall should use the running service.
        return Hybrid(store, config, index_owner=index_owner)
    # Only operator configuration can choose Python code, never a model tool argument.
    module, factory = entrypoint.split(":", 1)
    backend = getattr(importlib.import_module(module), factory)(store)
    if not callable(getattr(backend, "search", None)):
        raise ValueError("Retrieval backend must expose search(**query)")
    return backend
