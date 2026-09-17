"""Bundled source adapters that wrap the existing deterministic importers.

Provider-specific packages (Gmail and friends) live outside the core; these
wrappers keep export/legacy paths on the identical sync runtime and prove the
adapter protocol needs no source branches in the engine.
"""
import os
from datetime import datetime, timezone

from .common import required_text
from .importers import emails as parse_emails
from .ingestion import adapt_existing
from .source_sdk import (AdapterError, SourceAdapter, adapter_spec, normalized_item,
                         read_state, source_operation, source_page, stream_spec)


class EmailExportAdapter(SourceAdapter):
    """A snapshot stream over an EML/MBOX export (the legacy importer path).

    Historical records only; no live tail; progress is the position in a stable
    parse order. observed_at derives from the file's modification time so a
    replayed page is byte-identical and replays through the receipt.
    """

    def __init__(self, *, path, source="email", page_size=100,
                 adapter_id="email.export", adapter_version="1.0"):
        self.path = path
        self.source = required_text(source, "source", 200)
        self.page_size = max(1, min(int(page_size), 1000))
        self._spec = adapter_spec(adapter_id, adapter_version,
                                  capabilities={"history": True})

    def spec(self):
        return dict(self._spec)

    def _observed_at(self):
        modified = os.path.getmtime(self.path)
        return datetime.fromtimestamp(modified, tz=timezone.utc).isoformat()

    def check(self, context):
        if not os.path.exists(self.path):
            raise AdapterError("permanent", "Export file is not present on this host")
        return {"account_id": self.source, "read_only": True,
                "locator": str(self.path)}

    def discover(self, context):
        return [stream_spec("export", modes=["backfill"])]

    def _records(self):
        connector_id = self._spec["adapter_id"]
        version = self._spec["adapter_version"]
        observed = self._observed_at()
        for legacy in parse_emails(self.path, source=self.source):
            yield adapt_existing(legacy, connector_id=connector_id, connector_version=version,
                                 source_locator="file://" + os.path.basename(str(self.path)),
                                 observed_at=observed)

    def read_page(self, context, state):
        seen = (state["cursor"] or {}).get("seen", 0)
        if (state["cursor"] or {}).get("done"):
            return source_page(page_id="pg_done", operations=[], next_state=read_state(
                state_version=state["state_version"], cursor=state["cursor"], mode=state["mode"]))
        window = []
        exhausted = False
        for index, record in enumerate(self._records()):
            if index < seen:
                continue
            if len(window) >= self.page_size:
                break
            window.append((index, record))
        else:
            exhausted = True
        operations = [source_operation("upsert", record["source_id"], records=[record])
                      for _, record in window]
        next_seen = seen + len(window)
        done = exhausted or not window
        return source_page(page_id=f"pg_{seen}",
                           operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"seen": next_seen, "done": done},
                                                 mode=state["mode"]),
                           coverage=[{"start": seen, "end": next_seen, "state": "complete"}]
                           if window else [])

    def normalize(self, payload):
        record = payload["record"] if "record" in payload else adapt_existing(
            payload, connector_id=self._spec["adapter_id"],
            connector_version=self._spec["adapter_version"],
            source_locator="file://" + os.path.basename(str(self.path)),
            observed_at=self._observed_at())
        return normalized_item(record["source_id"], records=[record])
