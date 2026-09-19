"""Bundled source adapters that wrap the existing deterministic importers.

Provider-specific packages (Gmail and friends) live outside the core; these
wrappers keep export/legacy paths on the identical sync runtime and prove the
adapter protocol needs no source branches in the engine.
"""
import mailbox
import os
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path

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
    Attachments are surfaced as stable part descriptors; bytes are re-fetched
    from the file at job time, so the export remains the single authority.
    """

    def __init__(self, *, path, source="email", page_size=100,
                 adapter_id="email.export", adapter_version="1.0"):
        self.path = path
        self.source = required_text(source, "source", 200)
        self.page_size = max(1, min(int(page_size), 1000))
        self._spec = adapter_spec(adapter_id, adapter_version,
                                  capabilities={"history": True, "attachments": True})

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
            record = adapt_existing(legacy, connector_id=connector_id, connector_version=version,
                                    source_locator="file://" + os.path.basename(str(self.path)),
                                    observed_at=observed)
            yield record, (legacy.get("metadata") or {}).get("attachment_descriptors") or []

    def read_page(self, context, state):
        seen = (state["cursor"] or {}).get("seen", 0)
        if (state["cursor"] or {}).get("done"):
            return source_page(page_id="pg_done", operations=[], next_state=read_state(
                state_version=state["state_version"], cursor=state["cursor"], mode=state["mode"]))
        window = []
        exhausted = False
        for index, entry in enumerate(self._records()):
            if index < seen:
                continue
            if len(window) >= self.page_size:
                break
            window.append((index, entry))
        else:
            exhausted = True
        operations = [source_operation("upsert", record["source_id"], records=[record],
                                       attachments=attachments)
                      for _, (record, attachments) in window]
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
        descriptors = (payload.get("metadata") or {}).get("attachment_descriptors") or []
        return normalized_item(record["source_id"], records=[record], attachments=descriptors)

    def attachment(self, context, descriptor):
        """Return the exact bytes of one export part, located by message id and index."""
        source_id = required_text(descriptor.get("source_id"), "source_id", 1000)
        try:
            part_index = int(descriptor["part_id"])
        except (KeyError, TypeError, ValueError):
            raise AdapterError("permanent", "Attachment descriptor has no usable part identity") from None
        if str(part_index) != str(descriptor["part_id"]):
            raise AdapterError("permanent", "Attachment descriptor has no usable part identity")

        def matching(message):
            return str(message.get("Message-ID", "")).strip() == source_id

        if str(self.path).lower().endswith(".eml"):
            candidates = [BytesParser(policy=policy.default).parsebytes(Path(self.path).read_bytes())]
        else:
            box = mailbox.mbox(str(self.path), create=False,
                               factory=lambda f: BytesParser(policy=policy.default).parse(f))
            try:
                candidates = list(box)
            finally:
                box.close()
        for message in candidates:
            if not matching(message):
                continue
            attachments = list(message.iter_attachments())
            if part_index >= len(attachments):
                break
            part = attachments[part_index]
            filename = part.get_filename() or "attachment"
            if "filename" in descriptor and descriptor["filename"] != filename:
                raise AdapterError("permanent", "Attachment part identity no longer matches the export")
            return part.get_payload(decode=True) or b""
        raise AdapterError("permanent", "Attachment part is no longer present in the export")
