"""One adapter, several streams and partitions.

The sync runtime leases, cursors and retries every ``(stream, partition, role)``
independently and hands the current selectors to ``read_page`` through the
context. A multi-stream adapter reads exactly the slice named by
``context["stream"]`` and ``context["partition"]``; single-stream adapters can
ignore both. The runtime refuses to read a selector the adapter never declared.
"""
from personal_memory.source_sdk import (SourceAdapter, adapter_spec, normalized_item,
                                        read_state, source_operation, source_page, stream_spec)


def record(source, source_id, text):
    return {"schema_version": "1.0", "source": source, "source_id": source_id,
            "revision": "1", "kind": "document", "occurred_at": None,
            "observed_at": "2026-09-06T12:00:00Z", "text": text, "participants": [],
            "provenance": {"connector_id": "example.mailbox", "connector_version": "1.0.0",
                           "source_locator": f"mailbox://{source_id}", "origin": "source",
                           "parent_record_ids": []},
            "extensions": {}}


class MailboxAdapter(SourceAdapter):
    """Streams: 'messages' partitioned per folder, and an unpartitioned 'contacts'."""

    def __init__(self, *, messages=None, contacts=None):
        self.messages = messages or {"inbox": ["welcome"], "sent": ["reply"]}
        self.contacts = contacts or ["ada"]

    def spec(self):
        return adapter_spec("example.mailbox", "1.0.0", capabilities={"history": True})

    def check(self, context):
        return {"account_id": "example", "read_only": True}

    def discover(self, context):
        return [stream_spec("messages", modes=["backfill"],
                            partitions=[{"id": folder} for folder in self.messages]),
                stream_spec("contacts", modes=["backfill"])]

    def read_page(self, context, state):
        # The runtime already validated these selectors against discover().
        stream, partition = context["stream"], context.get("partition") or ""
        if stream == "messages":
            names = self.messages.get(partition, [])
        elif stream == "contacts":
            names = self.contacts
        else:
            names = []
        operations = [source_operation("upsert", f"{stream}/{partition}/{name}",
                                       records=[record(context["source"],
                                                       f"{stream}/{partition}/{name}", name)])
                      for name in names]
        return source_page(page_id=f"{stream}:{partition}", operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"done": True}, mode=state["mode"]))

    def normalize(self, payload):
        validated = record(payload["source"], payload["source_id"], payload["text"])
        return normalized_item(validated["source_id"], records=[validated])
