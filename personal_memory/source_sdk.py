"""Versioned source-adapter protocol, independent of record schema 1.0.

Adapters own upstream I/O and faithful normalization; the sync runtime owns
progress, leases and commits. read_page() and normalize() never advance
authoritative state. Pages and operations are plain JSON so non-Python
collectors can speak the same protocol.
"""
import copy
import json
from abc import ABC, abstractmethod

from .ingestion import ContractError, validate_record
from .common import required_text

PROTOCOL_VERSION = "1.0"
READ_MODES = ("backfill", "incremental", "reconcile")
OPERATION_ACTIONS = ("upsert", "metadata_update", "remove", "restore", "skip")
CAPABILITIES = ("history", "incremental", "reconciliation", "deletions", "attachments",
                "events", "subscriptions")
ERROR_KINDS = ("temporary", "rate_limit", "auth", "cursor", "schema", "permanent", "unsupported")


class AdapterError(Exception):
    """Classified adapter failure with a safe diagnostic; the runtime maps kind to policy."""

    def __init__(self, kind, message, retry_after=None):
        if kind not in ERROR_KINDS:
            raise ValueError(f"kind must be one of {ERROR_KINDS}")
        self.kind = kind
        self.message = required_text(message, "message", 2000)
        self.retry_after = retry_after if type(retry_after) in (int, float) and retry_after >= 0 else None
        super().__init__(f"{kind}: {message}")


def _text(value, label, maximum=1000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ContractError(label, "must be nonempty text")
    return value


def _flag(value, label):
    if value is not None and not isinstance(value, bool):
        raise ContractError(label, "must be boolean or null")
    return value


def adapter_spec(adapter_id, adapter_version, *, capabilities=None, config_schema=None,
                 secret_refs=(), protocol_versions=(PROTOCOL_VERSION,)):
    """Declare identity, versions and the exact capability subset the upstream provides."""
    if config_schema is None:
        config_schema = {}
    if not isinstance(config_schema, dict):
        raise ContractError("$.config_schema", "must be an object")
    declared = capabilities or {}
    unknown = set(declared) - set(CAPABILITIES)
    if unknown:
        raise ContractError("$.capabilities", f"Unknown capabilities: {sorted(unknown)}")
    refs = [ _text(r, "secret_refs[]", 200) for r in secret_refs ]
    if len(set(refs)) != len(refs):
        raise ContractError("$.secret_refs", "Duplicate secret references")
    return {"adapter_id": _text(adapter_id, "adapter_id", 200),
            "adapter_version": _text(adapter_version, "adapter_version", 100),
            "protocol_versions": [ _text(v, "protocol_versions[]", 100) for v in protocol_versions ],
            "capabilities": {name: bool(declared.get(name, False)) for name in CAPABILITIES},
            "config_schema": config_schema, "secret_refs": refs}


def adapter_supports(spec, capability):
    if capability not in CAPABILITIES:
        raise ValueError(f"Unknown capability {capability}")
    return bool(spec["capabilities"].get(capability))


def connection_context(*, connection_id, source, scope=None, secrets=None, deadline=None,
                       stream=None, partition=None):
    """Immutable per-run context. Secrets resolve by reference; no arbitrary admin client.

    ``stream`` and ``partition`` identify the exact slice the runtime leased for the
    current read; they default to ``None`` so connection-level calls (check, discover,
    attachment) stay valid, and single-stream adapters may ignore them entirely.
    """
    if not callable(secrets):
        secrets = lambda name, _missing=connection_id: (_ for _ in ()).throw(
            AdapterError("auth", f"Secret {name!r} is not resolvable for this connection"))
    return {"connection_id": _text(connection_id, "connection_id", 200),
            "source": _text(source, "source", 200),
            "scope": copy.deepcopy(scope or {}), "secrets": secrets, "deadline": deadline,
            "stream": _text(stream, "stream", 200) if stream else stream,
            "partition": _text(partition, "partition", 500) if partition else partition}


def stream_spec(stream_id, *, modes=("backfill",), partitions=(), history_limit=None,
                deletion_semantics=None, required_permission=None, version_order="opaque"):
    if version_order not in ("opaque", "integer", "timestamp"):
        raise ContractError("$.version_order", "must be opaque, integer or timestamp")
    bad = set(modes) - set(READ_MODES)
    if bad:
        raise ContractError("$.modes", f"Unknown read modes: {sorted(bad)}")
    parts = []
    for partition in partitions:
        if not isinstance(partition, dict) or "id" not in partition:
            raise ContractError("$.partitions[]", "Each partition requires an id")
        parts.append({"id": _text(partition["id"], "partition.id", 500),
                      "description": str(partition.get("description", ""))[:500]})
    if len({p["id"] for p in parts}) != len(parts):
        raise ContractError("$.partitions", "Duplicate partition ids")
    return {"stream_id": _text(stream_id, "stream_id", 200),
            "modes": [mode for mode in READ_MODES if mode in modes],
            "partitions": parts,
            "version_order": version_order,
            "history_limit": None if history_limit is None else _text(history_limit, "history_limit", 200),
            "deletion_semantics": None if deletion_semantics is None
            else _text(deletion_semantics, "deletion_semantics", 200),
            "required_permission": None if required_permission is None
            else _text(required_permission, "required_permission", 500)}


def read_state(*, state_version=1, cursor=None, mode="backfill", scope_hash="", snapshot_boundary=None):
    if mode not in READ_MODES:
        raise ContractError("$.mode", f"mode must be one of {READ_MODES}")
    if type(state_version) is not int or state_version < 1:
        raise ContractError("$.state_version", "must be a positive integer")
    if cursor is not None:
        try:
            if len(json.dumps(cursor, sort_keys=True, allow_nan=False)) > 8192:
                raise ContractError("$.cursor", "cursor exceeds 8192 characters")
        except (TypeError, ValueError):
            raise ContractError("$.cursor", "cursor must be JSON data") from None
    return {"state_version": state_version, "cursor": copy.deepcopy(cursor), "mode": mode,
            "scope_hash": str(scope_hash or "")[:128],
            "snapshot_boundary": None if snapshot_boundary is None
            else _text(snapshot_boundary, "snapshot_boundary", 500)}


def source_operation(action, source_id, *, records=None, source_version=None, metadata=None,
                     reason=None, coordinates=None, attachments=()):
    """One explicit change to a source item's durable state. Never an implicit guess."""
    if action not in OPERATION_ACTIONS:
        raise ContractError("$.action", f"action must be one of {OPERATION_ACTIONS}")
    normalized = []
    for record in records or []:
        validated = validate_record(record)
        if validated["source_id"] != _text(source_id, "source_id", 1000):
            raise ContractError("$.records[]", "record source_id must match the operation")
        normalized.append(validated)
    if action in ("upsert", "metadata_update") and not normalized:
        raise ContractError("$.records", f"{action} requires at least one 1.0 record")
    if action == "remove" and metadata:
        raise ContractError("$.metadata", "remove cannot carry metadata")
    return {"action": action, "source_id": _text(source_id, "source_id", 1000),
            "records": normalized,
            "source_version": None if source_version is None else _text(str(source_version), "source_version", 200),
            "metadata": copy.deepcopy(metadata or {}),
            "reason": None if reason is None else _text(reason, "reason", 500),
            "coordinates": copy.deepcopy(coordinates or {}),
            "attachments": copy.deepcopy(list(attachments))}


def normalized_item(source_id, *, records, head_version=None, attachments=(), projections=()):
    items = [validate_record(r) for r in records]
    return {"source_id": _text(source_id, "source_id", 1000), "records": items,
            "head_version": None if head_version is None else str(head_version)[:200],
            "attachments": copy.deepcopy(list(attachments)),
            "projections": copy.deepcopy(list(projections))}


def source_page(*, page_id, operations, next_state, complete=True, coverage=(), more=False):
    """A bounded unit of progress. Empty and removal-only pages are first-class.

    ``more`` is the contract-level continuation flag: the adapter declares
    True when ``next_state`` still represents unfinished catch-up work on this
    stream, and only that declaration - never a cursor field shape - tells
    the runtime a required pass has converged.
    """
    if type(more) is not bool:
        raise ContractError("$.more", "Must be boolean")
    observed_ids = set()
    for operation in operations:
        if operation["source_id"] in observed_ids:
            raise ContractError("$.operations", "One operation per source_id per page")
        observed_ids.add(operation["source_id"])
    observations = []
    for entry in coverage:
        if not isinstance(entry, dict) or not {"start", "end", "state"} <= set(entry):
            raise ContractError("$.coverage[]", "coverage requires start, end and state")
        if entry["state"] not in ("complete", "gap", "pending"):
            raise ContractError("$.coverage[].state", "must be complete, gap or pending")
        observations.append({"start": _text(str(entry["start"]), "coverage.start", 500),
                             "end": _text(str(entry["end"]), "coverage.end", 500),
                             "state": entry["state"], "note": str(entry.get("note", ""))[:2000]})
    return {"page_id": _text(page_id, "page_id", 200),
            "operations": copy.deepcopy(list(operations)),
            "next_state": next_state, "complete": bool(complete),
            "more": bool(more), "coverage": observations}


def validate_page(page):
    """Runtime-side validation; adapter output cannot advance state until this passes."""
    if not isinstance(page, dict):
        raise ContractError("$", "page must be an object")
    unknown = set(page) - {"page_id", "operations", "next_state", "complete", "coverage", "more"}
    if unknown:
        raise ContractError("$", f"Unknown page field(s): {sorted(unknown)}")
    for field in ("page_id", "operations", "next_state", "complete"):
        if field not in page:
            raise ContractError(f"$.{field}", "Required page field is missing")
    _text(page["page_id"], "page_id", 200)
    from .ingestion import _json
    _json(page)
    if len(json.dumps(page, ensure_ascii=False).encode()) > 64 * 1024 * 1024:
        raise ContractError("$", "Page exceeds 64 MiB")
    if type(page['complete']) is not bool:
        raise ContractError('$.complete', 'Must be boolean')
    if 'more' in page and type(page['more']) is not bool:
        raise ContractError('$.more', 'Must be boolean')
    if not isinstance(page['operations'], list) or len(page['operations']) > 1000:
        raise ContractError('$.operations', 'Expected at most 1000 operations')
    seen = set()
    for index, operation in enumerate(page["operations"]):
        if not isinstance(operation, dict):
            raise ContractError('$.operations', 'Expected operation objects')
        bad = set(operation) - {"action", "source_id", "records", "source_version",
                                "metadata", "reason", "coordinates", "attachments"}
        if bad:
            raise ContractError(f"$.operations[{index}]", f"Unknown field(s): {sorted(bad)}")
        if operation["action"] not in OPERATION_ACTIONS:
            raise ContractError(f"$.operations[{index}].action", "Unknown action")
        normalized = source_operation(**operation)
        if normalized['source_id'] in seen:
            raise ContractError('$.operations', 'Duplicate source item')
        seen.add(normalized['source_id'])
        if not isinstance(normalized['metadata'], dict) or not isinstance(normalized['coordinates'], dict):
            raise ContractError('$.operations', 'Metadata and coordinates must be objects')
        if len(normalized['records']) > 400:
            raise ContractError('$.operations.records', 'Too many record parts')
        for descriptor in normalized['attachments']:
            if not isinstance(descriptor, dict):
                raise ContractError('$.attachments', 'Expected attachment objects')
    state = page["next_state"]
    bad = set(state) - set(read_state())
    if bad:
        raise ContractError("$.next_state", f"Unknown field(s): {sorted(bad)}")
    if state["mode"] not in READ_MODES:
        raise ContractError("$.next_state.mode", "Unknown read mode")
    read_state(**state)
    source_page(page_id=page['page_id'], operations=page['operations'],
                next_state=state, complete=page['complete'], coverage=page.get('coverage', []),
                more=page.get('more', False))
    return page


# "more" is deliberately excluded: it is scheduling metadata, not page content,
# so replay identity stays stable for receipts committed before it existed.
PAGE_DIGEST_FIELDS = ("page_id", "operations", "coverage", "next_state", "complete")


def page_digest(page):
    """Stable identity for replay: same content replays, altered content conflicts."""
    from .common import digest
    return digest([{field: page.get(field) for field in PAGE_DIGEST_FIELDS}])


class SourceAdapter(ABC):
    """Required core: spec/check/discover/read_page/normalize. Optional interfaces below
    must also be declared in capabilities; the runtime never schedules undeclared work."""

    @abstractmethod
    def spec(self) -> dict: ...

    @abstractmethod
    def check(self, context) -> dict: ...

    @abstractmethod
    def discover(self, context) -> list: ...

    @abstractmethod
    def read_page(self, context, state) -> dict: ...

    @abstractmethod
    def normalize(self, payload) -> dict: ...

    def accept_state(self, state, *, supported_state_version, migrate=False):
        """Opaque cursors are only reinterpreted through an explicit migration."""
        if state["state_version"] == supported_state_version:
            return state
        if not migrate:
            raise AdapterError("cursor", f"Adapter state version {state['state_version']} is not "
                               f"supported ({supported_state_version}); migrate or rescan explicitly")
        return self.migrate_state(state["state_version"], state)

    # Optional capability interfaces. Declared in spec().capabilities, then overridden.
    def fetch_attachment(self, reference):
        raise AdapterError("unsupported", "This adapter does not fetch attachments")

    def attachment(self, context, descriptor):
        """Connection-scoped attachment fetch; old reference-only adapters still work."""
        return self.fetch_attachment(descriptor)

    def verify_event(self, request):
        raise AdapterError("unsupported", "This adapter does not verify webhook events")

    def maintain_subscription(self, state):
        raise AdapterError("unsupported", "This adapter has no subscription lifecycle")

    def migrate_state(self, old_version, state):
        raise AdapterError("unsupported", "This adapter cannot migrate prior state; rescan required")


class _ConnectorAdapter(SourceAdapter):
    """SDK-01 compatibility: an export-only stream over an existing IngestionConnector."""

    def __init__(self, connector):
        self._connector = connector
        declared = connector.spec
        self._spec = adapter_spec(declared.connector_id, declared.connector_version,
                                  capabilities={"history": True},
                                  config_schema={"type": "object", "properties": {},
                                                 "additionalProperties": False})
        self._source = declared.source

    def spec(self):
        return copy.deepcopy(self._spec)

    def check(self, context):
        return {"account_id": self._source, "read_only": True}

    def discover(self, context):
        return [stream_spec("export", modes=["backfill"])]

    def read_page(self, context, state):
        if state["cursor"] is not None and state["cursor"].get("done"):
            return source_page(page_id="pg_done", operations=[], next_state=read_state(
                state_version=state["state_version"], cursor={"done": True}, mode=state["mode"]))
        operations = []
        for record in self._connector.records():
            operations.append(source_operation("upsert", record["source_id"], records=[record]))
        return source_page(page_id="pg_export", operations=operations,
                           next_state=read_state(state_version=state["state_version"],
                                                 cursor={"done": True}, mode=state["mode"]))

    def normalize(self, payload):
        record = validate_record(payload["record"])
        return normalized_item(record["source_id"], records=[record])


def wrap_ingestion_connector(connector):
    """Expose a legacy IngestionConnector as an export-only SourceAdapter. The old class
    is unchanged; connectors can remain export-only until they declare more."""
    return _ConnectorAdapter(connector)
