"""Versioned, language-neutral ingestion contract and Python connector interface.

Core fields are closed. Namespaced extensions are open. No source-specific field
is required by the memory core; absent event time is explicit, never fabricated.
"""
import copy
import json
import math
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable

from .common import now, timestamp

VERSION = "1.0"
NAMESPACE = r"^[a-z][a-z0-9_-]*(?:\.[a-z][a-z0-9_-]*)+$"


def string(maximum=1000):
    return {"type":"string","minLength":1,"maxLength":maximum,"pattern":r"\S"}


def closed(properties):
    return {"type":"object","properties":properties,"required":list(properties),"additionalProperties":False}


PARTICIPANT_SCHEMA = closed({"namespace":string(500),"address":string(500),"label":string(500),"relation":string(100)})
PROVENANCE_SCHEMA = closed({"connector_id":string(200),"connector_version":string(100),
                           "source_locator":string(2000),"origin":{"enum":["source","assistant","derived"]},
                           "parent_record_ids":{"type":"array","items":string(100),"maxItems":100,"uniqueItems":True}})
EXTENSION_SCHEMA = closed({"version":string(100),"data":{"type":"object"}})
RECORD_SCHEMA = {
    "$schema":"https://json-schema.org/draft/2020-12/schema",
    "title":"Personal Memory Ingestion Record 1.0",
    **closed({
        "schema_version":{"const":VERSION}, "source":string(200), "source_id":string(1000),
        "revision":string(200), "kind":string(100),
        "occurred_at":{"type":["string","null"],"format":"date-time"},
        "observed_at":{"type":"string","format":"date-time"},
        "text":string(100000), "participants":{"type":"array","items":PARTICIPANT_SCHEMA,"maxItems":1000},
        "provenance":PROVENANCE_SCHEMA,
        "extensions":{"type":"object","patternProperties":{NAMESPACE:EXTENSION_SCHEMA},"additionalProperties":False}
    }),
    "allOf":[{"if":{"properties":{"provenance":{"properties":{"origin":{"const":"derived"}}}}},
              "then":{"properties":{"provenance":{"properties":{"parent_record_ids":{"minItems":1}}}}}}]
}


class ContractError(ValueError):
    def __init__(self,path,message):
        self.path=path; self.message=message
        super().__init__(f"{path}: {message}")


def _json(value,path="$",depth=0):
    if depth>24: raise ContractError(path,"JSON nesting exceeds 24 levels")
    if isinstance(value,dict):
        for k,v in value.items():
            if not isinstance(k,str): raise ContractError(path,"JSON object keys must be strings")
            _json(v,path+"."+k,depth+1)
    elif isinstance(value,list):
        for i,v in enumerate(value): _json(v,f"{path}[{i}]",depth+1)
    elif isinstance(value,float) and not math.isfinite(value):
        raise ContractError(path,"NaN and infinity are not JSON data")
    elif value is not None and not isinstance(value,(str,int,float,bool)):
        raise ContractError(path,"Value is not JSON-serializable")


def _validate(value,schema,path):
    """Validate the exported contract's small schema vocabulary, without dependencies."""
    if "const" in schema and value!=schema["const"]: raise ContractError(path,"Unsupported schema version or constant")
    if "enum" in schema and value not in schema["enum"]: raise ContractError(path,"Unsupported value")
    expected=schema.get("type")
    types={"object":lambda x:isinstance(x,dict),"array":lambda x:isinstance(x,list),
           "string":lambda x:isinstance(x,str),"null":lambda x:x is None}
    if expected and not any(types[t](value) for t in (expected if isinstance(expected,list) else [expected])):
        raise ContractError(path,"Expected "+str(expected))
    if isinstance(value,str):
        if len(value)<schema.get("minLength",0) or len(value)>schema.get("maxLength",10**9): raise ContractError(path,"Invalid string length")
        if "pattern" in schema and not re.search(schema["pattern"],value): raise ContractError(path,"Invalid text format")
        if schema.get("format")=="date-time":
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})",value):
                raise ContractError(path,"Expected RFC 3339 timestamp with timezone")
            try: timestamp(value)
            except (ValueError,TypeError): raise ContractError(path,"Expected ISO 8601 timestamp with timezone") from None
    if isinstance(value,dict):
        for key in schema.get("required",[]):
            if key not in value: raise ContractError(path+"."+key,"Required field is missing")
        for key,child in value.items():
            rule=schema.get("properties",{}).get(key)
            if rule is None:
                rule=next((r for pattern,r in schema.get("patternProperties",{}).items() if re.fullmatch(pattern,key)),None)
            if rule is not None: _validate(child,rule,path+"."+key)
            elif schema.get("additionalProperties") is False: raise ContractError(path+"."+key,"Unknown core field; place custom data in a namespaced extension")
    if isinstance(value,list):
        if len(value)>schema.get("maxItems",10**9): raise ContractError(path,"Too many items")
        for i,child in enumerate(value):
            if "items" in schema: _validate(child,schema["items"],f"{path}[{i}]")
        if schema.get("uniqueItems") and len({json.dumps(v,sort_keys=True) for v in value})!=len(value): raise ContractError(path,"Duplicate items")


def validate_record(record):
    _json(record)
    _validate(record,RECORD_SCHEMA,"$")
    if record["provenance"]["origin"]=="derived" and not record["provenance"]["parent_record_ids"]:
        raise ContractError("$.provenance.parent_record_ids","Derived records require evidence IDs")
    if len(json.dumps(record,ensure_ascii=False).encode())>1024*1024:
        raise ContractError("$","Record exceeds 1 MiB")
    # Normalize only timestamps. Unknown data and extension payloads round-trip.
    result=copy.deepcopy(record)
    result["observed_at"]=timestamp(result["observed_at"])
    if result["occurred_at"] is not None: result["occurred_at"]=timestamp(result["occurred_at"])
    return result


def validate_batch(items):
    if not isinstance(items,list) or not 1<=len(items)<=100:
        raise ContractError("$.items","Expected 1..100 records")
    records=[]
    for index,item in enumerate(items):
        try: records.append(validate_record(item))
        except ContractError as error:
            raise ContractError(f"$.items[{index}]"+error.path[1:],error.message) from None
    return records


@dataclass(frozen=True)
class ConnectorSpec:
    connector_id: str
    connector_version: str
    source: str


class IngestionConnector(ABC):
    """Extend this class; yield complete records. The server validates independently.

    Connector-specific extension validation can be strengthened in validate_extra.
    Unregistered extension payloads still pass the universal structural contract.
    """
    @property
    @abstractmethod
    def spec(self) -> ConnectorSpec: ...

    @abstractmethod
    def read(self, checkpoint=None) -> Iterable[dict]: ...

    def validate_extra(self, record):
        pass

    def records(self, checkpoint=None):
        for record in self.read(checkpoint):
            normalized=validate_record(record)
            spec=self.spec; provenance=normalized["provenance"]
            if normalized["source"]!=spec.source or provenance["connector_id"]!=spec.connector_id or provenance["connector_version"]!=spec.connector_version:
                raise ContractError("$.provenance","Record does not match the connector declaration")
            self.validate_extra(normalized)
            yield normalized


def submit(client, connector, checkpoint=None):
    """Per-record acknowledgments; connector persists its own source checkpoint.

    A failed record stops delivery. Replaying stable IDs/revisions is idempotent.
    Validation occurs before each write and again at the receiving boundary.
    """
    for record in connector.records(checkpoint):
        yield client.call("/v1/ingest",{"items":[record]})


def adapt_existing(record, *, connector_id, connector_version, source_locator, observed_at):
    """Explicit migration adapter used by bundled legacy parsers, never by HTTP.

    The caller supplies real connector provenance and observation time. Extra
    legacy metadata is preserved under a namespace, not promoted to core facts.
    """
    metadata=copy.deepcopy(record.get("metadata",{}))
    participants=[]
    for p in metadata.pop("participants",[]):
        participants.append({"namespace":p["namespace"],"address":p["address"],
                             "label":p.get("label",p["address"]),"relation":p.get("relation","participant")})
    generated=record["source"] in {"hermes","hermes-checkpoint","hermes-delegation"}
    return validate_record({"schema_version":VERSION,"source":record["source"],"source_id":record["source_id"],
        "revision":record.get("revision","1"),"kind":record.get("kind","episode"),
        "occurred_at":record.get("occurred_at"),"observed_at":observed_at,"text":record["text"],
        "participants":participants,"provenance":{"connector_id":connector_id,"connector_version":connector_version,
        "source_locator":source_locator,"origin":"assistant" if generated else "source","parent_record_ids":[]},
        "extensions":{"personal_memory.legacy":{"version":"1.0","data":metadata}} if metadata else {}})
