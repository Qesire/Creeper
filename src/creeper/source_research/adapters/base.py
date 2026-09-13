"""Contracts for deterministic V7.1 structured-root adapters."""
from __future__ import annotations
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any, Protocol
@dataclass(frozen=True)
class RootQuery:
    query_id: str; root_id: str; query_text: str; max_pages: int; max_wall_seconds: float; page_size: int = 1000
    native_filters: dict[str,str] = field(default_factory=dict); expected_signal: str = ""; expected_artifact_family: str = ""
@dataclass(frozen=True)
class SearchCheckpoint:
    cursor: str|None=None; next_url: str|None=None; page: int=1; start: int=0; query_variant: str|None=None
@dataclass(frozen=True)
class SearchHit:
    root_id: str; query_id: str; provider_native_id: str; provider_url: str; provider_type: str
    title: str=""; description: str=""; metadata: dict[str,Any]=field(default_factory=dict); observed_at: float=0.0; research_node_id: str=""
@dataclass(frozen=True)
class ArtifactLead:
    root_id: str; provider_native_id: str; locator: str; content_type: str=""; size: int|None=None; checksum: str|None=None
    persistent_id: str|None=None; parent_persistent_id: str|None=None; kind: str="ARTIFACT_LEAD"; evidence_year: None=None
@dataclass(frozen=True)
class SearchPage:
    hits: tuple[SearchHit,...]=(); artifact_leads: tuple[ArtifactLead,...]=(); next_checkpoint: SearchCheckpoint|None=None
    terminal: bool=True; requests: int=1; bytes_read: int=0; retry_after: float|None=None
@dataclass(frozen=True)
class RootCapabilityReport:
    root_id: str; available: bool; capabilities: tuple[str,...]=(); reason: str=""; status_code: int|None=None
class JsonTransport(Protocol):
    def __call__(self,url: str,params: dict[str,Any]|None=None,headers: dict[str,str]|None=None)->Awaitable[Any]: ...
def response_json(response: Any)->dict[str,Any]:
    payload=response.json() if callable(getattr(response,"json",None)) else response
    if not isinstance(payload,dict): raise ValueError("root response must be a JSON object")
    return payload
def retry_after_seconds(response: Any)->float|None:
    try: return float(getattr(response,"headers",{}).get("Retry-After"))
    except (TypeError,ValueError): return None
def is_retryable(response: Any)->bool:
    status=int(getattr(response,"status_code",200)); return status==429 or status>=500
