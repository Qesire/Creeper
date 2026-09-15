"""Durable record-schema bindings for structured direct evidence."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass

from creeper.evidence.contracts import EvidenceAuthority, SourceEvidenceContract


_SCHEMA_MARKER = ":sch1:"


@dataclass(frozen=True, slots=True)
class SourceRecordSchema:
    parser_kind: str
    hostname_field: str
    timestamp_field: str
    delimiter: str | None
    detection_method: str
    confidence: float
    sample_records: int
    matched_records: int
    direct_year_eligible: bool = False
    policy_version: str = "record-schema-v2"

    def __post_init__(self) -> None:
        parser = str(self.parser_kind).strip().lower()
        if parser not in {"jsonl", "delimited"}:
            raise ValueError("record schema supports jsonl or delimited parsers")
        hostname_field = str(self.hostname_field).strip()
        timestamp_field = str(self.timestamp_field).strip()
        method = str(self.detection_method).strip().lower()
        policy = str(self.policy_version).strip()
        if not hostname_field or not timestamp_field:
            raise ValueError("hostname_field and timestamp_field are required")
        if not method or not policy:
            raise ValueError("detection_method and policy_version are required")
        if parser == "delimited":
            if self.delimiter not in {",", "\t", ";", "|"}:
                raise ValueError("delimited schema requires a supported delimiter")
        elif self.delimiter is not None:
            raise ValueError("jsonl schema must not define delimiter")
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, (int, float))
            or not math.isfinite(float(self.confidence))
            or not 0.0 <= float(self.confidence) <= 1.0
        ):
            raise ValueError("schema confidence must be within [0,1]")
        for name in ("sample_records", "matched_records"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.matched_records > self.sample_records:
            raise ValueError("matched_records cannot exceed sample_records")
        if not isinstance(self.direct_year_eligible, bool):
            raise ValueError("direct_year_eligible must be boolean")
        object.__setattr__(self, "parser_kind", parser)
        object.__setattr__(self, "hostname_field", hostname_field)
        object.__setattr__(self, "timestamp_field", timestamp_field)
        object.__setattr__(self, "detection_method", method)
        object.__setattr__(self, "confidence", float(self.confidence))
        object.__setattr__(self, "policy_version", policy)

    @property
    def binding_token(self) -> str:
        raw = json.dumps(
            asdict(self),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(self.binding_token.encode("ascii")).hexdigest()

    @classmethod
    def from_binding_token(cls, token: str) -> "SourceRecordSchema":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid schema binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            payload = json.loads(
                base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid schema binding token") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid schema binding payload")
        allowed = {
            "parser_kind",
            "hostname_field",
            "timestamp_field",
            "delimiter",
            "detection_method",
            "confidence",
            "sample_records",
            "matched_records",
            "direct_year_eligible",
            "policy_version",
        }
        legacy_allowed = allowed - {"direct_year_eligible"}
        if set(payload) == legacy_allowed:
            payload["direct_year_eligible"] = False
        elif set(payload) != allowed:
            raise ValueError("invalid schema binding fields")
        return cls(**payload)

    def direct_contract(self) -> SourceEvidenceContract:
        if not self.direct_year_eligible:
            raise ValueError(
                "record schema lacks automatic direct-year semantic authority"
            )
        return SourceEvidenceContract(
            contract_id=f"auto-{self.parser_kind}-record-time-v1",
            authority=EvidenceAuthority.DIRECT_WEB_YEAR,
            parser_kind=self.parser_kind,
            temporal_semantics="record_field_timestamp",
            evidence_type="dated_structured_record",
            hostname_field=self.hostname_field,
            timestamp_field=self.timestamp_field,
            policy_version=self.policy_version,
        )


def bind_schema_to_adapter_id(
    adapter_id: str,
    schema: SourceRecordSchema,
) -> str:
    if _SCHEMA_MARKER in adapter_id:
        existing = schema_from_adapter_id(adapter_id)
        if existing != schema:
            raise ValueError("adapter_id is already bound to another record schema")
        return adapter_id
    head, ev_sep, ev_tail = adapter_id.partition(":evc1:")
    bound = f"{head}{_SCHEMA_MARKER}{schema.binding_token}"
    return bound if not ev_sep else f"{bound}:evc1:{ev_tail}"


def schema_from_adapter_id(adapter_id: str) -> SourceRecordSchema | None:
    head = adapter_id.split(":evc1:", 1)[0]
    if _SCHEMA_MARKER not in head:
        return None
    _prefix, token = head.rsplit(_SCHEMA_MARKER, 1)
    return SourceRecordSchema.from_binding_token(token)
