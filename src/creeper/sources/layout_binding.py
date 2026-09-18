"""Durable record-layout bindings for discovery-only structured sources."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass


_LAYOUT_MARKER = ":lay1:"


@dataclass(frozen=True, slots=True)
class SourceRecordLayout:
    """Describe how to recover a hostname from one structured record.

    Layout is execution metadata only. It carries no temporal semantics and can
    never grant annual-evidence authority.
    """

    parser_kind: str
    hostname_field: str
    delimiter: str | None
    detection_method: str
    confidence: float
    sample_records: int
    matched_records: int
    policy_version: str = "record-layout-v1"

    def __post_init__(self) -> None:
        parser = str(self.parser_kind).strip().lower()
        if parser not in {"jsonl", "delimited"}:
            raise ValueError("record layout supports jsonl or delimited parsers")
        hostname_field = str(self.hostname_field).strip()
        method = str(self.detection_method).strip().lower()
        policy = str(self.policy_version).strip()
        if not hostname_field:
            raise ValueError("hostname_field is required")
        if not method or not policy:
            raise ValueError("detection_method and policy_version are required")
        if parser == "delimited":
            if self.delimiter not in {",", "\t", ";", "|"}:
                raise ValueError("delimited layout requires a supported delimiter")
            normalized = hostname_field.lower().removeprefix("column:")
            if not normalized.isdigit() or int(normalized) > 255:
                raise ValueError(
                    "delimited hostname_field must be column:<zero-based-index>"
                )
        elif self.delimiter is not None:
            raise ValueError("jsonl layout must not define delimiter")
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, (int, float))
            or not math.isfinite(float(self.confidence))
            or not 0.0 <= float(self.confidence) <= 1.0
        ):
            raise ValueError("layout confidence must be within [0,1]")
        for name in ("sample_records", "matched_records"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.matched_records > self.sample_records:
            raise ValueError("matched_records cannot exceed sample_records")
        object.__setattr__(self, "parser_kind", parser)
        object.__setattr__(self, "hostname_field", hostname_field)
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
    def from_binding_token(cls, token: str) -> "SourceRecordLayout":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid layout binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            payload = json.loads(
                base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid layout binding token") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid layout binding payload")
        allowed = {
            "parser_kind",
            "hostname_field",
            "delimiter",
            "detection_method",
            "confidence",
            "sample_records",
            "matched_records",
            "policy_version",
        }
        if set(payload) != allowed:
            raise ValueError("invalid layout binding fields")
        return cls(**payload)


def bind_layout_to_adapter_id(
    adapter_id: str,
    layout: SourceRecordLayout,
) -> str:
    if _LAYOUT_MARKER in adapter_id:
        existing = layout_from_adapter_id(adapter_id)
        if existing != layout:
            raise ValueError("adapter_id is already bound to another record layout")
        return adapter_id
    head, ev_sep, ev_tail = adapter_id.partition(":evc1:")
    before_schema, schema_sep, schema_tail = head.partition(":sch1:")
    bound = f"{before_schema}{_LAYOUT_MARKER}{layout.binding_token}"
    if schema_sep:
        bound = f"{bound}:sch1:{schema_tail}"
    return bound if not ev_sep else f"{bound}:evc1:{ev_tail}"


def layout_from_adapter_id(adapter_id: str) -> SourceRecordLayout | None:
    head = adapter_id.split(":evc1:", 1)[0].split(":sch1:", 1)[0]
    if _LAYOUT_MARKER not in head:
        return None
    _prefix, token = head.rsplit(_LAYOUT_MARKER, 1)
    return SourceRecordLayout.from_binding_token(token)
