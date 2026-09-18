"""Durable parser/compression observations and adapter bindings."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass


_FORMAT_MARKER = ":fmt1:"
_LAYOUT_MARKER = ":lay1:"
_SCHEMA_MARKER = ":sch1:"
_EVIDENCE_MARKER = ":evc1:"
_SUPPORTED_PARSER_KINDS = frozenset(
    {
        "cdx",
        "cdxj",
        "jsonl",
        "delimited",
        "lines",
        "mbox_urls",
        "squid_access",
        "dmoz_rdf_urls",
        "ftp_sitelist_zip",
        "sbi_bbs_zip",
        "warc_arc",
    }
)
_SUPPORTED_COMPRESSION = frozenset({"none", "gzip"})


@dataclass(frozen=True, slots=True)
class SourceFormatObservation:
    parser_kind: str
    compression: str
    detection_method: str
    confidence: float
    content_type: str = ""
    delimiter: str | None = None
    policy_version: str = "source-format-v1"

    def __post_init__(self) -> None:
        parser_kind = str(self.parser_kind).strip().lower()
        compression = str(self.compression).strip().lower()
        method = str(self.detection_method).strip().lower()
        content_type = str(self.content_type).strip().lower()
        delimiter = self.delimiter
        if delimiter is not None:
            delimiter = str(delimiter)
        policy = str(self.policy_version).strip()
        if parser_kind not in _SUPPORTED_PARSER_KINDS:
            raise ValueError(f"unsupported parser_kind: {parser_kind}")
        if compression not in _SUPPORTED_COMPRESSION:
            raise ValueError(f"unsupported compression: {compression}")
        if not method:
            raise ValueError("detection_method is required")
        if delimiter is not None:
            if parser_kind != "delimited":
                raise ValueError("delimiter is valid only for delimited parser")
            if delimiter not in {",", "\t", ";", "|"}:
                raise ValueError("unsupported delimited parser delimiter")
        if (
            isinstance(self.confidence, bool)
            or not isinstance(self.confidence, (int, float))
            or not math.isfinite(float(self.confidence))
            or not 0.0 <= float(self.confidence) <= 1.0
        ):
            raise ValueError("format confidence must be within [0,1]")
        if not policy:
            raise ValueError("policy_version is required")
        object.__setattr__(self, "parser_kind", parser_kind)
        object.__setattr__(self, "compression", compression)
        object.__setattr__(self, "detection_method", method)
        object.__setattr__(self, "content_type", content_type)
        object.__setattr__(self, "delimiter", delimiter)
        object.__setattr__(self, "confidence", float(self.confidence))
        object.__setattr__(self, "policy_version", policy)

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(self.binding_token.encode("ascii")).hexdigest()

    @property
    def binding_token(self) -> str:
        raw = json.dumps(
            asdict(self),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @classmethod
    def from_binding_token(cls, token: str) -> "SourceFormatObservation":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid format binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            payload = json.loads(
                base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid format binding token") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid format binding payload")
        allowed = {
            "parser_kind",
            "compression",
            "detection_method",
            "confidence",
            "content_type",
            "delimiter",
            "policy_version",
        }
        legacy_allowed = allowed - {"delimiter"}
        if set(payload) == legacy_allowed:
            payload["delimiter"] = None
        elif set(payload) != allowed:
            raise ValueError("invalid format binding fields")
        return cls(**payload)


def bind_format_to_adapter_id(
    adapter_id: str,
    observation: SourceFormatObservation,
) -> str:
    if _FORMAT_MARKER in adapter_id:
        existing = format_from_adapter_id(adapter_id)
        if existing != observation:
            raise ValueError("adapter_id is already bound to another source format")
        return adapter_id
    head, sep, evidence_tail = adapter_id.partition(_EVIDENCE_MARKER)
    marker_positions = [
        position
        for marker in (_LAYOUT_MARKER, _SCHEMA_MARKER)
        if (position := head.find(marker)) >= 0
    ]
    split_at = min(marker_positions) if marker_positions else len(head)
    prefix, suffix = head[:split_at], head[split_at:]
    bound = f"{prefix}{_FORMAT_MARKER}{observation.binding_token}{suffix}"
    return bound if not sep else f"{bound}{_EVIDENCE_MARKER}{evidence_tail}"


def format_from_adapter_id(adapter_id: str) -> SourceFormatObservation | None:
    head = adapter_id.split(_EVIDENCE_MARKER, 1)[0]
    for marker in (_LAYOUT_MARKER, _SCHEMA_MARKER):
        head = head.split(marker, 1)[0]
    if _FORMAT_MARKER not in head:
        return None
    _prefix, token = head.rsplit(_FORMAT_MARKER, 1)
    return SourceFormatObservation.from_binding_token(token)
