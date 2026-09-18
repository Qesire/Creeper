"""Bounded unknown-format cases and declarative adapter validation.

LLM output is proposal-only. This module accepts only layouts that can be
executed by Creeper's already-mature structured readers; it never compiles or
executes model-generated code and never grants annual-evidence authority.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import math
import re
import zlib
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlsplit

from creeper.authority.normalizer import normalize_official
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.schema_binding import SourceRecordSchema


UNKNOWN_FORMAT_PREFIX = "UNKNOWN_FORMAT_V1:"
_MAX_REASON_CHARS = 8 * 1024
_MAX_PREVIEW_BYTES = 4 * 1024
_ALLOWED_DELIMITERS = {",", "\t", ";", "|"}


class UnknownFormatProtocolError(ValueError):
    """Raised when a proposed adapter cannot be deterministically validated."""


@dataclass(frozen=True, slots=True)
class UnknownFormatCase:
    sample_sha256: str
    content_type: str
    compression: str
    preview_b64: str
    sampled_bytes: int
    truncated: bool

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", self.sample_sha256):
            raise ValueError("invalid unknown-format sample digest")
        if self.compression not in {"none", "gzip"}:
            raise ValueError("unsupported unknown-format compression")
        if (
            isinstance(self.sampled_bytes, bool)
            or not isinstance(self.sampled_bytes, int)
            or self.sampled_bytes < 1
        ):
            raise ValueError("sampled_bytes must be positive")
        if not isinstance(self.truncated, bool):
            raise ValueError("truncated must be boolean")
        try:
            raw = base64.urlsafe_b64decode(
                self.preview_b64 + "=" * (-len(self.preview_b64) % 4)
            )
        except Exception as exc:
            raise ValueError("invalid unknown-format preview") from exc
        if not raw or len(raw) > _MAX_PREVIEW_BYTES:
            raise ValueError("unknown-format preview must be bounded and non-empty")

    @property
    def preview_bytes(self) -> bytes:
        return base64.urlsafe_b64decode(
            self.preview_b64 + "=" * (-len(self.preview_b64) % 4)
        )

    @property
    def preview_text(self) -> str:
        return self.preview_bytes.decode("utf-8", errors="replace")

    def prompt_payload(self) -> dict[str, object]:
        return {
            "sample_sha256": self.sample_sha256,
            "content_type": self.content_type,
            "compression": self.compression,
            "sampled_bytes": self.sampled_bytes,
            "truncated": self.truncated,
            "preview": self.preview_text,
            "allowed_parser_kinds": ["jsonl", "delimited"],
            "authority": "discovery_only",
        }


def _inflate_preview(payload: bytes) -> tuple[bytes, str] | None:
    if not payload.startswith(b"\x1f\x8b"):
        return payload[:_MAX_PREVIEW_BYTES], "none"
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        decoded = decoder.decompress(payload, _MAX_PREVIEW_BYTES + 1)
    except zlib.error:
        return None
    if not decoded:
        return None
    return decoded[:_MAX_PREVIEW_BYTES], "gzip"


def _textual_enough(payload: bytes) -> bool:
    if not payload:
        return False
    text = payload.decode("utf-8", errors="replace")
    if not text.strip():
        return False
    replacements = text.count("\ufffd")
    printable = sum(ch.isprintable() or ch in "\r\n\t" for ch in text)
    return replacements / max(1, len(text)) <= 0.05 and printable / max(1, len(text)) >= 0.90


def make_unknown_format_reason(
    payload: bytes,
    *,
    content_type: str,
    truncated: bool,
) -> str | None:
    """Encode one small textual sample into durable HOLD state.

    Binary or opaque payloads do not enter the LLM path. The full downloaded
    object is never persisted here.
    """

    if not payload:
        return None
    preview_info = _inflate_preview(payload)
    if preview_info is None:
        return None
    preview, compression = preview_info
    if not _textual_enough(preview):
        return None
    case = UnknownFormatCase(
        sample_sha256=hashlib.sha256(payload).hexdigest(),
        content_type=str(content_type or "")[:256],
        compression=compression,
        preview_b64=base64.urlsafe_b64encode(preview).decode("ascii").rstrip("="),
        sampled_bytes=len(payload),
        truncated=bool(truncated),
    )
    raw = json.dumps(
        {
            "sample_sha256": case.sample_sha256,
            "content_type": case.content_type,
            "compression": case.compression,
            "preview_b64": case.preview_b64,
            "sampled_bytes": case.sampled_bytes,
            "truncated": case.truncated,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    reason = UNKNOWN_FORMAT_PREFIX + token
    if len(reason) > _MAX_REASON_CHARS:
        raise ValueError("unknown-format state reason exceeded durable bound")
    return reason


def parse_unknown_format_reason(reason: str) -> UnknownFormatCase | None:
    if not isinstance(reason, str) or not reason.startswith(UNKNOWN_FORMAT_PREFIX):
        return None
    token = reason.removeprefix(UNKNOWN_FORMAT_PREFIX)
    if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
        return None
    try:
        payload = json.loads(
            base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode("utf-8")
        )
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return UnknownFormatCase(
            sample_sha256=str(payload["sample_sha256"]),
            content_type=str(payload.get("content_type", "")),
            compression=str(payload["compression"]),
            preview_b64=str(payload["preview_b64"]),
            sampled_bytes=int(payload["sampled_bytes"]),
            truncated=bool(payload["truncated"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _hostname_from_scalar(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip().strip('"').strip("'")
    if not text:
        return None
    if "://" in text or text.startswith("//"):
        try:
            parsed = urlsplit(text if not text.startswith("//") else "http:" + text)
        except ValueError:
            return None
        return normalize_official(parsed.hostname or "")
    if "/" in text:
        try:
            parsed = urlsplit("http://" + text)
        except ValueError:
            return None
        return normalize_official(parsed.hostname or "")
    return normalize_official(text)


def _target_year(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if not isinstance(value, (str, int, float)):
        return None
    text = str(value).strip()
    if len(text) < 4 or not text[:4].isdigit():
        return None
    year = int(text[:4])
    return year if 1996 <= year <= 2001 else None


def _field(value: object, name: str) -> object | None:
    if not isinstance(value, Mapping):
        return None
    lowered = {str(key).strip().lower(): item for key, item in value.items()}
    return lowered.get(name.strip().lower())


def _validate_jsonl(
    text: str,
    *,
    hostname_field: str,
    timestamp_field: str,
) -> tuple[int, int]:
    sampled = 0
    matched = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            sampled += 1
            continue
        sampled += 1
        if (
            _hostname_from_scalar(_field(value, hostname_field)) is not None
            and _target_year(_field(value, timestamp_field)) is not None
        ):
            matched += 1
        if sampled >= 64:
            break
    return sampled, matched


def _column_index(field: str) -> int:
    text = field.strip().lower().removeprefix("column:")
    if not text.isdigit():
        raise UnknownFormatProtocolError(
            "delimited adapter fields must use column:<zero-based-index>"
        )
    value = int(text)
    if value > 255:
        raise UnknownFormatProtocolError("delimited column index is unreasonably large")
    return value


def _validate_delimited(
    text: str,
    *,
    delimiter: str,
    hostname_field: str,
    timestamp_field: str,
) -> tuple[int, int]:
    host_index = _column_index(hostname_field)
    time_index = _column_index(timestamp_field)
    try:
        rows = csv.reader(io.StringIO(text), delimiter=delimiter)
        materialized = []
        for row in rows:
            if any(cell.strip() for cell in row):
                materialized.append(row)
            if len(materialized) >= 65:
                break
    except csv.Error as exc:
        raise UnknownFormatProtocolError("invalid proposed delimited layout") from exc
    if not materialized:
        return 0, 0

    def row_matches(row: list[str]) -> bool:
        return (
            host_index < len(row)
            and time_index < len(row)
            and _hostname_from_scalar(row[host_index]) is not None
            and _target_year(row[time_index]) is not None
        )

    data = materialized
    if len(materialized) >= 4 and not row_matches(materialized[0]):
        if sum(row_matches(row) for row in materialized[1:4]) >= 3:
            data = materialized[1:]
    sampled = len(data)
    matched = sum(row_matches(row) for row in data)
    return sampled, matched


def validate_adapter_proposal(
    proposal: Mapping[str, Any],
    case: UnknownFormatCase,
) -> tuple[SourceFormatObservation, SourceRecordSchema]:
    """Validate an LLM layout proposal against the durable sample.

    Only existing line-oriented parser families are accepted. The resulting
    schema is deliberately discovery-only: direct_year_eligible is forced false
    regardless of the proposed timestamp field.
    """

    if not isinstance(proposal, Mapping):
        raise UnknownFormatProtocolError("adapter proposal must be an object")
    allowed = {
        "parser_kind",
        "compression",
        "hostname_field",
        "timestamp_field",
        "delimiter",
    }
    unknown = set(proposal) - allowed
    if unknown:
        raise UnknownFormatProtocolError(
            f"unknown adapter proposal fields: {sorted(unknown)}"
        )
    parser_kind = str(proposal.get("parser_kind", "")).strip().lower()
    if parser_kind not in {"jsonl", "delimited"}:
        raise UnknownFormatProtocolError(
            "adapter proposal parser_kind must be jsonl or delimited"
        )
    compression = str(proposal.get("compression", "")).strip().lower()
    if compression != case.compression:
        raise UnknownFormatProtocolError(
            "adapter proposal compression disagrees with sampled object"
        )
    hostname_field = str(proposal.get("hostname_field", "")).strip()
    timestamp_field = str(proposal.get("timestamp_field", "")).strip()
    if not hostname_field or not timestamp_field:
        raise UnknownFormatProtocolError(
            "adapter proposal requires hostname_field and timestamp_field"
        )

    delimiter_raw = proposal.get("delimiter")
    delimiter = None if delimiter_raw is None else str(delimiter_raw)
    text = case.preview_text
    if parser_kind == "jsonl":
        if delimiter is not None:
            raise UnknownFormatProtocolError("jsonl adapter must not define delimiter")
        sampled, matched = _validate_jsonl(
            text,
            hostname_field=hostname_field,
            timestamp_field=timestamp_field,
        )
    else:
        if delimiter not in _ALLOWED_DELIMITERS:
            raise UnknownFormatProtocolError(
                "delimited adapter requires one of comma, tab, semicolon, or pipe"
            )
        sampled, matched = _validate_delimited(
            text,
            delimiter=delimiter,
            hostname_field=hostname_field,
            timestamp_field=timestamp_field,
        )

    if sampled < 3 or matched < 3:
        raise UnknownFormatProtocolError(
            "adapter proposal lacks three deterministic matching sample records"
        )
    confidence = matched / sampled
    if not math.isfinite(confidence) or confidence < 0.90:
        raise UnknownFormatProtocolError(
            "adapter proposal matches less than 90% of sampled records"
        )

    format_observation = SourceFormatObservation(
        parser_kind=parser_kind,
        compression=compression,
        detection_method="llm_declarative_validated",
        confidence=confidence,
        content_type=case.content_type,
        delimiter=delimiter,
        policy_version="source-format-llm-layout-v1",
    )
    schema = SourceRecordSchema(
        parser_kind=parser_kind,
        hostname_field=hostname_field,
        timestamp_field=timestamp_field,
        delimiter=delimiter,
        detection_method="llm_declarative_validated",
        confidence=confidence,
        sample_records=sampled,
        matched_records=matched,
        direct_year_eligible=False,
        policy_version="record-schema-llm-layout-v1",
    )
    return format_observation, schema
