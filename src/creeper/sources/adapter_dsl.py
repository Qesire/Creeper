"""Restricted adapter DSL for source formats without a mature parser.

The DSL is intentionally much less expressive than Python.  It supports one
bounded line-oriented grammar useful for historical logs and host inventories:
split a record on ASCII/Unicode whitespace, select a hostname/URL token by
column index, and optionally interpret another token as an observation time.

LLM output may propose this syntax, but Creeper validates it deterministically
against the exact bounded scout sample before persisting or executing it.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from urllib.parse import urlsplit

from creeper.authority.normalizer import normalize_official


_ADAPTER_MARKER = ":dsl1:"


class AdapterDSLKind(StrEnum):
    WHITESPACE_COLUMNS = "whitespace_columns"


class TimestampKind(StrEnum):
    NONE = "none"
    YEAR_PREFIX = "year_prefix"
    UNIX_SECONDS = "unix_seconds"


@dataclass(frozen=True, slots=True)
class AdapterDSLSpec:
    kind: AdapterDSLKind
    host_column: int
    timestamp_column: int | None = None
    timestamp_kind: TimestampKind = TimestampKind.NONE
    skip_lines: int = 0
    comment_prefixes: tuple[str, ...] = ()
    min_columns: int = 1
    policy_version: str = "adapter-dsl-v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", AdapterDSLKind(self.kind))
        object.__setattr__(self, "timestamp_kind", TimestampKind(self.timestamp_kind))
        object.__setattr__(self, "comment_prefixes", tuple(self.comment_prefixes))

        for name in ("host_column", "skip_lines", "min_columns"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{name} must be an integer")
        if not 0 <= self.host_column <= 31:
            raise ValueError("host_column must be within [0,31]")
        if not 0 <= self.skip_lines <= 64:
            raise ValueError("skip_lines must be within [0,64]")
        if not 1 <= self.min_columns <= 64:
            raise ValueError("min_columns must be within [1,64]")

        if self.timestamp_column is None:
            if self.timestamp_kind is not TimestampKind.NONE:
                raise ValueError("timestamp_kind requires timestamp_column")
        else:
            if (
                isinstance(self.timestamp_column, bool)
                or not isinstance(self.timestamp_column, int)
                or not 0 <= self.timestamp_column <= 31
            ):
                raise ValueError("timestamp_column must be within [0,31]")
            if self.timestamp_column == self.host_column:
                raise ValueError("host and timestamp columns must differ")
            if self.timestamp_kind is TimestampKind.NONE:
                raise ValueError("timestamp_column requires timestamp_kind")

        if max(
            self.host_column,
            -1 if self.timestamp_column is None else self.timestamp_column,
        ) >= self.min_columns:
            raise ValueError("min_columns must include selected columns")

        if len(self.comment_prefixes) > 8:
            raise ValueError("at most eight comment prefixes are allowed")
        for prefix in self.comment_prefixes:
            if (
                not isinstance(prefix, str)
                or not prefix
                or len(prefix) > 16
                or "\n" in prefix
                or "\r" in prefix
            ):
                raise ValueError("invalid comment prefix")
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version is required")

    @property
    def binding_token(self) -> str:
        payload = {
            **asdict(self),
            "kind": self.kind.value,
            "timestamp_kind": self.timestamp_kind.value,
            "comment_prefixes": list(self.comment_prefixes),
        }
        raw = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @property
    def binding_digest(self) -> str:
        return hashlib.sha256(self.binding_token.encode("ascii")).hexdigest()

    @classmethod
    def from_binding_token(cls, token: str) -> "AdapterDSLSpec":
        if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
            raise ValueError("invalid adapter DSL binding token")
        padded = token + "=" * (-len(token) % 4)
        try:
            payload = json.loads(
                base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            )
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid adapter DSL binding token") from exc
        if not isinstance(payload, dict):
            raise ValueError("invalid adapter DSL binding payload")
        allowed = {
            "kind",
            "host_column",
            "timestamp_column",
            "timestamp_kind",
            "skip_lines",
            "comment_prefixes",
            "min_columns",
            "policy_version",
        }
        if set(payload) != allowed:
            raise ValueError("invalid adapter DSL binding fields")
        prefixes = payload.get("comment_prefixes")
        if not isinstance(prefixes, list) or any(
            not isinstance(item, str) for item in prefixes
        ):
            raise ValueError("invalid adapter DSL comment_prefixes")
        payload["comment_prefixes"] = tuple(prefixes)
        return cls(**payload)


@dataclass(frozen=True, slots=True)
class AdapterDSLRecord:
    hostname: str
    source_year: int | None
    source_time: str | None


@dataclass(frozen=True, slots=True)
class AdapterDSLValidation:
    sampled_records: int
    host_records: int
    timed_records: int
    host_fraction: float
    timed_fraction: float
    sample_sha256: str

    def __post_init__(self) -> None:
        for name in ("sampled_records", "host_records", "timed_records"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.host_records > self.sampled_records:
            raise ValueError("host_records cannot exceed sampled_records")
        if self.timed_records > self.host_records:
            raise ValueError("timed_records cannot exceed host_records")
        for name in ("host_fraction", "timed_fraction"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0,1]")


def bind_adapter_dsl_to_adapter_id(
    adapter_id: str,
    spec: AdapterDSLSpec,
) -> str:
    if _ADAPTER_MARKER in adapter_id:
        existing = adapter_dsl_from_adapter_id(adapter_id)
        if existing != spec:
            raise ValueError("adapter_id is already bound to another adapter DSL")
        return adapter_id

    # Keep parser/schema/evidence token ordering stable. DSL is execution
    # syntax and belongs immediately after the base adapter identity.
    marker_positions = [
        position
        for marker in (":fmt1:", ":sch1:", ":evc1:")
        if (position := adapter_id.find(marker)) >= 0
    ]
    position = min(marker_positions) if marker_positions else len(adapter_id)
    return (
        adapter_id[:position]
        + _ADAPTER_MARKER
        + spec.binding_token
        + adapter_id[position:]
    )


def adapter_dsl_from_adapter_id(adapter_id: str) -> AdapterDSLSpec | None:
    head = adapter_id.split(":evc1:", 1)[0]
    if _ADAPTER_MARKER not in head:
        return None
    token_and_tail = head.split(_ADAPTER_MARKER, 1)[1]
    token = token_and_tail.split(":fmt1:", 1)[0].split(":sch1:", 1)[0]
    return AdapterDSLSpec.from_binding_token(token)


def _hostname_from_token(token: str) -> str | None:
    text = token.strip().strip('"').strip("'")
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
    if ":" in text and text.count(":") == 1:
        host, port = text.rsplit(":", 1)
        if port.isdigit():
            text = host
    return normalize_official(text)


def _timestamp(
    value: str,
    kind: TimestampKind,
    *,
    year_from: int,
    year_to: int,
) -> tuple[int | None, str | None]:
    text = value.strip()
    if kind is TimestampKind.NONE:
        return None, None
    if kind is TimestampKind.YEAR_PREFIX:
        match = re.match(r"^(\d{4})", text)
        if match is None:
            return None, None
        year = int(match.group(1))
        return (year, text) if year_from <= year <= year_to else (None, None)
    if kind is TimestampKind.UNIX_SECONDS:
        try:
            raw = float(text)
        except ValueError:
            return None, None
        if not math.isfinite(raw) or raw < 0:
            return None, None
        try:
            year = datetime.fromtimestamp(raw, tz=timezone.utc).year
        except (OverflowError, OSError, ValueError):
            return None, None
        return (year, text) if year_from <= year <= year_to else (None, None)
    raise AssertionError("unhandled timestamp kind")


def parse_adapter_dsl_line(
    spec: AdapterDSLSpec,
    line: str,
    *,
    line_number: int,
    year_from: int = 1996,
    year_to: int = 2001,
) -> AdapterDSLRecord | None:
    if line_number < spec.skip_lines:
        return None
    stripped = line.strip()
    if not stripped or any(stripped.startswith(prefix) for prefix in spec.comment_prefixes):
        return None
    if spec.kind is not AdapterDSLKind.WHITESPACE_COLUMNS:
        raise AssertionError("unsupported adapter DSL kind")
    columns = stripped.split()
    if len(columns) < spec.min_columns:
        return None
    hostname = _hostname_from_token(columns[spec.host_column])
    if hostname is None:
        return None
    if spec.timestamp_column is None:
        return AdapterDSLRecord(hostname, None, None)
    year, source_time = _timestamp(
        columns[spec.timestamp_column],
        spec.timestamp_kind,
        year_from=year_from,
        year_to=year_to,
    )
    return AdapterDSLRecord(hostname, year, source_time)


def validate_adapter_dsl(
    spec: AdapterDSLSpec,
    payload: bytes,
    *,
    year_from: int = 1996,
    year_to: int = 2001,
    max_records: int = 256,
    min_records: int = 3,
    min_host_fraction: float = 0.80,
    min_timed_fraction: float = 0.80,
) -> AdapterDSLValidation:
    if not 1996 <= year_from <= year_to <= 2001:
        raise ValueError("adapter validation years must be within 1996-2001")
    if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records < 1:
        raise ValueError("max_records must be positive")
    if isinstance(min_records, bool) or not isinstance(min_records, int) or min_records < 2:
        raise ValueError("min_records must be >= 2")
    for name, value in (
        ("min_host_fraction", min_host_fraction),
        ("min_timed_fraction", min_timed_fraction),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0.5 <= float(value) <= 1.0
        ):
            raise ValueError(f"{name} must be within [0.5,1]")

    sampled = 0
    hosts = 0
    timed = 0
    for line_number, line in enumerate(
        payload.decode("utf-8", errors="replace").splitlines()
    ):
        if line_number < spec.skip_lines:
            continue
        stripped = line.strip()
        if not stripped or any(
            stripped.startswith(prefix) for prefix in spec.comment_prefixes
        ):
            continue
        sampled += 1
        record = parse_adapter_dsl_line(
            spec,
            line,
            line_number=line_number,
            year_from=year_from,
            year_to=year_to,
        )
        if record is not None:
            hosts += 1
            if record.source_year is not None:
                timed += 1
        if sampled >= max_records:
            break

    host_fraction = hosts / sampled if sampled else 0.0
    timed_fraction = timed / hosts if hosts else 0.0
    validation = AdapterDSLValidation(
        sampled_records=sampled,
        host_records=hosts,
        timed_records=timed,
        host_fraction=host_fraction,
        timed_fraction=timed_fraction,
        sample_sha256=hashlib.sha256(payload).hexdigest(),
    )
    if sampled < min_records:
        raise ValueError("adapter DSL sample has too few records")
    if hosts < min_records or host_fraction < float(min_host_fraction):
        raise ValueError("adapter DSL hostname validation below threshold")
    if (
        spec.timestamp_column is not None
        and (timed < min_records or timed_fraction < float(min_timed_fraction))
    ):
        raise ValueError("adapter DSL timestamp validation below threshold")
    return validation
