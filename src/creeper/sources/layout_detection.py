"""Deterministic hostname-layout inference for structured discovery records."""

from __future__ import annotations

import csv
import io
import json
import math
import zlib
from collections import Counter
from urllib.parse import urlsplit

from creeper.authority.normalizer import normalize_official
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.layout_binding import SourceRecordLayout
from creeper.sources.schema_binding import SourceRecordSchema



_AUTO_DIRECT_LAYOUT_METHODS = frozenset(
    {
        "stable_json_host_field",
        "stable_delimited_host_column",
        "schema:stable_json_fields",
        "schema:stable_delimited_columns",
    }
)


def layout_allows_auto_direct(layout: SourceRecordLayout) -> bool:
    """Return whether layout provenance is deterministic enough for auto-direct.

    Layout itself never grants authority. This gate only prevents a model-chosen
    hostname field from becoming direct-year authority indirectly when a later
    deterministic scout discovers a capture timestamp.
    """

    return layout.detection_method in _AUTO_DIRECT_LAYOUT_METHODS

def _inflate_prefix(payload: bytes, *, limit: int = 512 * 1024) -> bytes | None:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        return decoder.decompress(payload, limit)
    except zlib.error:
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


def _confidence(matched: int, sampled: int) -> float:
    return 0.0 if sampled <= 0 else min(1.0, matched / sampled)


def layout_from_schema(schema: SourceRecordSchema) -> SourceRecordLayout:
    """Project a timestamp-bearing schema onto its authority-free host layout."""

    return SourceRecordLayout(
        parser_kind=schema.parser_kind,
        hostname_field=schema.hostname_field,
        delimiter=schema.delimiter,
        detection_method=f"schema:{schema.detection_method}",
        confidence=schema.confidence,
        sample_records=schema.sample_records,
        matched_records=schema.matched_records,
        policy_version="record-layout-from-schema-v1",
    )


def _jsonl_layout(
    text: str,
    *,
    min_records: int,
    min_fraction: float,
) -> SourceRecordLayout | None:
    records: list[dict[str, object]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append({str(key).strip().lower(): item for key, item in value.items()})
        if len(records) >= 64:
            break
    if len(records) < min_records:
        return None

    counts: Counter[str] = Counter()
    for record in records:
        for field, value in record.items():
            if _hostname_from_scalar(value) is not None:
                counts[field] += 1
    if not counts:
        return None
    best = max(counts.values())
    winners = sorted(field for field, count in counts.items() if count == best)
    if len(winners) != 1:
        # Two equally plausible columns are not enough evidence to choose which
        # one represents the source's hostname identity.
        return None
    confidence = _confidence(best, len(records))
    if best < min_records or confidence < min_fraction:
        return None
    return SourceRecordLayout(
        parser_kind="jsonl",
        hostname_field=winners[0],
        delimiter=None,
        detection_method="stable_json_host_field",
        confidence=confidence,
        sample_records=len(records),
        matched_records=best,
        policy_version="record-layout-detect-v1",
    )


def _delimited_layout(
    text: str,
    *,
    delimiter_hint: str | None,
    min_records: int,
    min_fraction: float,
) -> SourceRecordLayout | None:
    delimiter = delimiter_hint
    if delimiter is None:
        try:
            delimiter = csv.Sniffer().sniff(
                text[:16 * 1024],
                delimiters=",\t;|",
            ).delimiter
        except csv.Error:
            return None
    if delimiter not in {",", "\t", ";", "|"}:
        return None
    try:
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))[:65]
    except csv.Error:
        return None
    rows = [row for row in rows if any(cell.strip() for cell in row)]
    if not rows:
        return None

    def host_columns(row: list[str]) -> tuple[int, ...]:
        return tuple(
            index
            for index, cell in enumerate(row)
            if _hostname_from_scalar(cell) is not None
        )

    # Exclude an obvious header only when the next records make that decision
    # deterministic; labels themselves are never trusted as evidence.
    if len(rows) > min_records and not host_columns(rows[0]):
        probe = rows[1 : 1 + min_records]
        if probe and all(host_columns(row) for row in probe):
            rows = rows[1:]
    if len(rows) < min_records:
        return None

    counts: Counter[int] = Counter()
    for row in rows:
        for index in host_columns(row):
            counts[index] += 1
    if not counts:
        return None
    best = max(counts.values())
    winners = sorted(index for index, count in counts.items() if count == best)
    if len(winners) != 1:
        return None
    confidence = _confidence(best, len(rows))
    if best < min_records or confidence < min_fraction:
        return None
    return SourceRecordLayout(
        parser_kind="delimited",
        hostname_field=f"column:{winners[0]}",
        delimiter=delimiter,
        detection_method="stable_delimited_host_column",
        confidence=confidence,
        sample_records=len(rows),
        matched_records=best,
        policy_version="record-layout-detect-v1",
    )


def detect_record_layout(
    *,
    payload: bytes,
    format_observation: SourceFormatObservation,
    min_records: int = 3,
    min_fraction: float = 0.90,
) -> SourceRecordLayout | None:
    """Infer hostname extraction only; never infer temporal authority."""

    if isinstance(min_records, bool) or not isinstance(min_records, int) or min_records < 2:
        raise ValueError("min_records must be an integer >= 2")
    if (
        isinstance(min_fraction, bool)
        or not isinstance(min_fraction, (int, float))
        or not math.isfinite(float(min_fraction))
        or not 0.5 <= float(min_fraction) <= 1.0
    ):
        raise ValueError("min_fraction must be within [0.5,1]")

    parser = format_observation.parser_kind
    if parser not in {"jsonl", "delimited"}:
        return None
    sample = payload
    if format_observation.compression == "gzip":
        inflated = _inflate_prefix(payload)
        if inflated is None:
            return None
        sample = inflated
    text = sample.decode("utf-8", errors="replace")
    if parser == "jsonl":
        return _jsonl_layout(
            text,
            min_records=min_records,
            min_fraction=float(min_fraction),
        )
    return _delimited_layout(
        text,
        delimiter_hint=format_observation.delimiter,
        min_records=min_records,
        min_fraction=float(min_fraction),
    )
