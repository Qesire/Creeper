"""Deterministic record-schema inference for structured historical records."""

from __future__ import annotations

import csv
import io
import json
import math
import re
import zlib
from collections import Counter
from urllib.parse import urlsplit

from creeper.authority.normalizer import normalize_official
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.layout_binding import SourceRecordLayout
from creeper.sources.schema_binding import SourceRecordSchema


_HOST_KEYS = (
    "hostname",
    "host",
    "domain",
    "url",
    "original",
    "original_url",
    "uri",
)
_TIME_KEYS = (
    "timestamp",
    "capture_timestamp",
    "capture_time",
    "datetime",
    "date",
    "year",
    "source_year",
    "capture_year",
    "warc_date",
    "crawl_date",
)
_AUTO_DIRECT_TIME_KEYS = frozenset(
    {
        "timestamp",
        "capture_timestamp",
        "capture_time",
        "capture_year",
        "warc_date",
        "crawl_date",
    }
)
_YEAR_RE = re.compile(
    r"^(?:19(?:9[6-9])|200[01])(?:$|[-/T ])"
)


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


def _timestamp_year(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        text = str(int(value)) if isinstance(value, float) and value.is_integer() else str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    if not text:
        return None

    # Compact archive timestamp: YYYY[MMDDhhmmss]
    if re.fullmatch(r"\d{4}(?:\d{4,10})?", text):
        year = int(text[:4])
        return year if 1996 <= year <= 2001 else None

    # ISO/date-like values beginning with target year.
    match = _YEAR_RE.match(text)
    if match is not None:
        year = int(text[:4])
        return year if 1996 <= year <= 2001 else None
    return None


def _confidence(matched: int, sampled: int) -> float:
    return 0.0 if sampled <= 0 else min(1.0, matched / sampled)


def _jsonl_schema(
    text: str,
    *,
    min_records: int,
    min_fraction: float,
    layout: SourceRecordLayout | None = None,
) -> SourceRecordSchema | None:
    records: list[dict[str, object]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append({str(k).strip().lower(): v for k, v in value.items()})
        if len(records) >= 64:
            break

    if len(records) < min_records:
        return None

    pair_counts: Counter[tuple[str, str]] = Counter()
    for record in records:
        if layout is None:
            host_fields = [
                key
                for key in _HOST_KEYS
                if key in record and _hostname_from_scalar(record[key]) is not None
            ]
        else:
            field = layout.hostname_field.strip().lower()
            host_fields = (
                [field]
                if field in record and _hostname_from_scalar(record[field]) is not None
                else []
            )
        time_fields = [
            key
            for key in _TIME_KEYS
            if key in record and _timestamp_year(record[key]) is not None
        ]
        for host_field in host_fields:
            for time_field in time_fields:
                pair_counts[(host_field, time_field)] += 1

    if not pair_counts:
        return None
    (host_field, time_field), matched = pair_counts.most_common(1)[0]
    confidence = _confidence(matched, len(records))
    if matched < min_records or confidence < min_fraction:
        return None
    return SourceRecordSchema(
        parser_kind="jsonl",
        hostname_field=host_field,
        timestamp_field=time_field,
        delimiter=None,
        detection_method=(
            "stable_json_fields"
            if layout is None
            else "stable_json_layout_time_field"
        ),
        confidence=confidence,
        sample_records=len(records),
        matched_records=matched,
        direct_year_eligible=time_field in _AUTO_DIRECT_TIME_KEYS,
    )


def _row_candidate_pairs(row: list[str]) -> list[tuple[int, int]]:
    host_columns = [
        index
        for index, cell in enumerate(row)
        if _hostname_from_scalar(cell) is not None
    ]
    time_columns = [
        index
        for index, cell in enumerate(row)
        if _timestamp_year(cell) is not None
    ]
    return [
        (host_index, time_index)
        for host_index in host_columns
        for time_index in time_columns
        if host_index != time_index
    ]


def _delimited_schema(
    text: str,
    *,
    min_records: int,
    min_fraction: float,
    layout: SourceRecordLayout | None = None,
) -> SourceRecordSchema | None:
    try:
        dialect = csv.Sniffer().sniff(text[:16 * 1024], delimiters=",\t;|")
    except csv.Error:
        return None
    delimiter = dialect.delimiter
    if delimiter not in {",", "\t", ";", "|"}:
        return None

    try:
        rows = list(csv.reader(io.StringIO(text), dialect=dialect))[:64]
    except csv.Error:
        return None
    rows = [row for row in rows if any(cell.strip() for cell in row)]
    if not rows:
        return None

    # Preserve one obvious header row for semantic authority. Stable columns
    # alone prove a parser/schema mapping, but only an explicit web-observation
    # time label can auto-upgrade generic tabular records to DIRECT_YEAR.
    header: list[str] | None = None
    if len(rows) > min_records and not _row_candidate_pairs(rows[0]):
        header = [cell.strip().lower() for cell in rows[0]]
        rows = rows[1:]
    if len(rows) < min_records:
        return None

    layout_host_index: int | None = None
    if layout is not None:
        raw = layout.hostname_field.strip().lower().removeprefix("column:")
        if not raw.isdigit():
            return None
        layout_host_index = int(raw)

    pair_counts: Counter[tuple[int, int]] = Counter()
    for row in rows:
        for pair in _row_candidate_pairs(row):
            if layout_host_index is not None and pair[0] != layout_host_index:
                continue
            pair_counts[pair] += 1
    if not pair_counts:
        return None

    (host_index, time_index), matched = pair_counts.most_common(1)[0]
    confidence = _confidence(matched, len(rows))
    if matched < min_records or confidence < min_fraction:
        return None

    header_host = (
        header[host_index]
        if header is not None and host_index < len(header)
        else None
    )
    header_time = (
        header[time_index]
        if header is not None and time_index < len(header)
        else None
    )
    direct_year_eligible = (
        header_time in _AUTO_DIRECT_TIME_KEYS
        and (
            header_host in _HOST_KEYS
            or (
                layout_host_index is not None
                and host_index == layout_host_index
            )
        )
    )
    return SourceRecordSchema(
        parser_kind="delimited",
        hostname_field=f"column:{host_index}",
        timestamp_field=f"column:{time_index}",
        delimiter=delimiter,
        detection_method=(
            "stable_delimited_columns"
            if layout is None
            else "stable_delimited_layout_time_column"
        ),
        confidence=confidence,
        sample_records=len(rows),
        matched_records=matched,
        direct_year_eligible=direct_year_eligible,
    )


def detect_record_schema(
    *,
    payload: bytes,
    format_observation: SourceFormatObservation,
    layout_observation: SourceRecordLayout | None = None,
    min_records: int = 3,
    min_fraction: float = 0.90,
) -> SourceRecordSchema | None:
    """Infer a direct-evidence field mapping from a bounded sample.

    This function only returns a schema when the same hostname/timestamp field
    pair is stable across the sample. It does not infer semantic authority from
    a filename or dataset description.
    """

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
    if (
        layout_observation is not None
        and layout_observation.parser_kind != parser
    ):
        raise ValueError(
            "record layout parser_kind disagrees with source format"
        )

    sample = payload
    if format_observation.compression == "gzip":
        inflated = _inflate_prefix(payload)
        if inflated is None:
            return None
        sample = inflated
    text = sample.decode("utf-8", errors="replace")

    if parser == "jsonl":
        return _jsonl_schema(
            text,
            min_records=min_records,
            min_fraction=float(min_fraction),
            layout=layout_observation,
        )
    return _delimited_schema(
        text,
        min_records=min_records,
        min_fraction=float(min_fraction),
        layout=layout_observation,
    )
