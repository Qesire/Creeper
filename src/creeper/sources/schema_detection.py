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
        host_fields = [
            key
            for key in _HOST_KEYS
            if key in record and _hostname_from_scalar(record[key]) is not None
        ]
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
        detection_method="stable_json_fields",
        confidence=confidence,
        sample_records=len(records),
        matched_records=matched,
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

    # Ignore one obvious header row: no host/year pair in row 0 while data rows
    # consistently contain a pair.
    if len(rows) > min_records and not _row_candidate_pairs(rows[0]):
        rows = rows[1:]
    if len(rows) < min_records:
        return None

    pair_counts: Counter[tuple[int, int]] = Counter()
    for row in rows:
        for pair in _row_candidate_pairs(row):
            pair_counts[pair] += 1
    if not pair_counts:
        return None

    (host_index, time_index), matched = pair_counts.most_common(1)[0]
    confidence = _confidence(matched, len(rows))
    if matched < min_records or confidence < min_fraction:
        return None

    return SourceRecordSchema(
        parser_kind="delimited",
        hostname_field=f"column:{host_index}",
        timestamp_field=f"column:{time_index}",
        delimiter=delimiter,
        detection_method="stable_delimited_columns",
        confidence=confidence,
        sample_records=len(rows),
        matched_records=matched,
    )


def validate_record_schema_proposal(
    *,
    payload: bytes,
    format_observation: SourceFormatObservation,
    hostname_field: str,
    timestamp_field: str,
    delimiter: str | None = None,
    min_records: int = 3,
    min_fraction: float = 0.90,
) -> SourceRecordSchema | None:
    """Validate an externally proposed field mapping against the sample.

    The proposal has no authority. This function recomputes all record matches
    locally and returns a durable schema only when the exact mapping is stable.
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
    sample = payload
    if format_observation.compression == "gzip":
        inflated = _inflate_prefix(payload)
        if inflated is None:
            return None
        sample = inflated
    text = sample.decode("utf-8", errors="replace")

    if parser == "jsonl":
        host_key = str(hostname_field).strip().lower()
        time_key = str(timestamp_field).strip().lower()
        if not host_key or not time_key or host_key == time_key:
            return None
        records: list[dict[str, object]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(
                    {str(key).strip().lower(): item for key, item in value.items()}
                )
            if len(records) >= 64:
                break
        if len(records) < min_records:
            return None
        matched = sum(
            _hostname_from_scalar(record.get(host_key)) is not None
            and _timestamp_year(record.get(time_key)) is not None
            for record in records
        )
        confidence = _confidence(matched, len(records))
        if matched < min_records or confidence < min_fraction:
            return None
        return SourceRecordSchema(
            parser_kind="jsonl",
            hostname_field=host_key,
            timestamp_field=time_key,
            delimiter=None,
            detection_method="llm_mapping_locally_validated",
            confidence=confidence,
            sample_records=len(records),
            matched_records=matched,
        )

    if delimiter not in {",", "\t", ";", "|"}:
        return None
    try:
        host_index = int(str(hostname_field).removeprefix("column:"))
        time_index = int(str(timestamp_field).removeprefix("column:"))
    except ValueError:
        return None
    if (
        not str(hostname_field).startswith("column:")
        or not str(timestamp_field).startswith("column:")
        or host_index < 0
        or time_index < 0
        or host_index == time_index
    ):
        return None
    try:
        rows = list(csv.reader(io.StringIO(text), delimiter=delimiter))[:64]
    except csv.Error:
        return None
    rows = [row for row in rows if any(cell.strip() for cell in row)]
    if not rows:
        return None

    def matches(row: list[str]) -> bool:
        if max(host_index, time_index) >= len(row):
            return False
        return (
            _hostname_from_scalar(row[host_index]) is not None
            and _timestamp_year(row[time_index]) is not None
        )

    if len(rows) > min_records and not matches(rows[0]):
        rows = rows[1:]
    if len(rows) < min_records:
        return None
    matched = sum(matches(row) for row in rows)
    confidence = _confidence(matched, len(rows))
    if matched < min_records or confidence < min_fraction:
        return None
    return SourceRecordSchema(
        parser_kind="delimited",
        hostname_field=f"column:{host_index}",
        timestamp_field=f"column:{time_index}",
        delimiter=delimiter,
        detection_method="llm_mapping_locally_validated",
        confidence=confidence,
        sample_records=len(rows),
        matched_records=matched,
    )


def detect_record_schema(
    *,
    payload: bytes,
    format_observation: SourceFormatObservation,
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
        )
    return _delimited_schema(
        text,
        min_records=min_records,
        min_fraction=float(min_fraction),
    )
