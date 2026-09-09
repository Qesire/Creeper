"""Streaming parser for Arquivo.pt CDXJ index rows."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator

from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import SourceRecord


def complete_cdxj_lines(payload: bytes) -> list[str]:
    """Decode a byte prefix and return only newline-terminated rows."""
    text = payload.decode("utf-8", errors="replace")
    if not text.endswith(("\n", "\r")):
        text = text.rsplit("\n", 1)[0] if "\n" in text else ""
    return text.splitlines()


def parse_cdxj_line(line: str, *, source_id: str, locator: str) -> SourceRecord | None:
    """Parse one CDXJ row while retaining its source locator."""
    fields = line.rstrip("\r\n").split(" ", 2)
    if len(fields) != 3 or len(fields[1]) < 4 or not fields[1][:4].isdigit():
        return None
    try:
        payload = json.loads(fields[2])
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    original_url = payload.get("url")
    if not isinstance(original_url, str) or not original_url.strip():
        return None
    return SourceRecord(
        source_id=source_id,
        locator=locator,
        payload=original_url.strip(),
        scope=CandidateSourceScope.LOCAL_DISCOVERY,
        source_year=int(fields[1][:4]),
    )


def iter_cdxj_lines(
    lines: Iterable[str],
    *,
    source_id: str,
    locator_prefix: str,
    allowed_years: set[int] | None = None,
) -> Iterator[SourceRecord]:
    """Yield valid rows, optionally restricted to a finite set of years."""
    for line_number, line in enumerate(lines, 1):
        record = parse_cdxj_line(
            line,
            source_id=source_id,
            locator=f"{locator_prefix}:{line_number}",
        )
        if record is None:
            continue
        if allowed_years is not None and record.source_year not in allowed_years:
            continue
        yield record
