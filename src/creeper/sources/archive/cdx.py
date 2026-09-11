"""Parser for line-oriented CDX archive indexes with exact-year semantics."""

from __future__ import annotations

from creeper.authority.baseline_index import YEAR_BITS
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import SourceRecord


def parse_cdx_line(line: str, *, source_id: str, locator: str) -> SourceRecord | None:
    """Parse a standard/JISC CDX row into a direct year-specific record.

    The supported layout is the common CDX form::

        urlkey timestamp original mime status digest length offset filename

    A timestamp and original URL are mandatory. When a status field is
    present, only successful/redirect captures are eligible for annual
    evidence. Malformed or non-success rows fail closed.
    """
    fields = line.strip().split()
    if len(fields) < 3:
        return None
    timestamp = fields[1]
    if len(timestamp) < 4 or not timestamp[:4].isdigit():
        return None
    year = int(timestamp[:4])
    if year not in YEAR_BITS:
        return None
    original = fields[2].strip()
    if not original or "://" not in original:
        return None
    if len(fields) >= 5 and fields[4][:1] not in {"2", "3"}:
        return None
    return SourceRecord(
        source_id=source_id,
        locator=locator,
        payload=original,
        scope=CandidateSourceScope.LOCAL_DISCOVERY,
        source_year=year,
        record_type="CDX_CAPTURE",
        source_time=timestamp,
        artifact_ref=locator,
        direct_year_mask=YEAR_BITS[year],
    )
