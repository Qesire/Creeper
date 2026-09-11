"""Small dependency-free records for source adapters and scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from creeper.records.candidates import CandidateSourceScope


@dataclass(frozen=True)
class SourceRecord:
    source_id: str
    locator: str
    payload: str
    scope: CandidateSourceScope
    source_year: int | None = None
    record_type: str = ""
    source_time: str | None = None
    artifact_ref: str = ""
    direct_year_mask: int = 0
    year_hint_mask: int = 0


@dataclass(frozen=True)
class HostObservation:
    hostname: str
    source_id: str
    locator: str
    scope: CandidateSourceScope
    source_year: int | None = None
    source_time: str | None = None
    record_type: str = ""
    artifact_ref: str = ""
    direct_year_mask: int = 0
    year_hint_mask: int = 0


@dataclass(frozen=True)
class SourceStats:
    source_id: str
    attempts: int
    candidates: int
    baseline_external: int
    elapsed_seconds: float
    saturated: bool = False

    @property
    def yield_per_hour(self) -> float:
        return self.baseline_external / (self.elapsed_seconds / 3600) if self.elapsed_seconds else 0.0


def iter_source_records(
    path, source_id: str, scope: CandidateSourceScope, source_year: int | None = None
) -> Iterator[SourceRecord]:
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line_number, line in enumerate(source, 1):
            yield SourceRecord(
                source_id, f"{path}:{line_number}", line.rstrip("\n"), scope, source_year
            )
