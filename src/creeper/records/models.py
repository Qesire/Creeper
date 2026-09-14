"""Small dependency-free records for source adapters and scheduling."""

from __future__ import annotations

from dataclasses import dataclass
import math
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
    evidence_type: str = ""
    temporal_semantics: str = ""
    evidence_contract_id: str = ""
    evidence_contract_version: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise ValueError("source_id is required")
        if not isinstance(self.locator, str) or not self.locator.strip():
            raise ValueError("locator is required")
        if not isinstance(self.payload, str):
            raise ValueError("payload must be a string")
        object.__setattr__(self, "scope", CandidateSourceScope(self.scope))
        if self.source_year is not None and (
            isinstance(self.source_year, bool) or not isinstance(self.source_year, int)
        ):
            raise ValueError("source_year must be an integer when provided")
        for name in ("direct_year_mask", "year_hint_mask"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.source_time is not None and not isinstance(self.source_time, str):
            raise ValueError("source_time must be a string when provided")


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
    original_url: str = ""
    evidence_type: str = ""
    temporal_semantics: str = ""
    evidence_contract_id: str = ""
    evidence_contract_version: str = ""

    def __post_init__(self) -> None:
        for name in ("hostname", "source_id", "locator"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        object.__setattr__(self, "scope", CandidateSourceScope(self.scope))
        if self.source_year is not None and (
            isinstance(self.source_year, bool) or not isinstance(self.source_year, int)
        ):
            raise ValueError("source_year must be an integer when provided")
        for name in ("direct_year_mask", "year_hint_mask"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.source_time is not None and not isinstance(self.source_time, str):
            raise ValueError("source_time must be a string when provided")


@dataclass(frozen=True)
class SourceStats:
    source_id: str
    attempts: int
    candidates: int
    baseline_external: int
    elapsed_seconds: float
    saturated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise ValueError("source_id is required")
        for name in ("attempts", "candidates", "baseline_external"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if (
            isinstance(self.elapsed_seconds, bool)
            or not isinstance(self.elapsed_seconds, (int, float))
            or not math.isfinite(float(self.elapsed_seconds))
            or self.elapsed_seconds < 0
        ):
            raise ValueError("elapsed_seconds must be finite and non-negative")
        if not isinstance(self.saturated, bool):
            raise ValueError("saturated must be a boolean")

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
