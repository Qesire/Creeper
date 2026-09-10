"""Evidence acceptance primitives shared by providers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from creeper.authority.normalizer import normalize_official


class CDXQueryState(StrEnum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    PASS = "pass"
    EMPTY_EXHAUSTIVE = "empty_exhaustive"
    INCOMPLETE = "incomplete"
    TRANSIENT_ERROR = "transient_error"
    INVALID = "invalid"


@dataclass(frozen=True)
class TemporalScope:
    year_from: int
    year_to: int

    def __post_init__(self) -> None:
        if not 1996 <= self.year_from <= self.year_to <= 2001:
            raise ValueError("temporal scope must be within 1996-2001")


@dataclass(frozen=True)
class EvidenceQueryKey:
    hostname: str
    temporal_scope: TemporalScope
    provider: str
    policy_version: str

    def __post_init__(self) -> None:
        normalized = normalize_official(self.hostname)
        if normalized is None:
            raise ValueError("invalid hostname")
        object.__setattr__(self, "hostname", normalized)


@dataclass(frozen=True)
class EvidenceCapsule:
    hostname: str
    year: int
    provider: str
    temporal_semantics: str
    evidence_timestamp: str
    source_locator: str
    payload_hash: str
    policy_version: str


@dataclass(frozen=True)
class EvidenceQueryResult:
    hostname: str
    year: int
    state: CDXQueryState
    capsule: EvidenceCapsule | None = None
    pages_seen: int = 0
    records_seen: int = 0
    error: str | None = None
    key: EvidenceQueryKey | None = None


@dataclass(frozen=True)
class RangeEvidenceQueryResult:
    """A temporal range probe that discovers candidate years only."""

    hostname: str
    key: EvidenceQueryKey
    state: CDXQueryState
    candidate_years: tuple[int, ...] = ()
    pages_seen: int = 0
    records_seen: int = 0
    error: str | None = None

    def __post_init__(self) -> None:
        if self.key.hostname != self.hostname:
            raise ValueError("range result hostname must match query key")
        scope = self.key.temporal_scope
        if scope.year_from == scope.year_to:
            raise ValueError("range result requires a multi-year temporal scope")
        if any(year not in range(scope.year_from, scope.year_to + 1) for year in self.candidate_years):
            raise ValueError("range candidate years must be inside the query scope")


def is_year_timestamp(timestamp: str, year: int) -> bool:
    return len(timestamp) >= 4 and timestamp[:4].isdigit() and int(timestamp[:4]) == year
