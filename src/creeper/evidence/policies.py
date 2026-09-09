"""Evidence acceptance primitives shared by providers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CDXQueryState(StrEnum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    PASS = "pass"
    EMPTY_EXHAUSTIVE = "empty_exhaustive"
    INCOMPLETE = "incomplete"
    TRANSIENT_ERROR = "transient_error"
    INVALID = "invalid"


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


def is_year_timestamp(timestamp: str, year: int) -> bool:
    return len(timestamp) >= 4 and timestamp[:4].isdigit() and int(timestamp[:4]) == year
