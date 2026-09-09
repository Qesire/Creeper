"""Candidate records and V3 reconciliation rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.normalizer import normalize_official


class CandidateSourceScope(StrEnum):
    OFFICIAL_POOL = "official_pool"
    ISC_REFERENCE = "isc_reference"
    LOCAL_DISCOVERY = "local_discovery"
    COMMON_CRAWL_CORPUS_EXCLUDED = "common_crawl_corpus_excluded"


@dataclass(frozen=True)
class CandidateRecord:
    hostname: str
    source_id: str
    scope: CandidateSourceScope
    source_locator: str | None = None
    source_year: int | None = None

    def __post_init__(self) -> None:
        if not self.source_id.strip():
            raise ValueError("candidate provenance requires a non-empty source_id")


@dataclass(frozen=True)
class ActiveCandidateSet:
    active: tuple[CandidateRecord, ...]
    isc_reference: tuple[CandidateRecord, ...]
    excluded: tuple[CandidateRecord, ...]
    unparsed: tuple[str, ...]


def classify_candidate_source(source_id: str) -> CandidateSourceScope:
    value = source_id.strip().lower().replace("-", "_")
    if "common_crawl" in value or value.startswith("cc_"):
        return CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED
    if value.startswith("isc") or "isc_reference" in value:
        return CandidateSourceScope.ISC_REFERENCE
    if value in {"official_pool", "candidate_pool", "v3_candidate_pool"}:
        return CandidateSourceScope.OFFICIAL_POOL
    return CandidateSourceScope.LOCAL_DISCOVERY


def is_active_candidate_allowed(scope: CandidateSourceScope) -> bool:
    return scope in {
        CandidateSourceScope.OFFICIAL_POOL,
        CandidateSourceScope.LOCAL_DISCOVERY,
    }


def reconcile_active_candidates(
    candidates: Iterable[CandidateRecord], index: BaselineIndex
) -> ActiveCandidateSet:
    """Normalize, deduplicate, and apply the V3 candidate-scope gate."""
    active: dict[str, CandidateRecord] = {}
    isc: dict[str, CandidateRecord] = {}
    excluded: dict[str, CandidateRecord] = {}
    unparsed: list[str] = []
    for record in candidates:
        hostname = normalize_official(record.hostname)
        if hostname is None:
            unparsed.append(record.hostname)
            continue
        normalized = CandidateRecord(
            hostname=hostname,
            source_id=record.source_id,
            scope=record.scope,
            source_locator=record.source_locator,
            source_year=record.source_year,
        )
        if record.scope is CandidateSourceScope.ISC_REFERENCE:
            isc.setdefault(hostname, normalized)
            continue
        if not is_active_candidate_allowed(record.scope):
            excluded.setdefault(hostname, normalized)
            continue
        if index.year_mask(hostname):
            continue
        active.setdefault(hostname, normalized)
    return ActiveCandidateSet(
        active=tuple(active.values()),
        isc_reference=tuple(isc.values()),
        excluded=tuple(excluded.values()),
        unparsed=tuple(unparsed),
    )
