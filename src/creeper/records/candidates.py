"""Candidate records and V3 reconciliation rules."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from typing import Iterable

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.normalizer import normalize_official


class CandidateSourceScope(StrEnum):
    OFFICIAL_POOL = "official_pool"
    ISC_REFERENCE = "isc_reference"
    LOCAL_DISCOVERY = "local_discovery"
    COMMON_CRAWL_CORPUS_EXCLUDED = "common_crawl_corpus_excluded"


class CandidateStatus(StrEnum):
    """Durable research state. These values never grant evidence authority."""

    ACTIVE_CANDIDATE = "ACTIVE_CANDIDATE"
    ANNUAL_EVIDENCE_OBTAINED = "ANNUAL_EVIDENCE_OBTAINED"
    BASELINE_OVERLAP = "BASELINE_OVERLAP"
    ISC_REFERENCE = "ISC_REFERENCE"
    EXCLUDED_COMMON_CRAWL = "EXCLUDED_COMMON_CRAWL"
    UNPARSED = "UNPARSED"


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
    value = source_id.strip().lower()
    tokenized = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    compact = re.sub(r"[^a-z0-9]+", "", value)
    if "commoncrawl" in compact or tokenized.startswith("cc_"):
        return CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED
    if compact.startswith("isc") or "iscreference" in compact or "networkwizards" in compact:
        return CandidateSourceScope.ISC_REFERENCE
    if tokenized in {"official_pool", "candidate_pool", "v3_candidate_pool"}:
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
        inferred_scope = classify_candidate_source(record.source_id)
        # Source provenance is authoritative for the two restricted classes.
        # This prevents a caller from accidentally re-labelling Common Crawl
        # or raw ISC/Network Wizards data as ordinary local discovery.
        effective_scope = record.scope
        if inferred_scope in {
            CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
            CandidateSourceScope.ISC_REFERENCE,
        }:
            effective_scope = inferred_scope
        normalized = CandidateRecord(
            hostname=hostname,
            source_id=record.source_id,
            scope=effective_scope,
            source_locator=record.source_locator,
            source_year=record.source_year,
        )
        if effective_scope is CandidateSourceScope.ISC_REFERENCE:
            isc.setdefault(hostname, normalized)
            continue
        if not is_active_candidate_allowed(effective_scope):
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
