"""Small audited bootstrap for high-value historical-Web source roots.

These are discovery seeds only. They never grant evidence authority. Every
child resource still passes HTTP triage and deterministic measured scouting
against the configured baseline/EED model before activation.

The research roots below were selected by live source-search calibration:
queries that combine an explicit 1996-2001 period with a concrete source
archetype (link list, URL map, directory, crawl corpus) consistently returned
higher-density enumerable resources than generic "web archive/CDX" queries.
"""

from __future__ import annotations

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry


def curated_direct_catalogs() -> tuple[SourceCandidate, ...]:
    # Keep the bootstrap limited to broad catalogs that still have unresolved
    # residual opportunity. Narrow topical catalogs belong in measured search
    # history, not in every fresh runtime's unconditional seed set.
    return (
        SourceCandidate(
            canonical_entrypoint="https://arquivo.pt/datasets/cdxj/",
            source_family="PUBLIC_ARCHIVE_INDEX_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-official-seed",
            discovery_strategy="CURATED_DIRECT_CATALOG",
            expected_volume=150,
            temporal_semantics_prior=0.95,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.50,
            access_cost_prior=0.20,
            adapter_cost_prior=0.20,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        ),
    )


def ensure_curated_direct_catalogs(
    registry: SourceDiscoveryRegistry,
) -> int:
    """Idempotently add audited catalog entrypoints to the cold pool."""
    inserted = 0
    for candidate in curated_direct_catalogs():
        if registry.get_candidate(candidate.source_key) is not None:
            continue
        _, created = registry.register_proposal(candidate)
        inserted += int(created)
    return inserted


def curated_research_roots() -> tuple[SourceCandidate, ...]:
    """Audited non-CDX roots with dense target-period hostname discovery."""

    return (
        SourceCandidate(
            canonical_entrypoint=(
                "https://data.law.di.unimi.it/webdata/cnr-2000/"
                "cnr-2000.urls.gz"
            ),
            source_family="EARLY_WEB_URL_CORPUS",
            level=SourceLevel.SOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=2000,
            expected_year_to=2000,
            # 325,557 is the published Web-graph vertex/page count, not a
            # verified hostname reservoir. Keep volume unknown so it cannot
            # inflate pre-scout priority.
            expected_volume=None,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.50,
            access_cost_prior=0.05,
            adapter_cost_prior=0.05,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint=(
                "https://data.law.di.unimi.it/webdata/webbase-2001/"
                "webbase-2001.urls.gz"
            ),
            source_family="EARLY_WEB_URL_CORPUS",
            level=SourceLevel.SOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=2001,
            expected_year_to=2001,
            # 118,142,155 is the published Web-graph node/URL count, not a
            # hostname ceiling. Deterministic scout measurement must establish
            # the useful host reservoir instead of inheriting page-count scale.
            expected_volume=None,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.50,
            access_cost_prior=0.20,
            adapter_cost_prior=0.10,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint="https://archive95.net/sources",
            source_family="EARLY_WEB_SOURCE_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1996,
            expected_year_to=1998,
            expected_volume=90_000,
            temporal_semantics_prior=0.90,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.45,
            access_cost_prior=0.10,
            adapter_cost_prior=0.25,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint="https://hdl.handle.net/11299/200445",
            source_family="EARLY_WEB_LINK_DATASET",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1996,
            expected_year_to=2000,
            expected_volume=None,
            temporal_semantics_prior=1.0,
            enumerability_prior=0.95,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.45,
            access_cost_prior=0.10,
            adapter_cost_prior=0.20,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        ),
    )


def curated_source_seeds() -> tuple[SourceCandidate, ...]:
    """Return all audited bootstrap roots without granting evidence authority."""

    return curated_direct_catalogs() + curated_research_roots()


def ensure_curated_source_seeds(
    registry: SourceDiscoveryRegistry,
) -> int:
    """Idempotently add every audited source root to the cold pool."""

    inserted = 0
    for candidate in curated_source_seeds():
        if registry.get_candidate(candidate.source_key) is not None:
            continue
        _, created = registry.register_proposal(candidate)
        inserted += int(created)
    return inserted
