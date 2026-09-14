"""Small audited bootstrap for high-value historical-Web source roots.

These are discovery seeds only. They never grant evidence authority. Every
child resource still passes HTTP triage and deterministic measured scouting
against the configured baseline/EED model before activation.

The research roots below were selected by live source-search calibration:
queries that combine an explicit 1996-2001 period with a concrete source
archetype (link list, URL map, directory, offline-Web collection) consistently
returned higher-density enumerable resources than generic "web archive/CDX"
queries.
""

from __future__ import annotations

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry


def curated_direct_catalogs() -> tuple[SourceCandidate, ...]:
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
        SourceCandidate(
            canonical_entrypoint="https://data.labs.loc.gov/us-elections/",
            source_family="PUBLIC_ARCHIVE_INDEX_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-official-seed",
            discovery_strategy="CURATED_DIRECT_CATALOG",
            expected_year_from=2000,
            expected_year_to=2001,
            expected_volume=3_500,
            temporal_semantics_prior=1.0,
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
                "https://data.law.di.unimi.it/webdata/webbase-2001/"
                "webbase-2001.urls.gz"
            ),
            source_family="EARLY_WEB_URL_CORPUS",
            level=SourceLevel.SOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=2001,
            expected_year_to=2001,
            expected_volume=118_142_155,
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
            canonical_entrypoint="https://zenodo.org/records/8408539",
            source_family="EARLY_WEB_DIRECTORY_DERIVED_DATASET",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1999,
            expected_year_to=2001,
            expected_volume=22_915,
            temporal_semantics_prior=0.90,
            enumerability_prior=1.0,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.45,
            access_cost_prior=0.10,
            adapter_cost_prior=0.20,
            confidence=0.98,
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
        SourceCandidate(
            canonical_entrypoint="https://archive.org/details/pc-press-internet-cd",
            source_family="EARLY_WEB_OFFLINE_COLLECTION",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1996,
            expected_year_to=1996,
            expected_volume=34_580,
            temporal_semantics_prior=0.95,
            enumerability_prior=0.95,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.40,
            access_cost_prior=0.20,
            adapter_cost_prior=0.45,
            confidence=0.95,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint="https://archive.org/details/a-internet-em-cd-rom",
            source_family="EARLY_WEB_OFFLINE_COLLECTION",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1996,
            expected_year_to=1996,
            expected_volume=16_131,
            temporal_semantics_prior=0.90,
            enumerability_prior=0.80,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.40,
            access_cost_prior=0.20,
            adapter_cost_prior=0.55,
            confidence=0.90,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint="https://archive.org/details/amiga-plus-extra-cd-5-97",
            source_family="EARLY_WEB_OFFLINE_COLLECTION",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1997,
            expected_year_to=1997,
            expected_volume=12_808,
            temporal_semantics_prior=0.95,
            enumerability_prior=0.95,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.40,
            access_cost_prior=0.20,
            adapter_cost_prior=0.45,
            confidence=0.95,
            state=SourceState.DISCOVERED,
        ),
        SourceCandidate(
            canonical_entrypoint="https://archive.org/details/internet-on-a-cd",
            source_family="EARLY_WEB_OFFLINE_COLLECTION",
            level=SourceLevel.METASOURCE,
            discovered_by="curated-research-seed",
            discovery_strategy="CURATED_YEAR_ARCHETYPE_SEARCH",
            expected_year_from=1998,
            expected_year_to=1998,
            expected_volume=11_275,
            temporal_semantics_prior=0.95,
            enumerability_prior=0.95,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.40,
            access_cost_prior=0.20,
            adapter_cost_prior=0.45,
            confidence=0.95,
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
