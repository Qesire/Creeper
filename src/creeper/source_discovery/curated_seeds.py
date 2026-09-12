"""Small, audited metasource seed set for high-value archive indexes.

These are discovery seeds only. They never grant evidence authority. Every
child resource still passes HTTP triage and deterministic measured scouting
against the configured baseline/EED model before activation.
"""

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
            canonical_entrypoint=(
                "https://data.labs.loc.gov/us-elections/"
                "by-year/2000/manifest.html"
            ),
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
        _, created = registry.register_proposal(candidate)
        inserted += int(created)
    return inserted
