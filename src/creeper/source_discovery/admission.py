"""Pre-scout admission for source-search proposals.

This gate is deliberately based only on information the search stage must provide
before Creeper spends triage/scout capacity. It is not an evidence validator and
never queries the competition baseline or archive evidence providers.
"""

from __future__ import annotations

from dataclasses import dataclass

from creeper.source_discovery.models import (
    SourceCandidate,
    is_common_crawl_provenance,
    is_direct_evidence_entrypoint,
)


def _is_common_crawl_corpus(candidate: SourceCandidate) -> bool:
    """Recognize the explicitly excluded Common Crawl corpus family.

    The check is intentionally based on both provenance metadata and the
    entrypoint. Agent output must not bypass the V3 source exclusion merely by
    choosing a different spelling for the family name.
    """
    return is_common_crawl_provenance(
        candidate.source_family,
        candidate.canonical_entrypoint,
        candidate.discovered_by,
    )


@dataclass(frozen=True)
class SearchAdmissionPolicy:
    """Require search proposals to look large, enumerable, and temporally relevant.

    The policy protects a finite COLD reservoir from low-volume URLs. All fields
    are priors and are superseded by deterministic scout measurements later.
    """

    target_year_from: int = 1996
    target_year_to: int = 2001
    min_expected_volume: int = 100_000
    direct_min_expected_volume: int = 10_000
    min_enumerability_prior: float = 0.5
    min_confidence: float = 0.35
    require_year_bounds: bool = True

    def __post_init__(self) -> None:
        if self.target_year_from > self.target_year_to:
            raise ValueError("search admission target year range is reversed")
        if self.min_expected_volume < 1 or self.direct_min_expected_volume < 1:
            raise ValueError("expected-volume floors must be positive")
        for name in ("min_enumerability_prior", "min_confidence"):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if not isinstance(self.require_year_bounds, bool):
            raise ValueError("require_year_bounds must be a boolean")

    def rejection_reason(self, candidate: SourceCandidate) -> str | None:
        if _is_common_crawl_corpus(candidate):
            return "Common Crawl corpus is excluded from the active candidate pool"
        volume = candidate.expected_volume
        if volume is None:
            return "missing expected_volume high-reservoir estimate"
        volume_floor = (
            self.direct_min_expected_volume
            if is_direct_evidence_entrypoint(candidate.canonical_entrypoint)
            else self.min_expected_volume
        )
        if volume < volume_floor:
            kind = "direct-evidence" if volume_floor == self.direct_min_expected_volume else "high-reservoir"
            return (
                f"expected_volume={volume} below {kind} floor="
                f"{volume_floor}"
            )
        if candidate.enumerability_prior < self.min_enumerability_prior:
            return (
                f"enumerability_prior={candidate.enumerability_prior:g} below floor="
                f"{self.min_enumerability_prior:g}"
            )
        if candidate.confidence < self.min_confidence:
            return (
                f"confidence={candidate.confidence:g} below floor="
                f"{self.min_confidence:g}"
            )

        year_from = candidate.expected_year_from
        year_to = candidate.expected_year_to
        if year_from is None or year_to is None:
            if self.require_year_bounds:
                return "missing expected target-year bounds"
            return None
        if year_to < self.target_year_from or year_from > self.target_year_to:
            return (
                f"expected years {year_from}-{year_to} do not overlap target "
                f"{self.target_year_from}-{self.target_year_to}"
            )
        return None

    def accepts(self, candidate: SourceCandidate) -> bool:
        return self.rejection_reason(candidate) is None
