"""Pre-scout admission for source-search proposals.

This gate is deliberately based only on information the search stage must provide
before Creeper spends triage/scout capacity. It is not an evidence validator and
never queries the competition baseline or archive evidence providers.
"""

from __future__ import annotations

from dataclasses import dataclass

from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
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
    """Require search proposals to look useful, enumerable, and temporally relevant.

    SOURCE nodes need a high hostname reservoir, while COLLECTION/METASOURCE
    gateways may be smaller because their value is discovering many child
    resources. Direct CDX/CDXJ artifacts use the lowest floor because one row
    already carries annual evidence. All fields remain priors and are superseded
    by deterministic scout measurements later.
    """

    target_year_from: int = 1996
    target_year_to: int = 2001
    min_expected_volume: int = 100_000
    gateway_min_expected_volume: int = 50_000
    direct_min_expected_volume: int = 10_000
    min_enumerability_prior: float = 0.5
    gateway_min_enumerability_prior: float = 0.8
    min_confidence: float = 0.35
    require_year_bounds: bool = True

    def __post_init__(self) -> None:
        if self.target_year_from > self.target_year_to:
            raise ValueError("search admission target year range is reversed")
        if min(
            self.min_expected_volume,
            self.gateway_min_expected_volume,
            self.direct_min_expected_volume,
        ) < 1:
            raise ValueError("expected-volume floors must be positive")
        for name in (
            "min_enumerability_prior",
            "gateway_min_enumerability_prior",
            "min_confidence",
        ):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0, 1]")
        if not isinstance(self.require_year_bounds, bool):
            raise ValueError("require_year_bounds must be a boolean")

    def rejection_reason(self, candidate: SourceCandidate) -> str | None:
        if _is_common_crawl_corpus(candidate):
            return "Common Crawl corpus is excluded from the active candidate pool"
        direct_entrypoint = is_direct_evidence_entrypoint(
            candidate.canonical_entrypoint
        )
        if candidate.direct_evidence_prior > 0.0 and not direct_entrypoint:
            return (
                "agent direct_evidence_prior is not authoritative for a "
                "non-CDX/CDXJ entrypoint"
            )
        volume = candidate.expected_volume
        if volume is None:
            return "missing role-aware expected_volume estimate"
        direct_evidence = direct_entrypoint
        gateway = candidate.level in {
            SourceLevel.COLLECTION,
            SourceLevel.METASOURCE,
        }
        if direct_evidence:
            volume_floor = self.direct_min_expected_volume
            volume_kind = "direct-evidence"
        elif gateway:
            volume_floor = self.gateway_min_expected_volume
            volume_kind = "gateway"
        else:
            volume_floor = self.min_expected_volume
            volume_kind = "source"
        if volume < volume_floor:
            return (
                f"expected_volume={volume} below {volume_kind} floor="
                f"{volume_floor}"
            )

        enumerability_floor = (
            self.gateway_min_enumerability_prior
            if gateway and not direct_evidence
            else self.min_enumerability_prior
        )
        if candidate.enumerability_prior < enumerability_floor:
            return (
                f"enumerability_prior={candidate.enumerability_prior:g} below "
                f"{volume_kind} floor={enumerability_floor:g}"
            )
        if candidate.confidence < self.min_confidence:
            return (
                f"confidence={candidate.confidence:g} below floor="
                f"{self.min_confidence:g}"
            )

        year_from = candidate.expected_year_from
        year_to = candidate.expected_year_to
        if year_from is None or year_to is None:
            # Timestamp-bearing CDX/CDXJ rows self-describe their capture year.
            # Do not reject a high-value direct-evidence resource merely because
            # the search backend could not infer collection-level year bounds;
            # deterministic measured scouting still has to prove target-year
            # host-year novelty before promotion.
            if (
                self.require_year_bounds
                and not is_direct_evidence_entrypoint(
                    candidate.canonical_entrypoint
                )
            ):
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
