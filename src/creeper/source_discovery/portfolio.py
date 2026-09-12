"""Marginal coverage planning across harvest-ready historical-index regions.

The planner treats each RegionSynopsis as a compact, uncertain sample of a
larger baseline-external set.  It uses MinHash to estimate Jaccard overlap,
cardinality scaling to turn Jaccard into candidate containment, and a greedy
virtual union so every selection is scored against regions already harvested
plus regions selected earlier in the same plan.

This is intentionally a *planning* layer.  It never changes region state or
claims harvest work.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    HarvestRegion,
    RegionState,
    RegionSynopsis,
)
from creeper.source_discovery.overlap import MinHashSketch
from creeper.source_discovery.tomography import region_size_bytes


@dataclass(frozen=True)
class RegionPortfolioPolicy:
    """Risk/cost controls for greedy marginal-coverage selection."""

    unknown_overlap_penalty: float = 0.25
    confidence_floor: float = 0.25
    min_marginal_fraction: float = 0.01
    min_marginal_eed: float = 0.0
    min_score_per_mib: float = 0.0

    def __post_init__(self) -> None:
        if not 0 <= self.unknown_overlap_penalty <= 1:
            raise ValueError("unknown_overlap_penalty must be within [0, 1]")
        if not 0 <= self.confidence_floor <= 1:
            raise ValueError("confidence_floor must be within [0, 1]")
        if not 0 <= self.min_marginal_fraction <= 1:
            raise ValueError("min_marginal_fraction must be within [0, 1]")
        if self.min_marginal_eed < 0:
            raise ValueError("min_marginal_eed must be non-negative")
        if self.min_score_per_mib < 0:
            raise ValueError("min_score_per_mib must be non-negative")


@dataclass(frozen=True)
class RegionPortfolioEstimate:
    region: HarvestRegion
    estimated_harvest_bytes: int
    estimated_total_novel_items: float
    estimated_total_novel_eed: float
    risk_adjusted_total_eed: float
    estimated_jaccard: float | None
    estimated_containment: float
    marginal_fraction: float
    marginal_eed: float
    marginal_eed_per_mib: float
    synopsis_confidence: float

    def __post_init__(self) -> None:
        if self.estimated_harvest_bytes < 1:
            raise ValueError("estimated_harvest_bytes must be positive")
        for name in (
            "estimated_total_novel_items",
            "estimated_total_novel_eed",
            "risk_adjusted_total_eed",
            "estimated_containment",
            "marginal_fraction",
            "marginal_eed",
            "marginal_eed_per_mib",
            "synopsis_confidence",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.estimated_jaccard is not None and not (
            math.isfinite(self.estimated_jaccard)
            and 0 <= self.estimated_jaccard <= 1
        ):
            raise ValueError("estimated_jaccard must be within [0, 1]")
        if self.estimated_containment > 1:
            raise ValueError("estimated_containment must be <= 1")
        if self.marginal_fraction > 1:
            raise ValueError("marginal_fraction must be <= 1")
        if self.synopsis_confidence > 1:
            raise ValueError("synopsis_confidence must be <= 1")


@dataclass(frozen=True)
class RegionPortfolioPlan:
    selections: tuple[RegionPortfolioEstimate, ...]
    total_harvest_bytes: int
    total_marginal_eed: float
    candidate_regions_considered: int
    harvested_reference_regions: int


@dataclass
class _CoverageState:
    sketch: MinHashSketch
    estimated_cardinality: float


def _valid_sketch(synopsis: RegionSynopsis) -> MinHashSketch | None:
    values = synopsis.minhash_values
    if not values:
        return None
    # MinHashAccumulator uses an all-zero signature for an empty stream.  A
    # positive synopsis with all-zero hashes would be vanishingly unlikely, so
    # treating this sentinel as unavailable is conservative and deterministic.
    if not any(values):
        return None
    return MinHashSketch(tuple(values))


def _estimated_scale(
    region: HarvestRegion,
    synopsis: RegionSynopsis,
) -> tuple[int, float]:
    size = region_size_bytes(region)
    harvest_bytes = size or max(1, synopsis.bytes_read)
    if synopsis.complete:
        return harvest_bytes, 1.0
    if synopsis.bytes_read <= 0:
        return harvest_bytes, 1.0
    return harvest_bytes, max(1.0, harvest_bytes / synopsis.bytes_read)


def _estimated_cardinality(
    region: HarvestRegion,
    synopsis: RegionSynopsis,
) -> tuple[int, float]:
    harvest_bytes, scale = _estimated_scale(region, synopsis)
    return (
        harvest_bytes,
        float(synopsis.novel_count_for_value) * scale,
    )


def _intersection_from_jaccard(
    left_cardinality: float,
    right_cardinality: float,
    jaccard: float,
) -> float:
    """Recover |A∩B| from J(A,B), |A| and |B|."""

    if left_cardinality <= 0 or right_cardinality <= 0 or jaccard <= 0:
        return 0.0
    intersection = (
        jaccard * (left_cardinality + right_cardinality) / (1.0 + jaccard)
    )
    return min(left_cardinality, right_cardinality, max(0.0, intersection))


class RegionPortfolioPlanner:
    """Greedy approximate marginal-EED planner across all indexes."""

    def __init__(
        self,
        registry: IndexSpaceRegistry,
        *,
        policy: RegionPortfolioPolicy | None = None,
    ) -> None:
        self.registry = registry
        self.policy = policy or RegionPortfolioPolicy()

    def _reference_states(
        self,
    ) -> tuple[dict[int, _CoverageState], int, bool]:
        """Build the global virtual union of all already HARVESTED coverage."""

        states: dict[int, _CoverageState] = {}
        harvested = self.registry.list_regions_by_state(RegionState.HARVESTED)
        unknown_reference = False
        for region in harvested:
            synopsis = self.registry.get_synopsis(region.region_key)
            if synopsis is None or synopsis.novel_count_for_value <= 0:
                continue
            sketch = _valid_sketch(synopsis)
            _, cardinality = _estimated_cardinality(region, synopsis)
            if cardinality <= 0:
                continue
            if sketch is None:
                unknown_reference = True
                continue
            width = len(sketch.values)
            state = states.get(width)
            if state is None:
                states[width] = _CoverageState(sketch, cardinality)
                continue
            jaccard = sketch.similarity(state.sketch)
            intersection = _intersection_from_jaccard(
                cardinality,
                state.estimated_cardinality,
                jaccard,
            )
            state.estimated_cardinality = max(
                state.estimated_cardinality,
                state.estimated_cardinality + cardinality - intersection,
            )
            state.sketch = state.sketch.union(sketch)
        return states, len(harvested), unknown_reference

    def _estimate(
        self,
        region: HarvestRegion,
        synopsis: RegionSynopsis,
        states: dict[int, _CoverageState],
        *,
        unknown_reference: bool,
        per_region_overhead_bytes: int = 0,
    ) -> RegionPortfolioEstimate:
        harvest_bytes, cardinality = _estimated_cardinality(region, synopsis)
        cost_bytes = harvest_bytes + int(per_region_overhead_bytes)
        _, scale = _estimated_scale(region, synopsis)
        total_eed = max(0.0, synopsis.novel_eed * scale)
        reliability = self.policy.confidence_floor + (
            (1.0 - self.policy.confidence_floor) * synopsis.confidence
        )
        risk_adjusted = total_eed * reliability

        sketch = _valid_sketch(synopsis)
        jaccard: float | None = None
        if sketch is None:
            containment = (
                self.policy.unknown_overlap_penalty
                if states or unknown_reference
                else 0.0
            )
        else:
            state = states.get(len(sketch.values))
            if state is None or state.estimated_cardinality <= 0:
                # Existing coverage with an incompatible sketch width cannot be
                # compared safely. Treat it like unsketched coverage rather
                # than pretending the candidate is disjoint.
                containment = (
                    self.policy.unknown_overlap_penalty
                    if states or unknown_reference
                    else 0.0
                )
            elif cardinality <= 0:
                containment = 1.0
                jaccard = 0.0
            else:
                jaccard = sketch.similarity(state.sketch)
                intersection = _intersection_from_jaccard(
                    cardinality,
                    state.estimated_cardinality,
                    jaccard,
                )
                containment = min(1.0, intersection / cardinality)
                if unknown_reference:
                    containment = max(
                        containment,
                        self.policy.unknown_overlap_penalty,
                    )

        marginal_fraction = max(0.0, 1.0 - containment)
        marginal_eed = risk_adjusted * marginal_fraction
        per_mib = marginal_eed / cost_bytes * (1024 * 1024)
        return RegionPortfolioEstimate(
            region=region,
            estimated_harvest_bytes=cost_bytes,
            estimated_total_novel_items=cardinality,
            estimated_total_novel_eed=total_eed,
            risk_adjusted_total_eed=risk_adjusted,
            estimated_jaccard=jaccard,
            estimated_containment=containment,
            marginal_fraction=marginal_fraction,
            marginal_eed=marginal_eed,
            marginal_eed_per_mib=per_mib,
            synopsis_confidence=synopsis.confidence,
        )

    @staticmethod
    def _add_to_state(
        states: dict[int, _CoverageState],
        estimate: RegionPortfolioEstimate,
        synopsis: RegionSynopsis,
    ) -> None:
        sketch = _valid_sketch(synopsis)
        cardinality = estimate.estimated_total_novel_items
        if sketch is None or cardinality <= 0:
            return
        width = len(sketch.values)
        state = states.get(width)
        if state is None:
            states[width] = _CoverageState(sketch, cardinality)
            return
        jaccard = sketch.similarity(state.sketch)
        intersection = _intersection_from_jaccard(
            cardinality,
            state.estimated_cardinality,
            jaccard,
        )
        state.estimated_cardinality = max(
            state.estimated_cardinality,
            state.estimated_cardinality + cardinality - intersection,
        )
        state.sketch = state.sketch.union(sketch)

    def plan(
        self,
        *,
        max_regions: int = 8,
        byte_budget: int | None = None,
        index_keys: set[str] | frozenset[str] | tuple[str, ...] | None = None,
        per_region_overhead_bytes: int = 0,
    ) -> RegionPortfolioPlan:
        """Select a virtual harvest portfolio without mutating region state."""

        if max_regions < 1:
            raise ValueError("max_regions must be positive")
        if byte_budget is not None and byte_budget < 1:
            raise ValueError("byte_budget must be positive when supplied")
        if (
            isinstance(per_region_overhead_bytes, bool)
            or not isinstance(per_region_overhead_bytes, int)
            or per_region_overhead_bytes < 0
        ):
            raise ValueError(
                "per_region_overhead_bytes must be a non-negative integer"
            )
        allowed = (
            None
            if index_keys is None
            else frozenset(str(item) for item in index_keys)
        )
        if allowed is not None and not allowed:
            return RegionPortfolioPlan(
                selections=(),
                total_harvest_bytes=0,
                total_marginal_eed=0.0,
                candidate_regions_considered=0,
                harvested_reference_regions=0,
            )

        candidates: list[tuple[HarvestRegion, RegionSynopsis]] = []
        for region in self.registry.list_regions_by_state(
            RegionState.HARVEST_READY
        ):
            if allowed is not None and region.index_key not in allowed:
                continue
            synopsis = self.registry.get_synopsis(region.region_key)
            if synopsis is None or synopsis.novel_count_for_value <= 0:
                continue
            candidates.append((region, synopsis))

        states, harvested_count, unknown_reference = self._reference_states()
        remaining = list(candidates)
        selections: list[RegionPortfolioEstimate] = []
        spent = 0

        while remaining and len(selections) < max_regions:
            scored: list[
                tuple[RegionPortfolioEstimate, HarvestRegion, RegionSynopsis]
            ] = []
            for region, synopsis in remaining:
                estimate = self._estimate(
                    region,
                    synopsis,
                    states,
                    unknown_reference=unknown_reference,
                    per_region_overhead_bytes=per_region_overhead_bytes,
                )
                if (
                    byte_budget is not None
                    and spent + estimate.estimated_harvest_bytes > byte_budget
                ):
                    continue
                if estimate.marginal_fraction < self.policy.min_marginal_fraction:
                    continue
                if estimate.marginal_eed < self.policy.min_marginal_eed:
                    continue
                if estimate.marginal_eed_per_mib < self.policy.min_score_per_mib:
                    continue
                scored.append((estimate, region, synopsis))

            if not scored:
                break
            estimate, region, synopsis = max(
                scored,
                key=lambda item: (
                    item[0].marginal_eed_per_mib,
                    item[0].marginal_eed,
                    -item[0].estimated_harvest_bytes,
                    item[1].region_key,
                ),
            )
            selections.append(estimate)
            spent += estimate.estimated_harvest_bytes
            sketch = _valid_sketch(synopsis)
            self._add_to_state(states, estimate, synopsis)
            if (
                sketch is None
                and estimate.estimated_total_novel_items > 0
            ):
                unknown_reference = True
            remaining = [
                (candidate, candidate_synopsis)
                for candidate, candidate_synopsis in remaining
                if candidate.region_key != region.region_key
            ]

        return RegionPortfolioPlan(
            selections=tuple(selections),
            total_harvest_bytes=spent,
            total_marginal_eed=sum(
                item.marginal_eed for item in selections
            ),
            candidate_regions_considered=len(candidates),
            harvested_reference_regions=harvested_count,
        )
