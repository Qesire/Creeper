"""Adaptive byte-region tomography over historical archive indexes.

The planner is intentionally simple and auditable. It treats each finite index
region as a partially observed coverage opportunity, refines promising or still
uncertain regions, and promotes terminal positive regions for harvest. It does
not yet implement portfolio-overlap penalties; those are layered on top of the
durable region/synopsis state in the next stage.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    HarvestRegion,
    RegionKind,
    RegionState,
    RegionSynopsis,
    child_region,
)


class TomographyActionKind(StrEnum):
    PROBE = "PROBE"
    HARVEST = "HARVEST"


@dataclass(frozen=True)
class RegionTomographyPolicy:
    max_depth: int = 8
    min_child_bytes: int = 4 * 1024 * 1024
    min_observations_to_stop: int = 64
    zero_yield_stop_confidence: float = 0.01
    min_novel_fraction: float = 0.0
    min_novel_eed_per_mib: float = 0.0
    exploration_weight: float = 0.25

    def __post_init__(self) -> None:
        if self.max_depth < 0:
            raise ValueError("max_depth must be non-negative")
        if self.min_child_bytes < 1:
            raise ValueError("min_child_bytes must be positive")
        if self.min_observations_to_stop < 1:
            raise ValueError("min_observations_to_stop must be positive")
        if not 0 <= self.zero_yield_stop_confidence <= 1:
            raise ValueError("zero_yield_stop_confidence must be within [0, 1]")
        if not 0 <= self.min_novel_fraction <= 1:
            raise ValueError("min_novel_fraction must be within [0, 1]")
        if self.min_novel_eed_per_mib < 0:
            raise ValueError("min_novel_eed_per_mib must be non-negative")
        if self.exploration_weight < 0:
            raise ValueError("exploration_weight must be non-negative")


@dataclass(frozen=True)
class TomographyAction:
    kind: TomographyActionKind
    region: HarvestRegion
    priority: float
    expected_total_novel_eed: float
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", TomographyActionKind(self.kind))
        if math.isnan(self.priority):
            raise ValueError("priority cannot be NaN")
        if (
            not math.isfinite(self.expected_total_novel_eed)
            or self.expected_total_novel_eed < 0
        ):
            raise ValueError(
                "expected_total_novel_eed must be finite and non-negative"
            )


def region_size_bytes(region: HarvestRegion) -> int | None:
    if region.byte_start is None or region.byte_end is None:
        return None
    return region.byte_end - region.byte_start + 1


def split_byte_region(region: HarvestRegion) -> tuple[HarvestRegion, HarvestRegion]:
    """Bisect one finite region into deterministic non-overlapping children."""

    size = region_size_bytes(region)
    if size is None or size < 2:
        raise ValueError("region requires at least two known bytes to split")
    assert region.byte_start is not None
    assert region.byte_end is not None
    midpoint = region.byte_start + (size // 2) - 1
    left = child_region(
        region,
        kind=RegionKind.BYTE_RANGE,
        byte_start=region.byte_start,
        byte_end=midpoint,
    )
    right = child_region(
        region,
        kind=RegionKind.BYTE_RANGE,
        byte_start=midpoint + 1,
        byte_end=region.byte_end,
    )
    return left, right


class RegionTomographyPlanner:
    """Turn durable region synopses into finite next actions."""

    def __init__(
        self,
        registry: IndexSpaceRegistry,
        *,
        policy: RegionTomographyPolicy | None = None,
    ) -> None:
        self.registry = registry
        self.policy = policy or RegionTomographyPolicy()

    def _splittable(self, region: HarvestRegion) -> bool:
        size = region_size_bytes(region)
        return (
            size is not None
            and region.depth < self.policy.max_depth
            and size >= 2 * self.policy.min_child_bytes
        )

    def _should_refine(
        self,
        region: HarvestRegion,
        synopsis: RegionSynopsis,
    ) -> bool:
        if synopsis.complete or not self._splittable(region):
            return False
        if synopsis.novel_count_for_value > 0:
            return (
                synopsis.novel_fraction >= self.policy.min_novel_fraction
                or synopsis.novel_eed_per_byte * (1024 * 1024)
                >= self.policy.min_novel_eed_per_mib
            )
        return (
            synopsis.observed_count_for_value
            < self.policy.min_observations_to_stop
            or synopsis.confidence < self.policy.zero_yield_stop_confidence
        )

    def _worth_harvest(self, synopsis: RegionSynopsis) -> bool:
        if synopsis.novel_count_for_value <= 0:
            return False
        if synopsis.novel_fraction < self.policy.min_novel_fraction:
            return False
        return (
            synopsis.novel_eed_per_byte * (1024 * 1024)
            >= self.policy.min_novel_eed_per_mib
        )

    def _priority(
        self,
        region: HarvestRegion,
        synopsis: RegionSynopsis | None,
    ) -> tuple[float, float]:
        if synopsis is None:
            return float("inf"), 0.0
        size = region_size_bytes(region) or max(1, synopsis.bytes_read)
        density = (
            0.0
            if synopsis.bytes_read <= 0
            else synopsis.novel_eed / synopsis.bytes_read
        )
        expected = max(0.0, density * size)
        density_per_mib = density * (1024 * 1024)
        uncertainty = (
            self.policy.exploration_weight
            * (1.0 - synopsis.confidence)
            * max(1.0, density_per_mib)
        )
        return density_per_mib + uncertainty, expected

    def _leaf_regions(self, index_key: str) -> tuple[HarvestRegion, ...]:
        regions = self.registry.list_regions(index_key)
        parent_keys = {
            region.parent_region_key
            for region in regions
            if region.parent_region_key is not None
        }
        return tuple(
            region
            for region in regions
            if region.region_key not in parent_keys
            and region.state
            not in {
                RegionState.HARVESTED,
                RegionState.DROPPED,
            }
        )

    def advance(
        self,
        index_key: str,
        *,
        max_actions: int = 4,
    ) -> tuple[TomographyAction, ...]:
        """Refine one wave and return the best finite next actions."""

        if max_actions < 1:
            raise ValueError("max_actions must be positive")
        if self.registry.get_index(index_key) is None:
            raise KeyError(f"unknown source index: {index_key}")

        for region in self._leaf_regions(index_key):
            if region.state is not RegionState.PROBED:
                continue
            synopsis = self.registry.get_synopsis(region.region_key)
            if synopsis is None or not self._should_refine(region, synopsis):
                continue
            for child in split_byte_region(region):
                self.registry.put_region(child)

        actions: list[TomographyAction] = []
        for region in self._leaf_regions(index_key):
            synopsis = self.registry.get_synopsis(region.region_key)
            # A scout synopsis is useful as a value prior but cannot establish
            # exact byte boundaries. Force one real bounded probe whenever the
            # durable region still lacks a finite range; the probe may learn
            # object size with HEAD/Range or local stat before harvest.
            if (
                region.byte_start is None
                or region.byte_end is None
                or region.state is RegionState.DISCOVERED
                or synopsis is None
            ):
                priority, expected = self._priority(region, synopsis)
                actions.append(
                    TomographyAction(
                        kind=TomographyActionKind.PROBE,
                        region=region,
                        priority=priority,
                        expected_total_novel_eed=expected,
                        reason="region has no bounded synopsis",
                    )
                )
                continue

            if region.state is RegionState.HARVEST_READY:
                priority, expected = self._priority(region, synopsis)
                actions.append(
                    TomographyAction(
                        kind=TomographyActionKind.HARVEST,
                        region=region,
                        priority=priority,
                        expected_total_novel_eed=expected,
                        reason="terminal region is ready for deterministic harvest",
                    )
                )
                continue

            if region.state is not RegionState.PROBED:
                continue
            if self._should_refine(region, synopsis):
                continue
            if self._worth_harvest(synopsis):
                self.registry.mark_region_state(
                    region.region_key,
                    RegionState.HARVEST_READY,
                )
                refreshed = self.registry.get_region(region.region_key)
                assert refreshed is not None
                priority, expected = self._priority(refreshed, synopsis)
                actions.append(
                    TomographyAction(
                        kind=TomographyActionKind.HARVEST,
                        region=refreshed,
                        priority=priority,
                        expected_total_novel_eed=expected,
                        reason=(
                            "positive region reached tomography terminal "
                            "granularity"
                        ),
                    )
                )
            else:
                self.registry.mark_region_state(
                    region.region_key,
                    RegionState.DROPPED,
                )

        actions.sort(
            key=lambda item: (
                -item.priority,
                item.kind.value,
                item.region.region_key,
            )
        )
        return tuple(actions[:max_actions])
