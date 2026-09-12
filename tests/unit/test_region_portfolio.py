from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionState,
    RegionSynopsis,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import (
    MeasurementMode,
    SourceCandidate,
    SourceLevel,
)
from creeper.source_discovery.overlap import MinHashSketch
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.storage.control_store import ControlStore


class RegionPortfolioPlannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = IndexSpaceRegistry(self.control)
        self.counter = 0

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def _add_region(
        self,
        *,
        state: RegionState,
        size: int,
        novel_items: int,
        novel_eed: float,
        sketch: tuple[int, ...] = (),
        confidence: float = 1.0,
        complete: bool = True,
        bytes_read: int | None = None,
    ):
        self.counter += 1
        candidate = SourceCandidate(
            canonical_entrypoint=(
                f"https://archive{self.counter}.example/index{self.counter}.cdxj"
            ),
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DIRECT_EVIDENCE_BULK",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=max(1, novel_items),
            direct_evidence_prior=1.0,
            enumerability_prior=1.0,
            confidence=confidence,
        )
        compiled = compile_candidate_index_space(
            candidate,
            content_length=size,
            direct_evidence_authority=True,
        )
        self.registry.register_index_space(compiled)
        synopsis = RegionSynopsis(
            region_key=compiled.root_region.region_key,
            sampled_records=max(1, novel_items),
            unique_hosts=max(1, novel_items),
            novel_hosts=novel_items,
            observed_host_year_pairs=max(1, novel_items),
            novel_host_year_pairs=novel_items,
            novel_eed=novel_eed,
            bytes_read=(
                size
                if bytes_read is None
                else bytes_read
            ),
            requests=1,
            measurement_mode=MeasurementMode.HOST_YEAR,
            minhash_values=sketch,
            confidence=confidence,
            complete=complete,
        )
        self.registry.record_synopsis(synopsis)
        self.registry.mark_region_state(
            compiled.root_region.region_key,
            state,
        )
        region = self.registry.get_region(compiled.root_region.region_key)
        assert region is not None
        return region, synopsis

    @staticmethod
    def _sketch(seed: int, *, width: int = 16) -> tuple[int, ...]:
        return tuple(seed * 1000 + index for index in range(width))

    def test_identical_ready_regions_are_not_both_selected(self) -> None:
        identical = self._sketch(1)
        region_a, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=100,
            novel_eed=100.0,
            sketch=identical,
        )
        region_b, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=100,
            novel_eed=100.0,
            sketch=identical,
        )
        region_c, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=100,
            novel_eed=80.0,
            sketch=self._sketch(2),
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(
                confidence_floor=1.0,
                min_marginal_fraction=0.01,
            ),
        )

        plan = planner.plan(max_regions=2)

        selected = {item.region.region_key for item in plan.selections}
        self.assertIn(region_c.region_key, selected)
        self.assertEqual(
            len(selected & {region_a.region_key, region_b.region_key}),
            1,
        )
        self.assertEqual(len(plan.selections), 2)
        self.assertAlmostEqual(plan.total_marginal_eed, 180.0)

    def test_containment_penalizes_small_subset_even_when_jaccard_is_low(self) -> None:
        # Exactly one of ten MinHash coordinates matches: J=0.1. With
        # candidate cardinality 100 and reference cardinality 1000,
        # |intersection| = .1*(100+1000)/(1+.1) = 100, so the smaller
        # candidate is estimated as fully contained.
        candidate_sketch = tuple(range(1, 11))
        reference_sketch = (1,) + tuple(range(102, 111))
        self._add_region(
            state=RegionState.HARVESTED,
            size=10_000,
            novel_items=1_000,
            novel_eed=1_000.0,
            sketch=reference_sketch,
        )
        candidate, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=100,
            novel_eed=100.0,
            sketch=candidate_sketch,
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(
                confidence_floor=1.0,
                min_marginal_fraction=0.01,
            ),
        )

        plan = planner.plan(max_regions=1)

        self.assertEqual(plan.harvested_reference_regions, 1)
        self.assertEqual(plan.candidate_regions_considered, 1)
        self.assertEqual(plan.selections, ())
        self.assertEqual(plan.total_harvest_bytes, 0)
        self.assertEqual(plan.total_marginal_eed, 0.0)
        self.assertEqual(
            self.registry.get_region(candidate.region_key).state,
            RegionState.HARVEST_READY,
        )

    def test_byte_budget_is_enforced_before_selection(self) -> None:
        small, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=100,
            novel_items=10,
            novel_eed=5.0,
            sketch=self._sketch(1),
        )
        large, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=100,
            novel_eed=20.0,
            sketch=self._sketch(2),
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(confidence_floor=1.0),
        )

        plan = planner.plan(max_regions=2, byte_budget=500)

        self.assertEqual(len(plan.selections), 1)
        self.assertEqual(plan.selections[0].region.region_key, small.region_key)
        self.assertEqual(plan.total_harvest_bytes, 100)
        self.assertNotEqual(
            plan.selections[0].region.region_key,
            large.region_key,
        )

    def test_partial_synopsis_scales_total_reward_and_applies_confidence(self) -> None:
        region, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=1_000,
            novel_items=10,
            novel_eed=4.0,
            sketch=self._sketch(1),
            confidence=0.5,
            complete=False,
            bytes_read=100,
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(
                confidence_floor=0.2,
                min_marginal_fraction=0.0,
            ),
        )

        plan = planner.plan(max_regions=1)

        self.assertEqual(len(plan.selections), 1)
        estimate = plan.selections[0]
        self.assertEqual(estimate.region.region_key, region.region_key)
        self.assertEqual(estimate.estimated_total_novel_items, 100.0)
        self.assertEqual(estimate.estimated_total_novel_eed, 40.0)
        # reliability = .2 + .8*.5 = .6
        self.assertAlmostEqual(estimate.risk_adjusted_total_eed, 24.0)
        self.assertAlmostEqual(estimate.marginal_eed, 24.0)

    def test_missing_sketch_is_unpenalized_only_before_unknown_coverage_exists(self) -> None:
        first, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=100,
            novel_items=10,
            novel_eed=10.0,
            sketch=(),
        )
        second, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=100,
            novel_items=10,
            novel_eed=9.0,
            sketch=(),
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(
                confidence_floor=1.0,
                unknown_overlap_penalty=0.5,
                min_marginal_fraction=0.0,
            ),
        )

        plan = planner.plan(max_regions=2)

        self.assertEqual(len(plan.selections), 2)
        self.assertEqual(plan.selections[0].region.region_key, first.region_key)
        self.assertEqual(plan.selections[0].estimated_containment, 0.0)
        self.assertEqual(plan.selections[0].marginal_fraction, 1.0)
        self.assertEqual(plan.selections[1].region.region_key, second.region_key)
        self.assertEqual(plan.selections[1].estimated_containment, 0.5)
        self.assertEqual(plan.selections[1].marginal_fraction, 0.5)

    def test_incompatible_sketch_width_is_treated_as_unknown_overlap(self) -> None:
        self._add_region(
            state=RegionState.HARVESTED,
            size=100,
            novel_items=10,
            novel_eed=10.0,
            sketch=self._sketch(1, width=8),
        )
        candidate, _ = self._add_region(
            state=RegionState.HARVEST_READY,
            size=100,
            novel_items=10,
            novel_eed=10.0,
            sketch=self._sketch(2, width=16),
        )
        planner = RegionPortfolioPlanner(
            self.registry,
            policy=RegionPortfolioPolicy(
                confidence_floor=1.0,
                unknown_overlap_penalty=0.4,
                min_marginal_fraction=0.0,
            ),
        )

        plan = planner.plan(max_regions=1)

        self.assertEqual(len(plan.selections), 1)
        estimate = plan.selections[0]
        self.assertEqual(estimate.region.region_key, candidate.region_key)
        self.assertIsNone(estimate.estimated_jaccard)
        self.assertEqual(estimate.estimated_containment, 0.4)
        self.assertEqual(estimate.marginal_fraction, 0.6)

    def test_minhash_union_uses_elementwise_minimum(self) -> None:
        left = MinHashSketch((9, 2, 7, 4))
        right = MinHashSketch((3, 8, 5, 6))

        self.assertEqual(
            left.union(right),
            MinHashSketch((3, 2, 5, 4)),
        )


if __name__ == "__main__":
    unittest.main()
