from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.manager import (
    SearchDirectiveKind,
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import (
    ResidualSearchLedger,
    ResidualSearchPolicy,
    SearchCell,
    SearchCellScheduler,
    SearchCellState,
    default_search_cells,
)
from creeper.storage.control_store import ControlStore


class ResidualSearchLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.now = 1000.0
        self.policy = ResidualSearchPolicy(
            saturation_min_attempts=2,
            saturation_min_results=10,
            saturation_duplicate_fraction=0.90,
            saturation_max_new_family_fraction=0.10,
            exclusion_min_hits=2,
            max_exclusions=4,
        )
        self.ledger = ResidualSearchLedger(
            self.connection,
            clock=lambda: self.now,
            policy=self.policy,
        )
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )

    def tearDown(self) -> None:
        self.connection.close()

    def test_cell_identity_is_dimension_stable(self) -> None:
        same = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        different = SearchCell(
            mechanism="proxy_access",
            institution="isp",
            period="1998",
            artifact="trace",
        )
        self.assertEqual(self.cell.key, same.key)
        self.assertNotEqual(self.cell.key, different.key)

    def test_invalid_or_out_of_scope_cells_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported mechanism"):
            SearchCell("generic_web", "university", "1998", "trace")
        with self.assertRaisesRegex(ValueError, "period"):
            SearchCell("proxy_access", "university", "2005", "trace")

    def test_repeated_family_hits_become_cell_local_exclusions(self) -> None:
        self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=4,
            unique_roots=6,
            new_families=2,
            qualified_roots=1,
            family_keys=("Digital Proxy Trace", "BU Trace"),
        )
        self.now += 1
        self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=4,
            unique_roots=6,
            new_families=2,
            qualified_roots=1,
            family_keys=("Digital Proxy Trace",),
        )

        self.assertEqual(self.ledger.exclusions(self.cell), ("digital proxy trace",))
        plan = SearchCellScheduler(self.ledger).next_plans(limit=1)[0]
        self.assertIn('-"digital proxy trace"', plan.query)
        self.assertIn('"1998"', plan.query)
        self.assertIn("URL OR hostname OR host", plan.query)

    def test_saturation_is_driven_by_duplicate_results_not_query_count_alone(self) -> None:
        first = self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=1,
            qualified_roots=0,
        )
        self.assertEqual(first.state, SearchCellState.ACTIVE)

        second = self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=1,
            qualified_roots=0,
        )
        self.assertEqual(second.state, SearchCellState.SATURATED)
        self.assertEqual(SearchCellScheduler(self.ledger).next_plans(limit=1), ())

    def test_low_relevance_new_results_saturate_after_enough_evidence(self) -> None:
        first = self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=0,
            unique_roots=10,
            new_families=10,
            qualified_roots=0,
        )
        self.assertEqual(first.state, SearchCellState.ACTIVE)

        second = self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=0,
            unique_roots=10,
            new_families=10,
            qualified_roots=0,
        )
        self.assertEqual(second.state, SearchCellState.SATURATED)

    def test_repeated_empty_queries_saturate_without_waiting_for_result_floor(self) -> None:
        self.ledger.record_episode(
            self.cell,
            result_count=0,
            duplicate_results=0,
            unique_roots=0,
            new_families=0,
            qualified_roots=0,
        )
        stats = self.ledger.record_episode(
            self.cell,
            result_count=0,
            duplicate_results=0,
            unique_roots=0,
            new_families=0,
            qualified_roots=0,
        )
        self.assertEqual(stats.state, SearchCellState.SATURATED)

    def test_search_profile_change_reopens_cells_and_resets_local_metrics(self) -> None:
        self.assertFalse(self.ledger.ensure_search_profile("providers=datacite"))
        self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=1,
            qualified_roots=0,
            family_keys=("famous source",),
        )
        self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=1,
            qualified_roots=0,
            family_keys=("famous source",),
        )
        self.assertEqual(self.ledger.stats(self.cell).state, SearchCellState.SATURATED)

        changed = self.ledger.ensure_search_profile(
            "providers=datacite,oai"
        )

        self.assertTrue(changed)
        reset = self.ledger.stats(self.cell)
        self.assertEqual(reset.state, SearchCellState.OPEN)
        self.assertEqual(reset.attempts, 0)
        self.assertEqual(reset.result_count, 0)
        self.assertEqual(self.ledger.exclusions(self.cell), ())

    def test_high_duplicate_but_real_new_family_supply_does_not_saturate(self) -> None:
        self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=2,
            qualified_roots=1,
        )
        stats = self.ledger.record_episode(
            self.cell,
            result_count=10,
            duplicate_results=9,
            unique_roots=1,
            new_families=2,
            qualified_roots=1,
        )
        self.assertEqual(stats.state, SearchCellState.ACTIVE)

    def test_scheduler_prefers_unexplored_over_repeated_low_yield_cell(self) -> None:
        other = SearchCell(
            mechanism="dns_survey",
            institution="nic",
            period="1997",
            artifact="dump",
        )
        self.ledger.ensure_cells((self.cell, other))
        self.ledger.record_episode(
            self.cell,
            result_count=20,
            duplicate_results=15,
            unique_roots=5,
            new_families=1,
            qualified_roots=0,
            search_cost_seconds=5.0,
        )

        plan = SearchCellScheduler(self.ledger).next_plans(limit=1)[0]
        self.assertEqual(plan.cell, other)

    def test_scheduler_exploits_observed_residual_yield_without_losing_coverage(self) -> None:
        productive = self.cell
        unseen = SearchCell(
            mechanism="dns_survey",
            institution="nic",
            period="1997",
            artifact="dump",
        )
        self.ledger.ensure_cells((productive, unseen))
        self.ledger.record_episode(
            productive,
            result_count=20,
            duplicate_results=2,
            unique_roots=18,
            new_families=10,
            qualified_roots=12,
            accepted_novel_eed=40.0,
            search_cost_seconds=2.0,
        )

        plans = SearchCellScheduler(self.ledger).next_plans(limit=2)
        self.assertEqual(plans[0].cell, productive)
        self.assertEqual(plans[1].cell, unseen)

    def test_multi_plan_batch_prefers_distinct_mechanisms(self) -> None:
        cells = (
            SearchCell(
                mechanism="proxy_access",
                institution="university",
                period="1998",
                artifact="trace",
            ),
            SearchCell(
                mechanism="proxy_access",
                institution="research_lab",
                period="1997",
                artifact="log",
            ),
            SearchCell(
                mechanism="dns_survey",
                institution="nic",
                period="1998",
                artifact="dump",
            ),
        )
        self.ledger.ensure_cells(cells)

        plans = SearchCellScheduler(self.ledger).next_plans(limit=2)

        self.assertEqual(len(plans), 2)
        self.assertEqual(
            len({plan.cell.mechanism for plan in plans}),
            2,
        )

    def test_strong_measured_yield_can_override_batch_diversity_penalty(self) -> None:
        productive_a = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        productive_b = SearchCell(
            mechanism="proxy_access",
            institution="isp",
            period="1999",
            artifact="log",
        )
        unexplored = SearchCell(
            mechanism="dns_survey",
            institution="nic",
            period="1997",
            artifact="dump",
        )
        self.ledger.ensure_cells((productive_a, productive_b, unexplored))
        for cell in (productive_a, productive_b):
            self.ledger.record_episode(
                cell,
                result_count=20,
                duplicate_results=0,
                unique_roots=20,
                new_families=20,
                qualified_roots=20,
                accepted_novel_eed=100.0,
                search_cost_seconds=1.0,
            )

        plans = SearchCellScheduler(self.ledger).next_plans(limit=2)

        self.assertEqual(
            {plan.cell for plan in plans},
            {productive_a, productive_b},
        )

    def test_variant_rotation_is_finite_and_deterministic(self) -> None:
        self.ledger.ensure_cell(self.cell)
        scheduler = SearchCellScheduler(self.ledger)
        first = scheduler.next_plans(limit=1)[0]
        self.ledger.record_episode(
            self.cell,
            result_count=1,
            duplicate_results=0,
            unique_roots=1,
            new_families=1,
            qualified_roots=1,
        )
        second = scheduler.next_plans(limit=1)[0]
        self.assertNotEqual(first.variant, second.variant)
        self.assertNotEqual(first.query, second.query)

    def test_default_space_is_bounded_and_all_cells_target_competition_years(self) -> None:
        cells = default_search_cells()
        self.assertGreater(len(cells), 100)
        self.assertLess(len(cells), 2000)
        self.assertEqual(len({cell.key for cell in cells}), len(cells))
        self.assertTrue(all(cell.period in {"1996", "1997", "1998", "1999", "2000", "2001"} for cell in cells))


class ResidualSearchManagerIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.ledger = ResidualSearchLedger(self.registry.connection)
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.ledger.ensure_cell(self.cell)
        self.scheduler = SearchCellScheduler(self.ledger)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def manager(self) -> SourceReservoirManager:
        return SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=1,
                cold_target=1,
                max_search_directives=1,
            ),
            residual_search_scheduler=self.scheduler,
        )

    def test_deterministic_cell_refill_suppresses_broad_llm_search(self) -> None:
        plan = self.manager().plan()

        self.assertEqual(len(plan.deterministic_search_plans), 1)
        self.assertEqual(plan.deterministic_search_plans[0].cell, self.cell)
        self.assertEqual(plan.search_directives, ())
        self.assertTrue(plan.needs_search)

    def test_exhausted_cell_requests_mechanism_recovery_not_generic_refill(self) -> None:
        self.ledger.mark_exhausted(self.cell)

        plan = self.manager().plan()

        self.assertEqual(plan.deterministic_search_plans, ())
        self.assertEqual(len(plan.search_directives), 1)
        self.assertEqual(
            plan.search_directives[0].kind,
            SearchDirectiveKind.RECOVER_STAGNATION,
        )
        self.assertIn("data-generating mechanism", plan.search_directives[0].reason)


if __name__ == "__main__":
    unittest.main()
