from __future__ import annotations

import sqlite3
import unittest

from creeper.source_discovery.residual_search import (
    MECHANISM_QUERY_TERMS,
    ResidualSearchLedger,
    ResidualSearchPolicy,
    SearchCell,
    SearchCellScheduler,
    SearchCellState,
    query_program_length,
)


class ResidualSearchProgramCoverageTests(unittest.TestCase):
    def test_every_zero_result_variant_executes_before_saturation(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        try:
            ledger = ResidualSearchLedger(
                connection,
                policy=ResidualSearchPolicy(
                    saturation_min_attempts=2,
                    saturation_min_results=10,
                    saturation_duplicate_fraction=0.90,
                    saturation_max_new_family_fraction=0.10,
                    exclusion_min_hits=2,
                    max_exclusions=4,
                ),
            )
            cell = SearchCell(
                mechanism="proxy_access",
                institution="university",
                period="1998",
                artifact="trace",
            )
            scheduler = SearchCellScheduler(ledger)
            program_length = query_program_length(cell)
            self.assertEqual(
                program_length,
                len(MECHANISM_QUERY_TERMS["proxy_access"]) * 2,
            )

            seen_variants: list[int] = []
            for index in range(program_length):
                plans = scheduler.next_plans(limit=1)
                before = ledger.stats(cell)
                self.assertTrue(
                    plans,
                    (
                        f"cell disappeared before query {index + 1}/"
                        f"{program_length}: state={before.state.value}, "
                        f"attempts={before.attempts}, "
                        f"cursor={before.variant_cursor}, "
                        f"results={before.result_count}"
                    ),
                )
                plan = plans[0]
                seen_variants.append(plan.variant)
                after = ledger.record_episode(
                    cell,
                    result_count=0,
                    duplicate_results=0,
                    unique_roots=0,
                    new_families=0,
                    qualified_roots=0,
                )
                expected_state = (
                    SearchCellState.SATURATED
                    if index == program_length - 1
                    else SearchCellState.ACTIVE
                )
                self.assertEqual(
                    after.state,
                    expected_state,
                    (
                        f"wrong state after query {index + 1}/"
                        f"{program_length}: attempts={after.attempts}, "
                        f"cursor={after.variant_cursor}, "
                        f"results={after.result_count}"
                    ),
                )

            self.assertEqual(seen_variants, list(range(program_length)))
            self.assertEqual(scheduler.next_plans(limit=1), ())
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
