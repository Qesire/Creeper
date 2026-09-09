import unittest

from creeper.records.models import SourceStats
from creeper.scheduler.budgets import SourceBudget
from creeper.scheduler.priority import rank_sources


class SchedulerTests(unittest.TestCase):
    def test_rank_is_deterministic_and_penalizes_saturation(self):
        decisions = rank_sources(
            [
                SourceStats("slow", 10, 10, 100, 3600),
                SourceStats("fast", 10, 10, 200, 3600),
                SourceStats("saturated", 10, 10, 100, 3600, saturated=True),
            ]
        )
        self.assertEqual([x.source_id for x in decisions], ["fast", "slow", "saturated"])
        self.assertTrue(all(x.reason == "engineering_only_baseline_external" for x in decisions))

    def test_budget_is_finite(self):
        budget = SourceBudget(max_records=2, max_seconds=10)
        self.assertTrue(budget.allows())
        budget.records_used = 2
        self.assertFalse(budget.allows())


if __name__ == "__main__":
    unittest.main()
