import unittest
from decimal import Decimal

from creeper.metrics.performance import PerformanceReference, build_performance_model


class PerformanceTargetTests(unittest.TestCase):
    def test_reference_rates_and_target_projection(self):
        model = build_performance_model(
            PerformanceReference(
                observation_days=Decimal("23"),
                annual_raw=Decimal("2144570"),
                annual_eed=Decimal("1246435.78"),
                candidate_raw=Decimal("16953165"),
                candidate_eed=Decimal("9330214.38"),
                baseline_eed=Decimal("34887095.7393"),
            )
        )
        self.assertEqual(model.annual_eed_per_day, Decimal("54192.86"))
        self.assertEqual(model.candidate_eed_per_day, Decimal("405661.4947826086956521739130"))
        target = next(item for item in model.targets if item.target_eed_per_day == Decimal("250000"))
        self.assertEqual(target.raw_at_conservative_weight, Decimal("446428.5714285714285714285714"))
        self.assertEqual(target.eta_to_five_percent_days, Decimal("6.97741914786"))
        self.assertGreater(target.multiple_of_reference_annual_rate, Decimal("4.6"))

    def test_invalid_reference_is_rejected(self):
        with self.assertRaises(ValueError):
            build_performance_model(
                PerformanceReference(
                    observation_days=Decimal("0"),
                    annual_raw=Decimal("1"),
                    annual_eed=Decimal("1"),
                    candidate_raw=Decimal("1"),
                    candidate_eed=Decimal("1"),
                )
            )


if __name__ == "__main__":
    unittest.main()
