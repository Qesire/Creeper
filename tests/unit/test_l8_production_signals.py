from __future__ import annotations

import unittest
from types import SimpleNamespace

from creeper.source_discovery.production_value import ProductionValueModel


class _Registry:
    current_scout_authority = ("baseline:v1", "model:v1")

    def __init__(self, runs) -> None:
        self._runs = runs

    @staticmethod
    def clock() -> float:
        return 10_000.0

    def list_source_run_outcomes(
        self,
        source_key=None,
        *,
        baseline_signature=None,
        model_signature=None,
        closed_only=False,
    ):
        assert source_key is None
        assert baseline_signature == "baseline:v1"
        assert model_signature == "model:v1"
        assert closed_only is True
        return list(self._runs)


class L8ProductionResearchSignalTests(unittest.TestCase):
    def test_recent_final_zero_tail_is_authority_scoped(self) -> None:
        runs = [
            SimpleNamespace(closed_at=9_000.0, final_accepted_eed=7.0),
            SimpleNamespace(closed_at=9_600.0, final_accepted_eed=0.0),
            SimpleNamespace(closed_at=9_800.0, final_accepted_eed=0.0),
            SimpleNamespace(closed_at=9_900.0, final_accepted_eed=0.0),
            # Older than the 60-minute trigger window and therefore ignored.
            SimpleNamespace(closed_at=6_000.0, final_accepted_eed=1000.0),
        ]
        model = ProductionValueModel(_Registry(runs))  # type: ignore[arg-type]

        signals = model.research_signals(now=10_000.0)

        self.assertEqual(signals.closed_source_runs, 4)
        self.assertEqual(signals.recent_zero_reward_tail, 3)
        self.assertEqual(signals.final_eed_per_hour_60m, 7.0)
        # The 15-minute window contains only the three zero-reward tail runs.
        self.assertEqual(signals.final_eed_per_hour_15m, 0.0)

    def test_no_current_authority_produces_no_final_signal(self) -> None:
        registry = _Registry(())
        registry.current_scout_authority = None
        model = ProductionValueModel(registry)  # type: ignore[arg-type]

        signals = model.research_signals(now=10_000.0)

        self.assertIsNone(signals.final_eed_per_hour_15m)
        self.assertIsNone(signals.final_eed_per_hour_60m)
        self.assertEqual(signals.closed_source_runs, 0)
        self.assertEqual(signals.recent_zero_reward_tail, 0)


if __name__ == "__main__":
    unittest.main()
