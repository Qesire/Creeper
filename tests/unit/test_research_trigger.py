from __future__ import annotations

import unittest

from creeper.source_discovery.research_trigger import (
    ResearchTriggerDecision,
    ResearchTriggerGate,
    ResearchTriggerReason,
    ResearchTriggerSnapshot,
)


class ResearchTriggerGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = ResearchTriggerGate(
            min_seconds_between_llm_starts=120.0,
            same_context_failure_cooldown_seconds=600.0,
            ready_minutes_threshold=30.0,
            stagnation_min_closed_runs=3,
            stagnation_zero_tail=3,
        )

    def test_executable_region_suppresses_generic_research(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                executable_regions=2,
                ready_minutes=5.0,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertFalse(decision.allow)
        self.assertIsNone(decision.reason)

    def test_low_ready_inventory_allows_discovery_without_frontier(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=10.0,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(decision.reason, ResearchTriggerReason.READY_INVENTORY_LOW)

    def test_unknown_contract_bypasses_unrelated_frontier(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                executable_regions=3,
                unknown_contract_blockers=1,
                subject="dataset-x",
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.UNKNOWN_CONTRACT_FAMILY,
        )
        self.assertEqual(decision.task_type, "INTERPRET_EVIDENCE_CONTRACT")

    def test_active_llm_suppresses_every_trigger(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                active_llm_episode_id="llm:1",
                operator_requested=True,
                unknown_structure_blockers=1,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertFalse(decision.allow)

    def test_failed_same_context_is_cooled_down(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=0.0,
                context_hash="ctx",
                same_context_failures=1,
                last_llm_started_at=950.0,
                now=1_000.0,
            )
        )
        self.assertFalse(decision.allow)

    def test_one_zero_source_does_not_trigger_stagnation(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                final_eed_per_hour_60m=0.0,
                closed_source_runs=1,
                recent_zero_reward_tail=1,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertFalse(decision.allow)

    def test_sustained_final_zero_tail_triggers_recovery(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                final_eed_per_hour_60m=0.0,
                closed_source_runs=4,
                recent_zero_reward_tail=3,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.SUSTAINED_FINAL_YIELD_COLLAPSE,
        )
        self.assertEqual(decision.task_type, "RECOVER_STAGNATION")

    def test_explicit_request_is_single_bounded_directive(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                executable_regions=100,
                operator_requested=True,
                context_hash="ctx",
                now=1_000.0,
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.EXPLICIT_OPERATOR_REQUEST,
        )
        self.assertEqual(decision.desired_regions, 1)


if __name__ == "__main__":
    unittest.main()
