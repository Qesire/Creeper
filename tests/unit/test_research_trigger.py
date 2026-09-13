from __future__ import annotations

import unittest

from creeper.source_discovery.research_trigger import (
    ResearchTriggerGate,
    ResearchTriggerReason,
    ResearchTriggerSnapshot,
)


class ResearchTriggerGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = ResearchTriggerGate(
            min_seconds_between_llm_starts=10.0,
            same_context_failure_cooldown_seconds=60.0,
            ready_minutes_threshold=30.0,
        )

    def test_active_call_has_highest_precedence(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                active_llm_episode_id="active",
                unknown_contract_blockers=1,
                ready_minutes=0.0,
            )
        )
        self.assertFalse(decision.allow)

    def test_high_value_blocker_bypasses_unrelated_deterministic_frontier(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                unknown_structure_blockers=1,
                executable_regions=9,
                deterministic_candidate_backlog=100,
                context_hash="ctx",
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.UNKNOWN_STRUCTURE_FAMILY,
        )
        self.assertEqual(decision.task_type, "INTERPRET_STRUCTURE")

    def test_deterministic_frontier_suppresses_operator_request(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                executable_regions=1,
                operator_requested=True,
                context_hash="ctx",
            )
        )
        self.assertFalse(decision.allow)
        self.assertIn("deterministic work", decision.explanation)

    def test_ready_inventory_starvation_prefers_productive_pattern(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=5.0,
                productive_direct_inventory=2,
                context_hash="ctx",
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.READY_INVENTORY_LOW,
        )
        self.assertEqual(decision.task_type, "EXPLOIT_SUCCESS_PATTERN")

    def test_frontier_exhaustion_triggers_discovery(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=None,
                context_hash="ctx",
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.FRONTIER_EXHAUSTED,
        )
        self.assertEqual(decision.task_type, "DISCOVER_NEW_SOURCE")

    def test_sustained_final_zero_tail_triggers_recovery(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=60.0,
                final_eed_per_hour_15m=4.0,
                final_eed_per_hour_60m=0.0,
                closed_source_runs=4,
                recent_zero_reward_tail=3,
                context_hash="ctx",
            )
        )
        self.assertTrue(decision.allow)
        self.assertEqual(
            decision.reason,
            ResearchTriggerReason.SUSTAINED_FINAL_YIELD_COLLAPSE,
        )
        self.assertEqual(decision.task_type, "RECOVER_STAGNATION")

    def test_same_context_failure_uses_longer_cooldown(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=0.0,
                last_llm_started_at=100.0,
                same_context_failures=1,
                now=150.0,
                context_hash="ctx",
            )
        )
        self.assertFalse(decision.allow)
        self.assertIn("cooldown", decision.explanation)

    def test_explicit_request_is_last_bounded_trigger(self) -> None:
        decision = self.gate.decide(
            ResearchTriggerSnapshot(
                ready_minutes=60.0,
                operator_requested=True,
                context_hash="ctx",
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
