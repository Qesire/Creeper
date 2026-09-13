from __future__ import annotations

import unittest
from dataclasses import dataclass

from creeper.source_research.bandit import BanditStats, validate_probabilities
from creeper.source_research.decision_log import MemoryDecisionLog
from creeper.source_research.ope import (
    evaluate_policy, final_reward_map, verify_propensity_replay,
)
from creeper.source_research.policy import (
    ActionCandidate,
    HierarchicalAdaptivePolicy,
    PolicyConfig,
    PolicyLevel,
    PolicyLifecycle,
)
from creeper.source_research.rebase import plan_rebase, recompute_novelty_view
from creeper.source_research.drift import decayed_stats_view, detect_mean_drift
from creeper.source_research.scheduler import AdaptiveResearchScheduler
from creeper.source_research.saturation import (
    FamilyObservation,
    SaturationState,
    SaturationTracker,
)


class AdaptivePolicyTests(unittest.TestCase):
    def make_policy(self):
        return HierarchicalAdaptivePolicy(
            PolicyConfig(
                policy_id="adaptive-research",
                version="p1",
                snapshot_id="snap:1",
                exploration_fraction=0.075,
                half_life_seconds=1000.0,
            )
        )

    def root_candidates(self):
        return [
            ActionCandidate("root:a", PolicyLevel.ROOT),
            ActionCandidate("root:b", PolicyLevel.ROOT),
        ]

    def test_high_reward_increases_probability(self):
        policy = self.make_policy()
        candidates = self.root_candidates()
        low, _ = policy.probabilities(
            level=PolicyLevel.ROOT,
            candidates=candidates,
            stats_by_arm={
                "root:a": BanditStats("root:a", pulls=10, final_reward=1.0, updated_at=100),
                "root:b": BanditStats("root:b", pulls=10, final_reward=1.0, updated_at=100),
            },
            now=100,
        )
        high, _ = policy.probabilities(
            level=PolicyLevel.ROOT,
            candidates=candidates,
            stats_by_arm={
                "root:a": BanditStats("root:a", pulls=10, final_reward=20.0, updated_at=100),
                "root:b": BanditStats("root:b", pulls=10, final_reward=1.0, updated_at=100),
            },
            now=100,
        )
        self.assertGreater(high["root:a"], low["root:a"])
        validate_probabilities(high)

    def test_uncertain_arm_retains_exploration_probability(self):
        policy = self.make_policy()
        candidates = [
            ActionCandidate("root:known", PolicyLevel.ROOT),
            ActionCandidate(
                "root:uncertain", PolicyLevel.ROOT,
                uncertainty=5.0, novelty=2.0, orthogonality=1.0,
            ),
            ActionCandidate(
                "root:reject", PolicyLevel.ROOT,
                permanent_semantic_reject=True,
            ),
        ]
        probs, _ = policy.probabilities(
            level=PolicyLevel.ROOT,
            candidates=candidates,
            stats_by_arm={
                "root:known": BanditStats("root:known", pulls=100, final_reward=50.0, updated_at=0),
                "root:uncertain": BanditStats("root:uncertain", pulls=0, updated_at=0),
            },
            now=0,
        )
        self.assertGreater(probs["root:uncertain"], 0.0)
        self.assertNotIn("root:reject", probs)
        validate_probabilities(probs)

    def test_state_reload_from_snapshot(self):
        policy = self.make_policy()
        restored = HierarchicalAdaptivePolicy.from_snapshot(
            snapshot_id="snap:1", parameters=policy.snapshot_parameters()
        )
        self.assertEqual(restored.config.version, policy.config.version)
        self.assertEqual(
            restored.config.exploration_fraction, policy.config.exploration_fraction
        )

    def test_decision_log_has_propensity_and_candidates(self):
        policy = self.make_policy()
        decision = policy.select(
            task_id="t1",
            level=PolicyLevel.ROOT,
            candidates=self.root_candidates(),
            stats_by_arm={},
            context_features={"budget": 10},
            timestamp=123.0,
        )
        log = MemoryDecisionLog()
        self.assertTrue(log.append(decision))
        self.assertFalse(log.append(decision))
        event = log.events()[0]
        self.assertGreater(event.propensity, 0.0)
        self.assertEqual(set(event.candidate_action_ids), {"root:a", "root:b"})

    def test_policy_lifecycle_cannot_skip_offline_evaluation(self):
        policy = self.make_policy()
        with self.assertRaises(ValueError):
            policy.promote(PolicyLifecycle.ACTIVE)
        policy = policy.promote(PolicyLifecycle.OFFLINE_EVALUATED)
        policy = policy.promote(PolicyLifecycle.CANARY)
        policy = policy.promote(PolicyLifecycle.ACTIVE)
        self.assertEqual(policy.config.lifecycle, PolicyLifecycle.ACTIVE)
        policy = policy.promote(PolicyLifecycle.RETIRED)
        self.assertEqual(policy.config.lifecycle, PolicyLifecycle.RETIRED)

    def test_propensity_replay_exact_for_same_logged_distribution(self):
        policy = self.make_policy()
        decision = policy.select(
            task_id="replay",
            level=PolicyLevel.ROOT,
            candidates=self.root_candidates(),
            stats_by_arm={},
            context_features={"x": 1},
            timestamp=1.0,
        )
        log = MemoryDecisionLog()
        log.append(decision)
        event = log.events()[0]
        replay = verify_propensity_replay(
            event, target_policy=lambda e: dict(e.probabilities)
        )
        self.assertTrue(replay.matches)
        self.assertEqual(replay.absolute_error, 0.0)

    def test_drift_and_decay_are_derived_views(self):
        source = BanditStats(
            "root:a", pulls=2, final_reward=8.0, decayed_reward=8.0, updated_at=0.0
        )
        view = decayed_stats_view([source], now=1000.0, half_life_seconds=1000.0)
        self.assertAlmostEqual(view[0].decayed_reward, 4.0, places=7)
        self.assertEqual(source.decayed_reward, 8.0)
        signal = detect_mean_drift([10.0, 10.0], [2.0, 2.0], relative_threshold=0.5)
        self.assertTrue(signal.detected)

    def test_scheduler_hierarchy_reuses_real_frontier_task_id(self):
        class FakeRegistry:
            def __init__(self):
                self.decisions = []
                self.snapshots = []
            def record_decision(self, decision):
                self.decisions.append(decision)
                return True
            def upsert_policy_snapshot(self, snapshot):
                self.snapshots.append(snapshot)
            def arm_stats(self, *, policy_version):
                return ()
            def rebuild_arm_stats(self, *, policy_version, schema_version):
                return ()

        registry = FakeRegistry()
        scheduler = AdaptiveResearchScheduler(policy=self.make_policy(), registry=registry)
        decisions = scheduler.choose_path(
            task_id="frontier-task-1",
            hierarchy=[
                (PolicyLevel.ROOT, self.root_candidates()),
                (
                    PolicyLevel.QUERY_FAMILY,
                    [
                        ActionCandidate("query:a", PolicyLevel.QUERY_FAMILY),
                        ActionCandidate("query:b", PolicyLevel.QUERY_FAMILY),
                    ],
                ),
            ],
            context_features={"budget": 4},
            timestamp=10.0,
        )
        self.assertEqual(len(decisions), 2)
        self.assertEqual({row.task_id for row in registry.decisions}, {"frontier-task-1"})
        self.assertNotEqual(decisions[0].decision_id, decisions[1].decision_id)
        self.assertEqual(
            registry.decisions[1].metadata["context_features"]["parent_decision_ids"],
            (decisions[0].decision_id,),
        )

    def test_ope_consumes_propensity_log(self):
        policy = self.make_policy()
        log = MemoryDecisionLog()
        for i in range(8):
            decision = policy.select(
                task_id=f"t{i}",
                level=PolicyLevel.ROOT,
                candidates=self.root_candidates(),
                stats_by_arm={},
                context_features={"bucket": i % 2},
                timestamp=float(i),
                selection_nonce=str(i),
            )
            log.append(decision)
        events = log.events()
        rewards = {event.decision_id: 1.0 for event in events}
        estimate = evaluate_policy(
            events,
            rewards_by_decision=rewards,
            target_policy=lambda event: dict(event.probabilities),
            q_estimator=lambda event, action: 1.0,
        )
        self.assertEqual(estimate.count, len(events))
        self.assertAlmostEqual(estimate.ips, 1.0, places=7)
        self.assertAlmostEqual(estimate.self_normalized_ips, 1.0, places=7)
        self.assertAlmostEqual(estimate.doubly_robust, 1.0, places=7)

    def test_final_reward_map_ignores_proxy_and_dedupes_lineage_scopes(self):
        @dataclass
        class Reward:
            kind: str
            decision_id: str
            amount: float
            validation_closed: bool
            source_key: str = ""
            exposure_id: str = ""
            scope: str = ""

        result = final_reward_map(
            [
                Reward("PROXY", "d1", 100.0, False, "s1", "e1", "QUERY"),
                Reward("FINAL", "d1", 3.0, True, "s1", "e1", "ARTIFACT"),
                Reward("FINAL", "d1", 3.0, True, "s1", "e1", "QUERY"),
                Reward("FINAL", "d1", 2.0, True, "s2", "e2", "QUERY"),
            ]
        )
        self.assertEqual(result, {"d1": 5.0})

    def test_429_is_not_permanent_saturation(self):
        tracker = SaturationTracker(rate_limit_cooldown_seconds=10.0)
        status = tracker.observe(
            "root:a", FamilyObservation(timestamp=100.0, status_code=429)
        )
        self.assertEqual(status.state, SaturationState.COOLING)
        self.assertTrue(tracker.eligible("root:a", now=111.0))

    def test_429_does_not_clear_existing_semantic_saturation(self):
        tracker = SaturationTracker(
            minimum_samples=2, window_size=3, saturation_ttl_seconds=100.0,
            rate_limit_cooldown_seconds=5.0, low_final_threshold=0.1,
            duplicate_threshold=0.8, overlap_threshold=0.8,
        )
        for t in (1.0, 2.0):
            tracker.observe(
                "root:a",
                FamilyObservation(
                    timestamp=t, final_reward=0.0,
                    duplicate_ratio=0.95, overlap_ratio=0.95,
                ),
            )
        status = tracker.observe(
            "root:a", FamilyObservation(timestamp=3.0, status_code=429)
        )
        self.assertEqual(status.state, SaturationState.SATURATED)
        self.assertFalse(tracker.eligible("root:a", now=10.0))

    def test_low_final_duplicate_overlap_saturates_then_revisits(self):
        tracker = SaturationTracker(
            minimum_samples=3,
            window_size=5,
            saturation_ttl_seconds=10.0,
            low_final_threshold=0.1,
            duplicate_threshold=0.8,
            overlap_threshold=0.8,
        )
        for t in (1.0, 2.0, 3.0):
            status = tracker.observe(
                "root:a",
                FamilyObservation(
                    timestamp=t, final_reward=0.0,
                    duplicate_ratio=0.95, overlap_ratio=0.9,
                ),
            )
        self.assertEqual(status.state, SaturationState.SATURATED)
        self.assertFalse(tracker.eligible("root:a", now=5.0))
        self.assertTrue(tracker.eligible("root:a", now=14.0))

    def test_baseline_cutover_preserves_fact_objects(self):
        facts = [{"host": "a.example"}, {"host": "b.example"}]
        plan = plan_rebase(
            policy_baseline_version="baseline-v1",
            new_baseline_version="baseline-v2",
        )
        view = recompute_novelty_view(
            facts,
            is_novel=lambda fact: fact["host"].startswith("a"),
            reward_of=lambda fact: 2.0,
        )
        self.assertTrue(plan.stale_policy)
        self.assertTrue(plan.rebuild_required)
        self.assertTrue(plan.preserve_facts)
        self.assertIs(view[0].fact, facts[0])
        self.assertIs(view[1].fact, facts[1])
        self.assertEqual(view[0].derived_reward, 2.0)
        self.assertEqual(view[1].derived_reward, 0.0)

    def test_hierarchy_requires_matching_level(self):
        policy = self.make_policy()
        with self.assertRaises(ValueError):
            policy.probabilities(
                level=PolicyLevel.ROOT,
                candidates=[ActionCandidate("query:q", PolicyLevel.QUERY_FAMILY)],
                stats_by_arm={},
                now=0.0,
            )


if __name__ == "__main__":
    unittest.main()
