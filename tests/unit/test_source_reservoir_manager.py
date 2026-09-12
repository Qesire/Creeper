from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery import (
    ScoutMeasurement,
    SourceCandidate,
    SourceDiscoveryRegistry,
    SourceLevel,
    SourceState,
    SuppressionScope,
)
from creeper.source_discovery.manager import (
    SearchDirectiveKind,
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.storage.control_store import ControlStore


class SourceReservoirManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(
        name: str,
        *,
        family: str = "FAMILY",
        confidence: float = 0.5,
        direct_evidence_prior: float = 0.4,
        origin: str = "https://example.com",
    ):
        return SourceCandidate(
            canonical_entrypoint=f"{origin}/{name}/",
            source_family=family,
            level=SourceLevel.SOURCE,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_volume=1_000,
            temporal_semantics_prior=0.7,
            enumerability_prior=0.7,
            direct_evidence_prior=direct_evidence_prior,
            baseline_overlap_prior=0.4,
            confidence=confidence,
        )

    def to_scout_ready(self, candidate: SourceCandidate) -> None:
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)

    def to_warm(
        self,
        candidate: SourceCandidate,
        *,
        novel_eed: float,
        elapsed_seconds: float,
        direct_host_years: int = 0,
    ) -> None:
        self.to_scout_ready(candidate)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=100,
                unique_hosts=50,
                novel_hosts=20,
                direct_host_years=direct_host_years,
                requests=3,
                bytes_read=20_000,
                elapsed_seconds=elapsed_seconds,
                novel_eed=novel_eed,
            ),
        )
        self.registry.transition(candidate.source_key, SourceState.WARM)

    def test_activation_prefers_measured_novel_eed_rate_and_plan_is_pure(self) -> None:
        active = self.candidate("active")
        fast = self.candidate("fast")
        slow = self.candidate("slow")
        self.to_warm(active, novel_eed=2.0, elapsed_seconds=1.0)
        self.registry.transition(active.source_key, SourceState.ACTIVE)
        self.to_warm(fast, novel_eed=10.0, elapsed_seconds=2.0)
        self.to_warm(slow, novel_eed=4.0, elapsed_seconds=1.0)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=1,
                active_target=2,
                warm_min=0,
                warm_target=0,
                cold_min=0,
                cold_target=0,
            ),
        )

        plan = manager.plan()

        self.assertEqual(plan.activate_source_keys, (fast.source_key,))
        self.assertEqual(self.registry.get_candidate(fast.source_key).state, SourceState.WARM)
        self.assertFalse(plan.needs_search)

    def test_direct_bulk_without_volume_hint_scouts_before_generic_source(self) -> None:
        direct = SourceCandidate(
            canonical_entrypoint="https://archive.example/index.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="scrapy_sidecar",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
            expected_volume=None,
            temporal_semantics_prior=1.0,
            enumerability_prior=0.95,
            direct_evidence_prior=1.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.5,
            adapter_cost_prior=0.75,
            confidence=0.8,
        )
        generic = SourceCandidate(
            canonical_entrypoint="https://archive.example/huge-list.txt.gz",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="agent:test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_volume=10_000_000,
            temporal_semantics_prior=0.4,
            enumerability_prior=0.9,
            direct_evidence_prior=0.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.5,
            adapter_cost_prior=0.75,
            confidence=0.8,
        )
        self.to_scout_ready(generic)
        self.to_scout_ready(direct)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=0,
                cold_target=0,
                scout_parallelism=1,
            ),
        )

        plan = manager.plan()

        self.assertEqual(plan.scout_source_keys, (direct.source_key,))

    def test_existing_cold_reserve_is_consumed_before_agent_search(self) -> None:
        first = self.candidate("cold-a", confidence=0.9)
        second = self.candidate("cold-b", confidence=0.4)
        self.registry.register_proposal(first)
        self.registry.register_proposal(second)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=3,
                warm_target=5,
                cold_min=2,
                cold_target=4,
                triage_batch=1,
            ),
        )

        plan = manager.plan()

        self.assertFalse(plan.needs_search)
        self.assertEqual(plan.cold_count, 2)
        self.assertEqual(plan.triage_source_keys, (first.source_key,))

    def test_cold_deficit_emits_parallel_exploit_refill_and_exploration(self) -> None:
        proven = self.candidate("proven", family="HIGH_YIELD_FAMILY")
        self.to_warm(proven, novel_eed=20.0, elapsed_seconds=2.0)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=2,
                warm_target=4,
                cold_min=5,
                cold_target=9,
                max_search_directives=3,
            ),
        )

        plan = manager.plan()

        self.assertEqual(
            [directive.kind for directive in plan.search_directives],
            [
                SearchDirectiveKind.DIRECT_EVIDENCE,
                SearchDirectiveKind.EXPLOIT_SOURCE_FAMILY,
                SearchDirectiveKind.REFILL_RESERVOIR,
            ],
        )
        self.assertEqual(plan.search_directives[0].strategy, "DIRECT_EVIDENCE_BULK")
        self.assertEqual(plan.search_directives[1].subject, "HIGH_YIELD_FAMILY")
        self.assertEqual(plan.search_directives[1].strategy, "EXPLOIT_SUCCESS")
        self.assertEqual(plan.search_directives[2].strategy, "META_SOURCE_SEARCH")
        self.assertEqual(len({item.dedup_key for item in plan.search_directives}), 3)

    def test_cold_refill_exploits_proven_direct_origin(self) -> None:
        proven = self.candidate(
            "index.cdxj",
            family="BULK_ARTIFACT",
            direct_evidence_prior=1.0,
            origin="https://archive.example",
        )
        self.to_warm(
            proven,
            novel_eed=30.0,
            elapsed_seconds=2.0,
            direct_host_years=50,
        )
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=3,
                cold_target=6,
                max_search_directives=3,
            ),
        )

        plan = manager.plan()

        self.assertEqual(
            plan.search_directives[0].strategy,
            "DIRECT_EVIDENCE_BULK",
        )
        self.assertEqual(
            plan.search_directives[1].strategy,
            "EXPLOIT_DIRECT_ORIGIN",
        )
        self.assertEqual(
            plan.search_directives[1].subject,
            "https://archive.example",
        )

    def test_refill_uses_best_observed_search_strategy(self) -> None:
        episode = self.registry.begin_search_episode(
            strategy="RECOVERY",
            backend="web-search",
            query="known dead archive mirrors",
            actor="agent:recovery",
            episode_id="search:recovery",
        )
        self.registry.finish_search_episode(episode.episode_id, search_cost_seconds=2.0)
        self.registry.credit_search_episode(episode.episode_id, accepted_novel_eed=10.0)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=2,
                cold_target=4,
                max_search_directives=2,
            ),
        )

        plan = manager.plan()

        refill = next(
            item
            for item in plan.search_directives
            if item.kind is SearchDirectiveKind.REFILL_RESERVOIR
        )
        self.assertEqual(refill.strategy, "RECOVERY")

    def test_suppressed_candidates_do_not_satisfy_reserve_or_receive_work(self) -> None:
        candidate = self.candidate("suppressed", family="SATURATED")
        self.registry.register_proposal(candidate)
        self.registry.suppress_candidate(
            candidate,
            scope=SuppressionScope.FAMILY,
            reason="baseline saturated",
        )
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=1,
                cold_target=2,
            ),
        )

        plan = manager.plan()

        self.assertEqual(plan.cold_count, 0)
        self.assertEqual(plan.triage_source_keys, ())
        self.assertTrue(plan.needs_search)

    def test_scout_parallelism_subtracts_already_running_scouts(self) -> None:
        running = self.candidate("running")
        queued_a = self.candidate("queued-a", confidence=0.8)
        queued_b = self.candidate("queued-b", confidence=0.7)
        self.to_scout_ready(running)
        self.registry.transition(running.source_key, SourceState.SCOUTING)
        self.to_scout_ready(queued_a)
        self.to_scout_ready(queued_b)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=0,
                cold_target=0,
                scout_parallelism=2,
            ),
        )

        plan = manager.plan()

        self.assertEqual(len(plan.scout_source_keys), 1)
        self.assertEqual(plan.scout_source_keys[0], queued_a.source_key)


if __name__ == "__main__":
    unittest.main()
