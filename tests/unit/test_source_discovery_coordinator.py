from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.coordinator import (
    CoordinatorBusyError,
    ScoutDisposition,
    ScoutResult,
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageDisposition,
    TriageResult,
)
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class SourceDiscoveryCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.lock_path = self.root / "source-discovery.lock"

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(name: str, *, strategy: str = "META_SOURCE_SEARCH") -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://example.com/{name}/",
            source_family="TEST_FAMILY",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy=strategy,
            expected_volume=1000,
            confidence=0.7,
        )

    def to_scout_ready(self, candidate: SourceCandidate) -> None:
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)

    @staticmethod
    def measurement() -> ScoutMeasurement:
        return ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=20,
            direct_host_years=4,
            requests=3,
            bytes_read=4096,
            elapsed_seconds=1.0,
            novel_eed=8.0,
        )

    def manager(self, **overrides) -> SourceReservoirManager:
        values = dict(
            active_min=0,
            active_target=0,
            warm_min=0,
            warm_target=0,
            cold_min=0,
            cold_target=0,
            triage_batch=8,
            scout_parallelism=4,
            max_search_directives=3,
        )
        values.update(overrides)
        return SourceReservoirManager(self.registry, targets=SourcePoolTargets(**values))

    async def test_search_triage_and_scout_io_overlap_but_commits_complete_serially(self) -> None:
        discovered = self.candidate("triage")
        self.registry.register_proposal(discovered)
        scout = self.candidate("scout")
        self.to_scout_ready(scout)

        labels: set[str] = set()
        all_stages_entered = asyncio.Event()

        async def gate(label: str) -> None:
            labels.add(label)
            if {"triage", "scout", "search"}.issubset(labels):
                all_stages_entered.set()
            await asyncio.wait_for(all_stages_entered.wait(), timeout=1.0)

        async def triage_executor(_candidate: SourceCandidate) -> TriageResult:
            await gate("triage")
            return TriageResult(TriageDisposition.SCOUT)

        async def scout_executor(_candidate: SourceCandidate) -> ScoutResult:
            await gate("scout")
            return ScoutResult(ScoutDisposition.WARM, self.measurement())

        async def search_executor(directive) -> SearchBatch:
            await gate("search")
            found = SourceCandidate(
                canonical_entrypoint=f"https://search.example/{directive.strategy.lower()}/",
                source_family="SEARCHED",
                level=SourceLevel.COLLECTION,
                discovered_by="untrusted-agent-label",
                discovery_strategy="UNTRUSTED",
                expected_volume=5000,
                confidence=0.6,
            )
            return SearchBatch(
                backend="test-search",
                query=f"query:{directive.strategy}",
                actor="agent:test",
                candidates=(found,),
                search_cost_seconds=0.25,
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=3, cold_target=5),
            lock_path=self.lock_path,
            triage_executor=triage_executor,
            scout_executor=scout_executor,
            search_executor=search_executor,
        )

        report = await coordinator.run_once()

        self.assertTrue({"triage", "scout", "search"}.issubset(labels))
        self.assertEqual(
            self.registry.get_candidate(discovered.source_key).state,
            SourceState.SCOUT_READY,
        )
        self.assertEqual(self.registry.get_candidate(scout.source_key).state, SourceState.WARM)
        self.assertEqual(report.triaged_to_scout, 1)
        self.assertEqual(report.scouted_warm, 1)
        self.assertGreaterEqual(report.search_episodes, 1)
        searched = [
            item
            for item in self.registry.list_candidates(state=SourceState.DISCOVERED)
            if item.canonical_entrypoint.startswith("https://search.example/")
        ]
        self.assertGreaterEqual(len(searched), 1)
        self.assertEqual(searched[0].discovered_by, "agent:test")

    async def test_posix_flock_rejects_second_local_coordinator(self) -> None:
        candidate = self.candidate("blocking-triage")
        self.registry.register_proposal(candidate)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocking_triage(_candidate: SourceCandidate) -> TriageResult:
            entered.set()
            await release.wait()
            return TriageResult(TriageDisposition.SCOUT)

        async def unused_scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(ScoutDisposition.HOLD)

        async def unused_search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        manager = self.manager()
        first = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.lock_path,
            triage_executor=blocking_triage,
            scout_executor=unused_scout,
            search_executor=unused_search,
        )
        second = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.lock_path,
            triage_executor=blocking_triage,
            scout_executor=unused_scout,
            search_executor=unused_search,
        )

        running = asyncio.create_task(first.run_once())
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        with self.assertRaises(CoordinatorBusyError):
            await second.run_once()
        release.set()
        await asyncio.wait_for(running, timeout=1.0)

    async def test_startup_recovers_stranded_scout_with_retry_suppression(self) -> None:
        candidate = self.candidate("stranded")
        self.to_scout_ready(candidate)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise AssertionError("suppressed recovered scout must not run in same cycle")

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
            failure_retry_seconds=60.0,
        )

        report = await coordinator.run_once()

        recovered = self.registry.get_candidate(candidate.source_key)
        self.assertEqual(report.recovered_scouts, 1)
        self.assertEqual(recovered.state, SourceState.SCOUT_READY)
        self.assertIsNotNone(self.registry.suppression_reason(recovered))

    async def test_scout_exception_returns_claim_to_retryable_scout_ready(self) -> None:
        candidate = self.candidate("transient")
        self.to_scout_ready(candidate)

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            raise RuntimeError("temporary provider failure")

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
            failure_retry_seconds=60.0,
        )

        report = await coordinator.run_once()

        retried = self.registry.get_candidate(candidate.source_key)
        self.assertEqual(report.scout_failures, 1)
        self.assertEqual(retried.state, SourceState.SCOUT_READY)
        self.assertIn("temporary provider failure", self.registry.suppression_reason(retried) or "")

    async def test_scout_children_are_committed_after_io_with_dedup_and_lineage(self) -> None:
        parent = self.candidate("parent")
        self.to_scout_ready(parent)
        child = SourceCandidate(
            canonical_entrypoint="https://resources.example/catalog/",
            source_family="RESOURCE_CATALOG",
            level=SourceLevel.COLLECTION,
            discovered_by="scrapy_sidecar",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
            confidence=0.8,
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(
                ScoutDisposition.HOLD,
                discovered_candidates=(child, child, parent),
                edge_relation="links_to_resource",
            )

        async def search(_directive) -> SearchBatch:
            return SearchBatch(backend="test", query="unused", actor="test")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(self.registry.get_candidate(parent.source_key).state, SourceState.HOLD)
        stored_child = self.registry.get_candidate(child.source_key)
        self.assertIsNotNone(stored_child)
        self.assertEqual(stored_child.state, SourceState.DISCOVERED)
        self.assertEqual(self.registry.children(parent.source_key), [child.source_key])
        self.assertEqual(report.scout_children_registered, 1)
        self.assertEqual(report.scout_edges_added, 1)
        self.assertEqual(report.scout_children_dropped, 2)

    async def test_search_executor_cannot_overfill_directive_budget(self) -> None:
        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(ScoutDisposition.HOLD)

        async def search(directive) -> SearchBatch:
            candidates = tuple(self.candidate(f"found-{index}") for index in range(4))
            return SearchBatch(
                backend="test-search",
                query=f"query:{directive.strategy}",
                actor="agent:test",
                candidates=candidates,
                search_cost_seconds=1.0,
            )

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager(cold_min=1, cold_target=1),
            lock_path=self.lock_path,
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search,
        )

        report = await coordinator.run_once()

        self.assertEqual(report.search_candidates_registered, 1)
        self.assertEqual(report.search_candidates_dropped, 3)
        self.assertEqual(len(self.registry.list_candidates(state=SourceState.DISCOVERED)), 1)


if __name__ == "__main__":
    unittest.main()
