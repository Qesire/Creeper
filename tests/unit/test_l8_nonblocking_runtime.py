from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.coordinator import (
    ScoutDisposition,
    ScoutResult,
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageDisposition,
    TriageResult,
)
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.source_discovery.research_trigger import ResearchTriggerSnapshot
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class L8NonblockingRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=0,
                cold_target=0,
                triage_batch=1,
                scout_parallelism=1,
                max_search_directives=1,
            ),
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    async def _triage(self, _candidate) -> TriageResult:
        return TriageResult(TriageDisposition.HOLD)

    async def _scout(self, _candidate) -> ScoutResult:
        return ScoutResult(ScoutDisposition.HOLD)

    async def _search(self, _directive) -> SearchBatch:
        return SearchBatch(backend="unused", query="unused", actor="unused")

    def _snapshot(self) -> ResearchTriggerSnapshot:
        return ResearchTriggerSnapshot(
            ready_minutes=0.0,
            context_hash="l8-test",
        )

    async def test_slow_child_never_delays_cycle_and_only_one_is_active(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        committed: list[object] = []

        async def slow_research(_directive):
            started.set()
            await release.wait()
            return {"proposal": "bounded"}

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager,
            lock_path=self.root / "coordinator.lock",
            triage_executor=self._triage,
            scout_executor=self._scout,
            search_executor=self._search,
            research_snapshot_provider=self._snapshot,
            research_executor=slow_research,
            research_result_committer=lambda _directive, value, _elapsed: committed.append(value),
        )

        first = await asyncio.wait_for(coordinator.run_once(), timeout=0.5)
        await asyncio.wait_for(started.wait(), timeout=0.5)
        self.assertEqual(first.research_started, 1)
        self.assertTrue(first.research_active)
        self.assertEqual(first.agent_hot_path_block_seconds, 0.0)

        second = await asyncio.wait_for(coordinator.run_once(), timeout=0.5)
        self.assertEqual(second.research_started, 0)
        self.assertEqual(second.research_suppressed, 1)
        self.assertTrue(second.research_active)

        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        third = await asyncio.wait_for(coordinator.run_once(), timeout=0.5)
        self.assertEqual(third.research_completed, 1)
        self.assertEqual(committed, [{"proposal": "bounded"}])
        self.assertEqual(third.research_started, 0)
        self.assertFalse(third.research_active)
        self.assertEqual(third.agent_hot_path_block_seconds, 0.0)

    async def test_operator_request_waits_for_deterministic_frontier(self) -> None:
        executable = 1
        release = asyncio.Event()

        def snapshot() -> ResearchTriggerSnapshot:
            return ResearchTriggerSnapshot(
                ready_minutes=60.0,
                executable_regions=executable,
                context_hash=f"operator:{executable}",
            )

        async def research(_directive):
            await release.wait()
            return {"proposal": "operator"}

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager,
            lock_path=self.root / "coordinator.lock",
            triage_executor=self._triage,
            scout_executor=self._scout,
            search_executor=self._search,
            research_snapshot_provider=snapshot,
            research_executor=research,
            research_result_committer=lambda *_args: None,
        )
        coordinator.request_research_once(subject="https://example.invalid/root")

        first = await coordinator.run_once()
        self.assertEqual(first.research_started, 0)
        self.assertFalse(first.research_active)

        executable = 0
        second = await coordinator.run_once()
        self.assertEqual(second.research_started, 1)
        self.assertTrue(second.research_active)

        release.set()
        await coordinator.shutdown()

    async def test_child_failure_is_recorded_and_runtime_stays_alive(self) -> None:
        failures: list[str] = []

        async def failing_research(_directive):
            raise RuntimeError("synthetic research failure")

        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            self.manager,
            lock_path=self.root / "coordinator.lock",
            triage_executor=self._triage,
            scout_executor=self._scout,
            search_executor=self._search,
            research_snapshot_provider=self._snapshot,
            research_executor=failing_research,
            research_failure_recorder=(
                lambda _directive, error, _elapsed: failures.append(str(error))
            ),
        )

        first = await coordinator.run_once()
        self.assertEqual(first.research_started, 1)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        second = await coordinator.run_once()
        self.assertEqual(second.research_failures, 1)
        self.assertEqual(failures, ["synthetic research failure"])
        self.assertEqual(second.research_started, 0)
        self.assertFalse(second.research_active)

        # A failed child must not poison subsequent deterministic coordinator cycles.
        third = await coordinator.run_once()
        self.assertEqual(third.research_started, 0)
        self.assertEqual(third.agent_hot_path_block_seconds, 0.0)


if __name__ == "__main__":
    unittest.main()
