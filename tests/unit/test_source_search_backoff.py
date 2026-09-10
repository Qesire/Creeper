from __future__ import annotations

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
from creeper.source_discovery.models import SourceCandidate
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class SourceSearchBackoffTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.retry_now = [100.0]

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def coordinator(self, search_executor) -> SourceDiscoveryCoordinator:
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=0,
                warm_target=0,
                cold_min=1,
                cold_target=1,
                triage_batch=1,
                scout_parallelism=1,
                max_search_directives=1,
            ),
            search_cooldown_seconds=0.0,
        )

        async def triage(_candidate: SourceCandidate) -> TriageResult:
            return TriageResult(TriageDisposition.SCOUT)

        async def scout(_candidate: SourceCandidate) -> ScoutResult:
            return ScoutResult(ScoutDisposition.HOLD)

        return SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.root / "coordinator.lock",
            triage_executor=triage,
            scout_executor=scout,
            search_executor=search_executor,
            triage_parallelism=1,
            scout_parallelism=1,
            search_parallelism=1,
            failure_retry_seconds=30.0,
            retry_clock=lambda: self.retry_now[0],
        )

    async def test_failed_directive_is_skipped_until_monotonic_deadline(self) -> None:
        calls = 0

        async def failing_search(_directive) -> SearchBatch:
            nonlocal calls
            calls += 1
            raise RuntimeError("provider temporarily unavailable")

        coordinator = self.coordinator(failing_search)

        first = await coordinator.run_once()
        second = await coordinator.run_once()
        self.retry_now[0] += 31.0
        third = await coordinator.run_once()

        self.assertEqual(calls, 2)
        self.assertEqual(first.search_failures, 1)
        self.assertEqual(first.search_backoff_skipped, 0)
        self.assertEqual(second.search_failures, 0)
        self.assertEqual(second.search_backoff_skipped, 1)
        self.assertEqual(third.search_failures, 1)
        self.assertEqual(third.search_backoff_skipped, 0)

    async def test_success_after_backoff_clears_transient_deadline(self) -> None:
        calls = 0

        async def flaky_search(_directive) -> SearchBatch:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary failure")
            return SearchBatch(
                backend="test",
                query="successful retry",
                actor="test",
                candidates=(),
                search_cost_seconds=0.1,
            )

        coordinator = self.coordinator(flaky_search)
        await coordinator.run_once()
        self.retry_now[0] += 31.0
        recovered = await coordinator.run_once()
        immediate = await coordinator.run_once()

        self.assertEqual(recovered.search_episodes, 1)
        # Manager cooldown is disabled in this focused test, so a successful
        # invocation clearing transient state permits the next directive again.
        self.assertEqual(immediate.search_backoff_skipped, 0)
        self.assertEqual(calls, 3)


if __name__ == "__main__":
    unittest.main()
