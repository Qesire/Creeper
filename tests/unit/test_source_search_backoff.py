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
from creeper.source_discovery.models import SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.unknown_format import make_unknown_format_reason
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
        reason = make_unknown_format_reason(
            (
                b'{"host":"a.example"}\n'
                b'{"host":"b.example"}\n'
                b'{"host":"c.example"}\n'
            ),
            content_type="application/octet-stream",
            truncated=False,
        )
        assert reason is not None
        blocker = SourceCandidate(
            canonical_entrypoint="https://data.example/opaque-records",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DETERMINISTIC_FIXTURE",
            expected_volume=100_000,
            enumerability_prior=0.9,
            confidence=0.8,
            state=SourceState.HOLD,
            state_reason=reason,
        )
        self.registry.register_proposal(blocker)

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
                query="successful adapter retry",
                actor="agent:test",
                search_cost_seconds=0.1,
                llm_episode_id="llm:backoff-adapter",
                llm_task_type=_directive.task_type.value,
                context_hash="fixture-context",
                prompt_version="source-intelligence-v2",
                adapter_proposals=(
                    {
                        "parser_kind": "jsonl",
                        "compression": "none",
                        "hostname_field": "host",
                        "timestamp_field": None,
                        "delimiter": None,
                    },
                ),
            )

        coordinator = self.coordinator(flaky_search)
        await coordinator.run_once()
        self.retry_now[0] += 31.0
        recovered = await coordinator.run_once()
        immediate = await coordinator.run_once()

        self.assertEqual(recovered.search_episodes, 1)
        self.assertEqual(recovered.adapter_bindings_applied, 1)
        self.assertEqual(coordinator._search_retry_deadlines, {})
        # A successful adapter binding resolves the blocker, so the next cycle
        # has no directive to retry and no transient backoff entry to skip.
        self.assertEqual(immediate.search_backoff_skipped, 0)
        self.assertEqual(calls, 2)


if __name__ == "__main__":
    unittest.main()
