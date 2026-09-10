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
)
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.storage.control_store import ControlStore


class SourceRefillBudgetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def _warm(self) -> None:
        candidate = SourceCandidate(
            canonical_entrypoint="https://example.com/proven/",
            source_family="PROVEN",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_volume=1000,
            confidence=0.8,
        )
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=100,
                unique_hosts=80,
                novel_hosts=20,
                direct_host_years=0,
                requests=2,
                bytes_read=1000,
                elapsed_seconds=1.0,
                novel_eed=10.0,
            ),
        )
        self.registry.transition(candidate.source_key, SourceState.WARM)

    def test_parallel_directives_share_one_cold_deficit(self) -> None:
        self._warm()
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=2,
                warm_target=2,
                cold_min=5,
                cold_target=4,
                max_search_directives=3,
            ),
        )

        plan = manager.plan()

        self.assertGreater(len(plan.search_directives), 1)
        self.assertEqual(
            sum(item.desired_candidates for item in plan.search_directives),
            manager.targets.cold_target - plan.cold_count,
        )

    def test_tiny_deficit_limits_number_of_parallel_searches(self) -> None:
        self._warm()
        for index in range(2):
            candidate = SourceCandidate(
                canonical_entrypoint=f"https://example.com/cold-{index}/",
                source_family="COLD",
                level=SourceLevel.SOURCE,
                discovered_by="test",
                discovery_strategy="META_SOURCE_SEARCH",
                expected_volume=100,
                confidence=0.5,
            )
            self.registry.register_proposal(candidate)
        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=0,
                active_target=0,
                warm_min=2,
                warm_target=2,
                cold_min=4,
                cold_target=3,
                max_search_directives=3,
            ),
        )

        plan = manager.plan()

        self.assertEqual(plan.cold_count, 2)
        self.assertEqual(len(plan.search_directives), 1)
        self.assertEqual(plan.search_directives[0].desired_candidates, 1)


if __name__ == "__main__":
    unittest.main()
