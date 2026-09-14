from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.intelligence import SourceIntelligenceContextBuilder
from creeper.source_discovery.manager import SearchDirective, SearchDirectiveKind
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class SourceIntelligenceContextTests(unittest.TestCase):
    def test_terminal_zero_yield_is_visible_to_next_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            try:
                candidate = SourceCandidate(
                    canonical_entrypoint="https://zero.example/catalog/",
                    source_family="ZERO_FAMILY",
                    level=SourceLevel.METASOURCE,
                    discovered_by="agent:test",
                    discovery_strategy="META_SOURCE_SEARCH",
                    expected_year_from=1996,
                    expected_year_to=2001,
                    expected_volume=80_000,
                    enumerability_prior=0.95,
                    confidence=0.9,
                )
                registry.register_proposal(candidate)
                registry.record_scout_measurement(
                    candidate.source_key,
                    ScoutMeasurement(
                        sampled_records=100,
                        unique_hosts=100,
                        novel_hosts=0,
                        direct_host_years=0,
                        requests=1,
                        bytes_read=4096,
                        elapsed_seconds=2.0,
                        novel_eed=0.0,
                        estimated_unseen_fraction=0.0,
                    ),
                )
                registry.transition(candidate.source_key, SourceState.REJECTED)

                context = SourceIntelligenceContextBuilder(registry).build(
                    SearchDirective(
                        kind=SearchDirectiveKind.REFILL_RESERVOIR,
                        strategy="META_SOURCE_SEARCH",
                        desired_candidates=1,
                        subject=None,
                        reason="test refill",
                    )
                )

                terminal = context["recent_terminal_sources"][0]
                self.assertEqual(terminal["origin"], "https://zero.example")
                self.assertEqual(terminal["measurement"]["novel_eed"], 0.0)
                self.assertEqual(terminal["measurement"]["observed"], 100)
                self.assertEqual(
                    terminal["measurement"]["measured_baseline_overlap"],
                    1.0,
                )
                self.assertIs(
                    context["constraints"][
                        "measured_zero_yield_is_negative_search_feedback"
                    ],
                    True,
                )
                self.assertIs(
                    context["constraints"][
                        "do_not_infer_hostname_value_from_page_count_alone"
                    ],
                    True,
                )
                memory = context["research_memory"]
                by_id = {item["lead_id"]: item for item in memory}
                self.assertEqual(
                    by_id["ucb-home-ip-1996-public-trace"]["status"],
                    "REJECT_IDENTITY_LOSS",
                )
                self.assertEqual(
                    by_id["nlanr-uc-20000714"]["status"],
                    "RECOVER_PUBLIC_MIRROR",
                )
                self.assertEqual(
                    by_id["nus-nlanr-sample"]["status"],
                    "HOLD_PROVENANCE",
                )
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
