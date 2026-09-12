from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.value import InterpretableSourceValueModel
from creeper.storage.control_store import ControlStore


class SourceValueModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def source(name: str, *, family: str = "FAMILY") -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"https://archive.example/{name}",
            source_family=family,
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="TEST",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=100000,
            temporal_semantics_prior=0.8,
            enumerability_prior=0.9,
            direct_evidence_prior=0.5,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.5,
            adapter_cost_prior=0.5,
            confidence=0.8,
        )

    @staticmethod
    def measurement(eed: float) -> ScoutMeasurement:
        return ScoutMeasurement(
            sampled_records=100,
            unique_hosts=80,
            novel_hosts=20,
            direct_host_years=0,
            requests=1,
            bytes_read=1024,
            elapsed_seconds=2.0,
            novel_eed=eed,
        )

    def test_family_final_conversion_calibrates_future_source_value(self) -> None:
        calibrated = self.source("calibrated")
        target = self.source("target")
        for candidate in (calibrated, target):
            self.registry.register_proposal(candidate)
        self.registry.record_scout_measurement(
            calibrated.source_key,
            self.measurement(10.0),
        )
        self.registry.record_final_reward(
            calibrated.source_key,
            final_accepted_eed=5.0,
        )
        self.registry.record_scout_measurement(
            target.source_key,
            self.measurement(20.0),
        )

        estimate = InterpretableSourceValueModel(
            self.registry
        ).estimate(target)

        # One positive observation is intentionally shrunk by the
        # Beta(1,1) hurdle prior: P(success)=(1+1)/(1+2)=2/3, while the
        # positive final/scout conversion is 5/10=1/2.
        self.assertAlmostEqual(estimate.success_probability, 2 / 3)
        self.assertAlmostEqual(estimate.positive_conversion, 0.5)
        self.assertAlmostEqual(estimate.direct_value, 20.0 / 3.0)
        self.assertAlmostEqual(estimate.expected_final_eed, 20.0 / 3.0)
        self.assertGreater(estimate.uncertainty_bonus, 0.0)

    def test_gateway_receives_discounted_descendant_final_value(self) -> None:
        parent = SourceCandidate(
            canonical_entrypoint="https://archive.example/catalog/",
            source_family="RESOURCE_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="test",
            discovery_strategy="TEST",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=100000,
            enumerability_prior=0.9,
            confidence=0.8,
        )
        child = self.source("child", family="CHILD")
        self.registry.register_proposal(parent)
        self.registry.register_proposal(child)
        self.registry.add_edge(
            parent.source_key,
            child.source_key,
            relation="enumerates",
        )
        self.registry.record_final_reward(
            child.source_key,
            final_accepted_eed=4.0,
        )

        estimate = InterpretableSourceValueModel(
            self.registry,
            descendant_discount=0.5,
        ).estimate(parent)

        self.assertAlmostEqual(estimate.descendant_value, 2.0)
        self.assertGreater(estimate.expected_final_eed, 2.0)

    def test_overlap_penalty_reduces_marginal_score(self) -> None:
        source = self.source("overlap")
        self.registry.register_proposal(source)
        self.registry.record_scout_measurement(
            source.source_key,
            self.measurement(12.0),
        )
        model = InterpretableSourceValueModel(self.registry)

        clean = model.estimate(source, overlap_penalty=0.0)
        redundant = model.estimate(source, overlap_penalty=0.9)

        self.assertLess(redundant.score, clean.score)


if __name__ == "__main__":
    unittest.main()
