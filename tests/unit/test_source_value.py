from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
)
from creeper.source_discovery.production_value import ProductionValueModel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.value import InterpretableSourceValueModel
from creeper.storage.control_store import ControlStore


class FinalProductionValueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.registry.set_scout_authority(
            baseline_signature="baseline-v5",
            model_signature="model-v5",
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def candidate(
        self,
        name: str,
        *,
        family: str = "BULK_ARTIFACT",
        scout_eed: float = 1.0,
    ) -> SourceCandidate:
        candidate = SourceCandidate(
            canonical_entrypoint=f"https://example.com/{name}.cdxj",
            source_family=family,
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="test",
            expected_volume=1000,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.1,
            adapter_cost_prior=0.1,
            confidence=1.0,
        )
        self.registry.register_proposal(candidate)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=100,
                unique_hosts=80,
                novel_hosts=20,
                direct_host_years=20,
                requests=1,
                bytes_read=1024,
                elapsed_seconds=1.0,
                novel_eed=scout_eed,
            ),
        )
        return candidate

    def close_run(
        self,
        candidate: SourceCandidate,
        lease_id: str,
        *,
        final_eed: float,
        read_seconds: float,
        provider_seconds: float = 0.0,
        source_requests: int = 1,
        provider_requests: int = 0,
    ) -> None:
        reservoir_id = f"reservoir:{candidate.source_key[-12:]}"
        self.registry.begin_source_run(
            candidate.source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature="baseline-v5",
            model_signature="model-v5",
            read_started=100.0,
        )
        self.registry.record_source_run_read(
            candidate.source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature="baseline-v5",
            model_signature="model-v5",
            source_records=100,
            bytes_read=4096,
            source_requests=source_requests,
            read_finished=100.0 + read_seconds,
            read_complete=True,
        )
        self.registry.record_source_run_validation(
            candidate.source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature="baseline-v5",
            model_signature="model-v5",
            evidence_tasks_created=provider_requests,
            evidence_tasks_terminal=provider_requests,
            direct_capsules_committed=int(final_eed > 0),
            provider_requests=provider_requests,
            provider_elapsed_seconds=provider_seconds,
            accepted_host_years=int(final_eed > 0),
            final_accepted_eed=final_eed,
            max_evidence_sequence=0,
            validation_complete=True,
        )
        self.assertTrue(
            self.registry.close_source_run(
                candidate.source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
                baseline_signature="baseline-v5",
                model_signature="model-v5",
            )
        )

    def test_explicit_zero_is_durable_closed_outcome(self) -> None:
        candidate = self.candidate("zero", scout_eed=20.0)
        self.close_run(
            candidate,
            "lease-zero",
            final_eed=0.0,
            read_seconds=10.0,
        )

        rows = self.registry.list_source_run_outcomes(
            candidate.source_key,
            closed_only=True,
        )
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].closed)
        self.assertEqual(rows[0].final_accepted_eed, 0.0)

        projection = self.registry.connection.execute(
            """
            SELECT final_accepted_eed, closed_runs, zero_runs
            FROM source_final_rewards WHERE source_key = ?
            """,
            (candidate.source_key,),
        ).fetchone()
        self.assertEqual(float(projection["final_accepted_eed"]), 0.0)
        self.assertEqual(int(projection["closed_runs"]), 1)
        self.assertEqual(int(projection["zero_runs"]), 1)

    def test_final_value_demotes_scout_high_zero_source(self) -> None:
        source_a = self.candidate("source-a", scout_eed=100.0)
        source_b = self.candidate("source-b", scout_eed=1.0)
        for index in range(3):
            self.close_run(
                source_a,
                f"lease-a-{index}",
                final_eed=0.0,
                read_seconds=100.0,
                provider_seconds=20.0,
                provider_requests=5,
            )
        self.close_run(
            source_b,
            "lease-b-0",
            final_eed=2.0,
            read_seconds=1.0,
            provider_requests=1,
        )

        model = ProductionValueModel(self.registry)
        value_a = model.estimate(source_a)
        value_b = model.estimate(source_b)

        self.assertGreater(value_b.score, value_a.score)
        self.assertEqual(value_a.zero_runs, 3)
        self.assertGreaterEqual(value_a.score, value_a.exploration_floor)
        self.assertGreater(value_a.exploration_floor, 0.0)

    def test_new_source_retains_bounded_exploration(self) -> None:
        source = self.candidate("new-source", scout_eed=0.0)
        value = ProductionValueModel(self.registry).estimate(source)

        self.assertEqual(value.closed_runs, 0)
        self.assertGreater(value.uncertainty_bonus, 0.0)
        self.assertGreaterEqual(value.score, value.exploration_floor)
        self.assertGreater(value.score, 0.0)

    def test_recent_positive_run_recovers_weak_source(self) -> None:
        source = self.candidate("recover", scout_eed=1.0)
        for index in range(3):
            self.close_run(
                source,
                f"lease-zero-{index}",
                final_eed=0.0,
                read_seconds=20.0,
            )
        model = ProductionValueModel(
            self.registry,
            ewma_alpha=0.8,
        )
        weak = model.estimate(source)

        self.close_run(
            source,
            "lease-positive",
            final_eed=4.0,
            read_seconds=1.0,
        )
        recovered = model.estimate(source)

        self.assertGreater(recovered.score, weak.score)
        self.assertGreater(recovered.recent_marginal_eed_per_second, 0.0)

    def test_authority_change_ignores_old_learning_but_keeps_audit_rows(self) -> None:
        source = self.candidate("authority", scout_eed=5.0)
        self.close_run(
            source,
            "lease-old",
            final_eed=3.0,
            read_seconds=1.0,
        )
        before = ProductionValueModel(self.registry).estimate(source)
        self.assertEqual(before.closed_runs, 1)

        self.registry.set_scout_authority(
            baseline_signature="baseline-v5-new",
            model_signature="model-v5-new",
        )
        after = ProductionValueModel(self.registry).estimate(source)

        self.assertEqual(after.closed_runs, 0)
        audit = self.registry.list_source_run_outcomes(source.source_key)
        self.assertEqual(len(audit), 1)
        self.assertEqual(audit[0].baseline_signature, "baseline-v5")
        self.assertTrue(audit[0].closed)

    def test_family_model_counts_zero_runs_as_failures(self) -> None:
        source_a = self.candidate(
            "family-zero",
            family="FAMILY_X",
            scout_eed=10.0,
        )
        source_b = self.candidate(
            "family-positive",
            family="FAMILY_X",
            scout_eed=10.0,
        )
        self.close_run(
            source_a,
            "lease-family-zero",
            final_eed=0.0,
            read_seconds=1.0,
        )
        self.close_run(
            source_b,
            "lease-family-positive",
            final_eed=5.0,
            read_seconds=1.0,
        )

        estimate = InterpretableSourceValueModel(self.registry).estimate(source_a)

        # One success and one explicit zero under Beta(1,1).
        self.assertAlmostEqual(estimate.success_probability, 0.5)

    def test_recent_rates_are_exposure_normalized(self) -> None:
        source = self.candidate("normalized", scout_eed=100.0)
        self.close_run(
            source,
            "lease-fast",
            final_eed=1.0,
            read_seconds=1.0,
            source_requests=1,
        )
        self.close_run(
            source,
            "lease-slow",
            final_eed=1.0,
            read_seconds=9.0,
            source_requests=9,
        )

        estimate = ProductionValueModel(
            self.registry,
            recent_window=2,
            ewma_alpha=0.5,
        ).estimate(source)

        self.assertAlmostEqual(
            estimate.recent_marginal_eed_per_second,
            0.5 * 1.0 + 0.5 * (1.0 / 9.0),
        )
        self.assertAlmostEqual(
            estimate.recent_marginal_eed_per_request,
            0.5 * 1.0 + 0.5 * (1.0 / 9.0),
        )


if __name__ == "__main__":
    unittest.main()
