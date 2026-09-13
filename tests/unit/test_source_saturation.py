from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
    SuppressionScope,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.saturation import (
    SaturationPolicy,
    SourceSaturationController,
)
from creeper.storage.control_store import ControlStore


class _Clock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class SourceSaturationControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = _Clock()
        self.control = ControlStore(Path(self.tmp.name) / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(
            self.control,
            clock=self.clock,
        )
        self.registry.set_scout_authority(
            baseline_signature="baseline-v4",
            model_signature="eed-v4",
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    def _candidate(
        self,
        origin: str,
        index: int,
        *,
        source_family: str = "BULK_ARTIFACT",
        direct_evidence_prior: float = 0.0,
    ) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=f"{origin}/shard-{index:05d}.csv",
            source_family=source_family,
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=100,
            temporal_semantics_prior=0.5,
            enumerability_prior=1.0,
            direct_evidence_prior=direct_evidence_prior,
            baseline_overlap_prior=0.5,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        )

    def _complete_scout(
        self,
        candidate: SourceCandidate,
        *,
        novel_eed: float = 0.0,
        unique_hosts: int = 10,
    ) -> SourceCandidate:
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=10,
                unique_hosts=unique_hosts,
                novel_hosts=0 if novel_eed == 0 else 1,
                direct_host_years=0,
                requests=1,
                bytes_read=128,
                elapsed_seconds=0.1,
                novel_eed=novel_eed,
            ),
        )
        return self.registry.transition(candidate.source_key, SourceState.HOLD)

    def _measure_origin(
        self,
        origin: str,
        count: int,
        *,
        positive_index: int | None = None,
    ) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        for index in range(count):
            candidate = self._candidate(origin, index)
            candidates.append(
                self._complete_scout(
                    candidate,
                    novel_eed=1.0 if index == positive_index else 0.0,
                )
            )
        return candidates

    def test_31_zero_yield_siblings_do_not_suppress_origin(self) -> None:
        origin = "https://data.labs.loc.gov"
        self._measure_origin(origin, 31)
        controller = SourceSaturationController(self.registry)

        decision = controller.evaluate(origin)

        self.assertEqual(decision.measured_sources, 31)
        self.assertEqual(decision.positive_sources, 0)
        self.assertFalse(decision.should_suppress)
        self.assertFalse(controller.apply(decision))
        row = self.control.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM source_suppressions
            WHERE scope_type = ?
            """,
            (SuppressionScope.ORIGIN.value,),
        ).fetchone()
        self.assertEqual(int(row["n"]), 0)

    def test_32_zero_yield_siblings_suppress_origin(self) -> None:
        origin = "https://data.labs.loc.gov"
        siblings = self._measure_origin(origin, 32)
        controller = SourceSaturationController(self.registry)

        decision = controller.evaluate(origin)
        changed = controller.apply(decision)

        self.assertTrue(decision.should_suppress)
        self.assertEqual(decision.measured_sources, 32)
        self.assertEqual(decision.positive_sources, 0)
        self.assertEqual(decision.total_novel_eed, 0.0)
        self.assertTrue(changed)
        reason = self.registry.suppression_reason(siblings[0])
        self.assertIsNotNone(reason)
        self.assertIn("measured=32", reason)
        self.assertIn("positive=0", reason)
        self.assertIn("novel_eed=0", reason)

    def test_one_positive_sibling_blocks_zero_yield_suppression(self) -> None:
        origin = "https://data.labs.loc.gov"
        self._measure_origin(origin, 32, positive_index=31)
        controller = SourceSaturationController(self.registry)

        decision = controller.evaluate(origin)

        self.assertEqual(decision.measured_sources, 32)
        self.assertEqual(decision.positive_sources, 1)
        self.assertEqual(decision.total_novel_eed, 1.0)
        self.assertFalse(decision.should_suppress)
        self.assertFalse(controller.apply(decision))

    def test_origin_suppression_does_not_suppress_unrelated_origin(self) -> None:
        loc_origin = "https://data.labs.loc.gov"
        loc = self._measure_origin(loc_origin, 32)
        arquivo = self._candidate("https://arquivo.pt", 1)
        self.registry.register_proposal(arquivo)
        controller = SourceSaturationController(self.registry)

        self.assertTrue(controller.apply(controller.evaluate(loc_origin)))

        self.assertIsNotNone(self.registry.suppression_reason(loc[0]))
        self.assertIsNone(self.registry.suppression_reason(arquivo))

    def test_generic_bulk_artifact_family_remains_unsuppressed(self) -> None:
        loc_origin = "https://data.labs.loc.gov"
        self._measure_origin(loc_origin, 32)
        arquivo = self._candidate("https://arquivo.pt", 1)
        self.registry.register_proposal(arquivo)
        controller = SourceSaturationController(self.registry)

        controller.apply(controller.evaluate(loc_origin))

        family_row = self.control.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM source_suppressions
            WHERE scope_type = ? AND scope_key = ?
            """,
            (SuppressionScope.FAMILY.value, "BULK_ARTIFACT"),
        ).fetchone()
        self.assertEqual(int(family_row["n"]), 0)
        self.assertIsNone(self.registry.suppression_reason(arquivo))

    def test_repeated_apply_is_idempotent_and_does_not_refresh_ttl(self) -> None:
        origin = "https://data.labs.loc.gov"
        self._measure_origin(origin, 32)
        controller = SourceSaturationController(
            self.registry,
            policy=SaturationPolicy(suppression_ttl_seconds=60.0),
        )
        decision = controller.evaluate(origin)

        self.assertTrue(controller.apply(decision))
        first = self.control.connection.execute(
            """
            SELECT reason, created_at, expires_at
            FROM source_suppressions
            WHERE scope_type = ? AND scope_key = ?
            """,
            (SuppressionScope.ORIGIN.value, origin),
        ).fetchone()
        self.clock.advance(10.0)
        self.assertFalse(controller.apply(decision))
        second = self.control.connection.execute(
            """
            SELECT reason, created_at, expires_at
            FROM source_suppressions
            WHERE scope_type = ? AND scope_key = ?
            """,
            (SuppressionScope.ORIGIN.value, origin),
        ).fetchone()
        count = self.control.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM source_suppressions
            WHERE scope_type = ? AND scope_key = ?
            """,
            (SuppressionScope.ORIGIN.value, origin),
        ).fetchone()

        self.assertEqual(dict(first), dict(second))
        self.assertEqual(int(count["n"]), 1)

    def test_existing_suppression_reason_observes_origin_saturation(self) -> None:
        origin = "https://data.labs.loc.gov"
        siblings = self._measure_origin(origin, 32)
        controller = SourceSaturationController(self.registry)

        decision = controller.evaluate(origin)
        controller.apply(decision)

        self.assertEqual(
            self.registry.suppression_reason(siblings[-1]),
            decision.reason,
        )

    def test_expired_finite_ttl_becomes_schedulable_again(self) -> None:
        origin = "https://data.labs.loc.gov"
        siblings = self._measure_origin(origin, 32)
        controller = SourceSaturationController(
            self.registry,
            policy=SaturationPolicy(suppression_ttl_seconds=10.0),
        )

        controller.apply(controller.evaluate(origin))
        self.assertIsNotNone(self.registry.suppression_reason(siblings[0]))
        self.clock.advance(10.001)

        self.assertIsNone(self.registry.suppression_reason(siblings[0]))

    def test_controller_does_not_mutate_candidate_or_scout_proof_fields(self) -> None:
        origin = "https://data.labs.loc.gov"
        candidates: list[SourceCandidate] = []
        for index in range(32):
            candidate = self._candidate(
                origin,
                index,
                direct_evidence_prior=0.9,
            )
            candidates.append(self._complete_scout(candidate))
        target = candidates[0]
        candidate_before = self.registry.get_candidate(target.source_key)
        metric_before = self.control.connection.execute(
            """
            SELECT *
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (target.source_key,),
        ).fetchone()
        controller = SourceSaturationController(self.registry)

        controller.apply(controller.evaluate(origin))

        candidate_after = self.registry.get_candidate(target.source_key)
        metric_after = self.control.connection.execute(
            """
            SELECT *
            FROM source_scout_metrics
            WHERE source_key = ?
            """,
            (target.source_key,),
        ).fetchone()
        self.assertEqual(candidate_before, candidate_after)
        self.assertEqual(dict(metric_before), dict(metric_after))
        self.assertEqual(candidate_after.direct_evidence_prior, 0.9)

    def test_stale_measurements_do_not_count_toward_saturation(self) -> None:
        origin = "https://data.labs.loc.gov"
        self._measure_origin(origin, 32)
        self.registry.set_scout_authority(
            baseline_signature="baseline-v5",
            model_signature="eed-v4",
        )
        controller = SourceSaturationController(self.registry)

        decision = controller.evaluate(origin)

        self.assertEqual(decision.measured_sources, 0)
        self.assertFalse(decision.should_suppress)

    def test_run_evaluates_multiple_origins_but_suppresses_only_zero_class(self) -> None:
        zero_origin = "https://data.labs.loc.gov"
        positive_origin = "https://positive.example"
        self._measure_origin(zero_origin, 32)
        self._measure_origin(positive_origin, 32, positive_index=0)
        controller = SourceSaturationController(self.registry)

        decisions = controller.run()

        by_origin = {item.origin: item for item in decisions}
        self.assertTrue(by_origin[zero_origin].should_suppress)
        self.assertFalse(by_origin[positive_origin].should_suppress)
        rows = self.control.connection.execute(
            """
            SELECT scope_key
            FROM source_suppressions
            WHERE scope_type = ?
            ORDER BY scope_key
            """,
            (SuppressionScope.ORIGIN.value,),
        ).fetchall()
        self.assertEqual(
            [str(row["scope_key"]) for row in rows],
            [zero_origin],
        )


if __name__ == "__main__":
    unittest.main()
