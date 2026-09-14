from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.runtime.exposure import ProductionExposure, ProductionExposureState
from creeper.storage.control_store import ControlStore


RUNNING = "RUNNING"
READ_COMPLETE = "READ_COMPLETE"
VALIDATING = "VALIDATING"
FINAL_CLOSED = "FINAL_CLOSED"
ABORTED = "ABORTED"
EXPIRED = "EXPIRED"


class ProductionExposureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ControlStore(Path(self.tmp.name) / "control.sqlite3")

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_model_and_begin_reject_invalid_identity_clock_and_timeline(self) -> None:
        with self.assertRaisesRegex(ValueError, "source_records"):
            ProductionExposure(
                exposure_id="exp",
                source_key="source:a",
                reservoir_id="reservoir:a",
                lease_id="lease:a",
                task_id=None,
                lane="sequential",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
                source_records=1.5,
                source_requests=0,
                source_bytes=0,
                provider_requests=0,
                provider_bytes=0,
                source_elapsed_seconds=0.0,
                provider_elapsed_seconds=0.0,
                evidence_frontier=0,
                accepted_host_years=0,
                final_accepted_eed=0.0,
                state=ProductionExposureState.RUNNING,
                terminal_reason=None,
                created_at=2.0,
                updated_at=2.0,
                closed_at=None,
            )
        with self.assertRaisesRegex(ValueError, "updated_at"):
            ProductionExposure(
                exposure_id="exp",
                source_key="source:a",
                reservoir_id="reservoir:a",
                lease_id="lease:a",
                task_id=None,
                lane="sequential",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
                source_records=0,
                source_requests=0,
                source_bytes=0,
                provider_requests=0,
                provider_bytes=0,
                source_elapsed_seconds=0.0,
                provider_elapsed_seconds=0.0,
                evidence_frontier=0,
                accepted_host_years=0,
                final_accepted_eed=0.0,
                state=ProductionExposureState.RUNNING,
                terminal_reason=None,
                created_at=2.0,
                updated_at=1.0,
                closed_at=None,
            )

        self.store.clock = lambda: float("nan")
        with self.assertRaisesRegex(ValueError, "clock must be finite"):
            self.store.begin_production_exposure(
                source_key="source:a",
                reservoir_id="reservoir:a",
                lease_id="lease:a",
                lane="sequential",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
            )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM production_exposures"
            ).fetchone()[0],
            0,
        )

    def test_begin_rejects_non_string_identity_instead_of_coercing(self) -> None:
        with self.assertRaisesRegex(ValueError, "lease_id"):
            self.store.begin_production_exposure(
                source_key="source:a",
                reservoir_id="reservoir:a",
                lease_id=123,
                lane="sequential",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
            )

    def test_same_lane_identity_and_authority_reuses_one_exposure(self) -> None:
        first = self.store.begin_production_exposure(
            source_key="source:a",
            reservoir_id="reservoir:a",
            lease_id="lease:a",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        second = self.store.begin_production_exposure(
            source_key="source:a",
            reservoir_id="reservoir:a",
            lease_id="lease:a",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )

        self.assertEqual(first.exposure_id, second.exposure_id)
        self.assertEqual(first.state, RUNNING)
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM production_exposures"
            ).fetchone()[0],
            1,
        )

    def test_authority_mismatch_cannot_publish_final(self) -> None:
        exposure = self.store.begin_production_exposure(
            source_key="source:a",
            reservoir_id="reservoir:a",
            lease_id="lease:a",
            lane="historical_region",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=READ_COMPLETE,
            source_records=3,
            bytes_read=120,
            source_requests=1,
        )
        self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=VALIDATING,
            evidence_frontier=8,
        )

        self.assertFalse(
            self.store.finalize_production_exposure(
                exposure.exposure_id,
                final_accepted_eed=2.5,
                accepted_host_years=2,
                evidence_frontier=8,
                authority=("baseline:stale", "model:v1"),
            )
        )
        current = self.store.get_production_exposure(exposure.exposure_id)
        self.assertIsNotNone(current)
        assert current is not None
        self.assertEqual(current.state, VALIDATING)
        self.assertEqual(current.final_accepted_eed, 0.0)

    def test_progress_rejects_fractional_counts_and_nonfinite_elapsed(self) -> None:
        exposure = self.store.begin_production_exposure(
            source_key="source:finite",
            reservoir_id="reservoir:finite",
            lease_id="lease:finite",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        with self.assertRaisesRegex(ValueError, "count fields"):
            self.store.record_production_exposure_progress(
                exposure.exposure_id,
                state=READ_COMPLETE,
                source_records=1.5,
            )
        with self.assertRaisesRegex(ValueError, "finite and non-negative"):
            self.store.record_production_exposure_progress(
                exposure.exposure_id,
                state=VALIDATING,
                provider_elapsed_seconds=float("nan"),
            )

        stored = self.store.get_production_exposure(exposure.exposure_id)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state, RUNNING)
        self.assertEqual(stored.source_records, 0)
        self.assertEqual(stored.provider_elapsed_seconds, 0.0)

    def test_final_publication_rejects_nonfinite_or_fractional_counters(self) -> None:
        exposure = self.store.begin_production_exposure(
            source_key="source:finite-final",
            reservoir_id="reservoir:finite-final",
            lease_id="lease:finite-final",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=VALIDATING,
        )
        with self.assertRaisesRegex(ValueError, "final EED"):
            self.store.finalize_production_exposure(
                exposure.exposure_id,
                final_accepted_eed=float("nan"),
                accepted_host_years=0,
                evidence_frontier=0,
                authority=("baseline:v1", "model:v1"),
            )
        with self.assertRaisesRegex(ValueError, "final counts"):
            self.store.finalize_production_exposure(
                exposure.exposure_id,
                final_accepted_eed=0.0,
                accepted_host_years=1.5,
                evidence_frontier=0,
                authority=("baseline:v1", "model:v1"),
            )

    def test_final_publication_is_idempotent_and_authority_matched(self) -> None:
        exposure = self.store.begin_production_exposure(
            source_key="source:a",
            reservoir_id="reservoir:a",
            lease_id="lease:a",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=VALIDATING,
            provider_requests=2,
            provider_elapsed_seconds=1.5,
        )

        self.assertTrue(
            self.store.finalize_production_exposure(
                exposure.exposure_id,
                final_accepted_eed=4.0,
                accepted_host_years=3,
                evidence_frontier=10,
                authority=("baseline:v1", "model:v1"),
            )
        )
        self.assertTrue(
            self.store.finalize_production_exposure(
                exposure.exposure_id,
                final_accepted_eed=4.0,
                accepted_host_years=3,
                evidence_frontier=10,
                authority=("baseline:v1", "model:v1"),
            )
        )
        stored = self.store.get_production_exposure(exposure.exposure_id)
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored.state, FINAL_CLOSED)
        self.assertEqual(stored.final_accepted_eed, 4.0)
        self.assertEqual(stored.accepted_host_years, 3)

    def test_late_read_progress_does_not_regress_validation(self) -> None:
        exposure = self.store.begin_production_exposure(
            source_key="source:race",
            reservoir_id="reservoir:race",
            lease_id="lease:race",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=VALIDATING,
            evidence_frontier=4,
            accepted_host_years=2,
        )

        current = self.store.record_production_exposure_progress(
            exposure.exposure_id,
            state=READ_COMPLETE,
            source_records=10,
            source_bytes=100,
            source_requests=1,
        )

        self.assertEqual(current.state, VALIDATING)
        self.assertEqual(current.source_records, 10)
        self.assertEqual(current.source_bytes, 100)
        self.assertEqual(current.evidence_frontier, 4)
        self.assertEqual(current.accepted_host_years, 2)

    def test_abort_and_expire_are_terminal_without_reward(self) -> None:
        aborted = self.store.begin_production_exposure(
            source_key="source:abort",
            reservoir_id="reservoir:abort",
            lease_id="lease:abort",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )
        expired = self.store.begin_production_exposure(
            source_key="source:expire",
            reservoir_id="reservoir:expire",
            lease_id="lease:expire",
            lane="sequential",
            baseline_signature="baseline:v1",
            model_signature="model:v1",
        )

        self.assertTrue(
            self.store.abort_production_exposure(
                aborted.exposure_id,
                state=ABORTED,
                reason="provider failed",
            )
        )
        self.assertTrue(
            self.store.abort_production_exposure(
                expired.exposure_id,
                state=EXPIRED,
                reason="lease expired",
            )
        )
        for exposure_id, state in (
            (aborted.exposure_id, ABORTED),
            (expired.exposure_id, EXPIRED),
        ):
            current = self.store.get_production_exposure(exposure_id)
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(current.state, state)
            self.assertEqual(current.final_accepted_eed, 0.0)


if __name__ == "__main__":
    unittest.main()
