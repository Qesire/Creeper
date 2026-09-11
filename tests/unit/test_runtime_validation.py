from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey, TemporalScope
from creeper.metrics.validation import finish_validation_run, start_validation_run
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


class RuntimeValidationTests(unittest.TestCase):
    @staticmethod
    def _readiness(root: Path, *, eed: str, baseline: str = "base-a") -> None:
        path = root / "readiness" / "readiness.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "novel_eed": eed,
                    "baseline_signature": baseline,
                    "model_signature": "model-a",
                    "confirmed_fraction_of_five_percent": "0.1",
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _capsule(hostname: str, year: int, payload: str) -> EvidenceCapsule:
        return EvidenceCapsule(
            hostname,
            year,
            "wayback",
            "capture_timestamp_year",
            f"{year}0101000000",
            f"http://{hostname}/",
            payload * 64,
            "evidence-v1",
        )

    def test_finish_computes_windowed_competition_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            root.mkdir()
            run_dir = root / "validation" / "1k"

            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            telemetry = RuntimeTelemetryStore(root / "telemetry.sqlite3")
            try:
                control.enqueue_evidence_tasks(
                    [
                        EvidenceQueryKey(
                            "pending.example",
                            TemporalScope(1997, 1997),
                            "wayback",
                            "cdx-v1",
                        )
                    ]
                )
                evidence.put(self._capsule("before.example", 1997, "a"))
                telemetry.add_counters(
                    {
                        "source_records": 100,
                        "wayback_http_requests": 200,
                        "evidence_claimed_tasks": 50,
                        "evidence_pass_results": 10,
                    }
                )
            finally:
                telemetry.close()
                evidence.close()
                control.close()

            self._readiness(root, eed="10")
            start_validation_run(
                runtime_data_root=root,
                run_dir=run_dir,
                label="1k",
                target_source_records=1000,
                code_revision="abc123",
                clock=lambda: 100.0,
            )

            telemetry = RuntimeTelemetryStore(root / "telemetry.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            try:
                telemetry.add_counters(
                    {
                        "source_records": 1000,
                        "wayback_http_requests": 500,
                        "wayback_throttle_responses": 5,
                        "wayback_http_429": 5,
                        "wayback_http_5xx": 8,
                        "wayback_transport_errors": 2,
                        "evidence_claimed_tasks": 100,
                        "evidence_pass_results": 50,
                        "evidence_empty_exhaustive_results": 40,
                        "evidence_retryable_tasks": 10,
                    }
                )
                telemetry.append_resource_sample(
                    rss_bytes=200,
                    disk_free_bytes=900,
                    governor_state="normal",
                    sampled_at=200.0,
                )
                telemetry.append_resource_sample(
                    rss_bytes=300,
                    disk_free_bytes=700,
                    governor_state="throttled",
                    sampled_at=300.0,
                )
                evidence.put(self._capsule("after.example", 1998, "b"))
            finally:
                evidence.close()
                telemetry.close()

            self._readiness(root, eed="110")
            (root / "spool.bin").write_bytes(b"x" * 1024)

            report = finish_validation_run(
                runtime_data_root=root,
                run_dir=run_dir,
                clock=lambda: 3700.0,
            )

            self.assertTrue(report["valid_for_throughput"])
            self.assertEqual(report["novel_eed_delta"], "100")
            self.assertEqual(report["novel_eed_per_hour"], "100")
            self.assertEqual(
                report["novel_eed_per_1000_provider_requests"],
                "200",
            )
            self.assertEqual(report["target_source_records_progress"], "1")
            self.assertTrue(report["target_source_records_reached"])
            self.assertEqual(report["provider_429_fraction"], "0.01")
            self.assertEqual(report["provider_5xx_fraction"], "0.016")
            self.assertEqual(
                report["resource_window"]["peak_rss_bytes"],
                300,
            )
            self.assertEqual(
                report["resource_window"]["min_disk_free_bytes"],
                700,
            )
            self.assertGreater(
                report["storage"]["runtime_tree_growth_bytes"],
                0,
            )
            self.assertEqual(
                report["end_backlog"]["evidence_task_states"]["pending"],
                1,
            )
            self.assertTrue((run_dir / "start.json").is_file())
            self.assertTrue((run_dir / "end.json").is_file())
            self.assertTrue((run_dir / "report.json").is_file())

    def test_authority_change_invalidates_throughput_rate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            root.mkdir()
            RuntimeTelemetryStore(root / "telemetry.sqlite3").close()
            ControlStore(root / "control.sqlite3").close()
            EvidenceStore(root / "evidence.sqlite3").close()
            self._readiness(root, eed="10", baseline="base-a")
            run_dir = root / "validation" / "authority-change"
            start_validation_run(
                runtime_data_root=root,
                run_dir=run_dir,
                label="authority-change",
                clock=lambda: 10.0,
            )

            self._readiness(root, eed="50", baseline="base-b")
            report = finish_validation_run(
                runtime_data_root=root,
                run_dir=run_dir,
                clock=lambda: 20.0,
            )

            self.assertFalse(report["valid_for_throughput"])
            self.assertTrue(report["authority_changed"])
            self.assertIsNone(report["novel_eed_delta"])
            self.assertIsNone(report["novel_eed_per_hour"])
            self.assertTrue(
                any(
                    "authority changed" in reason
                    for reason in report["invalid_reasons"]
                )
            )


if __name__ == "__main__":
    unittest.main()
