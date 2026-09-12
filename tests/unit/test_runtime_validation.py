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
    def _readiness(
        root: Path,
        *,
        eed: str,
        baseline: str = "base-a",
        cursor: int = 0,
        latest: int = 0,
        sources: dict[str, dict[str, object]] | None = None,
        task_kinds: dict[str, dict[str, object]] | None = None,
    ) -> None:
        path = root / "readiness" / "readiness.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "novel_eed": eed,
                    "baseline_signature": baseline,
                    "model_signature": "model-a",
                    "confirmed_fraction_of_five_percent": "0.1",
                    "evidence_cursor": cursor,
                    "latest_evidence_sequence": latest,
                    "source_attribution": sources or {},
                    "task_kind_attribution": task_kinds or {},
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

            self._readiness(
                root,
                eed="10",
                cursor=1,
                latest=1,
                sources={
                    "source-a": {
                        "novel_host_years": 1,
                        "novel_eed": "10",
                    }
                },
                task_kinds={
                    "exact": {
                        "novel_host_years": 1,
                        "novel_eed": "5",
                    }
                },
            )
            start_validation_run(
                runtime_data_root=root,
                run_dir=run_dir,
                label="1k",
                target_source_records=1000,
                code_revision="abc123",
                clock=lambda: 100.0,
            )

            control = ControlStore(root / "control.sqlite3")
            try:
                exact_key = EvidenceQueryKey(
                    "exact-window.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "cdx-v1",
                )
                range_key = EvidenceQueryKey(
                    "range-window.example",
                    TemporalScope(1996, 1998),
                    "wayback",
                    "cdx-v1",
                )
                control.enqueue_evidence_tasks([exact_key, range_key])
                control.record_evidence_task_attempt_metric(
                    exact_key,
                    attempt=1,
                    state="pass",
                    provider_requests=4,
                    provider_elapsed_milliseconds=400,
                    pages_seen=1,
                    records_seen=2,
                )
                control.record_evidence_task_attempt_metric(
                    range_key,
                    attempt=1,
                    state="pass",
                    provider_requests=2,
                    provider_elapsed_milliseconds=200,
                    pages_seen=1,
                    records_seen=20,
                )
            finally:
                control.close()

            telemetry = RuntimeTelemetryStore(root / "telemetry.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            try:
                telemetry.add_counters(
                    {
                        "source_records": 1000,
                        "wayback_http_requests": 500,
                        "wayback_http_elapsed_ms": 2_500_000,
                        "wayback_latency_le_2s": 50,
                        "wayback_latency_le_4s": 150,
                        "wayback_latency_le_8s": 250,
                        "wayback_latency_le_16s": 50,
                        "wayback_throttle_responses": 5,
                        "wayback_http_429": 5,
                        "wayback_http_5xx": 8,
                        "wayback_transport_errors": 2,
                        "wayback_rate_limit_wait_ms": 1200,
                        "wayback_cooldown_wait_ms": 300,
                        "wayback_retry_backoff_wait_ms": 100,
                        "wayback_request_start_segments": 1,
                        "wayback_request_start_gaps": 400,
                        "wayback_request_start_gap_ms": 1_200_000,
                        "wayback_request_start_excess_gap_ms": 300_000,
                        "wayback_request_gap_le_2_5s": 100,
                        "wayback_request_gap_le_4s": 200,
                        "wayback_request_gap_le_8s": 80,
                        "wayback_request_gap_gt_16s": 20,
                        "evidence_stream_refill_claims": 25,
                        "evidence_stream_refill_tasks": 400,
                        "evidence_stream_refill_empty_claims": 2,
                        "evidence_host_lock_wait_ms": 40,
                        "evidence_provider_inflight_wait_ms": 50,
                        "evidence_claim_wait_ms": 60,
                        "evidence_poll_idle_ms": 70,
                        "evidence_claimed_tasks": 100,
                        "evidence_pass_results": 50,
                        "evidence_empty_exhaustive_results": 40,
                        "evidence_retryable_tasks": 10,
                    }
                )
                telemetry.set_gauges(
                    {"wayback_configured_requests_per_second": 0.5}
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

            self._readiness(
                root,
                eed="110",
                cursor=2,
                latest=2,
                sources={
                    "source-a": {
                        "novel_host_years": 2,
                        "novel_eed": "60",
                    },
                    "source-b": {
                        "novel_host_years": 1,
                        "novel_eed": "20",
                    },
                },
                task_kinds={
                    "exact": {
                        "novel_host_years": 2,
                        "novel_eed": "25",
                    },
                    "range": {
                        "novel_host_years": 2,
                        "novel_eed": "30",
                    },
                },
            )
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
            self.assertAlmostEqual(
                float(report["provider_request_starts_per_second"]),
                500 / 3600,
            )
            self.assertAlmostEqual(
                float(report["provider_pacing_utilization"]),
                (500 / 3600) / 0.5,
            )
            self.assertEqual(report["report_version"], "runtime-validation-report-v5")
            self.assertAlmostEqual(
                float(report["provider_active_request_starts_per_second"]),
                400 / 1200,
            )
            self.assertAlmostEqual(
                float(report["provider_active_pacing_utilization"]),
                (400 / 1200) / 0.5,
            )
            self.assertEqual(report["provider_request_stream_seconds"], "1200")
            self.assertEqual(report["provider_non_stream_seconds"], "2400.0")
            self.assertEqual(report["provider_request_stream_segments"], 1)
            self.assertEqual(
                report["provider_request_start_gap"],
                {
                    "count": 400,
                    "mean_seconds": "3",
                    "excess_seconds": "300",
                    "buckets": {
                        "gt_16s": 20,
                        "le_2_5s": 100,
                        "le_4s": 200,
                        "le_8s": 80,
                    },
                },
            )
            self.assertEqual(
                report["streaming_pump"],
                {
                    "refill_claims": 25,
                    "refill_tasks": 400,
                    "empty_refill_claims": 2,
                },
            )
            self.assertEqual(
                report["wait_state_milliseconds"],
                {
                    "wayback_rate_limit": 1200,
                    "wayback_cooldown": 300,
                    "wayback_retry_backoff": 100,
                    "host_lock": 40,
                    "provider_inflight": 50,
                    "claim": 60,
                    "poll_idle": 70,
                },
            )
            self.assertEqual(
                report["source_attribution"],
                {
                    "source-a": {
                        "novel_host_years_delta": 1,
                        "novel_eed_delta": "50",
                    },
                    "source-b": {
                        "novel_host_years_delta": 1,
                        "novel_eed_delta": "20",
                    },
                },
            )
            self.assertEqual(
                report["source_provider_request_deltas"],
                {"__unattributed__": 6},
            )
            self.assertEqual(
                report["source_yield"],
                {
                    "source-a": {
                        "provider_requests_delta": 0,
                        "novel_eed_delta": "50",
                        "novel_eed_per_1000_provider_requests": None,
                    },
                    "source-b": {
                        "provider_requests_delta": 0,
                        "novel_eed_delta": "20",
                        "novel_eed_per_1000_provider_requests": None,
                    },
                },
            )
            self.assertEqual(
                report["task_kind_attribution"],
                {
                    "exact": {
                        "novel_host_years_delta": 1,
                        "novel_eed_delta": "20",
                    },
                    "range": {
                        "novel_host_years_delta": 2,
                        "novel_eed_delta": "30",
                    },
                },
            )
            self.assertEqual(
                report["task_kind_yield"],
                {
                    "exact": {
                        "provider_requests_delta": 4,
                        "novel_host_years_delta": 1,
                        "novel_eed_delta": "20",
                        "novel_host_years_per_1000_provider_requests": "250",
                        "novel_eed_per_1000_provider_requests": "5000",
                    },
                    "range": {
                        "provider_requests_delta": 2,
                        "novel_host_years_delta": 2,
                        "novel_eed_delta": "30",
                        "novel_host_years_per_1000_provider_requests": "1000",
                        "novel_eed_per_1000_provider_requests": "15000",
                    },
                },
            )
            self.assertEqual(report["unattributed_provider_requests_delta"], 6)
            self.assertEqual(report["attributed_novel_eed_delta"], "70")
            self.assertEqual(report["unattributed_novel_eed_delta"], "30")
            self.assertEqual(report["target_source_records_progress"], "1")
            self.assertTrue(report["target_source_records_reached"])
            self.assertEqual(report["provider_429_fraction"], "0.01")
            self.assertEqual(report["provider_5xx_fraction"], "0.016")
            self.assertEqual(
                report["mean_provider_request_latency_seconds"],
                "5",
            )
            self.assertEqual(
                report["provider_latency_buckets"],
                {
                    "le_16s": 50,
                    "le_2s": 50,
                    "le_4s": 150,
                    "le_8s": 250,
                },
            )
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

    def test_start_rejects_readiness_backlog_to_prevent_false_yield(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "runtime"
            root.mkdir()
            RuntimeTelemetryStore(root / "telemetry.sqlite3").close()
            ControlStore(root / "control.sqlite3").close()
            evidence = EvidenceStore(root / "evidence.sqlite3")
            try:
                evidence.put(self._capsule("backlog.example", 1997, "a"))
            finally:
                evidence.close()
            self._readiness(root, eed="0", cursor=0, latest=1)

            with self.assertRaisesRegex(RuntimeError, "caught up"):
                start_validation_run(
                    runtime_data_root=root,
                    run_dir=root / "validation" / "lagging",
                    label="lagging",
                    clock=lambda: 10.0,
                )

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
