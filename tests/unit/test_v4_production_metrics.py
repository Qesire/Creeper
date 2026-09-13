from __future__ import annotations

import unittest

from creeper.metrics.validation import build_validation_report


class V4ProductionMetricTests(unittest.TestCase):
    @staticmethod
    def _snapshot(
        *,
        timestamp: float,
        eed: str,
        wayback_requests: int,
        task_kinds: dict[str, dict[str, object]],
        cursor: int,
    ) -> dict[str, object]:
        return {
            "timestamp_unix": timestamp,
            "telemetry_counters": {
                "wayback_http_requests": wayback_requests,
                "source_records": cursor,
            },
            "telemetry_gauges": {},
            "readiness": {
                "novel_eed": eed,
                "baseline_signature": "v4-baseline",
                "model_signature": "v4-model",
                "evidence_cursor": cursor,
                "latest_evidence_sequence": cursor,
                "source_attribution": {},
                "task_kind_attribution": task_kinds,
            },
            "evidence_attempt_metrics": {},
            "source_provider_requests": {},
            "evidence_action_value_state": {},
            "source_learning_state": {},
            "runtime_tree_bytes": 0,
            "tracked_state_bytes": 0,
        }

    def test_canary_reports_direct_fraction_and_wayback_eed_per_request(self) -> None:
        start = self._snapshot(
            timestamp=100.0,
            eed="0",
            wayback_requests=0,
            task_kinds={},
            cursor=0,
        )
        end = self._snapshot(
            timestamp=3700.0,
            eed="100",
            wayback_requests=10,
            task_kinds={
                "direct": {
                    "novel_host_years": 8,
                    "novel_eed": "80",
                },
                "exact": {
                    "novel_host_years": 1,
                    "novel_eed": "10",
                },
                "range": {
                    "novel_host_years": 1,
                    "novel_eed": "5",
                },
                "domain": {
                    "novel_host_years": 1,
                    "novel_eed": "5",
                },
            },
            cursor=1,
        )

        report = build_validation_report(start=start, end=end)

        self.assertEqual(report["novel_eed_per_hour"], "100")
        self.assertEqual(report["direct_final_eed_fraction"], "0.8")
        self.assertEqual(report["wayback_novel_eed_per_request"], "2")


if __name__ == "__main__":
    unittest.main()
