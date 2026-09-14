import tempfile
import unittest
from pathlib import Path

from creeper.evidence_cli import run_service
from creeper.evidence.worker import AsyncEvidenceWorker


class EvidenceServiceCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_once_mode_opens_empty_durable_queue_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = await run_service(
                Path(tmp),
                owner="test-worker",
                once=True,
                endpoint="https://example.invalid/cdx",
                claim_batch_size=4,
                lease_seconds=30.0,
                max_inflight=2,
                requests_per_second=0.0,
                max_connections=4,
                max_keepalive_connections=2,
                throttle_floor_seconds=0.0,
                timeout=1.0,
                max_retries=0,
                retry_base_seconds=1.0,
                retry_max_seconds=10.0,
                poll_min_seconds=0.01,
                poll_max_seconds=0.1,
            )

            self.assertEqual(report.claimed, 0)
            self.assertEqual(report.terminal, 0)
            self.assertTrue((Path(tmp) / "control.sqlite3").exists())
            self.assertTrue((Path(tmp) / "evidence.sqlite3").exists())

    async def test_retry_deadline_saturates_for_extreme_attempt_count(self):
        worker = AsyncEvidenceWorker.__new__(AsyncEvidenceWorker)
        worker.retry_base_seconds = 30.0
        worker.retry_max_seconds = 3600.0
        worker.clock = lambda: 100.0

        self.assertEqual(worker._retry_at(1_000_000), 3700.0)

    async def test_retry_deadline_rejects_invalid_attempt_and_clock(self):
        worker = AsyncEvidenceWorker.__new__(AsyncEvidenceWorker)
        worker.retry_base_seconds = 30.0
        worker.retry_max_seconds = 3600.0
        worker.clock = lambda: 100.0
        with self.assertRaisesRegex(ValueError, "attempt must be"):
            worker._retry_at(1.5)
        worker.clock = lambda: float("nan")
        with self.assertRaisesRegex(ValueError, "retry clock"):
            worker._retry_at(1)

    async def test_invalid_idle_backoff_is_rejected_before_worker_loop(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "poll bounds"):
                await run_service(
                    Path(tmp),
                    owner="test-worker",
                    once=True,
                    endpoint="https://example.invalid/cdx",
                    claim_batch_size=4,
                    lease_seconds=30.0,
                    max_inflight=2,
                    requests_per_second=0.0,
                    max_connections=4,
                    max_keepalive_connections=2,
                    throttle_floor_seconds=0.0,
                    timeout=1.0,
                    max_retries=0,
                    retry_base_seconds=1.0,
                    retry_max_seconds=10.0,
                    poll_min_seconds=1.0,
                    poll_max_seconds=0.5,
                )


if __name__ == "__main__":
    unittest.main()
