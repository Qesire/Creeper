import tempfile
import unittest
from pathlib import Path

from creeper.evidence_cli import run_service


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
                    timeout=1.0,
                    max_retries=0,
                    retry_base_seconds=1.0,
                    retry_max_seconds=10.0,
                    poll_min_seconds=1.0,
                    poll_max_seconds=0.5,
                )


if __name__ == "__main__":
    unittest.main()
