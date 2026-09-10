import asyncio
import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.worker import AsyncEvidenceWorker
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class FakeProvider:
    def __init__(self, *, state=CDXQueryState.EMPTY_EXHAUSTIVE, delay=0.0):
        self.state = state
        self.delay = delay
        self.active = 0
        self.max_active = 0
        self.keys = []

    async def query_key(self, key):
        self.keys.append(key)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            capsule = None
            if self.state is CDXQueryState.PASS:
                year = key.temporal_scope.year_from
                capsule = EvidenceCapsule(
                    hostname=key.hostname,
                    year=year,
                    provider=key.provider,
                    temporal_semantics="capture_timestamp_year",
                    evidence_timestamp=f"{year}0101000000",
                    source_locator=f"http://{key.hostname}/",
                    payload_hash="a" * 64,
                    policy_version=key.policy_version,
                )
            return EvidenceQueryResult(
                hostname=key.hostname,
                year=key.temporal_scope.year_from,
                state=self.state,
                capsule=capsule,
                key=key,
                error="retry me" if self.state is CDXQueryState.TRANSIENT_ERROR else None,
            )
        finally:
            self.active -= 1


class AsyncEvidenceWorkerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.now = 1_000.0
        self.control = ControlStore(root / "control.sqlite3", clock=lambda: self.now)
        self.evidence = EvidenceStore(root / "evidence.sqlite3")

    def tearDown(self):
        self.control.connection.close()
        self.evidence.connection.close()
        self.tmp.cleanup()

    @staticmethod
    def key(hostname, *, provider="wayback"):
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1997, 1997),
            provider,
            "cdx-v1",
        )

    async def test_worker_drains_preexisting_durable_backlog_with_bounded_inflight(self):
        keys = [self.key(f"host-{index}.example") for index in range(6)]
        self.assertEqual(self.control.enqueue_evidence_tasks(keys), 6)
        provider = FakeProvider(delay=0.01)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-a",
            claim_batch_size=6,
            provider_inflight={"wayback": 2},
        )

        report = await worker.run_once()

        self.assertEqual(report.claimed, 6)
        self.assertEqual(report.terminal, 6)
        self.assertEqual(report.retryable, 0)
        self.assertLessEqual(provider.max_active, 2)
        self.assertTrue(
            all(task.state == CDXQueryState.EMPTY_EXHAUSTIVE.value for task in self.control.list_evidence_tasks())
        )

    async def test_pass_capsules_are_batched_into_evidence_store(self):
        keys = [self.key("one.example"), self.key("two.example")]
        self.control.enqueue_evidence_tasks(keys)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": FakeProvider(state=CDXQueryState.PASS)},
            owner="worker-b",
            claim_batch_size=8,
        )

        report = await worker.run_until_idle()

        self.assertEqual(report.claimed, 2)
        self.assertEqual(report.inserted_capsules, 2)
        self.assertEqual(len(self.evidence.all_capsules()), 2)

    async def test_retryable_result_gets_durable_exponential_retry_time(self):
        key = self.key("retry.example")
        self.control.enqueue_evidence_tasks([key])
        provider = FakeProvider(state=CDXQueryState.TRANSIENT_ERROR)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-c",
            retry_base_seconds=30.0,
            retry_max_seconds=300.0,
            clock=lambda: self.now,
        )

        first = await worker.run_once()
        task = self.control.get_evidence_task(key)
        self.assertEqual(first.retryable, 1)
        self.assertIsNotNone(task)
        self.assertEqual(task.state, CDXQueryState.TRANSIENT_ERROR.value)
        self.assertEqual(task.retry_at, 1_030.0)

        # The persistent queue does not immediately reclaim future retry work.
        self.assertEqual((await worker.run_once()).claimed, 0)
        self.now = 1_031.0
        second = await worker.run_once()
        task = self.control.get_evidence_task(key)
        self.assertEqual(second.claimed, 1)
        self.assertEqual(task.retry_at, 1_091.0)

    async def test_missing_provider_is_operational_retry_not_evidence_invalid(self):
        key = self.key("missing.example", provider="arquivo")
        self.control.enqueue_evidence_tasks([key])
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": FakeProvider()},
            owner="worker-d",
            clock=lambda: self.now,
        )

        report = await worker.run_once()
        task = self.control.get_evidence_task(key)

        self.assertEqual(report.unknown_provider, 1)
        self.assertEqual(task.state, CDXQueryState.TRANSIENT_ERROR.value)
        self.assertGreater(task.retry_at, self.now)


if __name__ == "__main__":
    unittest.main()
