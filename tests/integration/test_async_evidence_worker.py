import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
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
        self.active_by_host = {}
        self.max_active_by_host = {}
        self.keys = []

    async def query_key(self, key):
        self.keys.append(key)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.active_by_host[key.hostname] = self.active_by_host.get(key.hostname, 0) + 1
        self.max_active_by_host[key.hostname] = max(
            self.max_active_by_host.get(key.hostname, 0),
            self.active_by_host[key.hostname],
        )
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
            self.active_by_host[key.hostname] -= 1


class SelectiveDelayProvider(FakeProvider):
    def __init__(self, release_slow: asyncio.Event, fast_done: asyncio.Event):
        super().__init__(state=CDXQueryState.EMPTY_EXHAUSTIVE)
        self.release_slow = release_slow
        self.fast_done = fast_done

    async def query_key(self, key):
        if key.hostname == "slow.example":
            await self.release_slow.wait()
        else:
            self.fast_done.set()
        return await super().query_key(key)


class FakeRangeProvider(FakeProvider):
    def __init__(self):
        super().__init__(state=CDXQueryState.PASS)
        self.range_keys = []

    async def query_range(self, key):
        self.range_keys.append(key)
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.PASS,
            candidate_years=(1997, 1999),
        )


class FakeRangeCapsuleProvider(FakeProvider):
    def __init__(self, *, state=CDXQueryState.PASS):
        super().__init__(state=state)
        self.range_keys = []

    async def query_range(self, key):
        self.range_keys.append(key)
        capsules = (
            EvidenceCapsule(
                hostname=key.hostname,
                year=1997,
                provider=key.provider,
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19970102030405",
                source_locator=f"http://{key.hostname}/",
                payload_hash="b" * 64,
                policy_version=key.policy_version,
                evidence_type="exact_host_cdx_capture",
                extraction_method="cdx_query_range",
            ),
            EvidenceCapsule(
                hostname=key.hostname,
                year=1999,
                provider=key.provider,
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19990102030405",
                source_locator=f"http://{key.hostname}/",
                payload_hash="c" * 64,
                policy_version=key.policy_version,
                evidence_type="exact_host_cdx_capture",
                extraction_method="cdx_query_range",
            ),
        )
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=self.state,
            candidate_years=(1997, 1999) if self.state is CDXQueryState.PASS else (),
            capsules=capsules,
            error="retry range" if self.state is CDXQueryState.TRANSIENT_ERROR else None,
        )


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

    async def test_fast_result_is_terminal_before_slow_batch_tail_finishes(self):
        fast = self.key("fast.example")
        slow = self.key("slow.example")
        self.control.enqueue_evidence_tasks([fast, slow])
        release_slow = asyncio.Event()
        fast_done = asyncio.Event()
        provider = SelectiveDelayProvider(release_slow, fast_done)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-incremental-finish",
            claim_batch_size=2,
            provider_inflight={"wayback": 2},
        )

        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(fast_done.wait(), timeout=1.0)
        for _ in range(20):
            await asyncio.sleep(0)
            fast_task = self.control.get_evidence_task(fast)
            if (
                fast_task is not None
                and fast_task.state == CDXQueryState.EMPTY_EXHAUSTIVE.value
            ):
                break

        fast_task = self.control.get_evidence_task(fast)
        slow_task = self.control.get_evidence_task(slow)
        self.assertIsNotNone(fast_task)
        self.assertIsNotNone(slow_task)
        self.assertEqual(
            fast_task.state,
            CDXQueryState.EMPTY_EXHAUSTIVE.value,
        )
        self.assertIsNone(fast_task.lease_owner)
        self.assertEqual(slow_task.lease_owner, "worker-incremental-finish")
        self.assertFalse(running.done())

        release_slow.set()
        report = await running
        self.assertEqual(report.terminal, 2)

    async def test_same_hostname_is_serialized_while_other_hosts_run_concurrently(self):
        keys = [
            EvidenceQueryKey("same.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"),
            EvidenceQueryKey("same.example", TemporalScope(1998, 1998), "wayback", "cdx-v1"),
            EvidenceQueryKey("other.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"),
        ]
        self.control.enqueue_evidence_tasks(keys)
        provider = FakeProvider(delay=0.03)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-host-lock",
            claim_batch_size=3,
            provider_inflight={"wayback": 3},
        )

        report = await worker.run_once()

        self.assertEqual(report.terminal, 3)
        self.assertEqual(provider.max_active_by_host["same.example"], 1)
        self.assertGreaterEqual(provider.max_active, 2)

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

        self.assertEqual((await worker.run_once()).claimed, 0)
        self.now = 1_031.0
        second = await worker.run_once()
        task = self.control.get_evidence_task(key)
        self.assertEqual(second.claimed, 1)
        self.assertEqual(task.retry_at, 1_091.0)

    async def test_unconfigured_provider_remains_unclaimed_for_its_own_worker(self):
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

        self.assertEqual(report.claimed, 0)
        self.assertEqual(report.unknown_provider, 0)
        self.assertEqual(task.state, CDXQueryState.PENDING.value)
        self.assertIsNone(task.lease_owner)

    async def test_visibility_is_renewed_while_slow_provider_is_in_flight(self):
        # Use a real monotonic wall-clock store so the heartbeat can extend the
        # visibility deadline during this short integration test.
        root = Path(self.tmp.name)
        self.control.close()
        self.control = ControlStore(root / "heartbeat.sqlite3", clock=time.time)
        key = self.key("slow.example")
        self.control.enqueue_evidence_tasks([key])
        provider = FakeProvider(delay=0.12)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-heartbeat",
            claim_batch_size=1,
            lease_seconds=0.09,
            heartbeat_interval=0.03,
        )

        task = asyncio.create_task(worker.run_once())
        await asyncio.sleep(0.07)
        visible = self.control.get_evidence_task(key)
        self.assertEqual(visible.lease_owner, "worker-heartbeat")
        self.assertGreater(visible.lease_until, time.time())
        report = await task
        self.assertEqual(report.terminal, 1)

    async def test_range_positive_capsules_avoid_exact_year_requery(self):
        key = EvidenceQueryKey(
            "range-capsule.example", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([key])
        provider = FakeRangeCapsuleProvider()
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-range-capsules",
        )

        report = await worker.run_once()

        self.assertEqual(report.claimed, 1)
        self.assertEqual(report.terminal, 1)
        self.assertEqual(report.inserted_capsules, 2)
        self.assertEqual(tuple(c.year for c in self.evidence.for_hostname(key.hostname)), (1997, 1999))
        tasks = self.control.list_evidence_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].key, key)
        self.assertEqual(tasks[0].state, CDXQueryState.PASS.value)

    async def test_retryable_range_commits_positive_capsules_but_keeps_parent_retryable(self):
        key = EvidenceQueryKey(
            "partial-range.example", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([key])
        provider = FakeRangeCapsuleProvider(state=CDXQueryState.TRANSIENT_ERROR)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-range-partial",
            retry_base_seconds=10.0,
            retry_max_seconds=10.0,
            clock=lambda: self.now,
        )

        report = await worker.run_once()

        self.assertEqual(report.retryable, 1)
        self.assertEqual(report.inserted_capsules, 2)
        task = self.control.get_evidence_task(key)
        self.assertEqual(task.state, CDXQueryState.TRANSIENT_ERROR.value)
        self.assertEqual(task.retry_at, self.now + 10.0)
        self.assertEqual(tuple(c.year for c in self.evidence.for_hostname(key.hostname)), (1997, 1999))

    async def test_range_task_fans_out_exact_year_tasks_without_writing_capsules(self):
        key = EvidenceQueryKey(
            "range.example", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([key])
        provider = FakeRangeProvider()
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-range",
        )

        report = await worker.run_once()

        self.assertEqual(report.claimed, 1)
        self.assertEqual(report.terminal, 1)
        self.assertEqual(report.inserted_capsules, 0)
        self.assertEqual(provider.range_keys, [key])
        followups = self.control.list_evidence_tasks()
        self.assertEqual(
            [(item.key.temporal_scope.year_from, item.key.temporal_scope.year_to) for item in followups],
            [(1996, 2000), (1997, 1997), (1999, 1999)],
        )
        exact_report = await worker.run_until_idle()
        self.assertEqual(exact_report.claimed, 2)
        self.assertEqual(exact_report.inserted_capsules, 2)
        self.assertEqual(len(self.evidence.all_capsules()), 2)


if __name__ == "__main__":
    unittest.main()
