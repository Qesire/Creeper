import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    DomainEvidenceQueryResult,
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


class HostLockBlockingProvider(FakeProvider):
    def __init__(self, release_first: asyncio.Event, other_started: asyncio.Event):
        super().__init__(state=CDXQueryState.EMPTY_EXHAUSTIVE)
        self.release_first = release_first
        self.other_started = other_started

    async def query_key(self, key):
        if key.hostname == "aa.example" and key.temporal_scope.year_from == 1997:
            await self.release_first.wait()
        if key.hostname == "zz.example":
            self.other_started.set()
        return await super().query_key(key)



class StreamingRefillProvider(FakeProvider):
    def __init__(self, release_slow: asyncio.Event, refill_started: asyncio.Event):
        super().__init__(state=CDXQueryState.EMPTY_EXHAUSTIVE)
        self.release_slow = release_slow
        self.refill_started = refill_started

    async def query_key(self, key):
        if key.hostname == "aa-slow.example":
            await self.release_slow.wait()
        elif key.hostname == "cc-refill.example":
            self.refill_started.set()
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


class FakeDecomposedRangeProvider(FakeProvider):
    async def query_range(self, key):
        capsule = EvidenceCapsule(
            hostname=key.hostname,
            year=1997,
            provider=key.provider,
            temporal_semantics="capture_timestamp_year",
            evidence_timestamp="19970102030405",
            source_locator=f"http://{key.hostname}/",
            payload_hash="d" * 64,
            policy_version=key.policy_version,
            evidence_type="exact_host_cdx_capture",
            extraction_method="cdx_query_range_bounded",
        )
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.DECOMPOSED,
            candidate_years=(1997,),
            followup_years=(1996, 1998),
            capsules=(capsule,),
            pages_seen=1,
            records_seen=3,
            provider_requests=1,
            provider_elapsed_milliseconds=12,
        )


class FakeDomainProvider(FakeProvider):
    async def query_range(self, key):
        capsules = (
            EvidenceCapsule(
                hostname="example.com",
                year=1997,
                provider=key.provider,
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19970102030405",
                source_locator="http://example.com/",
                payload_hash="e" * 64,
                policy_version=key.policy_version,
                evidence_type="domain_scope_cdx_capture",
            ),
            EvidenceCapsule(
                hostname="a.example.com",
                year=1998,
                provider=key.provider,
                temporal_semantics="capture_timestamp_year",
                evidence_timestamp="19980102030405",
                source_locator="http://a.example.com/",
                payload_hash="f" * 64,
                policy_version=key.policy_version,
                evidence_type="domain_scope_cdx_capture",
            ),
        )
        return DomainEvidenceQueryResult(
            domain=key.hostname,
            key=key,
            state=CDXQueryState.DECOMPOSED,
            capsules=capsules,
            pages_seen=1,
            records_seen=20,
            provider_requests=1,
            provider_elapsed_milliseconds=10,
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


    async def test_durable_claim_prefers_wide_probe_and_host_diversity(self):
        keys = [
            EvidenceQueryKey(
                "same.example",
                TemporalScope(1997, 1997),
                "wayback",
                "cdx-v1",
            ),
            EvidenceQueryKey(
                "same.example",
                TemporalScope(1998, 1998),
                "wayback",
                "cdx-v1",
            ),
            EvidenceQueryKey(
                "other.example",
                TemporalScope(1997, 1997),
                "wayback",
                "cdx-v1",
            ),
            EvidenceQueryKey(
                "wide.example",
                TemporalScope(1996, 2001),
                "wayback",
                "cdx-v1",
            ),
        ]
        self.control.enqueue_evidence_tasks(keys)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": FakeProvider()},
            owner="worker-claim-priority",
            claim_batch_size=3,
        )

        claimed = worker.queue.claim(
            owner=worker.owner,
            limit=3,
            providers=worker.providers,
            lease_seconds=worker.lease_seconds,
        )

        self.assertEqual(
            [
                (
                    task.key.hostname,
                    task.key.temporal_scope.year_from,
                    task.key.temporal_scope.year_to,
                )
                for task in claimed
            ],
            [
                ("wide.example", 1996, 2001),
                ("other.example", 1997, 1997),
                ("same.example", 1997, 1997),
            ],
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


    async def test_streaming_refills_before_slow_tail_finishes(self):
        keys = [
            self.key("aa-slow.example"),
            self.key("bb-fast.example"),
            self.key("cc-refill.example"),
        ]
        self.control.enqueue_evidence_tasks(keys)
        release_slow = asyncio.Event()
        refill_started = asyncio.Event()
        stop = asyncio.Event()
        provider = StreamingRefillProvider(release_slow, refill_started)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-streaming-refill",
            claim_batch_size=2,
            provider_inflight={"wayback": 2},
        )
        reports = []

        async def consume():
            async for report in worker.run_streaming(
                stop_event=stop,
                refill_batch_size=1,
            ):
                reports.append(report)

        running = asyncio.create_task(consume())
        await asyncio.wait_for(refill_started.wait(), timeout=1.0)

        # The third durable task must have entered provider execution while the
        # slow member of the original two-task claim window is still blocked.
        self.assertFalse(release_slow.is_set())
        self.assertFalse(running.done())

        stop.set()
        release_slow.set()
        await asyncio.wait_for(running, timeout=1.0)

        self.assertEqual(sum(item.claimed for item in reports), 3)
        self.assertEqual(sum(item.terminal for item in reports), 3)
        self.assertGreaterEqual(worker.stream_refill_claims, 1)
        self.assertEqual(worker.stream_refill_tasks, 1)

    async def test_same_host_waiter_does_not_consume_provider_inflight_slot(self):
        keys = [
            EvidenceQueryKey(
                "aa.example",
                TemporalScope(1997, 1997),
                "wayback",
                "cdx-v1",
            ),
            EvidenceQueryKey(
                "aa.example",
                TemporalScope(1998, 1998),
                "wayback",
                "cdx-v1",
            ),
            EvidenceQueryKey(
                "zz.example",
                TemporalScope(1997, 1997),
                "wayback",
                "cdx-v1",
            ),
        ]
        self.control.enqueue_evidence_tasks(keys)
        release_first = asyncio.Event()
        other_started = asyncio.Event()
        provider = HostLockBlockingProvider(release_first, other_started)
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": provider},
            owner="worker-host-lock-capacity",
            claim_batch_size=3,
            provider_inflight={"wayback": 2},
        )

        running = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(other_started.wait(), timeout=1.0)
        self.assertFalse(running.done())

        release_first.set()
        report = await running
        self.assertEqual(report.terminal, 3)
        self.assertEqual(provider.max_active_by_host["aa.example"], 1)

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


    async def test_domain_task_commits_multiple_hostnames_without_negative_coverage(self):
        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1996, 2001),
            "wayback",
            "cdx-domain-v1",
        )
        self.control.enqueue_evidence_tasks([key])
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": FakeDomainProvider()},
            owner="worker-domain",
        )

        report = await worker.run_once()

        self.assertEqual(report.terminal, 1)
        self.assertEqual(report.decomposed_count, 1)
        self.assertEqual(report.inserted_capsules, 2)
        self.assertEqual(
            {
                (row.hostname, row.year)
                for row in self.evidence.iter_after(0, limit=10)
            },
            {("example.com", 1997), ("a.example.com", 1998)},
        )
        self.assertEqual(
            self.control.resolve_provider_coverage_masks(
                ["example.com"],
                provider="wayback",
                policy_version="cdx-domain-v1",
            )["example.com"],
            0,
        )
        kinds = self.control.resolve_host_year_task_kinds(
            [("example.com", 1997), ("a.example.com", 1998)]
        )
        self.assertEqual(set(kinds.values()), {"domain"})
        self.assertEqual(
            self.control.evidence_attempt_metric_summary()["domain"][
                "provider_requests"
            ],
            1,
        )

    async def test_decomposed_range_commits_positive_and_fans_out_missing_years(self):
        key = EvidenceQueryKey(
            "bounded-range.example",
            TemporalScope(1996, 1998),
            "wayback",
            "cdx-v1",
        )
        self.control.enqueue_evidence_tasks([key])
        worker = AsyncEvidenceWorker(
            control_store=self.control,
            evidence_store=self.evidence,
            providers={"wayback": FakeDecomposedRangeProvider()},
            owner="worker-bounded-range",
        )

        report = await worker.run_once()

        self.assertEqual(report.claimed, 1)
        self.assertEqual(report.terminal, 1)
        self.assertEqual(report.decomposed_count, 1)
        self.assertEqual(report.inserted_capsules, 1)
        self.assertEqual(
            tuple(c.year for c in self.evidence.for_hostname(key.hostname)),
            (1997,),
        )
        tasks = self.control.list_evidence_tasks()
        self.assertEqual(
            [
                (
                    item.key.temporal_scope.year_from,
                    item.key.temporal_scope.year_to,
                    item.state,
                )
                for item in tasks
            ],
            [
                (1996, 1996, CDXQueryState.PENDING.value),
                (1996, 1998, CDXQueryState.DECOMPOSED.value),
                (1998, 1998, CDXQueryState.PENDING.value),
            ],
        )
        metrics = self.control.evidence_attempt_metric_summary()
        self.assertEqual(metrics["range"]["attempts"], 1)
        self.assertEqual(metrics["range"]["provider_requests"], 1)
        self.assertEqual(metrics["range"]["pages_seen"], 1)

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
