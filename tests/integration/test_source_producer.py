import tempfile
import threading
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey, TemporalScope
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.source_producer import SourceProducer
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.candidate_store import CandidateStore
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class FakeSource:
    adapter_id = "fixture-source"

    def __init__(self, records):
        self.records = tuple(records)
        self.executions = 0

    def execute(self, lease: WorkLease):
        self.executions += 1
        selected = self.records[: lease.max_records]
        return iter(selected), LeaseResult(
            lease_id=lease.lease_id,
            records=len(selected),
            requests=1,
            bytes_read=sum(len(record.payload) for record in selected),
            elapsed_seconds=0.001,
            next_cursor=None,
        )

    def extract_hosts(self, record: SourceRecord):
        yield HostObservation(
            hostname=record.payload,
            source_id=record.source_id,
            locator=record.locator,
            scope=record.scope,
            source_year=record.source_year,
            source_time=record.source_time,
            record_type=record.record_type,
            artifact_ref=record.artifact_ref,
            direct_year_mask=record.direct_year_mask,
            year_hint_mask=record.year_hint_mask,
        )


class SourceProducerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        task_root = self.root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(task_root, self.root / "baseline.sqlite3")
        self.control = ControlStore(self.root / "control.sqlite3")
        self.evidence = EvidenceStore(self.root / "evidence.sqlite3")

    def tearDown(self):
        self.evidence.close()
        self.control.close()
        self.baseline.close()
        self.tmp.cleanup()

    def build_runtime(
        self,
        *,
        backlog_capacity: int,
        expected_tasks: int = 1,
        reservation_tasks: int | None = None,
        range_first_fraction: float = 0.0,
        source_key: str | None = None,
        source_registry=None,
    ):
        record = SourceRecord(
            source_id="fixture-source",
            locator="fixture://1",
            payload="novel.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=1997,
        )
        adapter = FakeSource([record])
        domain = SourceDomain(
            domain_id="fixture-domain",
            family="LOCAL_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="fixture-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://records",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start="0",
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
            expected_evidence_tasks=expected_tasks,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 1, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=expected_tasks,
            reservation_evidence_tasks=reservation_tasks,
            source_key=source_key,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            source_registry=source_registry,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": backlog_capacity},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            range_first_fraction=range_first_fraction,
        )
        return runtime, adapter

    def build_direct_runtime(
        self,
        *,
        backlog_capacities: dict[str, int],
        include_fallback_hint: bool = False,
        owner: str = "direct-producer",
        control_store: ControlStore | None = None,
    ):
        control = self.control if control_store is None else control_store
        direct_bit = 1 << (1997 - 1996)
        hint_bit = (1 << (1998 - 1996)) if include_fallback_hint else 0
        record = SourceRecord(
            source_id="direct-fixture",
            locator="fixture://direct/1",
            payload="direct-novel.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=1997,
            source_time="19970101000000",
            record_type="CDX_CAPTURE",
            artifact_ref="fixture://direct/1",
            direct_year_mask=direct_bit,
            year_hint_mask=hint_bit,
        )
        adapter = FakeSource([record])
        domain = SourceDomain(
            domain_id="direct-isolation-domain",
            family="DIRECT_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="direct-isolation-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://direct-isolation",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        control.save_domain(domain)
        control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start="0",
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
            # Deliberately stale/overstated: direct production must ignore
            # provider reservation estimates from compatibility callers.
            expected_evidence_tasks=100,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 100, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_mode="direct_year",
            evidence_provider="wayback",
            expected_evidence_tasks=100,
            reservation_evidence_tasks=100,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 0})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities=backlog_capacities,
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            owner=owner,
        )
        return runtime, adapter, candidate

    def test_source_run_lifecycle_is_registered_for_final_reward_reconciliation(self):
        calls = []

        class Registry:
            current_scout_authority = ("baseline-v4", "eed-v4")

            def begin_source_run(self, source_key, **kwargs):
                calls.append(("begin", source_key, kwargs))

            def record_source_run_read(self, source_key, **kwargs):
                calls.append(("read", source_key, kwargs))

        runtime, _adapter = self.build_runtime(
            backlog_capacity=1,
            source_key="src:fixture",
            source_registry=Registry(),
        )

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual([call[0] for call in calls], ["begin", "read"])
        self.assertEqual(calls[0][1], "src:fixture")
        self.assertEqual(calls[0][2]["baseline_signature"], "baseline-v4")
        self.assertEqual(calls[0][2]["model_signature"], "eed-v4")
        self.assertEqual(calls[1][2]["source_records"], 1)
        self.assertEqual(calls[1][2]["source_requests"], 1)
        self.assertTrue(calls[1][2]["read_complete"])

    def test_source_producer_only_enqueues_durable_work(self):
        runtime, adapter = self.build_runtime(backlog_capacity=1)

        report = runtime.run_once()

        key = EvidenceQueryKey(
            "novel.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        task = self.control.get_evidence_task(key)
        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(adapter.executions, 1)
        self.assertIsNotNone(task)
        self.assertEqual(task.state, "pending")
        self.assertIsNone(task.lease_owner)
        self.assertEqual(runtime.admission.reserved("wayback"), 0)
        self.assertEqual(self.evidence.count(), 0)
        origin = self.control.connection.execute(
            """
            SELECT source_key, reservoir_id
            FROM evidence_task_origins
            WHERE hostname = ? AND year_from = ? AND year_to = ?
            """,
            ("novel.example", 1997, 1997),
        ).fetchone()
        self.assertIsNotNone(origin)
        self.assertEqual(origin["source_key"], "fixture-reservoir")
        self.assertEqual(origin["reservoir_id"], "fixture-reservoir")

    def test_single_year_discovery_hint_enqueues_exact_year_without_direct_capsule(self):
        record = SourceRecord(
            source_id="webbase-fixture",
            locator="fixture://webbase/1",
            payload="novel.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=2001,
            year_hint_mask=1 << (2001 - 1996),
        )
        adapter = FakeSource([record])
        domain = SourceDomain(
            domain_id="webbase-domain",
            family="RESEARCH_CRAWL",
            discovery_mechanism="test",
            temporal_scope=(2001, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="webbase-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://webbase",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=30,
            expected_evidence_tasks=1,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 1, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=1,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 1},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
        )

        report = runtime.run_once()

        key = EvidenceQueryKey(
            "novel.example",
            TemporalScope(2001, 2001),
            "wayback",
            "cdx-v1",
        )
        self.assertIsNotNone(self.control.get_evidence_task(key))
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(report.direct_capsules_committed, 0)
        self.assertEqual(self.evidence.count(), 0)


    def test_source_records_flow_through_bounded_parallel_pipeline(self):
        records = [
            SourceRecord(
                source_id="pipeline-source",
                locator=f"fixture://pipeline/{index}",
                payload=hostname,
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_year=1997,
            )
            for index, hostname in enumerate(
                ("one.example", "two.example", "three.example", "four.example"),
                1,
            )
        ]
        adapter = FakeSource(records)
        domain = SourceDomain(
            domain_id="pipeline-domain",
            family="LOCAL_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="pipeline-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://pipeline",
            enumeration_kind="finite_list",
            capacity_lower=4,
            capacity_upper=4,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=4,
            max_requests=1,
            max_bytes=4096,
            max_seconds=30,
            expected_evidence_tasks=4,
            expected_novel_eed=4.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=4.0,
            costs=ResourceCost(0, 4, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=4,
            reservation_evidence_tasks=4,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 4})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 4},
            queue_capacities={
                "source_records": 2,
                "observations": 2,
                "evidence_tasks": 2,
                "commits": 2,
            },
            pipeline_batch_size=2,
            extract_workers=2,
        )

        report = runtime.run_once()

        self.assertEqual(report.source_records, 4)
        self.assertEqual(report.observations, 4)
        self.assertEqual(report.pipeline_batches, 2)
        self.assertEqual(report.evidence_tasks_enqueued, 4)
        self.assertGreaterEqual(report.max_source_record_queue_depth, 1)
        self.assertLessEqual(report.max_source_record_queue_depth, 2)
        self.assertGreaterEqual(report.max_observation_queue_depth, 1)
        self.assertLessEqual(report.max_observation_queue_depth, 2)

    def test_range_first_reservation_covers_parent_and_future_fanout(self):
        runtime, adapter = self.build_runtime(
            backlog_capacity=6,
            expected_tasks=1,
            reservation_tasks=6,
            range_first_fraction=1.0,
        )

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 1)
        self.assertEqual(adapter.executions, 1)
        tasks = self.control.list_evidence_tasks()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(
            (
                tasks[0].key.temporal_scope.year_from,
                tasks[0].key.temporal_scope.year_to,
            ),
            (1996, 2001),
        )
        # One parent row is durable; five extra slots remain held for a
        # worst-case DECOMPOSED exact-year fanout.
        self.assertEqual(runtime.admission.reserved("wayback"), 5)

    def test_completed_wayback_range_suppresses_redundant_exact_year_work(self):
        range_key = EvidenceQueryKey(
            "novel.example", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([range_key])
        self.control.claim_evidence_tasks(owner="evidence-a", limit=1)
        self.control.finish_range_task(
            range_key,
            CDXQueryState.PASS,
            owner="evidence-a",
        )
        runtime, adapter = self.build_runtime(backlog_capacity=2)

        report = runtime.run_once()

        exact_key = EvidenceQueryKey(
            "novel.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)
        self.assertEqual(adapter.executions, 1)
        self.assertIsNone(self.control.get_evidence_task(exact_key))

    def test_duplicate_direct_host_years_commit_one_capsule_per_batch(self):
        records = [
            SourceRecord(
                source_id="direct-fixture",
                locator=f"fixture://{index}",
                payload="repeat.example",
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_year=1997,
                source_time=f"1997010{index}000000",
                record_type="CDX_CAPTURE",
                artifact_ref=f"fixture://{index}",
                direct_year_mask=1 << (1997 - 1996),
            )
            for index in range(1, 4)
        ]
        adapter = FakeSource(records)
        domain = SourceDomain(
            domain_id="direct-domain",
            family="DIRECT_FIXTURE",
            discovery_mechanism="test",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="direct-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://direct",
            enumeration_kind="finite_list",
            capacity_lower=3,
            capacity_upper=3,
            evidence_mode="direct_year",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=3,
            max_requests=1,
            max_bytes=4096,
            max_seconds=30,
            expected_evidence_tasks=0,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=1.0,
            costs=ResourceCost(0, 0, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_mode="direct_year",
            expected_evidence_tasks=0,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 1},
            queue_capacities={
                "source_records": 4,
                "observations": 4,
                "evidence_tasks": 4,
                "commits": 4,
            },
            baseline_batch_size=10,
        )

        report = runtime.run_once()

        self.assertEqual(report.direct_capsules_committed, 1)
        self.assertEqual(report.observations, 3)
        self.assertEqual(report.planning_observations, 1)
        self.assertEqual(self.evidence.count(), 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)


    def test_high_fanout_parent_enqueues_one_domain_probe(self):
        records = [
            SourceRecord(
                source_id="webbase-fanout",
                locator=f"fixture://fanout/{index}",
                payload=hostname,
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_year=2001,
                year_hint_mask=1 << (2001 - 1996),
            )
            for index, hostname in enumerate(
                (
                    "example.com",
                    "a.example.com",
                    "b.example.com",
                    "c.example.com",
                    "d.example.com",
                ),
                1,
            )
        ]
        adapter = FakeSource(records)
        domain = SourceDomain(
            domain_id="fanout-domain",
            family="RESEARCH_CRAWL",
            discovery_mechanism="test",
            temporal_scope=(2001, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="fanout-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://fanout",
            enumeration_kind="finite_list",
            capacity_lower=5,
            capacity_upper=5,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        self.control.save_domain(domain)
        self.control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=5,
            max_requests=1,
            max_bytes=4096,
            max_seconds=30,
            expected_evidence_tasks=5,
            expected_novel_eed=5.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=5.0,
            costs=ResourceCost(0, 5, 1, 1),
            reservoir=reservoir,
            lease=template,
            evidence_provider="wayback",
            expected_evidence_tasks=5,
            reservation_evidence_tasks=35,
        )
        runtime = SourceProducer(
            baseline=self.baseline,
            control_store=self.control,
            evidence_store=self.evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": 35})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            backlog_capacities={"wayback": 35, "rdap": 2},
            queue_capacities={
                "source_records": 8,
                "observations": 8,
                "evidence_tasks": 8,
                "commits": 8,
            },
            baseline_batch_size=10,
            domain_fanout_min_children=4,
        )

        report = runtime.run_once()

        domain_key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1996, 2001),
            "wayback",
            "cdx-domain-v1",
        )
        self.assertEqual(report.leases_succeeded, 1)
        rdap_key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1996, 2001),
            "rdap",
            "rdap-registration-v1",
        )
        self.assertEqual(report.evidence_tasks_enqueued, 7)
        self.assertIsNotNone(self.control.get_evidence_task(domain_key))
        self.assertIsNotNone(self.control.get_evidence_task(rdap_key))
        state = self.control.connection.execute(
            """
            SELECT observed_self, child_count, child_sketch,
                   query_enqueued, rdap_enqueued
            FROM domain_fanout_state
            WHERE parent_hostname = 'example.com'
            """
        ).fetchone()
        self.assertEqual(state["observed_self"], 0)
        self.assertEqual(state["child_count"], 4)
        self.assertEqual(int(state["child_sketch"]).bit_count(), 4)
        self.assertEqual(state["query_enqueued"], 1)
        # RDAP now relies on EvidenceTask identity instead of a second per-host
        # enqueue flag.
        self.assertEqual(state["rdap_enqueued"], 0)
        self.assertEqual(
            self.control.connection.execute(
                "SELECT COUNT(*) FROM domain_fanout_members"
            ).fetchone()[0],
            0,
        )

    def test_wayback_full_does_not_block_direct_proof_or_lose_fallback(self):
        occupied = EvidenceQueryKey(
            "occupied.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([occupied])
        runtime, adapter, _candidate = self.build_direct_runtime(
            backlog_capacities={"wayback": 1},
            include_fallback_hint=True,
        )

        report = runtime.run_once()

        fallback = EvidenceQueryKey(
            "direct-novel.example",
            TemporalScope(1998, 1998),
            "wayback",
            "cdx-v1",
        )
        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.direct_capsules_committed, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)
        self.assertEqual(adapter.executions, 1)
        self.assertEqual(self.evidence.count(), 1)
        self.assertIsNone(self.control.get_evidence_task(fallback))
        self.assertEqual(
            runtime.evidence_router.pending_count(provider="wayback"),
            1,
        )
        self.assertEqual(runtime.admission.reserved("wayback"), 0)
        stored = self.control.get_reservoir("direct-isolation-reservoir")
        self.assertEqual(stored.state, ReservoirState.EXHAUSTED)

    def test_direct_proof_runs_without_any_wayback_capacity_config(self):
        runtime, adapter, _candidate = self.build_direct_runtime(
            backlog_capacities={},
        )

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 1)
        self.assertEqual(report.direct_capsules_committed, 1)
        self.assertEqual(report.evidence_tasks_enqueued, 0)
        self.assertEqual(adapter.executions, 1)
        self.assertEqual(self.evidence.count(), 1)

    def test_two_producers_cannot_claim_same_direct_reservoir(self):
        runtime, _adapter, candidate = self.build_direct_runtime(
            backlog_capacities={},
            owner="seed-owner",
        )
        barrier = threading.Barrier(2)
        results: list[bool] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def compete(owner: str) -> None:
            control = ControlStore(self.root / "control.sqlite3")
            try:
                producer = SourceProducer(
                    baseline=self.baseline,
                    control_store=control,
                    evidence_store=self.evidence,
                    scheduler=GlobalScheduler(CreditLedger({"wayback": 0})),
                    candidates=[candidate],
                    adapters=runtime.adapters,
                    backlog_capacities={},
                    queue_capacities={
                        "source_records": 2,
                        "observations": 2,
                        "evidence_tasks": 2,
                        "commits": 2,
                    },
                    owner=owner,
                )
                barrier.wait(timeout=2.0)
                granted = producer._grant_fresh_lease()
                with lock:
                    results.append(granted is not None)
            except BaseException as exc:
                with lock:
                    errors.append(exc)
            finally:
                control.close()

        workers = [
            threading.Thread(target=compete, args=(f"worker-{index}",))
            for index in (1, 2)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5.0)

        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(sorted(results), [False, True])
        stored = self.control.get_reservoir(candidate.reservoir_id)
        self.assertEqual(stored.state, ReservoirState.LEASED)

    def test_full_backlog_blocks_source_before_adapter_execution(self):
        occupied = EvidenceQueryKey(
            "occupied.example", TemporalScope(1997, 1997), "wayback", "cdx-v1"
        )
        self.control.enqueue_evidence_tasks([occupied])
        runtime, adapter = self.build_runtime(backlog_capacity=1)

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 0)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(adapter.executions, 0)
        stored = self.control.get_reservoir("fixture-reservoir")
        self.assertEqual(stored.state, ReservoirState.READY)

    def test_conservative_reservation_rejects_lease_larger_than_remaining_capacity(self):
        runtime, adapter = self.build_runtime(backlog_capacity=1, expected_tasks=2)

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 0)
        self.assertTrue(report.admission_blocked)
        self.assertEqual(adapter.executions, 0)


    def test_source_producer_records_durable_active_candidate(self):
        ledger = CandidateStore(self.root / "candidates.sqlite3")
        runtime, _adapter = self.build_runtime(backlog_capacity=1)
        runtime.candidate_store = ledger

        report = runtime.run_once()

        self.assertEqual(report.leases_succeeded, 1)
        active = list(ledger.iter_active_candidates())
        self.assertEqual([row.hostname for row in active], ["novel.example"])
        self.assertEqual(active[0].source_id, "fixture-source")
        ledger.close()


if __name__ == "__main__":
    unittest.main()
