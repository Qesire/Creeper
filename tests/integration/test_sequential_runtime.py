import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey, TemporalScope
from creeper.records.candidates import CandidateSourceScope
from creeper.runtime.pipeline import SyncRuntime
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.local.static_dataset import StaticDatasetAdapter
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class DurableSequentialRuntimeIntegrationTests(unittest.TestCase):
    YEARS = range(1996, 2002)

    def _build_baseline(self, root: Path) -> tuple[Path, BaselineIndex]:
        task_root = root / "task"
        baseline_root = task_root / "merged260909-3"
        baseline_root.mkdir(parents=True)
        for year in self.YEARS:
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
        index_path = root / "baseline.sqlite3"
        return index_path, BaselineIndex.build(task_root, index_path)

    def _candidate(
        self,
        *,
        dataset: Path,
        reservoir: Reservoir,
        max_records: int,
        expected_evidence_tasks: int,
    ) -> tuple[StaticDatasetAdapter, LeaseCandidate]:
        adapter = StaticDatasetAdapter(
            dataset,
            source_id=reservoir.adapter_id,
            source_year=1997,
        )
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            cursor_start=reservoir.cursor,
            max_records=max_records,
            max_requests=1,
            max_bytes=10_000,
            max_seconds=30,
            expected_evidence_tasks=expected_evidence_tasks,
            expected_novel_eed=float(max_records),
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            expected_novel_eed=float(max_records),
            costs=ResourceCost(
                general_network=0,
                evidence_network=1,
                cpu=1,
                ssd=1,
            ),
            reservoir=reservoir,
            lease=template,
            expected_evidence_tasks=expected_evidence_tasks,
            evidence_provider="wayback",
        )
        return adapter, candidate

    def _runtime_fixture(
        self,
        *,
        control: ControlStore,
        evidence: EvidenceStore,
        baseline: BaselineIndex,
        adapter: StaticDatasetAdapter,
        candidate: LeaseCandidate,
        evidence_capacity: int,
        evidence_queue_capacity: int,
        transport,
    ) -> SyncRuntime:
        return SyncRuntime(
            baseline=baseline,
            control_store=control,
            evidence_store=evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": evidence_capacity})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            evidence_transport=transport,
            queue_capacities={
                "source_records": 3,
                "observations": 3,
                "evidence_tasks": evidence_queue_capacity,
                "commits": 1,
            },
            evidence_provider="wayback",
            evidence_policy_version="cdx-v1",
        )

    def _save_reservoir(
        self,
        control: ControlStore,
        *,
        reservoir_id: str,
        adapter_id: str,
        capacity: int,
        cursor: str | None = None,
    ) -> Reservoir:
        domain = SourceDomain(
            domain_id=f"{reservoir_id}-domain",
            family="LOCAL_STATIC_DATASET",
            discovery_mechanism="integration fixture",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id=reservoir_id,
            domain_id=domain.domain_id,
            adapter_id=adapter_id,
            root_locator="fixture://dataset",
            enumeration_kind="line_file",
            capacity_lower=capacity,
            capacity_upper=capacity,
            cursor=cursor,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        control.save_domain(domain)
        control.save_reservoir(reservoir)
        return reservoir

    @staticmethod
    def _transport(calls: list[tuple[str, int]]):
        def transport(hostname: str, year: int):
            calls.append((hostname, year))
            return [
                (
                    [
                        {
                            "timestamp": f"{year}0101000000",
                            "original": f"http://{hostname}/",
                            "status": "200",
                        }
                    ],
                    True,
                )
            ]

        return transport

    def test_two_run_once_calls_consume_two_plus_one_without_repeat_and_exhaust(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset.txt"
            dataset.write_bytes(b"one.example\ntwo.example\nthree.example\n")
            _, baseline = self._build_baseline(root)
            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            try:
                reservoir = self._save_reservoir(
                    control,
                    reservoir_id="sequential-reservoir",
                    adapter_id="sequential-reservoir",
                    capacity=3,
                )
                adapter, candidate = self._candidate(
                    dataset=dataset,
                    reservoir=reservoir,
                    max_records=2,
                    expected_evidence_tasks=2,
                )
                calls: list[tuple[str, int]] = []
                runtime = self._runtime_fixture(
                    control=control,
                    evidence=evidence,
                    baseline=baseline,
                    adapter=adapter,
                    candidate=candidate,
                    evidence_capacity=2,
                    evidence_queue_capacity=2,
                    transport=self._transport(calls),
                )

                first = runtime.run_once()
                second = runtime.run_once()

                self.assertEqual(first.source_records, 2)
                self.assertEqual(second.source_records, 1)
                self.assertEqual(
                    calls,
                    [
                        ("one.example", 1997),
                        ("two.example", 1997),
                        ("three.example", 1997),
                    ],
                )
                self.assertEqual(evidence.count(), 3)
                self.assertEqual(
                    control.get_reservoir("sequential-reservoir").state,
                    ReservoirState.EXHAUSTED,
                )
            finally:
                evidence.close()
                control.close()
                baseline.close()

    def test_fresh_store_reopen_resumes_persisted_byte_cursor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset.txt"
            dataset.write_bytes(b"one.example\ntwo.example\nthree.example\n")
            baseline_path, baseline = self._build_baseline(root)
            control_path = root / "control.sqlite3"
            evidence_path = root / "evidence.sqlite3"
            calls: list[tuple[str, int]] = []
            control = ControlStore(control_path)
            evidence = EvidenceStore(evidence_path)
            try:
                reservoir = self._save_reservoir(
                    control,
                    reservoir_id="restart-reservoir",
                    adapter_id="restart-reservoir",
                    capacity=3,
                )
                adapter, candidate = self._candidate(
                    dataset=dataset,
                    reservoir=reservoir,
                    max_records=1,
                    expected_evidence_tasks=1,
                )
                first_runtime = self._runtime_fixture(
                    control=control,
                    evidence=evidence,
                    baseline=baseline,
                    adapter=adapter,
                    candidate=candidate,
                    evidence_capacity=1,
                    evidence_queue_capacity=1,
                    transport=self._transport(calls),
                )
                self.assertEqual(first_runtime.run_once().source_records, 1)
            finally:
                evidence.close()
                control.close()
                baseline.close()

            reopened_baseline = BaselineIndex(baseline_path)
            reopened_control = ControlStore(control_path)
            reopened_evidence = EvidenceStore(evidence_path)
            try:
                reopened = reopened_control.get_reservoir("restart-reservoir")
                adapter, candidate = self._candidate(
                    dataset=dataset,
                    reservoir=reopened,
                    max_records=1,
                    expected_evidence_tasks=1,
                )
                second_runtime = self._runtime_fixture(
                    control=reopened_control,
                    evidence=reopened_evidence,
                    baseline=reopened_baseline,
                    adapter=adapter,
                    candidate=candidate,
                    evidence_capacity=1,
                    evidence_queue_capacity=1,
                    transport=self._transport(calls),
                )

                second = second_runtime.run_once()

                self.assertEqual(second.source_records, 1)
                self.assertEqual(
                    reopened_control.get_reservoir("restart-reservoir").cursor,
                    str(len(b"one.example\ntwo.example\n")),
                )
                self.assertEqual(
                    calls,
                    [("one.example", 1997), ("two.example", 1997)],
                )
                self.assertEqual(reopened_evidence.count(), 2)
                self.assertEqual(
                    reopened_control.get_reservoir("restart-reservoir").state,
                    ReservoirState.READY,
                )
            finally:
                reopened_evidence.close()
                reopened_control.close()
                reopened_baseline.close()

    def test_capacity_one_evidence_queue_drains_three_hinted_hosts_without_drop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "dataset.txt"
            dataset.write_bytes(b"hint-one.example\nhint-two.example\nhint-three.example\n")
            _, baseline = self._build_baseline(root)
            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            try:
                reservoir = self._save_reservoir(
                    control,
                    reservoir_id="hinted-reservoir",
                    adapter_id="hinted-reservoir",
                    capacity=3,
                )
                adapter, candidate = self._candidate(
                    dataset=dataset,
                    reservoir=reservoir,
                    max_records=3,
                    expected_evidence_tasks=1,
                )
                calls: list[tuple[str, int]] = []
                runtime = self._runtime_fixture(
                    control=control,
                    evidence=evidence,
                    baseline=baseline,
                    adapter=adapter,
                    candidate=candidate,
                    evidence_capacity=1,
                    evidence_queue_capacity=1,
                    transport=self._transport(calls),
                )

                report = runtime.run_once()

                self.assertEqual(report.source_records, 3)
                self.assertEqual(report.observations, 3)
                self.assertEqual(report.evidence_tasks_enqueued, 3)
                self.assertEqual(report.evidence_tasks_completed, 3)
                self.assertEqual(report.evidence_capsules_committed, 3)
                self.assertEqual(
                    calls,
                    [
                        ("hint-one.example", 1997),
                        ("hint-two.example", 1997),
                        ("hint-three.example", 1997),
                    ],
                )
                self.assertLessEqual(report.max_evidence_queue_depth, 1)
                self.assertEqual(evidence.count(), 3)
                for hostname in (
                    "hint-one.example",
                    "hint-two.example",
                    "hint-three.example",
                ):
                    key = EvidenceQueryKey(
                        hostname,
                        TemporalScope(1997, 1997),
                        "wayback",
                        "cdx-v1",
                    )
                    self.assertEqual(
                        control.get_evidence_task(key).state,
                        CDXQueryState.PASS.value,
                    )
            finally:
                evidence.close()
                control.close()
                baseline.close()


if __name__ == "__main__":
    unittest.main()
