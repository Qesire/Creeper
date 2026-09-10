import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import CDXQueryState
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.pipeline import SyncRuntime
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, LeaseState, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class OneRecordReservoir:
    adapter_id = "lease-boundary-fixture"

    def execute(self, lease):
        record = SourceRecord(
            source_id=self.adapter_id,
            locator="fixture://lease-boundary/1",
            payload="boundary.example",
            scope=CandidateSourceScope.LOCAL_DISCOVERY,
            source_year=1997,
        )
        return iter((record,)), LeaseResult(
            lease_id=lease.lease_id,
            records=1,
            requests=1,
            bytes_read=len(record.payload),
            elapsed_seconds=0.001,
            next_cursor=None,
        )

    def extract_hosts(self, record):
        yield HostObservation(
            hostname=record.payload,
            source_id=record.source_id,
            locator=record.locator,
            scope=record.scope,
            source_year=record.source_year,
        )


class RuntimeLeaseBoundaryTests(unittest.TestCase):
    def _fixture(self, root: Path, *, evidence_capacity: int, transport):
        task_root = root / "task"
        baseline_dir = task_root / "merged260909-3"
        baseline_dir.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        baseline = BaselineIndex.build(task_root, root / "baseline.sqlite3")
        control = ControlStore(root / "control.sqlite3")
        evidence = EvidenceStore(root / "evidence.sqlite3")
        adapter = OneRecordReservoir()
        domain = SourceDomain(
            domain_id="lease-boundary-domain",
            family="LOCAL_FIXTURE",
            discovery_mechanism="integration fixture",
            temporal_scope=(1996, 2001),
            state=DomainState.EXPLORING,
        )
        reservoir = Reservoir(
            reservoir_id="lease-boundary-reservoir",
            domain_id=domain.domain_id,
            adapter_id=adapter.adapter_id,
            root_locator="fixture://lease-boundary",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="discovery_only",
            state=ReservoirState.READY,
        )
        control.save_domain(domain)
        control.save_reservoir(reservoir)
        template = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=1,
            max_requests=1,
            max_bytes=1024,
            max_seconds=0.001,
            expected_evidence_tasks=1,
            expected_novel_eed=1.0,
        )
        candidate = LeaseCandidate(
            reservoir_id=reservoir.reservoir_id,
            reservoir=reservoir,
            lease=template,
            expected_novel_eed=1.0,
            expected_evidence_tasks=1,
            evidence_provider="wayback",
            costs=ResourceCost(0, 1, 1, 1),
        )
        runtime = SyncRuntime(
            baseline=baseline,
            control_store=control,
            evidence_store=evidence,
            scheduler=GlobalScheduler(CreditLedger({"wayback": evidence_capacity})),
            candidates=[candidate],
            adapters={adapter.adapter_id: adapter},
            evidence_transport=transport,
            queue_capacities={
                "source_records": 1,
                "observations": 1,
                "evidence_tasks": 1,
                "commits": 1,
            },
        )
        return baseline, control, evidence, runtime

    @staticmethod
    def _only_lease_state(control: ControlStore) -> str:
        row = control.connection.execute("SELECT state FROM work_leases LIMIT 1").fetchone()
        if row is None:
            raise AssertionError("expected one persisted work lease")
        return str(row["state"])

    def test_provider_observes_source_lease_already_finalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            holder = {}

            def transport(hostname, year):
                control = holder["control"]
                reservoir = control.get_reservoir("lease-boundary-reservoir")
                self.assertEqual(reservoir.state, ReservoirState.EXHAUSTED)
                self.assertEqual(
                    self._only_lease_state(control),
                    LeaseState.SUCCEEDED.value,
                )
                return [([], True)]

            baseline, control, evidence, runtime = self._fixture(
                root, evidence_capacity=1, transport=transport
            )
            holder["control"] = control
            try:
                report = runtime.run_once()
                self.assertEqual(report.leases_succeeded, 1)
                self.assertEqual(report.evidence_tasks_completed, 1)
            finally:
                evidence.close()
                control.close()
                baseline.close()

    def test_provider_failure_cannot_roll_back_completed_source_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def transport(_hostname, _year):
                raise AssertionError("synthetic provider crash after source finalize")

            baseline, control, evidence, runtime = self._fixture(
                root, evidence_capacity=1, transport=transport
            )
            try:
                with self.assertRaisesRegex(AssertionError, "provider crash"):
                    runtime.run_once()
                self.assertEqual(
                    control.get_reservoir("lease-boundary-reservoir").state,
                    ReservoirState.EXHAUSTED,
                )
                self.assertEqual(
                    self._only_lease_state(control),
                    LeaseState.SUCCEEDED.value,
                )
                task = control.list_evidence_tasks()[0]
                self.assertEqual(task.state, CDXQueryState.PENDING.value)
            finally:
                evidence.close()
                control.close()
                baseline.close()

    def test_zero_provider_capacity_leaves_durable_task_after_source_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = []

            def transport(hostname, year):
                calls.append((hostname, year))
                return [([], True)]

            baseline, control, evidence, runtime = self._fixture(
                root, evidence_capacity=0, transport=transport
            )
            try:
                report = runtime.run_once()
                self.assertEqual(report.leases_succeeded, 1)
                self.assertEqual(report.evidence_tasks_enqueued, 1)
                self.assertEqual(report.evidence_tasks_completed, 0)
                self.assertEqual(calls, [])
                self.assertEqual(
                    control.get_reservoir("lease-boundary-reservoir").state,
                    ReservoirState.EXHAUSTED,
                )
                task = control.list_evidence_tasks()[0]
                self.assertEqual(task.state, CDXQueryState.PENDING.value)
                self.assertIsNone(task.lease_owner)
            finally:
                evidence.close()
                control.close()
                baseline.close()


if __name__ == "__main__":
    unittest.main()
