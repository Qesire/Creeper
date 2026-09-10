import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.records.candidates import CandidateSourceScope
from creeper.records.models import HostObservation, SourceRecord
from creeper.runtime.pipeline import SyncRuntime
from creeper.runtime.submission import RuntimeSubmissionContext, build_runtime_snapshot
from creeper.scheduler.credits import CreditLedger
from creeper.scheduler.global_scheduler import GlobalScheduler
from creeper.scheduler.leases import LeaseResult, WorkLease
from creeper.scheduler.priority import LeaseCandidate, ResourceCost
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class RuntimeSubmissionIntegrationTests(unittest.TestCase):
    def test_build_runtime_snapshot_uses_real_stores_and_is_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_dir = task_root / "merged260909-3"
            baseline_dir.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_dir / f"{year}.txt").write_text(
                    "baseline.example\n" if year == 1996 else "",
                    encoding="utf-8",
                )
            (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline = BaselineIndex.build(task_root, root / "baseline.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            evidence.put_many([
                EvidenceCapsule(
                    "baseline.example", 1996, "wayback-cdx", "capture_timestamp_year",
                    "19960101000000", "http://baseline.example/", "b" * 64, "evidence-v1",
                ),
                EvidenceCapsule(
                    "zeta.example", 1998, "wayback-cdx", "capture_timestamp_year",
                    "19980101000000", "http://zeta.example/", "z" * 64, "evidence-v1",
                ),
                EvidenceCapsule(
                    "alpha.example", 1997, "wayback-cdx", "capture_timestamp_year",
                    "19970101000000", "http://alpha.example/", "a" * 64, "evidence-v1",
                ),
            ])
            context = RuntimeSubmissionContext(
                baseline_manifest={
                    "baseline_id": "merged260909-3",
                    "annual_file_hashes": {f"{year}.txt": "b" * 64 for year in range(1996, 2002)},
                },
                code_revision="c" * 64,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                eed_report={"equivalent_english_domains": "2"},
                novel_eed="2",
                growth_rate="0.05",
            )

            snapshot = build_runtime_snapshot(
                context=context,
                evidence_store=evidence,
                baseline=baseline,
                snapshot_id="runtime-snapshot-1",
            )

            self.assertTrue(snapshot.ready)
            self.assertEqual(
                [(record.hostname, record.year) for record in snapshot.novel_records],
                [("alpha.example", 1997), ("zeta.example", 1998)],
            )
            self.assertEqual(snapshot.overlap_count, 0)
            evidence.close()
            baseline.close()

    def test_runtime_report_builds_submission_snapshot_after_commits(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_dir = task_root / "merged260909-3"
            baseline_dir.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline = BaselineIndex.build(task_root, root / "baseline.sqlite3")
            control = ControlStore(root / "control.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")

            class FixtureAdapter:
                adapter_id = "runtime-submission-fixture"

                def execute(self, lease):
                    record = SourceRecord(
                        source_id=self.adapter_id,
                        locator="fixture://submission/1",
                        payload="novel.example",
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

            adapter = FixtureAdapter()
            domain = SourceDomain(
                domain_id="runtime-submission-domain",
                family="LOCAL_FIXTURE",
                discovery_mechanism="integration fixture",
                temporal_scope=(1996, 2001),
                state=DomainState.EXPLORING,
            )
            reservoir = Reservoir(
                reservoir_id="runtime-submission-reservoir",
                domain_id=domain.domain_id,
                adapter_id=adapter.adapter_id,
                root_locator="fixture://submission",
                enumeration_kind="finite_list",
                capacity_lower=1,
                capacity_upper=1,
                evidence_mode="discovery_only",
                state=ReservoirState.READY,
            )
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
                costs=ResourceCost(general_network=0, evidence_network=1, cpu=1, ssd=1),
                reservoir=reservoir,
                lease=template,
                expected_evidence_tasks=1,
                evidence_provider="wayback",
            )
            control.save_domain(domain)
            control.save_reservoir(reservoir)
            context = RuntimeSubmissionContext(
                baseline_manifest={
                    "baseline_id": "merged260909-3",
                    "annual_file_hashes": {
                        f"{year}.txt": "a" * 64 for year in range(1996, 2002)
                    },
                },
                code_revision="c" * 64,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                eed_report={"equivalent_english_domains": "1"},
                novel_eed="1",
                growth_rate="0.05",
            )

            def transport(hostname, year):
                return [
                    ([{
                        "timestamp": f"{year}0101000000",
                        "original": f"http://{hostname}/",
                        "status": "200",
                    }], True)
                ]

            runtime = SyncRuntime(
                baseline=baseline,
                control_store=control,
                evidence_store=evidence,
                scheduler=GlobalScheduler(CreditLedger({"wayback": 1})),
                candidates=[candidate],
                adapters={adapter.adapter_id: adapter},
                evidence_transport=transport,
                queue_capacities={
                    "source_records": 1,
                    "observations": 1,
                    "evidence_tasks": 1,
                    "commits": 1,
                },
                submission_context=context,
                snapshot_id="runtime-snapshot-1",
            )
            try:
                report = runtime.run_once()
                self.assertTrue(report.snapshot_ready)
                self.assertEqual(report.novel_records, 1)
            finally:
                evidence.close()
                control.close()
                baseline.close()

    def test_runtime_snapshot_below_formal_growth_gate_is_not_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_dir = task_root / "next-baseline"
            baseline_dir.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline = BaselineIndex.build(task_root, root / "baseline.sqlite3")
            evidence = EvidenceStore(root / "evidence.sqlite3")
            evidence.put(
                EvidenceCapsule(
                    "alpha.example", 1997, "wayback-cdx", "capture_timestamp_year",
                    "19970101000000", "http://alpha.example/", "a" * 64, "evidence-v1",
                )
            )
            context = RuntimeSubmissionContext(
                baseline_manifest={
                    "baseline_id": "next-baseline",
                    "annual_file_hashes": {f"{year}.txt": "b" * 64 for year in range(1996, 2002)},
                },
                code_revision="c" * 64,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                eed_report={"equivalent_english_domains": "1"},
                novel_eed="1",
                growth_rate="0.049999",
            )
            try:
                snapshot = build_runtime_snapshot(
                    context=context,
                    evidence_store=evidence,
                    baseline=baseline,
                    snapshot_id="runtime-snapshot-low-growth",
                )
                self.assertFalse(snapshot.ready)
            finally:
                evidence.close()
                baseline.close()


if __name__ == "__main__":
    unittest.main()
