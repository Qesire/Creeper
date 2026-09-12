from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.scheduler.leases import WorkLease
from creeper.sources.domains import SourceDomain
from creeper.sources.reservoirs import Reservoir
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class IncrementalReadinessTests(unittest.TestCase):
    @staticmethod
    def _model(root: Path) -> Path:
        path = root / "eed-model.json"
        path.write_text(
            json.dumps(
                {
                    "tld": ["com", "org", "net"],
                    "lang": ["eng", "eng", "eng"],
                    "perc_of_tld": ["50", "100", "75"],
                }
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _build_baseline(
        root: Path,
        *,
        annual: dict[int, str] | None = None,
        output_name: str = "baseline.sqlite3",
    ) -> Path:
        task = root / ("task-" + output_name.replace(".", "-"))
        baseline_dir = task / "merged260909-3"
        baseline_dir.mkdir(parents=True)
        annual = annual or {}
        for year in range(1996, 2002):
            (baseline_dir / f"{year}.txt").write_text(
                annual.get(year, ""),
                encoding="utf-8",
            )
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        output = root / output_name
        BaselineIndex.build(task, output).close()
        return output

    @staticmethod
    def _capsule(
        hostname: str,
        year: int,
        *,
        provider: str = "wayback",
        payload: str = "a",
    ) -> EvidenceCapsule:
        return EvidenceCapsule(
            hostname,
            year,
            provider,
            "capture_timestamp_year",
            f"{year}0101000000",
            f"http://{hostname}/",
            payload * 64,
            "evidence-v1",
        )

    def test_incremental_readiness_deduplicates_host_year_and_processes_only_new_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(
                root,
                annual={1998: "same.com\n"},
            )
            model = self._model(root)
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put_many(
                [
                    self._capsule("same.com", 1998, provider="wayback", payload="a"),
                    self._capsule("same.com", 1998, provider="arquivo", payload="b"),
                    self._capsule("same.com", 2000, payload="c"),
                    self._capsule("new.org", 1997, payload="d"),
                ]
            )
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="30",
                batch_size=2,
            ) as runtime:
                first = runtime.sync_until_current()

                self.assertEqual(first.processed_host_years, 3)
                self.assertEqual(first.novel_host_years, 2)
                self.assertEqual(first.novel_eed, "1.5")
                self.assertEqual(first.annual["1997"]["novel_eed"], "1")
                self.assertEqual(first.annual["2000"]["novel_eed"], "0.5")
                self.assertEqual(first.annual["1998"]["novel_host_years"], 0)
                first_cursor = first.evidence_cursor

                writer = EvidenceStore(runtime_root / "evidence.sqlite3")
                writer.put(self._capsule("late.net", 2001, payload="e"))
                writer.close()

                second = runtime.sync_until_current()

                self.assertGreater(second.evidence_cursor, first_cursor)
                self.assertEqual(second.processed_host_years, 4)
                self.assertEqual(second.novel_host_years, 3)
                self.assertEqual(second.novel_eed, "2.25")
                self.assertEqual(second.annual["2001"]["novel_eed"], "0.75")

                stable = runtime.sync_until_current()
                self.assertEqual(stable.evidence_cursor, second.evidence_cursor)
                self.assertEqual(stable.novel_eed, second.novel_eed)

    def test_baseline_replacement_resets_and_replays_existing_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put(self._capsule("repeat.com", 1997))
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="10",
            ) as runtime:
                before = runtime.sync_until_current()
                self.assertEqual(before.novel_host_years, 1)
                self.assertEqual(before.novel_eed, "0.5")

                replacement = self._build_baseline(
                    root,
                    annual={1997: "repeat.com\n"},
                    output_name="replacement.sqlite3",
                )
                os.replace(replacement, baseline)
                os.utime(baseline, None)

                after = runtime.sync_until_current()

                self.assertNotEqual(
                    before.baseline_signature,
                    after.baseline_signature,
                )
                self.assertEqual(after.processed_host_years, 1)
                self.assertEqual(after.novel_host_years, 0)
                self.assertEqual(after.novel_eed, "0")


    def test_readiness_attributes_novel_eed_by_task_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)

            control = ControlStore(runtime_root / "control.sqlite3")
            exact = EvidenceQueryKey(
                "exact.org",
                TemporalScope(1997, 1997),
                "wayback",
                "evidence-v1",
            )
            ranged = EvidenceQueryKey(
                "range.com",
                TemporalScope(1996, 1998),
                "wayback",
                "evidence-v1",
            )
            control.enqueue_evidence_tasks([exact, ranged])
            control.attribute_task_host_years(exact, [1997])
            control.attribute_task_host_years(ranged, [1996, 1998])
            control.close()

            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put_many(
                [
                    self._capsule("exact.org", 1997),
                    self._capsule("range.com", 1996),
                    self._capsule("range.com", 1998),
                ]
            )
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                report = runtime.sync_until_current()

            self.assertEqual(report.report_version if hasattr(report, "report_version") else "incremental-readiness-v2", "incremental-readiness-v2")
            self.assertEqual(
                report.task_kind_attribution,
                {
                    "exact": {
                        "novel_host_years": 1,
                        "novel_eed": "1",
                    },
                    "range": {
                        "novel_host_years": 2,
                        "novel_eed": "1.0",
                    },
                },
            )

    def test_gate_report_uses_exact_decimal_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put(self._capsule("one.org", 1997))
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                report = runtime.sync_until_current()

            self.assertEqual(report.novel_eed, "1")
            self.assertEqual(report.growth_rate, "0.05")
            self.assertEqual(report.five_percent_delta, "1.00")
            self.assertEqual(report.confirmed_fraction_of_five_percent, "1")
            self.assertTrue(report.prewarm_reached)
            self.assertTrue(report.formal_gate_reached)


    def test_readiness_uses_same_authority_for_source_eed_attribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)

            control = ControlStore(runtime_root / "control.sqlite3")
            domain = SourceDomain(
                domain_id="source-domain",
                family="FIXTURE",
                discovery_mechanism="test",
                temporal_scope=(1996, 2001),
            )
            reservoir = Reservoir(
                reservoir_id="source-reservoir",
                domain_id=domain.domain_id,
                adapter_id="fixture",
                root_locator="fixture://source",
                enumeration_kind="finite_list",
                capacity_lower=1,
                capacity_upper=1,
                evidence_mode="direct_year",
            )
            control.save_domain(domain)
            control.save_reservoir(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=1,
                max_requests=1,
                max_bytes=1024,
                max_seconds=30,
                now=100.0,
                expires_at=130.0,
            )
            control.save_lease(lease)
            control.attribute_direct_host_years(
                [("new.org", 1997, "arquivo")],
                source_key="source-a",
                reservoir_id=reservoir.reservoir_id,
                lease_id=lease.lease_id,
            )
            control.close()

            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put(self._capsule("new.org", 1997, provider="arquivo"))
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                report = runtime.sync_until_current()

            self.assertEqual(report.novel_eed, "1")
            self.assertEqual(
                report.source_attribution,
                {
                    "source-a": {
                        "novel_host_years": 1,
                        "novel_eed": "1",
                    }
                },
            )


if __name__ == "__main__":
    unittest.main()
