from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey, TemporalScope
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.scheduler.leases import WorkLease
from creeper.source_discovery import (
    ScoutMeasurement,
    SourceCandidate,
    SourceDiscoveryRegistry,
    SourceLevel,
)
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

            self.assertEqual(
                report.as_dict()["report_version"],
                "incremental-readiness-v2",
            )
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

    def test_readiness_closes_search_and_codex_rewards_with_final_eed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            baseline = self._build_baseline(root)
            model = self._model(root)

            control = ControlStore(runtime_root / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            episode_id = "llm:readiness"
            registry.begin_search_episode(
                strategy="EXPLOIT_SUCCESS",
                backend="codex-cli-subagent",
                query="annual archive siblings",
                actor="codex:source-intelligence",
                episode_id=episode_id,
            )
            registry.begin_llm_episode(
                episode_id=episode_id,
                task_type="EXPLOIT_SUCCESS_PATTERN",
                backend="codex-cli-subagent",
                actor="codex:source-intelligence",
                context_hash="readiness-context",
                prompt_version="source-intelligence-v2",
            )
            registry.register_llm_hypothesis(
                episode_id,
                {
                    "hypothesis_id": f"{episode_id}:h1",
                    "action": "PROBE_URL",
                    "confidence": 0.9,
                },
            )
            candidate = SourceCandidate(
                canonical_entrypoint="https://archive.example/1997/index.cdxj",
                source_family="BULK_ARTIFACT",
                level=SourceLevel.SOURCE,
                discovered_by="codex:source-intelligence",
                discovery_strategy="EXPLOIT_SUCCESS",
                expected_year_from=1997,
                expected_year_to=1997,
                expected_volume=100000,
                temporal_semantics_prior=1.0,
                enumerability_prior=0.95,
                direct_evidence_prior=1.0,
                confidence=0.9,
            )
            registry.register_proposal(candidate, episode_id=episode_id)
            registry.link_llm_source(
                candidate.source_key,
                hypothesis_id=f"{episode_id}:h1",
            )
            registry.finish_search_episode(
                episode_id,
                search_cost_seconds=2.0,
            )
            registry.finish_llm_episode(
                episode_id,
                cost_seconds=2.0,
            )
            registry.record_scout_measurement(
                candidate.source_key,
                ScoutMeasurement(
                    sampled_records=100,
                    unique_hosts=80,
                    novel_hosts=20,
                    direct_host_years=0,
                    requests=1,
                    bytes_read=1024,
                    elapsed_seconds=1.0,
                    novel_eed=5.0,
                ),
            )
            self.assertEqual(
                registry.get_search_episode(episode_id).accepted_novel_eed,
                5.0,
            )

            domain = SourceDomain(
                domain_id="reward-domain",
                family="FIXTURE",
                discovery_mechanism="test",
                temporal_scope=(1996, 2001),
            )
            reservoir = Reservoir(
                reservoir_id="reward-reservoir",
                domain_id=domain.domain_id,
                adapter_id="fixture",
                root_locator="fixture://reward",
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
                [("final.org", 1997, "arquivo")],
                source_key=candidate.source_key,
                reservoir_id=reservoir.reservoir_id,
                lease_id=lease.lease_id,
            )
            control.close()

            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put(
                self._capsule(
                    "final.org",
                    1997,
                    provider="arquivo",
                )
            )
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                report = runtime.sync_until_current()
                self.assertEqual(
                    report.source_attribution[candidate.source_key]["novel_eed"],
                    "1",
                )

            check_control = ControlStore(runtime_root / "control.sqlite3")
            check_registry = SourceDiscoveryRegistry(check_control)
            self.assertEqual(
                check_registry.get_search_episode(
                    episode_id
                ).accepted_novel_eed,
                1.0,
            )
            self.assertEqual(
                check_registry.llm_task_rewards()[0]["credited_eed"],
                1.0,
            )
            check_control.close()

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
