from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import (
    baseline_authority_signature,
    eed_model_authority_signature,
)
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.scheduler.leases import WorkLease
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.domains import SourceDomain
from creeper.sources.reservoirs import Reservoir
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class FinalSourceRewardIntegrationTests(unittest.TestCase):
    @staticmethod
    def model(root: Path) -> Path:
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
    def baseline(root: Path) -> Path:
        task = root / "task"
        merged = task / "merged260912-3"
        merged.mkdir(parents=True)
        for year in range(1996, 2002):
            (merged / f"{year}.txt").write_text("", encoding="utf-8")
        (merged / "candidate_pool.txt").write_text("", encoding="utf-8")
        output = root / "baseline.sqlite3"
        BaselineIndex.build(task, output).close()
        return output

    @staticmethod
    def direct_capsule(
        hostname: str,
        year: int,
        *,
        source_key: str,
        payload: str,
    ) -> EvidenceCapsule:
        return EvidenceCapsule(
            hostname=hostname,
            year=year,
            provider=f"direct:{source_key}",
            temporal_semantics="source_direct_year",
            evidence_timestamp=f"{year}0101000000",
            source_locator=f"fixture://{source_key}/{hostname}/{year}",
            payload_hash=payload * 64,
            policy_version="historical-region-v1",
            evidence_type="source_direct_year",
            source_id=source_key,
            original_url=f"http://{hostname}/",
            record_locator=f"fixture://{source_key}/{hostname}/{year}",
            extraction_method="fixture",
        )

    def prepare_run(
        self,
        root: Path,
        *,
        name: str,
        read_complete: bool = True,
    ) -> tuple[
        Path,
        Path,
        SourceCandidate,
        str,
        str,
        str,
        str,
    ]:
        runtime_root = root / "runtime"
        runtime_root.mkdir(exist_ok=True)
        baseline = self.baseline(root)
        model = self.model(root)
        baseline_signature = baseline_authority_signature(baseline)
        model_signature = eed_model_authority_signature(model)

        control = ControlStore(runtime_root / "control.sqlite3")
        registry = SourceDiscoveryRegistry(control)
        registry.set_scout_authority(
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        candidate = SourceCandidate(
            canonical_entrypoint=f"https://archive.example/{name}.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="integration",
            discovery_strategy="FINAL_REWARD_TEST",
            expected_volume=100,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            confidence=1.0,
        )
        registry.register_proposal(candidate)

        domain = SourceDomain(
            domain_id=f"domain:{name}",
            family="FIXTURE",
            discovery_mechanism="integration",
            temporal_scope=(1996, 2001),
        )
        reservoir = Reservoir(
            reservoir_id=f"reservoir:{name}",
            domain_id=domain.domain_id,
            adapter_id="fixture",
            root_locator=f"fixture://{name}",
            enumeration_kind="finite_list",
            capacity_lower=1,
            capacity_upper=1,
            evidence_mode="direct_year",
        )
        control.save_domain(domain)
        control.save_reservoir(reservoir)

        granted = WorkLease.create(
            reservoir_id=reservoir.reservoir_id,
            max_records=100,
            max_requests=10,
            max_bytes=10_000,
            max_seconds=30,
            now=100.0,
            expires_at=130.0,
        ).grant(owner="integration")
        control.save_lease(granted)
        running = granted.start()
        control.save_lease(running)
        control.save_lease(running.complete())

        registry.begin_source_run(
            candidate.source_key,
            reservoir_id=reservoir.reservoir_id,
            lease_id=granted.lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
            read_started=100.0,
        )
        registry.record_source_run_read(
            candidate.source_key,
            reservoir_id=reservoir.reservoir_id,
            lease_id=granted.lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
            source_records=10,
            bytes_read=1000,
            source_requests=1,
            read_finished=101.0 if read_complete else None,
            read_complete=read_complete,
        )
        control.close()
        return (
            baseline,
            model,
            candidate,
            reservoir.reservoir_id,
            granted.lease_id,
            baseline_signature,
            model_signature,
        )

    @staticmethod
    def outcome(
        runtime_root: Path,
        candidate: SourceCandidate,
        reservoir_id: str,
        lease_id: str,
        baseline_signature: str,
        model_signature: str,
    ):
        control = ControlStore(runtime_root / "control.sqlite3")
        registry = SourceDiscoveryRegistry(control)
        row = registry.get_source_run_outcome(
            candidate.source_key,
            reservoir_id=reservoir_id,
            lease_id=lease_id,
            baseline_signature=baseline_signature,
            model_signature=model_signature,
        )
        control.close()
        return row

    def test_closed_direct_run_publishes_positive_final_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                baseline,
                model,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            ) = self.prepare_run(root, name="positive")
            runtime_root = root / "runtime"

            control = ControlStore(runtime_root / "control.sqlite3")
            control.attribute_direct_host_years(
                [("novel.org", 1997, f"direct:{candidate.source_key}")],
                source_key=candidate.source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
            )
            control.close()
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put(
                self.direct_capsule(
                    "novel.org",
                    1997,
                    source_key=candidate.source_key,
                    payload="a",
                )
            )
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                runtime.sync_until_current()

            row = self.outcome(
                runtime_root,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            )
            self.assertIsNotNone(row)
            self.assertTrue(row.closed)
            self.assertEqual(row.accepted_host_years, 1)
            self.assertEqual(row.final_accepted_eed, 1.0)

    def test_completed_no_novelty_run_publishes_explicit_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                baseline,
                model,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            ) = self.prepare_run(root, name="zero")
            runtime_root = root / "runtime"

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                runtime.sync_until_current()

            row = self.outcome(
                runtime_root,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            )
            self.assertTrue(row.closed)
            self.assertEqual(row.accepted_host_years, 0)
            self.assertEqual(row.final_accepted_eed, 0.0)

    def test_inflight_evidence_prevents_premature_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                baseline,
                model,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            ) = self.prepare_run(root, name="inflight")
            runtime_root = root / "runtime"

            key = EvidenceQueryKey(
                "pending.org",
                TemporalScope(1997, 1997),
                "wayback",
                "evidence-v1",
            )
            control = ControlStore(runtime_root / "control.sqlite3")
            control.enqueue_evidence_tasks([key])
            control.record_evidence_task_origins(
                [key],
                source_key=candidate.source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
            )
            control.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                runtime.sync_until_current()

            pending = self.outcome(
                runtime_root,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            )
            self.assertFalse(pending.closed)
            self.assertFalse(pending.validation_complete)
            self.assertEqual(pending.evidence_tasks_created, 1)
            self.assertEqual(pending.evidence_tasks_terminal, 0)

            control = ControlStore(runtime_root / "control.sqlite3")
            control.finish_evidence_task(
                key,
                CDXQueryState.EMPTY_EXHAUSTIVE,
            )
            control.close()
            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
            ) as runtime:
                runtime.sync_until_current()

            closed = self.outcome(
                runtime_root,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            )
            self.assertTrue(closed.closed)
            self.assertEqual(closed.final_accepted_eed, 0.0)

    def test_readiness_cursor_lag_blocks_closure_and_retry_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (
                baseline,
                model,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            ) = self.prepare_run(root, name="cursor")
            runtime_root = root / "runtime"

            control = ControlStore(runtime_root / "control.sqlite3")
            control.attribute_direct_host_years(
                [
                    ("first.org", 1997, f"direct:{candidate.source_key}"),
                    ("second.net", 1998, f"direct:{candidate.source_key}"),
                ],
                source_key=candidate.source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
            )
            control.close()
            evidence = EvidenceStore(runtime_root / "evidence.sqlite3")
            evidence.put_many(
                [
                    self.direct_capsule(
                        "first.org",
                        1997,
                        source_key=candidate.source_key,
                        payload="b",
                    ),
                    self.direct_capsule(
                        "second.net",
                        1998,
                        source_key=candidate.source_key,
                        payload="c",
                    ),
                ]
            )
            evidence.close()

            with IncrementalReadinessRuntime(
                runtime_root,
                baseline_index=baseline,
                eed_model=model,
                baseline_eed="20",
                batch_size=1,
            ) as runtime:
                first = runtime.sync_once()
                self.assertLess(
                    first.evidence_cursor,
                    first.latest_evidence_sequence,
                )
                mid = SourceDiscoveryRegistry(runtime._control_store())
                mid_row = mid.get_source_run_outcome(
                    candidate.source_key,
                    reservoir_id=reservoir_id,
                    lease_id=lease_id,
                    baseline_signature=baseline_signature,
                    model_signature=model_signature,
                )
                self.assertFalse(mid_row.closed)

                final = runtime.sync_until_current()
                self.assertEqual(
                    final.evidence_cursor,
                    final.latest_evidence_sequence,
                )
                stable = runtime.sync_until_current()
                self.assertEqual(stable.evidence_cursor, final.evidence_cursor)

            row = self.outcome(
                runtime_root,
                candidate,
                reservoir_id,
                lease_id,
                baseline_signature,
                model_signature,
            )
            self.assertTrue(row.closed)
            self.assertEqual(row.accepted_host_years, 2)

            control = ControlStore(runtime_root / "control.sqlite3")
            projection = control.connection.execute(
                """
                SELECT closed_runs
                FROM source_final_rewards
                WHERE source_key = ?
                """,
                (candidate.source_key,),
            ).fetchone()
            self.assertEqual(int(projection["closed_runs"]), 1)
            control.close()


if __name__ == "__main__":
    unittest.main()
