from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.scheduler.leases import LeaseState, WorkLease
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore


EXPIRED = "EXPIRED"


class ProductionExposureRecoveryTests(unittest.TestCase):
    def test_recovery_expires_open_exposure_after_lease_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "control.sqlite3"
            first = ControlStore(path, clock=lambda: 200.0)
            first.save_domain(
                SourceDomain(
                    domain_id="domain:a",
                    family="fixture",
                    discovery_mechanism="test",
                    temporal_scope=(1996, 2001),
                    state=DomainState.EXPLORING,
                )
            )
            first.save_reservoir(
                Reservoir(
                    reservoir_id="reservoir:a",
                    domain_id="domain:a",
                    adapter_id="fixture",
                    root_locator="fixture://a",
                    enumeration_kind="finite_list",
                    capacity_lower=1,
                    state=ReservoirState.READY,
                )
            )
            lease = WorkLease.create(
                reservoir_id="reservoir:a",
                max_records=10,
                max_requests=10,
                max_bytes=1000,
                max_seconds=10,
                now=100.0,
                expires_at=150.0,
            ).grant(owner="worker")
            first.save_lease(lease)
            running = lease.start()
            first.save_lease(running)
            exposure = first.begin_production_exposure(
                source_key="source:a",
                reservoir_id="reservoir:a",
                lease_id=lease.lease_id,
                lane="sequential",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
            )
            first.close()

            second = ControlStore(path, clock=lambda: 200.0)
            self.assertEqual(second.recover_expired_leases(now=200.0), 1)
            self.assertEqual(second.recover_open_production_exposures(), 1)
            self.assertEqual(second.recover_open_production_exposures(), 0)
            stored = second.get_production_exposure(exposure.exposure_id)
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored.state, EXPIRED)
            self.assertEqual(stored.final_accepted_eed, 0.0)
            self.assertEqual(second.get_lease(lease.lease_id).state, LeaseState.EXPIRED)
            second.close()

    def test_source_run_compatibility_projection_only_exposes_final_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            registry = SourceDiscoveryRegistry(control)
            candidate_key = "source:compat"
            control.connection.execute(
                """
                INSERT INTO source_candidates(
                    source_key, canonical_entrypoint, source_family, source_level,
                    discovered_by, discovery_strategy, temporal_semantics_prior,
                    enumerability_prior, direct_evidence_prior, baseline_overlap_prior,
                    access_cost_prior, adapter_cost_prior, confidence, state,
                    created_at, updated_at
                ) VALUES (?, ?, 'fixture', 'SOURCE', 'test', 'test', 1, 1, 1, 0, 1, 1, 1,
                          'DISCOVERED', 1, 1)
                """,
                (candidate_key, "https://compat.example/source"),
            )
            control.connection.commit()
            registry.begin_source_run(
                candidate_key,
                reservoir_id="reservoir:compat",
                lease_id="lease:compat",
                baseline_signature="baseline:v1",
                model_signature="model:v1",
            )
            self.assertEqual(
                registry.list_source_run_outcomes(
                    candidate_key, closed_only=True
                ),
                [],
            )
            control.close()


if __name__ == "__main__":
    unittest.main()
