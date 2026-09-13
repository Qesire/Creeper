from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import (
    baseline_authority_signature,
    eed_model_authority_signature,
)
from creeper.evidence.policies import (
    EvidenceCapsule,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.historical_index_service import (
    HistoricalIndexOptimizerConfig,
    HistoricalIndexOptimizerRuntime,
)
from creeper.runtime.exposure import ProductionExposureState
from creeper.runtime.readiness import IncrementalReadinessRuntime
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    RegionState,
    compile_candidate_index_space,
)
from creeper.source_discovery.models import (
    SourceCandidate,
    SourceLevel,
    SourceState,
)
from creeper.source_discovery.production_value import ProductionValueModel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import (
    EvidenceStore,
    EvidenceTaskProvenance,
)


class HistoricalProductionExposureIntegrationTests(
    unittest.IsolatedAsyncioTestCase
):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.runtime_root = self.root / "runtime"
        self.runtime_root.mkdir()

        task_root = self.root / "task"
        baseline_root = task_root / "merged260913-3"
        baseline_root.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_root / "candidate_pool.txt").write_text(
            "", encoding="utf-8"
        )
        self.baseline_path = self.root / "baseline.sqlite3"
        BaselineIndex.build(task_root, self.baseline_path).close()
        self.model_path = self.root / "eed-model.json"
        self.model_path.write_text(
            json.dumps({"tld": ["com"], "lang": ["eng"], "perc_of_tld": ["100"]}),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _prepare_index(self) -> tuple[Path, SourceCandidate, str, str]:
        path = self.root / "historical.cdxj"
        path.write_text(
            'com,novel)/ 19970101000000 {"url":"http://novel.com/"}\n',
            encoding="utf-8",
        )
        candidate = SourceCandidate(
            canonical_entrypoint="https://historical.example/historical.cdxj",
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="integration",
            discovery_strategy="historical-production",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=1,
            direct_evidence_prior=1.0,
            enumerability_prior=1.0,
            confidence=1.0,
            state=SourceState.ACTIVE,
        )
        control = ControlStore(self.runtime_root / "control.sqlite3")
        try:
            SourceDiscoveryRegistry(control).register_proposal(candidate)
            domain = SourceDomain(
                domain_id="domain:historical-production",
                family="BULK_ARTIFACT",
                discovery_mechanism="integration",
                temporal_scope=(1996, 2001),
                state=DomainState.EXPLORING,
            )
            reservoir = Reservoir(
                reservoir_id="reservoir:historical-production",
                domain_id=domain.domain_id,
                adapter_id="structured",
                root_locator=str(path),
                enumeration_kind="structured_records",
                capacity_lower=1,
                capacity_upper=1,
                evidence_mode="direct_year",
                state=ReservoirState.READY,
            )
            control.save_activation(
                source_key=candidate.source_key,
                domain=domain,
                reservoir=reservoir,
                adapter_kind="structured",
                config_hash="historical-config-v1",
            )
            compiled = compile_candidate_index_space(
                candidate,
                content_length=path.stat().st_size,
                direct_evidence_authority=True,
            )
            compiled = replace(
                compiled,
                index=replace(compiled.index, locator=str(path)),
                root_region=replace(compiled.root_region, locator=str(path)),
            )
            indexes = IndexSpaceRegistry(control)
            indexes.register_index_space(compiled)
            indexes.mark_region_state(
                compiled.root_region.region_key,
                RegionState.HARVEST_READY,
            )
            baseline_signature = baseline_authority_signature(self.baseline_path)
            model_signature = eed_model_authority_signature(self.model_path)
            SourceDiscoveryRegistry(control).set_scout_authority(
                baseline_signature=baseline_signature,
                model_signature=model_signature,
            )
            return (
                path,
                candidate,
                compiled.root_region.region_key,
                reservoir.reservoir_id,
            )
        finally:
            control.close()

    def _config(self) -> HistoricalIndexOptimizerConfig:
        from creeper.source_discovery.harvest import RegionHarvestPolicy

        return HistoricalIndexOptimizerConfig(
            runtime_data_root=self.runtime_root,
            baseline_index=self.baseline_path,
            eed_model=self.model_path,
            enabled=True,
            harvest=RegionHarvestPolicy(
                baseline_batch_size=1,
                max_records_per_lease=100,
            ),
        )

    async def test_historical_region_final_reward_reaches_production_value(self) -> None:
        _path, candidate, region_key, reservoir_id = self._prepare_index()
        async with HistoricalIndexOptimizerRuntime(
            self._config(), owner="historical-integration"
        ) as runtime:
            claimed = runtime._claim_harvest_reservoirs(
                [(runtime.index_registry.get_index_for_source(candidate.source_key),
                  runtime.control.get_reservoir(reservoir_id))]
            )
            self.assertEqual(len(claimed), 1)
            exposure = runtime._begin_historical_exposure(
                candidate.source_key,
                region_key,
                claimed[0][2],
            )
            report = runtime.harvest_executor.harvest(
                region_key,
                exposure_id=exposure.exposure_id,
            )
            self.assertIsNotNone(report)
            self.assertTrue(report.completed)
            stored = runtime.control.get_production_exposure(
                exposure.exposure_id
            )
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored.state, ProductionExposureState.READ_COMPLETE)
            self.assertEqual(stored.lane, "historical_region")
            self.assertEqual(stored.source_records, 1)
            self.assertGreater(stored.source_bytes, 0)
            runtime._release_harvest_reservoirs(claimed)

            with IncrementalReadinessRuntime(
                self.runtime_root,
                baseline_index=self.baseline_path,
                eed_model=self.model_path,
                baseline_eed="10",
            ) as readiness:
                readiness.sync_until_current()

            final = runtime.control.get_production_exposure(
                exposure.exposure_id
            )
            self.assertIsNotNone(final)
            assert final is not None
            self.assertEqual(final.state, ProductionExposureState.FINAL_CLOSED)
            self.assertEqual(final.accepted_host_years, 1)
            self.assertGreater(final.final_accepted_eed, 0.0)

            lineage = runtime.evidence.connection.execute(
                """
                SELECT source_key, reservoir_id, lease_id
                FROM evidence_capsule_task_provenance
                WHERE hostname = 'novel.com' AND year = 1997
                """
            ).fetchone()
            self.assertIsNotNone(lineage)
            assert lineage is not None
            self.assertEqual(lineage["source_key"], candidate.source_key)
            self.assertEqual(lineage["reservoir_id"], reservoir_id)
            self.assertEqual(lineage["lease_id"], exposure.exposure_id)

            origins = runtime.control.connection.execute(
                """
                SELECT source_key, reservoir_id, origin_kind, origin_unit_id
                FROM evidence_host_year_origin_units
                WHERE hostname = 'novel.com' AND year = 1997
                """
            ).fetchone()
            self.assertEqual(
                tuple(origins),
                (candidate.source_key, reservoir_id, "region_harvest", region_key),
            )

            registry = SourceDiscoveryRegistry(runtime.control)
            runs = registry.list_source_run_outcomes(
                candidate.source_key, closed_only=True
            )
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0].exposure_id, exposure.exposure_id)
            estimate = ProductionValueModel(registry).estimate(candidate)
            self.assertEqual(estimate.closed_runs, 1)
            self.assertGreater(estimate.recent_marginal_eed_per_second, 0.0)
    async def test_committed_region_proof_recovery_is_terminal_and_idempotent(self) -> None:
        _path, candidate, region_key, reservoir_id = self._prepare_index()
        control_path = self.runtime_root / "control.sqlite3"
        control = ControlStore(control_path, clock=lambda: 200.0)
        ownership = control.grant_fresh_lease(
            reservoir_id,
            owner="historical-crashed",
            max_records=1,
            max_requests=1,
            max_bytes=100,
            max_seconds=10,
            resource_class="historical-index",
            expected_evidence_tasks=0,
            expected_novel_eed=0.0,
            now=100.0,
            lease_ttl_seconds=50.0,
        )
        self.assertIsNotNone(ownership)
        assert ownership is not None
        running = ownership.start()
        control.save_lease(running)
        exposure = control.begin_production_exposure(
            source_key=candidate.source_key,
            reservoir_id=reservoir_id,
            lease_id="exposure:crashed-region",
            task_id=ownership.lease_id,
            lane="historical_region",
            baseline_signature=baseline_authority_signature(self.baseline_path),
            model_signature=eed_model_authority_signature(self.model_path),
            exposure_id="exposure:crashed-region",
        )
        control.connection.execute(
            """
            INSERT INTO source_run_outcomes(
                source_key, reservoir_id, lease_id, exposure_id,
                baseline_signature, model_signature, read_started,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate.source_key,
                reservoir_id,
                exposure.exposure_id,
                exposure.exposure_id,
                exposure.baseline_signature,
                exposure.model_signature,
                100.0,
                100.0,
                100.0,
            ),
        )
        control.connection.commit()
        control.close()

        evidence = EvidenceStore(self.runtime_root / "evidence.sqlite3")
        capsule = EvidenceCapsule(
            hostname="novel.com",
            year=1997,
            provider="direct:historical",
            temporal_semantics="source_direct_year",
            evidence_timestamp="19970101000000",
            source_locator="fixture://historical/novel.com/1997",
            payload_hash="a" * 64,
            policy_version="historical-region-v1",
            source_id=candidate.source_key,
        )
        key = EvidenceQueryKey(
            "novel.com",
            TemporalScope(1997, 1997),
            capsule.provider,
            capsule.policy_version,
        )
        evidence.put_many_with_task_provenance(
            [
                (
                    capsule,
                    EvidenceTaskProvenance(
                        key,
                        source_key=candidate.source_key,
                        reservoir_id=reservoir_id,
                        lease_id=exposure.exposure_id,
                        committed_at=101.0,
                    ),
                )
            ]
        )
        evidence.close()

        async with HistoricalIndexOptimizerRuntime(
            self._config(), owner="historical-restarted"
        ) as runtime:
            self.assertEqual(runtime._recover_historical_exposures(now=200.0), 0)
            stored = runtime.control.get_production_exposure(
                exposure.exposure_id
            )
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored.state, ProductionExposureState.EXPIRED)
            self.assertEqual(stored.final_accepted_eed, 0.0)
            self.assertEqual(runtime.evidence.host_year_count(), 1)
if __name__ == "__main__":
    unittest.main()
