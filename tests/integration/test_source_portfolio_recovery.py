from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.arquivo_catalog_scout import (
    AUDITED_ARQUIVO_CATALOG_URL,
    ArquivoCatalogFetch,
    ArquivoCatalogScoutExecutor,
)
from creeper.source_discovery.coordinator import (
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageResult,
)
from creeper.source_discovery.curated_seeds import curated_direct_catalogs
from creeper.source_discovery.manager import (
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.source_discovery.models import (
    ScoutMeasurement,
    SourceCandidate,
    SourceLevel,
    SourceState,
    SuppressionScope,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.saturation import SourceSaturationController
from creeper.source_discovery.scout_router import SourceScoutRouter
from creeper.source_discovery_service import (
    _requeue_unexpanded_audited_arquivo_catalog,
)
from creeper.storage.control_store import ControlStore


class SourcePortfolioRecoveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    LOC_ORIGIN = "https://data.labs.loc.gov"

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.control = ControlStore(self.root / "control.sqlite3")
        self.registry = SourceDiscoveryRegistry(self.control)
        self.registry.set_scout_authority(
            baseline_signature="baseline-v4",
            model_signature="eed-v4",
        )

    def tearDown(self) -> None:
        self.control.close()
        self.tmp.cleanup()

    @classmethod
    def _loc_candidate(cls, index: int) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=(
                f"{cls.LOC_ORIGIN}/us-elections/shard-{index:05d}.csv"
            ),
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="loc-manifest-expansion",
            discovery_strategy="DETERMINISTIC_LINK_EXPANSION",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=100_000,
            temporal_semantics_prior=1.0,
            enumerability_prior=1.0,
            direct_evidence_prior=1.0,
            baseline_overlap_prior=0.5,
            access_cost_prior=0.2,
            adapter_cost_prior=0.2,
            confidence=1.0,
            state=SourceState.DISCOVERED,
        )

    def _register_zero_yield_hold(self, index: int) -> SourceCandidate:
        candidate = self._loc_candidate(index)
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        self.registry.transition(candidate.source_key, SourceState.SCOUT_READY)
        self.registry.transition(candidate.source_key, SourceState.SCOUTING)
        self.registry.record_scout_measurement(
            candidate.source_key,
            ScoutMeasurement(
                sampled_records=10,
                unique_hosts=10,
                novel_hosts=0,
                direct_host_years=0,
                requests=1,
                bytes_read=128,
                elapsed_seconds=0.1,
                novel_eed=0.0,
            ),
            baseline_signature="baseline-v4",
            model_signature="eed-v4",
        )
        return self.registry.transition(
            candidate.source_key,
            SourceState.HOLD,
        )

    def _register_scout_ready(self, index: int) -> SourceCandidate:
        candidate = self._loc_candidate(index)
        self.registry.register_proposal(candidate)
        self.registry.transition(candidate.source_key, SourceState.TRIAGED)
        return self.registry.transition(
            candidate.source_key,
            SourceState.SCOUT_READY,
        )

    async def test_production_starvation_fixture_recovers_source_portfolio(self) -> None:
        # Reproduce the frozen production shape: measured zero-yield siblings
        # plus a much larger same-origin cold backlog.
        measured = [
            self._register_zero_yield_hold(index)
            for index in range(1_164)
        ]
        pending = [
            self._register_scout_ready(1_164 + index)
            for index in range(2_353)
        ]

        arquivo_parent = curated_direct_catalogs()[0]
        self.assertEqual(
            arquivo_parent.canonical_entrypoint,
            AUDITED_ARQUIVO_CATALOG_URL,
        )
        self.registry.register_proposal(arquivo_parent)
        self.registry.transition(
            arquivo_parent.source_key,
            SourceState.HOLD,
        )

        # Restart migration: legacy HOLD parent gets exactly one deterministic
        # catalog opportunity until a catalog edge is durably committed.
        self.assertEqual(
            _requeue_unexpanded_audited_arquivo_catalog(self.registry),
            1,
        )

        async def forbidden_triage(_candidate: SourceCandidate) -> TriageResult:
            raise AssertionError("no triage I/O is needed in this fixture")

        async def forbidden_structural(_candidate: SourceCandidate):
            raise AssertionError("audited Arquivo must use dedicated executor")

        async def forbidden_measured(_candidate: SourceCandidate):
            raise AssertionError(
                "saturated LoC siblings must not consume scout slots"
            )

        catalog_calls: list[str] = []

        async def catalog_fetcher(
            url: str,
            _max_bytes: int,
            _timeout_seconds: float,
        ) -> ArquivoCatalogFetch:
            catalog_calls.append(url)
            return ArquivoCatalogFetch(
                body=(
                    b'<a href="one.cdxj">one</a> '
                    b'<a href="two.cdxj">two</a> '
                    b'<a href="three.cdxj">three</a>'
                ),
                status_code=200,
                final_url=AUDITED_ARQUIVO_CATALOG_URL,
            )

        scout = SourceScoutRouter(
            structural_executor=forbidden_structural,
            measured_executor=forbidden_measured,
            arquivo_catalog_executor=ArquivoCatalogScoutExecutor(
                fetcher=catalog_fetcher,
            ),
        )
        search_strategies: list[str] = []

        async def search(directive):
            search_strategies.append(directive.strategy)
            return SearchBatch(
                backend="fixture",
                query=f"fixture:{directive.strategy}",
                actor="fixture-agent",
                candidates=(),
                search_cost_seconds=0.001,
            )

        manager = SourceReservoirManager(
            self.registry,
            targets=SourcePoolTargets(
                active_min=2,
                active_target=3,
                warm_min=10,
                warm_target=20,
                cold_min=50,
                cold_target=100,
                max_cold_credit_per_origin=12,
                triage_batch=16,
                scout_parallelism=4,
                max_search_directives=3,
            ),
            search_cooldown_seconds=0.0,
        )
        coordinator = SourceDiscoveryCoordinator(
            self.registry,
            manager,
            lock_path=self.root / "coordinator.lock",
            triage_executor=forbidden_triage,
            scout_executor=scout,
            search_executor=search,
            scout_authority=("baseline-v4", "eed-v4"),
            saturation_controller=SourceSaturationController(self.registry),
            triage_parallelism=2,
            scout_parallelism=4,
            search_parallelism=3,
        )

        report = await coordinator.run_once()

        # The zero-yield LoC origin is suppressed before planning in the same
        # cycle, while its rows and measurements remain fully auditable.
        self.assertEqual(report.saturated_origins, 1)
        self.assertEqual(report.saturation_updates, 1)
        self.assertIsNotNone(self.registry.suppression_reason(pending[0]))
        self.assertIsNotNone(self.registry.suppression_reason(measured[0]))
        self.assertEqual(
            self.registry.inventory()[SourceState.SCOUT_READY],
            2_353,
        )
        self.assertEqual(
            self.control.connection.execute(
                """
                SELECT COUNT(*)
                FROM source_suppressions
                WHERE scope_type = ? AND scope_key = 'BULK_ARTIFACT'
                """,
                (SuppressionScope.FAMILY.value,),
            ).fetchone()[0],
            0,
        )

        # Raw same-origin inventory no longer blocks search. The direct refill
        # arm executes even though 2,353 suppressed SCOUT_READY rows still
        # exist durably in the registry.
        self.assertIn("DIRECT_EVIDENCE_BULK", search_strategies)
        self.assertGreater(report.search_episodes, 0)
        self.assertLess(report.effective_cold_count, 50)

        # The legacy HOLD Arquivo parent is routed to the exact deterministic
        # catalog executor, not the generic structural parser.
        self.assertEqual(catalog_calls, [AUDITED_ARQUIVO_CATALOG_URL])
        self.assertEqual(report.scout_children_registered, 3)
        children = self.registry.children(arquivo_parent.source_key)
        self.assertEqual(len(children), 3)
        for child_key in children:
            child = self.registry.get_candidate(child_key)
            self.assertIsNotNone(child)
            self.assertEqual(child.source_family, "BULK_ARTIFACT")
            self.assertEqual(child.level, SourceLevel.SOURCE)
            self.assertEqual(child.direct_evidence_prior, 1.0)

        # The durable catalog edge is the startup idempotency marker: a restart
        # does not immediately re-fetch/re-register the same catalog.
        self.assertEqual(
            _requeue_unexpanded_audited_arquivo_catalog(self.registry),
            0,
        )


if __name__ == "__main__":
    unittest.main()
