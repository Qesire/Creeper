from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import CommandAgentSearchPolicy
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import RegionState, compile_candidate_index_space
from creeper.source_discovery.manager import SourcePoolTargets
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.residual_search import ResidualSearchLedger, SearchCell
from creeper.source_discovery.search_identity import (
    RawSearchResult,
    SearchIdentityLedger,
    canonicalize_search_result,
)
from creeper.source_discovery.scrapy_scout import ScrapyStructuralScoutPolicy
from creeper.source_discovery.triage import HttpTriagePolicy
from creeper.source_discovery_service import (
    AgentConfig,
    CoordinatorConfig,
    ResidualSearchConfig,
    SourceDiscoveryServiceConfig,
    load_source_discovery_config,
    run_source_discovery_cycles,
)
from creeper.storage.telemetry_store import RuntimeTelemetryStore
from creeper.storage.control_store import ControlStore
from creeper.source_discovery_service import _publish_discovery_telemetry


class SourceDiscoveryServiceConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.scrapy = self.root / "scrapy"
        self.scrapy.mkdir()
        (self.scrapy / "pyproject.toml").write_text(
            "[project]\nname='sidecar'\nversion='0'\n", encoding="utf-8"
        )
        (self.scrapy / "uv.lock").write_text("version = 1\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_config(self, *, follow_query: str = "false") -> Path:
        config = self.root / "discovery.toml"
        config.write_text(
            f'''runtime_data_root = "runtime"
scrapy_project_dir = "scrapy"

[pool]
active_min = 1
active_target = 2
warm_min = 3
warm_target = 4
cold_min = 5
cold_target = 7
max_cold_credit_per_origin = 9
triage_batch = 8
scout_parallelism = 2
max_search_directives = 3

[saturation]
min_measured_siblings = 11
min_total_observations = 25
max_total_novel_eed_for_zero_class = 0.25
suppression_ttl_seconds = 1800.0

[coordinator]
triage_parallelism = 6
scout_parallelism = 2
search_parallelism = 3
failure_retry_seconds = 15.0

[triage]
timeout_seconds = 4.0

[scrapy]
max_pages = 20
max_depth = 1
max_seconds = 30
max_memory_mb = 256
follow_query = {follow_query}

[admission]
min_expected_volume = 123456
gateway_min_expected_volume = 54321
direct_min_expected_volume = 12345
min_enumerability_prior = 0.6
gateway_min_enumerability_prior = 0.75
min_confidence = 0.4
require_year_bounds = true

[agent]
command = ["python", "./agent.py"]
backend = "test-backend"
actor = "agent:test"
timeout_seconds = 11.0
termination_grace_seconds = 1.0
max_response_bytes = 4096
max_returned_candidates = 17
''',
            encoding="utf-8",
        )
        return config

    def test_config_resolves_paths_and_preserves_pipeline_limits(self) -> None:
        config = load_source_discovery_config(self.write_config())

        self.assertEqual(config.runtime_data_root, (self.root / "runtime").resolve())
        self.assertEqual(config.scrapy_project_dir, self.scrapy.resolve())
        self.assertEqual(config.pool.cold_target, 7)
        self.assertEqual(config.pool.max_cold_credit_per_origin, 9)
        self.assertEqual(config.saturation.min_measured_siblings, 11)
        self.assertEqual(config.saturation.min_total_observations, 25)
        self.assertEqual(
            config.saturation.max_total_novel_eed_for_zero_class,
            0.25,
        )
        self.assertEqual(config.saturation.suppression_ttl_seconds, 1800.0)
        self.assertEqual(config.coordinator.triage_parallelism, 6)
        self.assertEqual(config.coordinator.scout_parallelism, 2)
        self.assertEqual(config.scrapy.max_pages, 20)
        self.assertFalse(config.scrapy.follow_query)
        self.assertEqual(
            config.agent.command,
            ("python", str((self.root / "agent.py").resolve())),
        )
        self.assertEqual(config.agent.policy.max_returned_candidates, 17)
        self.assertEqual(config.agent.admission.min_expected_volume, 123456)
        self.assertEqual(
            config.agent.admission.gateway_min_expected_volume,
            54321,
        )
        self.assertEqual(config.agent.admission.direct_min_expected_volume, 12345)
        self.assertEqual(
            config.agent.admission.gateway_min_enumerability_prior,
            0.75,
        )
        self.assertEqual(config.agent.admission.min_enumerability_prior, 0.6)
        self.assertTrue(config.residual_search.enabled)
        self.assertEqual(
            config.residual_search.providers,
            ("datacite", "zenodo", "harvard_dataverse"),
        )

    def test_residual_search_config_is_explicit_and_fail_closed(self) -> None:
        path = self.write_config()
        with path.open("a", encoding="utf-8") as stream:
            stream.write(
                """
[residual_search]
enabled = true
providers = ["datacite", "zenodo", "harvard_dataverse"]
results_per_provider = 77
max_total_results = 155
min_relevance_score = 0.65
timeout_seconds = 9.0
"""
            )

        config = load_source_discovery_config(path)

        self.assertTrue(config.residual_search.enabled)
        self.assertEqual(
            config.residual_search.providers,
            ("datacite", "zenodo", "harvard_dataverse"),
        )
        self.assertEqual(config.residual_search.policy.results_per_provider, 77)
        self.assertEqual(config.residual_search.policy.max_total_results, 155)
        self.assertEqual(config.residual_search.policy.min_relevance_score, 0.65)

        text = path.read_text(encoding="utf-8").replace(
            'providers = ["datacite", "zenodo", "harvard_dataverse"]',
            'providers = ["unknown-provider"]',
        )
        path.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unsupported residual_search.providers"):
            load_source_discovery_config(path)

    def test_follow_query_rejects_string_truthiness(self) -> None:
        with self.assertRaisesRegex(ValueError, "scrapy.follow_query must be a boolean"):
            load_source_discovery_config(self.write_config(follow_query='"false"'))

    def test_invalid_pool_watermarks_fail_closed(self) -> None:
        path = self.write_config()
        text = path.read_text(encoding="utf-8").replace("cold_min = 5", "cold_min = 9")
        path.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "cold_min cannot exceed cold_target"):
            load_source_discovery_config(path)

    def test_missing_scrapy_project_is_rejected(self) -> None:
        path = self.write_config()
        text = path.read_text(encoding="utf-8").replace(
            'scrapy_project_dir = "scrapy"', 'scrapy_project_dir = "missing"'
        )
        path.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "scrapy_project_dir does not exist"):
            load_source_discovery_config(path)


class SourceDiscoveryServiceSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_registry_does_not_fall_back_to_ordinary_llm_refill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            sidecar = root / "sidecar"
            sidecar.mkdir()
            (sidecar / "pyproject.toml").write_text(
                "[project]\nname='sidecar'\nversion='0'\n", encoding="utf-8"
            )
            (sidecar / "uv.lock").write_text("version = 1\n", encoding="utf-8")

            agent = root / "agent.py"
            agent.write_text(
                "raise SystemExit('ordinary LLM refill must not run')\n",
                encoding="utf-8",
            )

            config = SourceDiscoveryServiceConfig(
                runtime_data_root=root / "runtime",
                scrapy_project_dir=sidecar,
                pool=SourcePoolTargets(
                    active_min=0,
                    active_target=0,
                    warm_min=0,
                    warm_target=0,
                    cold_min=3,
                    cold_target=3,
                    triage_batch=8,
                    scout_parallelism=2,
                    max_search_directives=3,
                ),
                coordinator=CoordinatorConfig(
                    triage_parallelism=2,
                    scout_parallelism=2,
                    search_parallelism=3,
                    failure_retry_seconds=5.0,
                ),
                triage=HttpTriagePolicy(timeout_seconds=1.0),
                scrapy=ScrapyStructuralScoutPolicy(
                    max_pages=2,
                    max_depth=0,
                    max_seconds=2,
                    max_memory_mb=128,
                    follow_query=False,
                ),
                agent=AgentConfig(
                    command=(sys.executable, str(agent)),
                    backend="fake-agent",
                    actor="agent:test",
                    cwd=root,
                    policy=CommandAgentSearchPolicy(
                        timeout_seconds=5.0,
                        termination_grace_seconds=1.0,
                        max_response_bytes=64 * 1024,
                        max_returned_candidates=8,
                    ),
                    admission=SearchAdmissionPolicy(
                        min_expected_volume=100000,
                        min_enumerability_prior=0.5,
                        min_confidence=0.35,
                    ),
                ),
                # Explicitly disable deterministic discovery to prove that a
                # cold deficit alone does not reactivate the legacy LLM URL
                # refill path.
                residual_search=ResidualSearchConfig(enabled=False),
            )

            reports = await run_source_discovery_cycles(config, cycles=1)

            self.assertEqual(len(reports), 1)
            report = reports[0]
            self.assertEqual(report["cycle"], 1)
            self.assertGreaterEqual(report["elapsed_seconds"], 0.0)
            self.assertEqual(report["search_episodes"], 0)
            self.assertEqual(report["search_candidates_registered"], 0)
            self.assertEqual(report["inventory"].get("DISCOVERED", 0), 0)
            invocation_root = (
                root / "runtime" / "source-discovery" / "agent-invocations"
            )
            self.assertFalse(invocation_root.exists())

            with RuntimeTelemetryStore(
                root / "runtime" / "telemetry.sqlite3"
            ) as telemetry:
                snapshot = telemetry.snapshot()

            self.assertEqual(snapshot.counters["discovery_cycles"], 1)
            self.assertEqual(
                snapshot.counters.get("discovery_search_episodes", 0),
                0,
            )
            self.assertEqual(
                snapshot.counters.get(
                    "discovery_search_candidates_registered",
                    0,
                ),
                0,
            )
            self.assertEqual(snapshot.gauges["active_candidates"], 0.0)
            self.assertEqual(snapshot.gauges["active_direct_sources"], 0.0)
            self.assertEqual(
                snapshot.gauges.get("source_candidates_discovered", 0.0),
                0.0,
            )


class ResidualDiscoveryTelemetryTests(unittest.TestCase):
    def test_residual_search_coverage_and_identity_are_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            control = ControlStore(root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                coverage = ResidualSearchLedger(registry.connection)
                cell = SearchCell(
                    mechanism="proxy_access",
                    institution="university",
                    period="1998",
                    artifact="trace",
                )
                coverage.ensure_cell(cell)
                coverage.record_episode(
                    cell,
                    result_count=2,
                    duplicate_results=0,
                    unique_roots=2,
                    new_families=2,
                    qualified_roots=1,
                    search_cost_seconds=0.1,
                )
                identities = SearchIdentityLedger(registry.connection)
                result = canonicalize_search_result(
                    RawSearchResult(
                        provider="fixture",
                        provider_result_id="r1",
                        url="https://example.edu/proxy98.zip",
                        title="1998 University Proxy Trace",
                        publisher="Example University",
                    ),
                    relevance_score=1.0,
                    qualified=True,
                )
                identities.register(cell_key=cell.key, result=result)

                _publish_discovery_telemetry(
                    registry,
                    {
                        "deterministic_search_episodes": 1,
                        "deterministic_search_failures": 0,
                    },
                )

                with RuntimeTelemetryStore(root / "telemetry.sqlite3") as telemetry:
                    snapshot = telemetry.snapshot()
                self.assertEqual(
                    snapshot.counters["discovery_deterministic_search_episodes"],
                    1,
                )
                self.assertEqual(snapshot.gauges["residual_search_cell_total"], 1.0)
                self.assertEqual(snapshot.gauges["residual_search_cell_active"], 1.0)
                self.assertEqual(snapshot.gauges["residual_search_unique_urls"], 1.0)
                self.assertEqual(snapshot.gauges["residual_search_unique_datasets"], 1.0)
                self.assertEqual(snapshot.gauges["residual_search_unique_families"], 1.0)
            finally:
                control.close()


class DiscoveryTelemetryStateTests(unittest.TestCase):
    def test_historical_and_platform_state_gauges_are_durable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            control = ControlStore(root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                index_registry = IndexSpaceRegistry(control)
                candidate = SourceCandidate(
                    canonical_entrypoint="https://archive.example/index.cdxj",
                    source_family="BULK_ARTIFACT",
                    level=SourceLevel.SOURCE,
                    discovered_by="test",
                    discovery_strategy="DIRECT_EVIDENCE_BULK",
                    expected_year_from=1996,
                    expected_year_to=2001,
                    expected_volume=100,
                    direct_evidence_prior=1.0,
                    enumerability_prior=1.0,
                    confidence=1.0,
                )
                compiled = compile_candidate_index_space(
                    candidate,
                    direct_evidence_authority=True,
                )
                index_registry.register_index_space(compiled)
                index_registry.mark_region_state(
                    compiled.root_region.region_key,
                    RegionState.HARVEST_READY,
                )
                index_registry.claim_region_for_harvest(
                    compiled.root_region.region_key,
                    owner="telemetry-test",
                    ttl_seconds=60.0,
                )
                control.enqueue_platform_year_harvest(
                    provider="wayback",
                    subject="example.org",
                    target_year=1997,
                    request_template_hash="template-v1",
                    policy_version="platform-v1",
                )

                _publish_discovery_telemetry(registry, {})

                with RuntimeTelemetryStore(root / "telemetry.sqlite3") as telemetry:
                    snapshot = telemetry.snapshot()
                self.assertEqual(snapshot.gauges["historical_index_total"], 1.0)
                self.assertEqual(snapshot.gauges["historical_region_harvesting"], 1.0)
                self.assertEqual(snapshot.gauges["platform_year_total"], 1.0)
                self.assertEqual(snapshot.gauges["platform_year_ready"], 1.0)
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
