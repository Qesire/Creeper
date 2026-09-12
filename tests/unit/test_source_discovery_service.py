from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import CommandAgentSearchPolicy
from creeper.source_discovery.manager import SourcePoolTargets
from creeper.source_discovery.scrapy_scout import ScrapyStructuralScoutPolicy
from creeper.source_discovery.triage import HttpTriagePolicy
from creeper.source_discovery_service import (
    AgentConfig,
    CoordinatorConfig,
    SourceDiscoveryServiceConfig,
    load_source_discovery_config,
    run_source_discovery_cycles,
)


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
triage_batch = 8
scout_parallelism = 2
max_search_directives = 3

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
direct_min_expected_volume = 12345
min_enumerability_prior = 0.6
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
        self.assertEqual(config.coordinator.triage_parallelism, 6)
        self.assertEqual(config.coordinator.scout_parallelism, 2)
        self.assertEqual(config.scrapy.max_pages, 20)
        self.assertFalse(config.scrapy.follow_query)
        self.assertEqual(\n            config.agent.command,\n            ("python", str((self.root / "agent.py").resolve())),\n        )
        self.assertEqual(config.agent.policy.max_returned_candidates, 17)
        self.assertEqual(config.agent.admission.min_expected_volume, 123456)
        self.assertEqual(config.agent.admission.direct_min_expected_volume, 12345)
        self.assertEqual(config.agent.admission.min_enumerability_prior, 0.6)

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
    async def test_empty_registry_runs_parallel_agent_refill_and_reports_inventory(self) -> None:
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
                '''import argparse
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("--request", required=True)
p.add_argument("--response", required=True)
a = p.parse_args()
request = json.loads(Path(a.request).read_text(encoding="utf-8"))
strategy = request["strategy"]
payload = {
    "query": f"fake:{strategy}",
    "candidates": [{
        "canonical_entrypoint": f"https://example.invalid/{strategy.lower()}/",
        "source_family": "TEST_META",
        "level": "METASOURCE",
        "expected_year_from": 1996,
        "expected_year_to": 2001,
        "expected_volume": 100000,
        "temporal_semantics_prior": 0.5,
        "enumerability_prior": 0.9,
        "baseline_overlap_prior": 0.5,
        "confidence": 0.7
    }]
}
Path(a.response).write_text(json.dumps(payload), encoding="utf-8")
''',
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
            )

            reports = await run_source_discovery_cycles(config, cycles=1)

            self.assertEqual(len(reports), 1)
            report = reports[0]
            self.assertEqual(report["cycle"], 1)
            self.assertGreaterEqual(report["elapsed_seconds"], 0.0)
            self.assertEqual(report["search_episodes"], 3)
            self.assertEqual(report["search_candidates_registered"], 3)
            self.assertEqual(report["inventory"]["DISCOVERED"], 3)
            invocation_root = root / "runtime" / "source-discovery" / "agent-invocations"
            invocation_dirs = [path for path in invocation_root.iterdir() if path.is_dir()]
            self.assertEqual(len(invocation_dirs), 3)
            for invocation in invocation_dirs:
                request = __import__("json").loads(
                    (invocation / "request.json").read_text(encoding="utf-8")
                )
                self.assertEqual(request["admission"]["min_expected_volume"], 100000)
                self.assertEqual(request["admission"]["direct_min_expected_volume"], 10000)
                self.assertTrue(request["requirements"]["prefer_direct_evidence_bulk"])
                audit = __import__("json").loads(
                    (invocation / "admission.json").read_text(encoding="utf-8")
                )
                self.assertEqual(audit["accepted_count"], 1)
                self.assertEqual(audit["rejected_count"], 0)


if __name__ == "__main__":
    unittest.main()
