from __future__ import annotations

from pathlib import Path
from threading import Event
import json
import tempfile
import unittest

from creeper.autopilot import (
    AutopilotConfig,
    EvidenceServicePolicy,
    ReadinessServicePolicy,
    ResourceGovernorPolicy,
    SupervisorPolicy,
    _desired_children,
    build_child_specs,
    load_autopilot_config,
    run_autopilot,
)
from creeper.runtime.resource_governor import GovernorState, ResourceSample
from creeper.source_cli import SourceProducerMode


class _FakeProcess:
    def __init__(self, *, exit_code=None):
        self.exit_code = exit_code
        self.terminated = False
        self.killed = False

    def poll(self):
        if self.terminated or self.killed:
            return 0
        return self.exit_code

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self.killed = True


class AutopilotTests(unittest.TestCase):
    def test_manual_static_topology_is_rejected_before_child_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(),
                evidence=EvidenceServicePolicy(),
                producer_mode=SourceProducerMode.STATIC,
                producer_mode_explicit=True,
            )
            with self.assertRaisesRegex(ValueError, "static producer"):
                build_child_specs(config)

    def _config(self, root: Path, **supervisor_changes):
        policy = SupervisorPolicy(**supervisor_changes)
        return AutopilotConfig(
            source_discovery_config=root / "discovery.toml",
            source_producer_config=root / "producer.toml",
            runtime_data_root=root / "runtime",
            supervisor=policy,
            evidence=EvidenceServicePolicy(),
        )

    def test_builds_three_isolated_service_processes(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp))
            specs = build_child_specs(config)

        self.assertEqual(
            [spec.name for spec in specs],
            ["source-discovery", "source-producer", "evidence-worker"],
        )
        evidence = specs[2].argv
        self.assertIn("--max-inflight", evidence)
        self.assertIn("4", evidence)
        self.assertIn("--max-connections", evidence)
        self.assertIn("8", evidence)
        self.assertIn("--max-keepalive-connections", evidence)
        self.assertIn("--keepalive-expiry-seconds", evidence)
        self.assertIn("30.0", evidence)
        self.assertIn("--requests-per-second", evidence)
        self.assertIn("0.5", evidence)

    def test_platform_harvest_uses_independent_supervised_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(),
                evidence=EvidenceServicePolicy(
                    platform_harvest_enabled=True,
                    platform_claim_batch_size=2,
                    platform_requests_per_second=0.125,
                    platform_max_connections=3,
                ),
            )
            specs = build_child_specs(config)

        platform = next(
            spec for spec in specs if spec.name == "platform-year-harvest"
        )
        self.assertIn("creeper.platform_harvest_cli", platform.argv)
        self.assertEqual(
            platform.argv[platform.argv.index("--claim-batch-size") + 1],
            "2",
        )
        self.assertEqual(
            platform.argv[platform.argv.index("--requests-per-second") + 1],
            "0.125",
        )
        self.assertEqual(
            platform.argv[platform.argv.index("--max-connections") + 1],
            "3",
        )
        throttled = _desired_children(
            GovernorState.THROTTLED,
            {spec.name for spec in specs},
        )
        self.assertNotIn("platform-year-harvest", throttled)
        self.assertIn("evidence-worker", throttled)

    def test_multiple_source_workers_are_independent_children(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(),
                evidence=EvidenceServicePolicy(),
                source_producer_workers=3,
            )

            specs = build_child_specs(config)

        self.assertEqual(
            [spec.name for spec in specs],
            [
                "source-discovery",
                "source-producer-1",
                "source-producer-2",
                "source-producer-3",
                "evidence-worker",
            ],
        )
        owners = [
            spec.argv[spec.argv.index("--owner") + 1]
            for spec in specs
            if spec.name.startswith("source-producer")
        ]
        self.assertEqual(
            owners,
            ["source-producer-1", "source-producer-2", "source-producer-3"],
        )

    def test_historical_index_adds_supervised_service_and_throttles_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model = root / "eed-model.json"
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(),
                evidence=EvidenceServicePolicy(),
                historical_index_enabled=True,
                historical_index_eed_model=model,
            )

            specs = build_child_specs(config)

        self.assertEqual(
            [spec.name for spec in specs],
            [
                "source-discovery",
                "historical-index",
                "source-producer",
                "evidence-worker",
            ],
        )
        optimizer = specs[1].argv
        self.assertIn("creeper.historical_index_service", optimizer)
        self.assertIn("--eed-model", optimizer)
        self.assertIn(str(model), optimizer)
        desired = _desired_children(
            GovernorState.THROTTLED,
            {spec.name for spec in specs},
        )
        self.assertNotIn("source-discovery", desired)
        self.assertNotIn("historical-index", desired)
        self.assertIn("source-producer", desired)
        self.assertIn("evidence-worker", desired)

    def test_readiness_adds_fourth_isolated_service_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(),
                evidence=EvidenceServicePolicy(),
                baseline_index=root / "baseline.sqlite3",
                readiness=ReadinessServicePolicy(
                    eed_model=root / "eed-model.json",
                    baseline_eed="35266393.8852",
                    batch_size=1234,
                    max_batches_per_cycle=7,
                    poll_seconds=11.0,
                ),
            )
            specs = build_child_specs(config)

        self.assertEqual(
            [spec.name for spec in specs],
            [
                "source-discovery",
                "source-producer",
                "evidence-worker",
                "readiness-worker",
            ],
        )
        readiness = specs[3].argv
        self.assertIn("--baseline-index", readiness)
        self.assertIn(str(root / "baseline.sqlite3"), readiness)
        self.assertIn("--eed-model", readiness)
        self.assertIn(str(root / "eed-model.json"), readiness)
        self.assertIn("--baseline-eed", readiness)
        self.assertIn("35266393.8852", readiness)
        self.assertIn("1234", readiness)
        self.assertIn("7", readiness)
        self.assertIn("11.0", readiness)

    def test_supervisor_stops_all_children_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(Path(tmp), poll_seconds=0.01)
            stop = Event()
            processes = []

            def spawn(_argv):
                process = _FakeProcess()
                processes.append(process)
                return process

            run_autopilot(
                config,
                stop_event=stop,
                popen_factory=spawn,
                sleep_fn=lambda _seconds: stop.set(),
                monotonic=lambda: 0.0,
            )

        self.assertEqual(len(processes), 3)
        self.assertTrue(all(process.terminated for process in processes))

    def test_resource_governor_degrades_and_recovers_child_set_without_restart_penalty(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(
                    poll_seconds=0.01,
                    shutdown_grace_seconds=0.01,
                ),
                evidence=EvidenceServicePolicy(),
                resource_governor=ResourceGovernorPolicy(
                    rss_throttle_bytes=100,
                    rss_stop_bytes=200,
                    disk_throttle_bytes=100,
                    disk_stop_bytes=50,
                    recovery_samples=2,
                ),
            )
            stop = Event()
            spawned: list[str] = []
            terminated: list[str] = []

            class NamedProcess(_FakeProcess):
                _next_pid = 10_000

                def __init__(self, name):
                    super().__init__()
                    self.name = name
                    self.pid = NamedProcess._next_pid
                    NamedProcess._next_pid += 1

                def terminate(self):
                    terminated.append(self.name)
                    super().terminate()

            def spawn(argv):
                if "creeper.source_discovery_service" in argv:
                    name = "source-discovery"
                elif "creeper.source_cli" in argv:
                    name = "source-producer"
                elif "creeper.evidence_cli" in argv:
                    name = "evidence-worker"
                else:
                    name = "other"
                spawned.append(name)
                return NamedProcess(name)

            samples = iter(
                [
                    ResourceSample(rss_bytes=10, disk_free_bytes=500),
                    ResourceSample(rss_bytes=150, disk_free_bytes=500),
                    ResourceSample(rss_bytes=10, disk_free_bytes=90),
                    ResourceSample(rss_bytes=10, disk_free_bytes=500),
                    ResourceSample(rss_bytes=10, disk_free_bytes=500),
                ]
            )

            calls = [0]

            def sleep(_seconds):
                calls[0] += 1
                if calls[0] >= 5:
                    stop.set()

            run_autopilot(
                config,
                stop_event=stop,
                popen_factory=spawn,
                sleep_fn=sleep,
                monotonic=lambda: float(calls[0]),
                wall_clock=lambda: 1234.0 + calls[0],
                resource_sample_fn=lambda _pids: next(samples),
            )

            self.assertEqual(spawned.count("source-discovery"), 2)
            self.assertEqual(spawned.count("source-producer"), 2)
            self.assertEqual(spawned.count("evidence-worker"), 2)
            # Discovery is stopped at THROTTLED; producer/evidence are stopped
            # only when the lower disk watermark enters DRAIN_ONLY.
            self.assertGreaterEqual(terminated.count("source-discovery"), 2)
            self.assertGreaterEqual(terminated.count("source-producer"), 2)
            self.assertGreaterEqual(terminated.count("evidence-worker"), 2)
            status = json.loads(
                (root / "runtime" / "governor" / "state.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(status["state"], "normal")
            self.assertEqual(
                status["desired_children"],
                ["evidence-worker", "source-discovery", "source-producer"],
            )

    def test_resource_governor_emergency_stops_children_and_aborts_autopilot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = AutopilotConfig(
                source_discovery_config=root / "discovery.toml",
                source_producer_config=root / "producer.toml",
                runtime_data_root=root / "runtime",
                supervisor=SupervisorPolicy(
                    poll_seconds=0.01,
                    shutdown_grace_seconds=0.01,
                ),
                evidence=EvidenceServicePolicy(),
                resource_governor=ResourceGovernorPolicy(
                    rss_throttle_bytes=100,
                    rss_stop_bytes=200,
                    disk_throttle_bytes=100,
                    disk_stop_bytes=50,
                    recovery_samples=2,
                ),
            )
            processes: list[_FakeProcess] = []

            class PidProcess(_FakeProcess):
                def __init__(self, pid):
                    super().__init__()
                    self.pid = pid

            def spawn(_argv):
                process = PidProcess(20_000 + len(processes))
                processes.append(process)
                return process

            samples = iter(
                [
                    ResourceSample(rss_bytes=10, disk_free_bytes=500),
                    ResourceSample(rss_bytes=250, disk_free_bytes=500),
                ]
            )
            ticks = [0]

            def sleep(_seconds):
                ticks[0] += 1

            with self.assertRaisesRegex(RuntimeError, "emergency stop"):
                run_autopilot(
                    config,
                    popen_factory=spawn,
                    sleep_fn=sleep,
                    monotonic=lambda: float(ticks[0]),
                    resource_sample_fn=lambda _pids: next(samples),
                )

            self.assertEqual(len(processes), 3)
            self.assertTrue(all(process.terminated for process in processes))
            status = json.loads(
                (root / "runtime" / "governor" / "state.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(status["state"], "emergency_stop")
            self.assertEqual(status["desired_children"], [])

    def test_transient_child_exit_restarts_then_fails_at_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self._config(
                Path(tmp),
                poll_seconds=0.1,
                restart_base_seconds=0.1,
                restart_max_seconds=0.1,
                max_restarts=1,
                stable_reset_seconds=100.0,
            )
            now = [0.0]
            discovery_spawns = [0]

            def spawn(argv):
                if "creeper.source_discovery_service" in argv:
                    discovery_spawns[0] += 1
                    return _FakeProcess(exit_code=2)
                return _FakeProcess()

            def sleep(seconds):
                now[0] += seconds

            with self.assertRaisesRegex(RuntimeError, "restart budget"):
                run_autopilot(
                    config,
                    popen_factory=spawn,
                    sleep_fn=sleep,
                    monotonic=lambda: now[0],
                )

        self.assertEqual(discovery_spawns[0], 2)

    def test_config_parses_optional_readiness_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapy = root / "scrapy"
            scrapy.mkdir()
            runtime = root / "runtime"
            baseline = root / "baseline.sqlite3"
            baseline.write_bytes(b"fixture")
            model = root / "eed-model.json"
            model.write_text(
                '{"tld":["org"],"lang":["eng"],"perc_of_tld":["100"]}',
                encoding="utf-8",
            )
            discovery = root / "discovery.toml"
            discovery.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'scrapy_project_dir = "{scrapy}"',
                        "",
                        "[agent]",
                        'command = ["python", "agent.py"]',
                        'backend = "fixture"',
                        'actor = "agent:test"',
                    ]
                ),
                encoding="utf-8",
            )
            producer = root / "producer.toml"
            producer.write_text(
                "\n".join(
                    [
                        'source_mode = "activated"',
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{baseline}"',
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "autopilot.toml"
            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                        "",
                        "[readiness]",
                        "enabled = true",
                        f'eed_model = "{model}"',
                        'baseline_eed = "35266393.8852"',
                        "batch_size = 123",
                        "max_batches_per_cycle = 4",
                        "poll_seconds = 9.0",
                    ]
                ),
                encoding="utf-8",
            )

            loaded = load_autopilot_config(config)
            specs = build_child_specs(loaded)

            self.assertIsNotNone(loaded.readiness)
            assert loaded.readiness is not None
            self.assertEqual(loaded.baseline_index, baseline.resolve())
            self.assertEqual(loaded.readiness.eed_model, model.resolve())
            self.assertEqual(loaded.readiness.baseline_eed, "35266393.8852")
            self.assertEqual(loaded.readiness.batch_size, 123)
            self.assertEqual(
                [spec.name for spec in specs][-1],
                "readiness-worker",
            )

            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                        "",
                        "[readiness]",
                        "enabled = false",
                    ]
                ),
                encoding="utf-8",
            )
            disabled = load_autopilot_config(config)
            self.assertIsNone(disabled.readiness)
            self.assertEqual(disabled.source_producer_workers, 4)
            self.assertEqual(len(build_child_specs(disabled)), 6)

    def test_config_enables_historical_index_from_producer_and_discovery_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapy = root / "scrapy"
            scrapy.mkdir()
            runtime = root / "runtime"
            baseline = root / "baseline.sqlite3"
            baseline.write_bytes(b"fixture")
            model = root / "eed-model.json"
            model.write_text(
                '{"tld":["com"],"lang":["eng"],"perc_of_tld":["100"]}',
                encoding="utf-8",
            )
            discovery = root / "discovery.toml"
            discovery.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'scrapy_project_dir = "{scrapy}"',
                        "",
                        "[agent]",
                        'command = ["python", "agent.py"]',
                        'backend = "fixture"',
                        'actor = "agent:test"',
                        "",
                        "[measurement]",
                        f'baseline_index = "{baseline}"',
                        f'eed_model = "{model}"',
                    ]
                ),
                encoding="utf-8",
            )
            producer = root / "producer.toml"
            producer.write_text(
                "\n".join(
                    [
                        'source_mode = "activated"',
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{baseline}"',
                        "",
                        "[historical_index]",
                        "enabled = true",
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "autopilot.toml"
            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded = load_autopilot_config(config)
            specs = build_child_specs(loaded)

        self.assertTrue(loaded.historical_index_enabled)
        self.assertEqual(
            loaded.historical_index_eed_model,
            model.resolve(),
        )
        self.assertIn(
            "historical-index",
            [spec.name for spec in specs],
        )

    def test_config_parses_optional_resource_governor(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapy = root / "scrapy"
            scrapy.mkdir()
            runtime = root / "runtime"
            discovery = root / "discovery.toml"
            discovery.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'scrapy_project_dir = "{scrapy}"',
                        "",
                        "[agent]",
                        'command = ["python", "agent.py"]',
                        'backend = "fixture"',
                        'actor = "agent:test"',
                    ]
                ),
                encoding="utf-8",
            )
            producer = root / "producer.toml"
            producer.write_text(
                "\n".join(
                    [
                        'source_mode = "activated"',
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "autopilot.toml"
            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                        "",
                        "[resource_governor]",
                        "enabled = true",
                        "rss_throttle_gib = 8",
                        "rss_stop_gib = 12",
                        "disk_throttle_gib = 40",
                        "disk_stop_gib = 10",
                        "recovery_samples = 7",
                    ]
                ),
                encoding="utf-8",
            )

            loaded = load_autopilot_config(config)

            self.assertIsNotNone(loaded.resource_governor)
            assert loaded.resource_governor is not None
            self.assertEqual(
                loaded.resource_governor.rss_throttle_bytes,
                8 * 1024 ** 3,
            )
            self.assertEqual(
                loaded.resource_governor.disk_stop_bytes,
                10 * 1024 ** 3,
            )
            self.assertEqual(loaded.resource_governor.recovery_samples, 7)

    def test_config_requires_shared_runtime_and_rejects_static_producer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapy = root / "scrapy"
            scrapy.mkdir()
            runtime = root / "runtime"
            discovery = root / "discovery.toml"
            discovery.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'scrapy_project_dir = "{scrapy}"',
                        "",
                        "[agent]",
                        'command = ["python", "agent.py"]',
                        'backend = "fixture"',
                        'actor = "agent:test"',
                    ]
                ),
                encoding="utf-8",
            )
            producer = root / "producer.toml"
            producer.write_text(
                "\n".join(
                    [
                        'source_mode = "activated"',
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "autopilot.toml"
            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded = load_autopilot_config(config)
            self.assertEqual(loaded.runtime_data_root, runtime.resolve())

            producer.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                        f'dataset = "{root / "static-hosts.txt"}"',
                    ]
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ValueError,
                "static producer topology is rejected",
            ):
                load_autopilot_config(config)


    def test_config_omitted_source_mode_defaults_to_activated_and_surfaces_topology(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scrapy = root / "scrapy"
            scrapy.mkdir()
            runtime = root / "runtime"
            discovery = root / "discovery.toml"
            discovery.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'scrapy_project_dir = "{scrapy}"',
                        "",
                        "[agent]",
                        'command = ["python", "agent.py"]',
                        'backend = "fixture"',
                        'actor = "agent:test"',
                    ]
                ),
                encoding="utf-8",
            )
            producer = root / "producer.toml"
            producer.write_text(
                "\n".join(
                    [
                        f'runtime_data_root = "{runtime}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                        "",
                        "[limits]",
                        "source_workers = 3",
                    ]
                ),
                encoding="utf-8",
            )
            config = root / "autopilot.toml"
            config.write_text(
                "\n".join(
                    [
                        f'source_discovery_config = "{discovery}"',
                        f'source_producer_config = "{producer}"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded = load_autopilot_config(config)

            self.assertEqual(loaded.producer_mode, SourceProducerMode.ACTIVATED)
            self.assertFalse(loaded.producer_mode_explicit)
            self.assertEqual(loaded.source_producer_workers, 3)
            self.assertFalse(loaded.historical_index_enabled)
            self.assertEqual(loaded.runtime_data_root, runtime.resolve())
            self.assertEqual(
                loaded.baseline_index,
                (root / "baseline.sqlite3").resolve(),
            )



if __name__ == "__main__":
    unittest.main()
