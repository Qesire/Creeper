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
    build_child_specs,
    load_autopilot_config,
    run_autopilot,
)
from creeper.runtime.resource_governor import ResourceSample


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
        self.assertIn("2", evidence)
        self.assertIn("--requests-per-second", evidence)
        self.assertIn("0.5", evidence)

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
            self.assertEqual(len(build_child_specs(disabled)), 3)

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

    def test_config_requires_shared_runtime_and_activated_producer(self):
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
                    ]
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "source_mode"):
                load_autopilot_config(config)


if __name__ == "__main__":
    unittest.main()
