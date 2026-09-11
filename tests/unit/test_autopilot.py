from __future__ import annotations

from pathlib import Path
from threading import Event
import tempfile
import unittest

from creeper.autopilot import (
    AutopilotConfig,
    EvidenceServicePolicy,
    ReadinessServicePolicy,
    SupervisorPolicy,
    build_child_specs,
    load_autopilot_config,
    run_autopilot,
)


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
