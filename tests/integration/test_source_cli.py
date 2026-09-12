import json
import tempfile
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.source_cli import ActivatedSourceRuntime, run_once, run_watch
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate, SourceLevel, SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.control_store import ControlStore


class SourceProducerCliTests(unittest.TestCase):
    def test_watch_reuses_durable_once_runner_until_stop(self):
        stop = Event()
        reports = iter(
            [
                {
                    "leases_succeeded": 1,
                    "source_records": 2,
                    "observations": 2,
                    "evidence_tasks_enqueued": 2,
                    "direct_capsules_committed": 0,
                    "admission_blocked": False,
                    "max_source_record_queue_depth": 1,
                    "max_observation_queue_depth": 1,
                },
                {
                    "leases_succeeded": 0,
                    "source_records": 0,
                    "observations": 0,
                    "evidence_tasks_enqueued": 0,
                    "direct_capsules_committed": 0,
                    "admission_blocked": True,
                    "max_source_record_queue_depth": 0,
                    "max_observation_queue_depth": 0,
                },
            ]
        )

        def fake_once(config, *, owner):
            return next(reports)

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "watch.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        "",
                        "[limits]",
                    ]
                ),
                encoding="utf-8",
            )
            with patch("creeper.source_cli.run_once", side_effect=fake_once):
                result = run_watch(
                    config,
                    owner="test",
                    stop_event=stop,
                    sleep_fn=lambda _: stop.set(),
                )

        self.assertEqual(result["leases_succeeded"], 1)
        self.assertEqual(result["source_records"], 2)
        self.assertTrue(result["admission_blocked"])

    def test_admission_backpressure_uses_short_fixed_poll(self):
        stop = Event()
        sleeps: list[float] = []
        blocked = {
            "leases_succeeded": 0,
            "source_records": 0,
            "observations": 0,
            "evidence_tasks_enqueued": 0,
            "direct_capsules_committed": 0,
            "admission_blocked": True,
            "max_source_record_queue_depth": 0,
            "max_observation_queue_depth": 0,
        }

        def sleep(seconds):
            sleeps.append(seconds)
            if len(sleeps) >= 3:
                stop.set()

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "watch.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        "",
                        "[limits]",
                    ]
                ),
                encoding="utf-8",
            )
            with patch("creeper.source_cli.run_once", return_value=blocked):
                result = run_watch(
                    config,
                    owner="backpressure-poll-test",
                    stop_event=stop,
                    idle_backoff_seconds=1.0,
                    max_idle_backoff_seconds=60.0,
                    sleep_fn=sleep,
                )

        self.assertEqual(sleeps, [1.0, 1.0, 1.0])
        self.assertTrue(result["admission_blocked"])

    def test_static_watch_reuses_one_persistent_runtime(self):
        stop = Event()
        reports = iter(
            [
                {
                    "leases_succeeded": 1,
                    "source_records": 1,
                    "observations": 1,
                    "evidence_tasks_enqueued": 1,
                    "direct_capsules_committed": 0,
                    "admission_blocked": False,
                    "max_source_record_queue_depth": 1,
                    "max_observation_queue_depth": 1,
                },
                {
                    "leases_succeeded": 0,
                    "source_records": 0,
                    "observations": 0,
                    "evidence_tasks_enqueued": 0,
                    "direct_capsules_committed": 0,
                    "admission_blocked": True,
                    "max_source_record_queue_depth": 0,
                    "max_observation_queue_depth": 0,
                },
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "watch-static.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'runtime_data_root = "{root / "runtime"}"',
                        f'baseline_index = "{root / "baseline.sqlite3"}"',
                        f'dataset = "{root / "hosts.txt"}"',
                        "",
                        "[limits]",
                        "queue_source_records = 4",
                        "queue_observations = 4",
                        "queue_evidence_tasks = 4",
                        "queue_commits = 4",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 4",
                    ]
                ),
                encoding="utf-8",
            )

            with patch("creeper.source_cli.StaticSourceRuntime") as runtime_cls:
                runtime = runtime_cls.return_value.__enter__.return_value
                runtime.run_once.side_effect = lambda: next(reports)
                result = run_watch(
                    config,
                    owner="persistent-static-test",
                    stop_event=stop,
                    sleep_fn=lambda _: stop.set(),
                )

        runtime_cls.assert_called_once()
        self.assertEqual(runtime.run_once.call_count, 2)
        self.assertEqual(result["leases_succeeded"], 1)
        self.assertTrue(result["admission_blocked"])

    def test_activated_runtime_shrinks_lease_to_backlog_headroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                registry = SourceDiscoveryRegistry(control)
                candidate = SourceCandidate(
                    canonical_entrypoint="https://archive.example/source.txt",
                    source_family="BULK_ARTIFACT",
                    level=SourceLevel.SOURCE,
                    discovered_by="test",
                    discovery_strategy="fixture",
                    expected_year_from=1996,
                    expected_year_to=2001,
                    expected_volume=1000,
                    enumerability_prior=1.0,
                    confidence=1.0,
                    state=SourceState.ACTIVE,
                )
                registry.register_proposal(candidate)
                registry.record_scout_measurement(
                    candidate.source_key,
                    ScoutMeasurement(
                        sampled_records=100,
                        unique_hosts=100,
                        novel_hosts=50,
                        direct_host_years=0,
                        requests=1,
                        bytes_read=1024,
                        elapsed_seconds=1.0,
                        novel_eed=50.0,
                    ),
                )
                for index in range(2):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = {
                "source_mode": "activated",
                "baseline_index": str(baseline_path),
                "runtime_data_root": str(runtime_root),
            }
            limits = {
                "queue_source_records": 10,
                "queue_observations": 10,
                "queue_evidence_tasks": 10,
                "queue_commits": 10,
                "lease_max_records": 4,
                "lease_max_requests": 4,
                "lease_max_bytes": 4096,
                "lease_max_seconds": 30,
                "evidence_backlog_capacity": 4,
            }
            with ActivatedSourceRuntime(
                root / "activated.toml",
                config=config,
                limits=limits,
                owner="headroom-test",
            ) as runtime:
                count = runtime.refresh_workset()
                self.assertEqual(count, 1)
                lease = runtime.producer.candidates[0].lease
                assert lease is not None
                self.assertEqual(lease.max_records, 2)
                self.assertEqual(lease.expected_evidence_tasks, 2)
                candidate_runtime = runtime.producer.candidates[0]
                self.assertAlmostEqual(candidate_runtime.expected_novel_eed, 1.0)
                self.assertEqual(candidate_runtime.costs.evidence_network, 2.0)

    def test_static_runtime_shrinks_lease_to_backlog_headroom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text(
                "\n".join(
                    [
                        "one.example",
                        "two.example",
                        "three.example",
                        "four.example",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                for index in range(3):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = root / "static.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "webbase-static"',
                        'domain_id = "webbase-domain"',
                        'reservoir_id = "webbase-static"',
                        "source_year = 2001",
                        "",
                        "[limits]",
                        "queue_source_records = 8",
                        "queue_observations = 8",
                        "queue_evidence_tasks = 8",
                        "queue_commits = 8",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 4",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="static-headroom-test")

            self.assertEqual(report["leases_succeeded"], 1)
            self.assertEqual(report["source_records"], 1)
            self.assertEqual(report["evidence_tasks_enqueued"], 1)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                reservoir = control.get_reservoir("webbase-static")
                self.assertIsNotNone(reservoir)
                self.assertIsNotNone(reservoir.cursor)
                self.assertEqual(
                    sum(
                        control.evidence_task_state_counts().values()
                    ),
                    4,
                )
            finally:
                control.close()

    def test_static_runtime_does_not_advance_when_backlog_is_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text("blocked.example\n", encoding="utf-8")
            runtime_root = root / "runtime"
            runtime_root.mkdir()
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                for index in range(2):
                    control.enqueue_evidence_tasks(
                        [
                            EvidenceQueryKey(
                                f"occupied-full-{index}.example",
                                TemporalScope(1997, 1997),
                                "wayback",
                                "cdx-v1",
                            )
                        ]
                    )
            finally:
                control.close()

            config = root / "static-full.toml"
            config.write_text(
                "\n".join(
                    [
                        'source_mode = "static"',
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "webbase-full"',
                        'domain_id = "webbase-full-domain"',
                        'reservoir_id = "webbase-full"',
                        "source_year = 2001",
                        "",
                        "[limits]",
                        "queue_source_records = 4",
                        "queue_observations = 4",
                        "queue_evidence_tasks = 4",
                        "queue_commits = 4",
                        "lease_max_records = 4",
                        "lease_max_requests = 4",
                        "lease_max_bytes = 4096",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 2",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="static-full-test")

            self.assertEqual(report["leases_succeeded"], 0)
            self.assertTrue(report["admission_blocked"])
            self.assertEqual(report["source_records"], 0)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                reservoir = control.get_reservoir("webbase-full")
                self.assertIsNotNone(reservoir)
                self.assertIsNone(reservoir.cursor)
                self.assertEqual(
                    sum(control.evidence_task_state_counts().values()),
                    2,
                )
            finally:
                control.close()

    def test_once_mode_stops_at_durable_evidence_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            task_root = root / "task"
            baseline_root = task_root / "merged260909-3"
            baseline_root.mkdir(parents=True)
            for year in range(1996, 2002):
                (baseline_root / f"{year}.txt").write_text("", encoding="utf-8")
            (baseline_root / "candidate_pool.txt").write_text("", encoding="utf-8")
            baseline_path = root / "baseline.sqlite3"
            BaselineIndex.build(task_root, baseline_path).close()

            dataset = root / "hosts.txt"
            dataset.write_text("novel.example\n", encoding="utf-8")
            runtime_root = root / "runtime"
            config = root / "creeper.toml"
            config.write_text(
                "\n".join(
                    [
                        f'baseline_index = {json.dumps(str(baseline_path))}',
                        f'dataset = {json.dumps(str(dataset))}',
                        f'runtime_data_root = {json.dumps(str(runtime_root))}',
                        'source_id = "fixture"',
                        'domain_id = "fixture-domain"',
                        'reservoir_id = "fixture"',
                        "source_year = 1997",
                        "",
                        "[limits]",
                        "queue_source_records = 2",
                        "queue_observations = 2",
                        "queue_evidence_tasks = 2",
                        "queue_commits = 2",
                        "lease_max_records = 1",
                        "lease_max_requests = 1",
                        "lease_max_bytes = 1024",
                        "lease_max_seconds = 30",
                        "evidence_backlog_capacity = 4",
                    ]
                ),
                encoding="utf-8",
            )

            report = run_once(config, owner="source-cli-test")

            self.assertEqual(report["leases_succeeded"], 1)
            self.assertEqual(report["evidence_tasks_enqueued"], 1)
            control = ControlStore(runtime_root / "control.sqlite3")
            try:
                key = EvidenceQueryKey(
                    "novel.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "cdx-v1",
                )
                task = control.get_evidence_task(key)
                self.assertIsNotNone(task)
                self.assertEqual(task.state, "pending")
                self.assertIsNone(task.lease_owner)
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
