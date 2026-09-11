import json
import tempfile
import unittest
from pathlib import Path
from threading import Event
from unittest.mock import patch

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.source_cli import run_once, run_watch
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
