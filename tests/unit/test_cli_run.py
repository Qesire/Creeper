import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.cli import main
from creeper.sources.reservoirs import ReservoirState
from creeper.storage.control_store import ControlStore


class RunOnceCliTests(unittest.TestCase):
    def _workspace(self):
        root = Path(tempfile.mkdtemp())
        task_root = root / "task"
        baseline_dir = task_root / "merged260909-3"
        baseline_dir.mkdir(parents=True)
        for year in range(1996, 2002):
            (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        baseline_path = root / "baseline.sqlite3"
        index = BaselineIndex.build(task_root, baseline_path)
        index.close()
        dataset = root / "hosts.txt"
        dataset.write_text("new.example\n", encoding="utf-8")
        config = root / "run.toml"
        config.write_text(
            """baseline_index = \"baseline.sqlite3\"
dataset = \"hosts.txt\"
runtime_data_root = \"runtime\"
source_year = 1997

[limits]
queue_source_records = 2
queue_observations = 2
queue_evidence_tasks = 2
queue_commits = 2
lease_max_records = 1
lease_max_requests = 1
lease_max_bytes = 1024
lease_max_seconds = 30
evidence_capacity = 1
""",
            encoding="utf-8",
        )
        return root, config

    def test_run_once_builds_offline_pipeline_and_prints_json_report(self):
        root, config = self._workspace()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = main(["run", "--once", str(config)])

        report = json.loads(output.getvalue())
        self.assertEqual(status, 0)
        self.assertEqual(report["leases_succeeded"], 1)
        self.assertEqual(report["source_records"], 1)
        self.assertEqual(report["evidence_tasks_enqueued"], 1)
        self.assertEqual(report["evidence_tasks_completed"], 1)
        self.assertEqual(report["evidence_capsules_committed"], 0)
        self.assertEqual(report["max_evidence_queue_depth"], 1)
        self.assertTrue((root / "runtime" / "control.sqlite3").exists())
        self.assertTrue((root / "runtime" / "evidence.sqlite3").exists())
        control = ControlStore(root / "runtime" / "control.sqlite3")
        try:
            self.assertEqual(
                [task.key.hostname for task in control.list_evidence_tasks()],
                ["new.example"],
            )
        finally:
            control.close()

    def test_second_run_once_preserves_exhausted_reservoir_progress(self):
        root, config = self._workspace()

        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(["run", "--once", str(config)]), 0)
        second_output = io.StringIO()
        with contextlib.redirect_stdout(second_output):
            self.assertEqual(main(["run", "--once", str(config)]), 0)

        second_report = json.loads(second_output.getvalue())
        self.assertEqual(second_report["leases_succeeded"], 0)
        self.assertEqual(second_report["source_records"], 0)
        control = ControlStore(root / "runtime" / "control.sqlite3")
        try:
            reservoir = control.get_reservoir("local_dataset")
            self.assertEqual(reservoir.state, ReservoirState.EXHAUSTED)
        finally:
            control.close()

    def test_run_once_rejects_non_positive_queue_limit(self):
        _root, config = self._workspace()
        text = config.read_text(encoding="utf-8").replace(
            "queue_observations = 2", "queue_observations = 0"
        )
        config.write_text(text, encoding="utf-8")
        with self.assertRaises(SystemExit) as raised:
            main(["run", "--once", str(config)])
        self.assertEqual(raised.exception.code, 2)

    def test_run_once_rejects_mismatched_source_and_reservoir_ids(self):
        _root, config = self._workspace()
        text = config.read_text(encoding="utf-8").replace(
            'source_year = 1997',
            'source_year = 1997\nsource_id = "other_dataset"\nreservoir_id = "local_dataset"',
        )
        config.write_text(text, encoding="utf-8")
        with self.assertRaises(SystemExit) as raised:
            main(["run", "--once", str(config)])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
