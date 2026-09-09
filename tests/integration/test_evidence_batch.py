import tempfile
import unittest
from pathlib import Path

from creeper.evidence.batch import EvidenceBatchRunner
from creeper.storage.evidence_store import EvidenceStore


class EvidenceBatchRunnerTests(unittest.TestCase):
    def test_resume_skips_terminal_tasks_and_retries_transient_tasks(self):
        calls: list[tuple[str, int]] = []
        transient_attempts = {"retry.example": 0}

        def transport(hostname: str, year: int):
            calls.append((hostname, year))
            if hostname == "retry.example":
                transient_attempts[hostname] += 1
                if transient_attempts[hostname] == 1:
                    raise ConnectionError("temporary")
            if hostname == "pass.example":
                return [
                    ([
                        {
                            "timestamp": "19970101000000",
                            "original": "http://pass.example/",
                            "status": "200",
                        }
                    ], True)
                ]
            return [([], True)]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = EvidenceStore(root / "evidence.sqlite3")
            runner = EvidenceBatchRunner(
                transport,
                store=store,
                audit_path=root / "audit.jsonl",
            )
            tasks = [("pass.example", 1997), ("empty.example", 1997), ("retry.example", 1997)]
            first = runner.run(tasks)
            self.assertEqual(first.executed, 3)
            self.assertEqual(first.skipped, 0)
            self.assertEqual(first.accepted, 1)
            self.assertEqual(first.states["transient_error"], 1)

            second = runner.run(tasks)
            self.assertEqual(second.executed, 1)
            self.assertEqual(second.skipped, 2)
            self.assertEqual(second.accepted, 0)
            self.assertEqual(second.states["empty_exhaustive"], 1)
            self.assertEqual(store.count(), 1)
            store.close()

        self.assertEqual(
            calls,
            [
                ("pass.example", 1997),
                ("empty.example", 1997),
                ("retry.example", 1997),
                ("retry.example", 1997),
            ],
        )


if __name__ == "__main__":
    unittest.main()
