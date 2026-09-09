import tempfile
import unittest
from pathlib import Path

from creeper.evidence.batch import EvidenceBatchRunner
from creeper.evidence.policies import CDXQueryState
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

    def test_same_hostname_year_with_different_provider_is_not_skipped(self):
        calls: list[str] = []

        def transport(hostname: str, year: int):
            calls.append(hostname)
            return [([], True)]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = EvidenceBatchRunner(
                transport,
                audit_path=root / "audit.jsonl",
                provider="wayback",
                policy_version="v1",
            )
            second = EvidenceBatchRunner(
                transport,
                audit_path=root / "audit.jsonl",
                provider="arquivo",
                policy_version="v1",
            )

            self.assertEqual(first.run([("example.com", 1997)]).executed, 1)
            report = second.run([("example.com", 1997)])

            self.assertEqual(report.executed, 1)
            self.assertEqual(report.skipped, 0)
            self.assertEqual(calls, ["example.com", "example.com"])

    def test_incomplete_task_is_claimed_again_on_next_run(self):
        attempts = 0

        def transport(hostname: str, year: int):
            nonlocal attempts
            attempts += 1
            return [([], False)]

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = EvidenceBatchRunner(
                transport,
                audit_path=root / "audit.jsonl",
                provider="wayback",
                policy_version="v1",
            )

            first = runner.run([("example.com", 1997)])
            second = runner.run([("example.com", 1997)])

            self.assertEqual(first.states[CDXQueryState.INCOMPLETE.value], 1)
            self.assertEqual(second.executed, 1)
            self.assertEqual(attempts, 2)

    def test_task_iterator_is_consumed_in_bounded_batches(self):
        events: list[str] = []

        def tasks():
            events.append("yield-1")
            yield ("one.example", 1997)
            events.append("yield-2")
            yield ("two.example", 1997)
            events.append("yield-3")
            yield ("three.example", 1997)

        def transport(hostname: str, year: int):
            events.append(f"query-{hostname}")
            return [([], True)]

        with tempfile.TemporaryDirectory() as tmp:
            runner = EvidenceBatchRunner(
                transport,
                audit_path=Path(tmp) / "audit.jsonl",
                task_batch_size=2,
            )
            report = runner.run(tasks())

            self.assertEqual(report.executed, 3)
            self.assertLess(events.index("query-one.example"), events.index("yield-3"))


if __name__ == "__main__":
    unittest.main()
