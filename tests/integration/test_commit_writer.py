import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    TemporalScope,
)
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class RecordingEvidenceStore:
    def __init__(self, store):
        self.store = store
        self.put_many_calls = []
        self.put_many_with_task_provenance_calls = []

    def put_many(self, capsules):
        batch = list(capsules)
        self.put_many_calls.append(batch)
        return self.store.put_many(batch)

    def put_many_with_task_provenance(self, items):
        batch = list(items)
        self.put_many_with_task_provenance_calls.append(batch)
        return self.store.put_many_with_task_provenance(batch)


class RecordingControlStore:
    def __init__(self):
        self.batch_calls = []

    def finish_evidence_tasks(self, results, *, owner):
        batch = list(results)
        self.batch_calls.append((batch, owner))
        return len(batch)

    def finish_evidence_task(self, *args, **kwargs):
        raise AssertionError("CommitWriter must use the batch completion API")


class CommitWriterTests(unittest.TestCase):
    def test_flush_batches_capsules_and_repeated_flush_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            recording = RecordingEvidenceStore(evidence)
            control = ControlStore(root / "control.sqlite3")
            capsules = [
                EvidenceCapsule(
                    f"host-{index}.example.com", 1997, "wayback",
                    "capture_timestamp_year", f"1997010100000{index}",
                    f"http://host-{index}.example.com/", str(index) * 64, "v1"
                )
                for index in (1, 2)
            ]
            results = [
                EvidenceQueryResult(
                    capsule.hostname,
                    capsule.year,
                    CDXQueryState.PASS,
                    capsule=capsule,
                    key=EvidenceQueryKey(
                        capsule.hostname,
                        TemporalScope(capsule.year, capsule.year),
                        capsule.provider,
                        capsule.policy_version,
                    ),
                )
                for capsule in capsules
            ]
            control.enqueue_evidence_tasks([result.key for result in results])
            control.claim_evidence_tasks(owner="worker-1", limit=10)
            writer = CommitWriter(recording, control, owner="worker-1")

            for capsule, result in zip(capsules, results):
                writer.submit(capsule, result)
            writer.flush()
            writer.flush()

            self.assertEqual(len(recording.put_many_calls), 0)
            self.assertEqual(
                len(recording.put_many_with_task_provenance_calls),
                1,
            )
            self.assertEqual(
                len(recording.put_many_with_task_provenance_calls[0]),
                2,
            )
            self.assertEqual(evidence.count(), 2)
            self.assertEqual(writer.inserted_capsules, 2)
            self.assertEqual(writer.finished_tasks, 2)
            self.assertEqual(
                [task for task in control.list_evidence_tasks() if task.state != CDXQueryState.PASS.value],
                [],
            )
            self.assertTrue(all(
                task.state == CDXQueryState.PASS.value
                for task in control.list_evidence_tasks()
            ))
            writer.close()
            control.close()
            evidence.close()

    def test_proof_provenance_survives_crash_before_control_attribution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            control = ControlStore(root / "control.sqlite3")
            key = EvidenceQueryKey(
                "crash.example.com",
                TemporalScope(1997, 1997),
                "wayback",
                "v1",
            )
            capsule = EvidenceCapsule(
                key.hostname,
                1997,
                key.provider,
                "capture_timestamp_year",
                "19970101000000",
                "http://crash.example.com/",
                "c" * 64,
                key.policy_version,
            )
            result = EvidenceQueryResult(
                key.hostname,
                1997,
                CDXQueryState.PASS,
                capsule=capsule,
                key=key,
            )
            control.enqueue_evidence_tasks([key])
            control.claim_evidence_tasks(owner="worker-crash", limit=1)

            def fail_attribution(*_args, **_kwargs):
                raise RuntimeError("simulated control-store crash")

            control.attribute_task_host_years = fail_attribution
            writer = CommitWriter(
                evidence,
                control,
                owner="worker-crash",
                flush_count=100,
            )
            writer.submit(capsule, result)

            with self.assertRaisesRegex(
                RuntimeError,
                "simulated control-store crash",
            ):
                writer.flush()

            provenance = evidence.resolve_provider_task_provenance(
                [(key.hostname, 1997)]
            )
            self.assertEqual(provenance[(key.hostname, 1997)].task_kind, "exact")
            self.assertEqual(
                control.resolve_host_year_task_kinds([(key.hostname, 1997)]),
                {},
            )
            self.assertEqual(evidence.count(), 1)
            control.close()
            evidence.close()

    def test_flush_finishes_all_results_with_one_batch_control_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            recording = RecordingEvidenceStore(evidence)
            control = RecordingControlStore()
            results = [
                EvidenceQueryResult(
                    f"host-{index}.example.com",
                    1997,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                    key=EvidenceQueryKey(
                        f"host-{index}.example.com",
                        TemporalScope(1997, 1997),
                        "wayback",
                        "v1",
                    ),
                )
                for index in (1, 2)
            ]
            writer = CommitWriter(recording, control, owner="worker-1")
            for result in results:
                writer.submit(None, result)

            self.assertEqual(writer.flush(), 2)
            self.assertEqual(len(control.batch_calls), 1)
            self.assertEqual(control.batch_calls[0][0], results)
            self.assertEqual(control.batch_calls[0][1], "worker-1")
            evidence.close()

    def test_submit_flushes_at_count_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            recording = RecordingEvidenceStore(evidence)
            control = RecordingControlStore()
            writer = CommitWriter(
                recording,
                control,
                owner="worker-1",
                flush_count=2,
                flush_interval_seconds=60,
                clock=lambda: 0.0,
            )
            results = [
                EvidenceQueryResult(
                    f"host-{index}.example.com",
                    1997,
                    CDXQueryState.EMPTY_EXHAUSTIVE,
                    key=EvidenceQueryKey(
                        f"host-{index}.example.com",
                        TemporalScope(1997, 1997),
                        "wayback",
                        "v1",
                    ),
                )
                for index in range(3)
            ]

            writer.submit(None, results[0])
            self.assertEqual(writer.pending_count, 1)
            writer.submit(None, results[1])
            self.assertEqual(writer.pending_count, 0)
            self.assertEqual(len(control.batch_calls), 1)
            writer.submit(None, results[2])
            self.assertEqual(writer.pending_count, 1)
            writer.close()
            self.assertEqual(writer.pending_count, 0)
            self.assertEqual(len(control.batch_calls), 2)
            evidence.close()

    def test_submit_flushes_after_interval(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            evidence = EvidenceStore(root / "evidence.sqlite3")
            recording = RecordingEvidenceStore(evidence)
            control = RecordingControlStore()
            now = [0.0]
            writer = CommitWriter(
                recording,
                control,
                owner="worker-1",
                flush_count=100,
                flush_interval_seconds=1.0,
                clock=lambda: now[0],
            )
            result = EvidenceQueryResult(
                "host.example.com",
                1997,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                key=EvidenceQueryKey(
                    "host.example.com",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "v1",
                ),
            )

            writer.submit(None, result)
            self.assertEqual(writer.pending_count, 1)
            now[0] = 1.1
            writer.submit(None, EvidenceQueryResult(
                "host-2.example.com",
                1997,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                key=EvidenceQueryKey(
                    "host-2.example.com",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "v1",
                ),
            ))
            self.assertEqual(writer.pending_count, 0)
            self.assertEqual(len(control.batch_calls), 1)
            evidence.close()


if __name__ == "__main__":
    unittest.main()
