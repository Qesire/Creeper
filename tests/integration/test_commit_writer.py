import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey, EvidenceQueryResult, TemporalScope, CDXQueryState
from creeper.storage.commit_writer import CommitWriter
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class RecordingEvidenceStore:
    def __init__(self, store):
        self.store = store
        self.put_many_calls = []

    def put_many(self, capsules):
        batch = list(capsules)
        self.put_many_calls.append(batch)
        return self.store.put_many(batch)


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

            self.assertEqual(len(recording.put_many_calls), 1)
            self.assertEqual(len(recording.put_many_calls[0]), 2)
            self.assertEqual(evidence.count(), 2)
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


if __name__ == "__main__":
    unittest.main()
