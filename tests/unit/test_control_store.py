import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.storage.control_store import ControlStore


class ControlStoreTests(unittest.TestCase):
    def _key(self, provider="wayback", policy="v1"):
        return EvidenceQueryKey(
            "example.com",
            TemporalScope(1997, 1997),
            provider,
            policy,
        )

    def test_enqueue_and_claim_preserve_full_query_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key_a = self._key("wayback", "v1")
            key_b = self._key("arquivo", "v1")

            self.assertEqual(store.enqueue_evidence_tasks([key_a, key_b, key_a]), 2)
            claimed = store.claim_evidence_tasks(owner="worker-1", limit=10)

            self.assertEqual({task.key for task in claimed}, {key_a, key_b})
            self.assertEqual({task.attempt for task in claimed}, {1})
            store.close()

    def test_terminal_tasks_are_not_claimed_but_retryable_tasks_are(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            terminal_key = self._key("wayback", "v1")
            retry_key = self._key("arquivo", "v1")
            store.enqueue_evidence_tasks([terminal_key, retry_key])

            first = store.claim_evidence_tasks(owner="worker-1", limit=10)
            store.finish_evidence_task(
                terminal_key,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                owner="worker-1",
            )
            store.finish_evidence_task(
                retry_key,
                CDXQueryState.TRANSIENT_ERROR,
                owner="worker-1",
            )

            second = store.claim_evidence_tasks(owner="worker-2", limit=10)
            self.assertEqual([task.key for task in second], [retry_key])
            self.assertEqual(second[0].attempt, 2)
            self.assertEqual(first[0].attempt, 1)
            store.close()

    def test_expired_ownership_can_be_claimed_by_another_worker(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._key()
            store.enqueue_evidence_tasks([key])

            first = store.claim_evidence_tasks(
                owner="worker-1", limit=1, lease_seconds=0
            )
            second = store.claim_evidence_tasks(owner="worker-2", limit=1)

            self.assertEqual(len(first), 1)
            self.assertEqual(len(second), 1)
            self.assertEqual(second[0].key, key)
            self.assertEqual(second[0].attempt, 2)
            store.close()


if __name__ == "__main__":
    unittest.main()
