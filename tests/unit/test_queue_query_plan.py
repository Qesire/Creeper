import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_queue import DurableEvidenceQueue


class QueueQueryPlanTests(unittest.TestCase):
    def test_provider_claim_index_is_narrow_and_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                DurableEvidenceQueue(control)
                rows = control.connection.execute(
                    "PRAGMA index_info('idx_evidence_tasks_provider_claim')"
                ).fetchall()
                self.assertEqual(
                    [row[2] for row in rows],
                    ["provider", "state", "retry_at", "lease_until"],
                )
            finally:
                control.close()

    def test_claim_and_renew_reject_nonfinite_clock_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3", clock=lambda: 100.0)
            try:
                key = EvidenceQueryKey(
                    "example.com",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "v1",
                )
                control.enqueue_evidence_tasks([key])
                queue = DurableEvidenceQueue(control)
                control.clock = lambda: float("nan")
                with self.assertRaisesRegex(ValueError, "queue clock"):
                    queue.claim(
                        owner="worker-a",
                        limit=1,
                        providers=("wayback",),
                        lease_seconds=30.0,
                    )
                row = control.connection.execute(
                    "SELECT attempt, lease_owner, lease_until FROM evidence_tasks"
                ).fetchone()
                self.assertEqual(int(row["attempt"]), 0)
                self.assertIsNone(row["lease_owner"])
                self.assertIsNone(row["lease_until"])

                control.clock = lambda: 100.0
                claimed = queue.claim(
                    owner="worker-a",
                    limit=1,
                    providers=("wayback",),
                    lease_seconds=30.0,
                )
                self.assertEqual(len(claimed), 1)
                original_until = claimed[0].lease_until
                control.clock = lambda: float("nan")
                with self.assertRaisesRegex(ValueError, "queue clock"):
                    queue.renew(
                        [key],
                        owner="worker-a",
                        lease_seconds=30.0,
                    )
                row = control.connection.execute(
                    "SELECT lease_until FROM evidence_tasks"
                ).fetchone()
                self.assertEqual(float(row["lease_until"]), original_until)
            finally:
                control.close()

    def test_claim_rejects_fractional_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                queue = DurableEvidenceQueue(control)
                with self.assertRaisesRegex(ValueError, "limit must be an integer"):
                    queue.claim(
                        owner="worker-a",
                        limit=1.5,
                        providers=("wayback",),
                        lease_seconds=30.0,
                    )
            finally:
                control.close()

    def test_provider_filtered_claim_uses_provider_first_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            control = ControlStore(Path(tmp) / "control.sqlite3")
            try:
                DurableEvidenceQueue(control)
                plan = control.connection.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT * FROM evidence_tasks
                    WHERE provider = ?
                      AND state = ?
                    LIMIT 16
                    """,
                    ("wayback", "pending"),
                ).fetchall()
                detail = "\n".join(str(row[3]) for row in plan)
                self.assertIn("idx_evidence_tasks_provider_claim", detail)
                self.assertNotIn("SCAN evidence_tasks", detail)
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
