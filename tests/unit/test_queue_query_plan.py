import tempfile
import unittest
from pathlib import Path

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
