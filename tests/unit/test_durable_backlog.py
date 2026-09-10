import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.scheduler.backlog import load_provider_backlog, restore_credit_ledger
from creeper.scheduler.credits import CreditLedger
from creeper.storage.control_store import ControlStore


class DurableBacklogTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 1_000.0
        self.control = ControlStore(
            Path(self.tmp.name) / "control.sqlite3",
            clock=lambda: self.now,
        )

    def tearDown(self):
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def key(hostname):
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1997, 1997),
            "wayback",
            "cdx-v1",
        )

    def test_restore_counts_live_claims_and_queued_durable_work(self):
        keys = [self.key("one.example"), self.key("two.example"), self.key("three.example")]
        self.control.enqueue_evidence_tasks(keys)
        claimed = self.control.claim_evidence_tasks(
            owner="worker-a",
            limit=1,
            lease_seconds=60,
        )
        self.assertEqual(len(claimed), 1)

        backlog = load_provider_backlog(self.control)
        self.assertEqual(backlog["wayback"].claimed, 1)
        self.assertEqual(backlog["wayback"].queued, 2)

        ledger = CreditLedger({"wayback": 10})
        restore_credit_ledger(ledger, self.control)
        balance = ledger.balance("wayback")
        self.assertEqual(balance.claimed, 1)
        self.assertEqual(balance.queued, 2)
        self.assertEqual(balance.available, 7)

    def test_expired_claim_reenters_queued_backlog(self):
        key = self.key("expired.example")
        self.control.enqueue_evidence_tasks([key])
        self.control.claim_evidence_tasks(owner="worker-a", limit=1, lease_seconds=5)
        self.now = 1_006.0

        backlog = load_provider_backlog(self.control)
        self.assertEqual(backlog["wayback"].claimed, 0)
        self.assertEqual(backlog["wayback"].queued, 1)

    def test_lowered_capacity_reports_zero_available_instead_of_negative(self):
        keys = [self.key(f"host-{index}.example") for index in range(4)]
        self.control.enqueue_evidence_tasks(keys)
        ledger = CreditLedger({"wayback": 2})

        restore_credit_ledger(ledger, self.control)

        balance = ledger.balance("wayback")
        self.assertEqual(balance.queued, 4)
        self.assertEqual(balance.available, 0)
        self.assertFalse(ledger.reserve_evidence("wayback", 1))


if __name__ == "__main__":
    unittest.main()
