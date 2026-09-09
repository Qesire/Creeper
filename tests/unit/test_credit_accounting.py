import unittest

from creeper.scheduler.credits import CreditLedger


class CreditAccountingTests(unittest.TestCase):
    def test_reservation_respects_queued_and_reserved_capacity(self):
        ledger = CreditLedger({"wayback": 5})

        ledger.note_queued("wayback", 2)

        self.assertFalse(ledger.reserve_evidence("wayback", 4))
        self.assertTrue(ledger.reserve_evidence("wayback", 3))
        self.assertEqual(ledger.balance("wayback").available, 0)

    def test_claim_moves_queued_work_and_completion_releases_claimed_work(self):
        ledger = CreditLedger({"wayback": 5})
        ledger.note_queued("wayback", 2)

        ledger.claim_evidence("wayback", 1)
        balance = ledger.balance("wayback")
        self.assertEqual(balance.queued, 1)
        self.assertEqual(balance.claimed, 1)

        ledger.complete_evidence("wayback", 1)
        balance = ledger.balance("wayback")
        self.assertEqual(balance.claimed, 0)
        self.assertEqual(balance.available, 4)

    def test_release_evidence_returns_reserved_capacity(self):
        ledger = CreditLedger({"wayback": 5})
        self.assertTrue(ledger.reserve_evidence("wayback", 3))

        ledger.release_evidence("wayback", 2)

        balance = ledger.balance("wayback")
        self.assertEqual(balance.reserved, 1)
        self.assertEqual(balance.available, 4)

    def test_queueing_reserved_work_transfers_reservation_to_queued(self):
        ledger = CreditLedger({"wayback": 5})
        self.assertTrue(ledger.reserve_evidence("wayback", 3))

        ledger.note_queued("wayback", 3)

        balance = ledger.balance("wayback")
        self.assertEqual(balance.queued, 3)
        self.assertEqual(balance.reserved, 0)
        self.assertEqual(balance.available, 2)

    def test_queueing_more_than_capacity_keeps_accounting_unchanged(self):
        ledger = CreditLedger({"wayback": 5})
        ledger.note_queued("wayback", 2)
        self.assertTrue(ledger.reserve_evidence("wayback", 3))

        with self.assertRaises(ValueError):
            ledger.note_queued("wayback", 4)

        balance = ledger.balance("wayback")
        self.assertEqual((balance.queued, balance.reserved), (2, 3))


if __name__ == "__main__":
    unittest.main()
