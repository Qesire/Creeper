import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.scheduler.admission import EvidenceBacklogAdmission
from creeper.storage.control_store import ControlStore


class EvidenceBacklogAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "control.sqlite3"
        self.now = 1_000.0
        self.control = ControlStore(self.path, clock=lambda: self.now)
        self.admission = EvidenceBacklogAdmission(self.control)

    def tearDown(self):
        self.control.close()
        self.tmp.cleanup()

    @staticmethod
    def key(hostname: str) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1997, 1997),
            "wayback",
            "cdx-v1",
        )

    def test_concurrent_connections_cannot_oversubscribe_capacity(self):
        second_control = ControlStore(self.path, clock=lambda: self.now)
        second = EvidenceBacklogAdmission(second_control)
        try:
            first = self.admission.try_reserve(
                provider="wayback", amount=4, capacity=5, ttl_seconds=30
            )
            rejected = second.try_reserve(
                provider="wayback", amount=2, capacity=5, ttl_seconds=30
            )
            self.assertIsNotNone(first)
            self.assertIsNone(rejected)
            self.assertEqual(second.reserved("wayback"), 4)
        finally:
            second_control.close()

    def test_reserved_slots_transfer_atomically_to_durable_tasks(self):
        reservation = self.admission.try_reserve(
            provider="wayback", amount=2, capacity=2, ttl_seconds=30
        )
        assert reservation is not None

        inserted = self.admission.enqueue_reserved(
            reservation,
            [self.key("one.example")],
        )

        self.assertEqual(inserted, 1)
        self.assertEqual(self.admission.reserved("wayback"), 1)
        self.assertIsNotNone(self.control.get_evidence_task(self.key("one.example")))
        self.assertIsNone(
            self.admission.try_reserve(
                provider="wayback", amount=1, capacity=2, ttl_seconds=30
            )
        )

    def test_duplicate_task_does_not_consume_reserved_slot(self):
        key = self.key("duplicate.example")
        self.control.enqueue_evidence_tasks([key])
        reservation = self.admission.try_reserve(
            provider="wayback", amount=1, capacity=2, ttl_seconds=30
        )
        assert reservation is not None

        inserted = self.admission.enqueue_reserved(reservation, [key])

        self.assertEqual(inserted, 0)
        self.assertEqual(self.admission.reserved("wayback"), 1)

    def test_underestimated_work_rolls_back_task_inserts(self):
        reservation = self.admission.try_reserve(
            provider="wayback", amount=1, capacity=3, ttl_seconds=30
        )
        assert reservation is not None
        keys = [self.key("a.example"), self.key("b.example")]

        with self.assertRaisesRegex(RuntimeError, "exceeded"):
            self.admission.enqueue_reserved(reservation, keys)

        self.assertTrue(all(self.control.get_evidence_task(key) is None for key in keys))
        self.assertEqual(self.admission.reserved("wayback"), 1)

    def test_expired_crash_reservation_is_reaped_on_next_admission(self):
        reservation = self.admission.try_reserve(
            provider="wayback", amount=2, capacity=2, ttl_seconds=10
        )
        self.assertIsNotNone(reservation)
        self.now = 1_011.0

        replacement = self.admission.try_reserve(
            provider="wayback", amount=2, capacity=2, ttl_seconds=10
        )

        self.assertIsNotNone(replacement)
        self.assertEqual(self.admission.reserved("wayback"), 2)

    def test_zero_reservation_accepts_direct_only_lease_but_not_external_work(self):
        reservation = self.admission.try_reserve(
            provider="wayback", amount=0, capacity=0, ttl_seconds=10
        )
        self.assertIsNotNone(reservation)
        assert reservation is not None
        self.assertIsNone(reservation.reservation_id)
        with self.assertRaisesRegex(RuntimeError, "zero reservation"):
            self.admission.enqueue_reserved(reservation, [self.key("unexpected.example")])


if __name__ == "__main__":
    unittest.main()
