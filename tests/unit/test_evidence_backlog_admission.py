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

    def test_available_capacity_counts_backlog_and_live_reservations(self):
        self.control.enqueue_evidence_tasks(
            [self.key("queued.example"), self.key("queued-two.example")]
        )
        reservation = self.admission.try_reserve(
            provider="wayback",
            amount=2,
            capacity=6,
            ttl_seconds=30,
        )
        self.assertIsNotNone(reservation)

        self.assertEqual(
            self.admission.available_capacity(
                provider="wayback",
                capacity=6,
            ),
            2,
        )

        self.now += 31.0
        self.assertEqual(
            self.admission.available_capacity(
                provider="wayback",
                capacity=6,
            ),
            4,
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


    def test_range_parent_retains_capacity_for_exact_fanout(self):
        parent = EvidenceQueryKey(
            "range.example",
            TemporalScope(1996, 1998),
            "wayback",
            "cdx-v1",
        )
        reservation = self.admission.try_reserve(
            provider="wayback",
            amount=3,
            capacity=3,
            ttl_seconds=30,
        )
        assert reservation is not None

        inserted = self.admission.enqueue_reserved(reservation, [parent])

        self.assertEqual(inserted, 1)
        # Parent occupies one nonterminal slot; two additional slots remain
        # persistently reserved for worst-case exact fanout.
        self.assertEqual(self.admission.reserved("wayback"), 2)
        self.assertEqual(
            self.admission.available_capacity(provider="wayback", capacity=3),
            0,
        )

        self.control.claim_evidence_tasks(owner="worker", limit=1)
        children = [
            EvidenceQueryKey(
                "range.example",
                TemporalScope(year, year),
                "wayback",
                "cdx-v1",
            )
            for year in (1996, 1997, 1998)
        ]
        self.control.finish_range_task(
            parent,
            "decomposed",
            followup_keys=children,
            owner="worker",
        )

        self.assertEqual(self.admission.reserved("wayback"), 0)
        self.assertEqual(
            self.admission.available_capacity(provider="wayback", capacity=3),
            0,
        )
        self.assertEqual(
            sum(
                1
                for task in self.control.list_evidence_tasks()
                if task.state == "pending"
            ),
            3,
        )

    def test_live_reservation_can_renew_but_expired_one_cannot(self):
        reservation = self.admission.try_reserve(
            provider="wayback",
            amount=1,
            capacity=1,
            ttl_seconds=10,
        )
        assert reservation is not None
        self.now = 1_005.0

        renewed = self.admission.renew(reservation, ttl_seconds=20)

        self.assertGreaterEqual(renewed.expires_at, 1_025.0)
        self.now = 1_026.0
        with self.assertRaisesRegex(RuntimeError, "expired before renewal"):
            self.admission.renew(renewed, ttl_seconds=20)

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
