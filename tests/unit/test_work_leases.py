import unittest

from creeper.scheduler.leases import LeaseState, StateTransitionError, WorkLease


class WorkLeaseTests(unittest.TestCase):
    def _lease(self):
        return WorkLease.create(
            reservoir_id="arquivo:demo",
            max_records=10,
            max_requests=2,
            max_bytes=4096,
            max_seconds=30,
            now=100.0,
            expires_at=130.0,
        )

    def test_allows_enforces_every_finite_work_limit(self):
        lease = self._lease()
        self.assertTrue(
            lease.allows(
                records=9,
                requests=1,
                bytes_read=4095,
                elapsed_seconds=29.9,
            )
        )
        self.assertFalse(lease.allows(records=10, requests=1, bytes_read=1, elapsed_seconds=1))
        self.assertFalse(lease.allows(records=1, requests=2, bytes_read=1, elapsed_seconds=1))
        self.assertFalse(lease.allows(records=1, requests=1, bytes_read=4096, elapsed_seconds=1))
        self.assertFalse(lease.allows(records=1, requests=1, bytes_read=1, elapsed_seconds=30))

    def test_state_transitions_are_guarded_and_immutable(self):
        created = self._lease()
        granted = created.grant(owner="worker-1")
        running = granted.start()

        self.assertEqual(created.state, LeaseState.CREATED)
        self.assertEqual(granted.state, LeaseState.GRANTED)
        self.assertEqual(running.state, LeaseState.RUNNING)
        with self.assertRaises(StateTransitionError):
            created.start()

    def test_expiry_and_retry_do_not_resume_terminal_lease(self):
        lease = self._lease().grant(owner="worker-1")
        self.assertEqual(lease.expire(130.1), LeaseState.EXPIRED)
        expired = lease.expired()
        with self.assertRaises(StateTransitionError):
            expired.resume()

        retry = expired.retry()
        self.assertNotEqual(retry.lease_id, expired.lease_id)
        self.assertEqual(retry.state, LeaseState.CREATED)


if __name__ == "__main__":
    unittest.main()
