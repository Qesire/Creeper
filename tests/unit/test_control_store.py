import tempfile
import unittest
from pathlib import Path

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.storage.control_store import ControlStore
from creeper.scheduler.leases import LeaseState, WorkLease
from creeper.sources.domains import DomainState, SourceDomain
from creeper.sources.reservoirs import Reservoir, ReservoirState


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

    def _domain(self):
        return SourceDomain(
            domain_id="archive",
            family="NATIONAL_WEB_ARCHIVE",
            discovery_mechanism="catalog",
            temporal_scope=(1996, 2001),
        )

    def _reservoir(self):
        return Reservoir(
            reservoir_id="archive:demo",
            domain_id="archive",
            adapter_id="demo",
            root_locator="https://example.test/catalog",
            enumeration_kind="pagination",
            capacity_lower=100,
            capacity_upper=200,
            evidence_mode="direct_year",
        )

    def test_domain_reservoir_and_lease_round_trip_requires_domain_first(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            reservoir = self._reservoir()
            with self.assertRaises(KeyError):
                store.save_reservoir(reservoir)

            domain = self._domain()
            store.save_domain(domain)
            store.save_reservoir(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                cursor_start="0",
                cursor_end="99",
                max_records=10,
                max_requests=2,
                max_bytes=4096,
                max_seconds=30,
                now=100.0,
                expires_at=130.0,
            )
            store.save_lease(lease)

            self.assertEqual(store.get_domain(domain.domain_id), domain)
            self.assertEqual(store.get_reservoir(reservoir.reservoir_id), reservoir)
            self.assertEqual(store.get_lease(lease.lease_id), lease)
            store.close()

    def test_restores_non_initial_domain_and_reservoir_states(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            productive = self._domain().transition(DomainState.EXPLORING).transition(
                DomainState.PRODUCTIVE
            )
            ready = self._reservoir().transition(ReservoirState.QUALIFYING).transition(
                ReservoirState.READY
            )
            store.save_domain(self._domain())
            store.save_reservoir(self._reservoir())
            store.connection.execute(
                "UPDATE source_domains SET state = ? WHERE domain_id = ?",
                (productive.state.value, productive.domain_id),
            )
            store.connection.execute(
                "UPDATE reservoirs SET state = ? WHERE reservoir_id = ?",
                (ready.state.value, ready.reservoir_id),
            )
            store.connection.commit()

            self.assertEqual(
                store.get_domain(productive.domain_id).state,
                DomainState.PRODUCTIVE,
            )
            self.assertEqual(
                store.get_reservoir(ready.reservoir_id).state,
                ReservoirState.READY,
            )
            store.close()

    def test_running_requires_granted_lease_and_recovery_expires_only_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3", clock=lambda: 200.0)
            store.save_domain(self._domain())
            store.save_reservoir(self._reservoir())
            created = WorkLease.create(
                reservoir_id="archive:demo",
                max_records=10,
                max_requests=2,
                max_bytes=4096,
                max_seconds=30,
                now=100.0,
                expires_at=150.0,
            )
            running = created.grant(owner="worker-1").start()
            with self.assertRaises(ValueError):
                store.save_lease(running)

            granted = created.grant(owner="worker-1")
            store.save_lease(granted)
            store.save_lease(granted.start())
            completed = granted.start().complete()
            store.save_lease(completed)

            expired_granted = WorkLease.create(
                reservoir_id="archive:demo",
                max_records=10,
                max_requests=2,
                max_bytes=4096,
                max_seconds=30,
                now=100.0,
                expires_at=150.0,
            ).grant(owner="worker-2")
            store.save_lease(expired_granted)
            expired = expired_granted.start()
            store.save_lease(expired)

            self.assertEqual(store.recover_expired_leases(now=200.0), 1)
            self.assertEqual(store.get_lease(expired.lease_id).state, LeaseState.EXPIRED)
            self.assertEqual(store.get_lease(completed.lease_id).state, LeaseState.SUCCEEDED)
            store.close()


if __name__ == "__main__":
    unittest.main()
