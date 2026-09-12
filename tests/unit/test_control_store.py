import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    EvidenceQueryResult,
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


    def test_claim_order_prefers_wide_scope_then_interleaves_hosts_by_year(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            keys = [
                EvidenceQueryKey(
                    "same.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "v1",
                ),
                EvidenceQueryKey(
                    "same.example",
                    TemporalScope(1998, 1998),
                    "wayback",
                    "v1",
                ),
                EvidenceQueryKey(
                    "other.example",
                    TemporalScope(1997, 1997),
                    "wayback",
                    "v1",
                ),
                EvidenceQueryKey(
                    "wide.example",
                    TemporalScope(1996, 2001),
                    "wayback",
                    "v1",
                ),
            ]
            store.enqueue_evidence_tasks(keys)

            claimed = store.claim_evidence_tasks(owner="worker-priority", limit=3)

            self.assertEqual(
                [
                    (
                        task.key.hostname,
                        task.key.temporal_scope.year_from,
                        task.key.temporal_scope.year_to,
                    )
                    for task in claimed
                ],
                [
                    ("wide.example", 1996, 2001),
                    ("other.example", 1997, 1997),
                    ("same.example", 1997, 1997),
                ],
            )
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

    def _ready_reservoir(self, *, cursor="0"):
        reservoir = self._reservoir()
        return Reservoir(
            reservoir_id=reservoir.reservoir_id,
            domain_id=reservoir.domain_id,
            adapter_id=reservoir.adapter_id,
            root_locator=reservoir.root_locator,
            enumeration_kind=reservoir.enumeration_kind,
            capacity_lower=reservoir.capacity_lower,
            capacity_upper=reservoir.capacity_upper,
            evidence_mode=reservoir.evidence_mode,
            cursor=cursor,
            state=ReservoirState.READY,
        )

    @staticmethod
    def _lease_limits():
        return {
            "max_records": 10,
            "max_requests": 2,
            "max_bytes": 4096,
            "max_seconds": 10,
            "resource_class": "general",
            "expected_evidence_tasks": 2,
            "expected_novel_eed": 1.5,
        }

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


    def test_attempt_metrics_and_origin_coverage_are_operational_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3", clock=lambda: 123.0)
            store.save_domain(self._domain())
            reservoir = self._reservoir()
            store.save_reservoir(reservoir)
            lease = WorkLease.create(
                reservoir_id=reservoir.reservoir_id,
                max_records=10,
                max_requests=2,
                max_bytes=4096,
                max_seconds=30,
                now=100.0,
                expires_at=130.0,
            )
            store.save_lease(lease)
            key = self._key()
            store.enqueue_evidence_tasks([key])
            store.record_evidence_task_origins(
                [key],
                source_key="source-a",
                reservoir_id=reservoir.reservoir_id,
                lease_id=lease.lease_id,
            )

            inserted = store.record_evidence_task_attempt_metric(
                key,
                attempt=1,
                state=CDXQueryState.PASS,
                provider_requests=2,
                provider_elapsed_milliseconds=50,
                pages_seen=2,
                records_seen=3,
            )

            self.assertTrue(inserted)
            self.assertFalse(
                store.record_evidence_task_attempt_metric(
                    key,
                    attempt=1,
                    state=CDXQueryState.PASS,
                    provider_requests=99,
                    provider_elapsed_milliseconds=99,
                    pages_seen=99,
                    records_seen=99,
                )
            )
            self.assertEqual(
                store.evidence_attempt_metric_summary()["exact"],
                {
                    "attempts": 1,
                    "provider_requests": 2,
                    "provider_elapsed_milliseconds": 50,
                    "pages_seen": 2,
                    "records_seen": 3,
                },
            )
            self.assertEqual(store.source_provider_request_totals(), {"source-a": 2})
            self.assertEqual(
                store.evidence_task_origin_coverage()[CDXQueryState.PENDING.value],
                {"tasks": 1, "with_origin": 1},
            )
            store.close()

    def test_decomposed_range_is_terminal_but_not_provider_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            parent = EvidenceQueryKey(
                "range.example",
                TemporalScope(1996, 1998),
                "wayback",
                "v1",
            )
            children = [
                EvidenceQueryKey(
                    "range.example",
                    TemporalScope(year, year),
                    "wayback",
                    "v1",
                )
                for year in (1996, 1998)
            ]
            store.enqueue_evidence_tasks([parent])
            claimed = store.claim_evidence_tasks(owner="worker", limit=1)
            self.assertEqual([task.key for task in claimed], [parent])

            store.finish_range_task(
                parent,
                CDXQueryState.DECOMPOSED,
                followup_keys=children,
                owner="worker",
            )

            parent_task = store.get_evidence_task(parent)
            self.assertEqual(parent_task.state, CDXQueryState.DECOMPOSED.value)
            self.assertEqual(
                store.resolve_provider_coverage_masks(
                    ["range.example"],
                    provider="wayback",
                    policy_version="v1",
                )["range.example"],
                0,
            )
            self.assertEqual(
                {
                    task.key.temporal_scope.year_from
                    for task in store.list_evidence_tasks()
                    if task.state == CDXQueryState.PENDING.value
                },
                {1996, 1998},
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

    def test_fresh_grant_uses_cursor_and_prevents_second_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            domain = self._domain()
            ready = self._ready_reservoir(cursor="128")
            store.save_domain(domain)
            store.save_reservoir(ready)

            first = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-a",
                now=100.0,
                **self._lease_limits(),
            )
            second = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-b",
                now=100.0,
                **self._lease_limits(),
            )

            self.assertIsNotNone(first)
            self.assertEqual(first.cursor_start, "128")
            self.assertEqual(first.state, LeaseState.GRANTED)
            self.assertIsNone(second)
            self.assertEqual(
                store.get_reservoir(ready.reservoir_id).state,
                ReservoirState.LEASED,
            )
            store.close()

    def test_finalize_lease_atomically_advances_reservoir_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            ready = self._ready_reservoir(cursor="128")
            store.save_domain(self._domain())
            store.save_reservoir(ready)
            granted = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-a",
                now=100.0,
                **self._lease_limits(),
            )
            running = granted.start()
            store.save_lease(running)

            store.finalize_lease(running, next_cursor="256", exhausted=False)

            self.assertEqual(
                store.get_lease(running.lease_id).state,
                LeaseState.SUCCEEDED,
            )
            advanced = store.get_reservoir(ready.reservoir_id)
            self.assertEqual(
                (advanced.state, advanced.cursor),
                (ReservoirState.READY, "256"),
            )
            store.close()

    def test_finalize_lease_wrong_owner_rolls_back_both_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            ready = self._ready_reservoir(cursor="128")
            store.save_domain(self._domain())
            store.save_reservoir(ready)
            granted = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-a",
                now=100.0,
                **self._lease_limits(),
            )
            running = granted.start()
            store.save_lease(running)

            with self.assertRaises(ValueError):
                store.finalize_lease(
                    replace(running, owner="worker-b"),
                    next_cursor="256",
                    exhausted=False,
                )

            self.assertEqual(
                store.get_lease(running.lease_id).state,
                LeaseState.RUNNING,
            )
            unchanged = store.get_reservoir(ready.reservoir_id)
            self.assertEqual(
                (unchanged.state, unchanged.cursor),
                (ReservoirState.LEASED, "128"),
            )
            store.close()

    def test_abort_lease_restores_original_cursor(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            ready = self._ready_reservoir(cursor="128")
            store.save_domain(self._domain())
            store.save_reservoir(ready)
            granted = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-a",
                now=100.0,
                **self._lease_limits(),
            )
            running = granted.start()
            store.save_lease(running)

            store.abort_lease(running)

            self.assertEqual(
                store.get_lease(running.lease_id).state,
                LeaseState.ABORTED,
            )
            restored = store.get_reservoir(ready.reservoir_id)
            self.assertEqual(
                (restored.state, restored.cursor),
                (ReservoirState.READY, "128"),
            )
            store.close()

    def test_expired_lease_restores_reservoir_at_prior_cursor(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            domain = self._domain()
            ready = self._ready_reservoir(cursor="256")
            store.save_domain(domain)
            store.save_reservoir(ready)
            granted = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="worker-a",
                now=0.0,
                **self._lease_limits(),
            )
            store.save_lease(granted.start())

            changed = store.recover_expired_leases(now=11.0)

            self.assertEqual(changed, 1)
            self.assertEqual(
                store.get_lease(granted.lease_id).state,
                LeaseState.EXPIRED,
            )
            restored = store.get_reservoir(granted.reservoir_id)
            self.assertEqual(
                (restored.state, restored.cursor),
                (ReservoirState.READY, "256"),
            )
            store.close()

    def test_finish_evidence_tasks_batches_terminal_and_retryable_updates(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            keys = [self._key("wayback", "v1"), self._key("arquivo", "v1")]
            store.enqueue_evidence_tasks(keys)
            store.claim_evidence_tasks(owner="worker-1", limit=10)

            results = [
                EvidenceQueryResult("example.com", 1997, CDXQueryState.PASS, key=keys[0]),
                EvidenceQueryResult("example.com", 1997, CDXQueryState.TRANSIENT_ERROR, key=keys[1]),
            ]

            self.assertEqual(
                store.finish_evidence_tasks(results, owner="worker-1"),
                2,
            )
            self.assertEqual(store.get_evidence_task(keys[0]).state, CDXQueryState.PASS)
            self.assertEqual(
                store.get_evidence_task(keys[1]).state,
                CDXQueryState.TRANSIENT_ERROR,
            )
            store.close()

    def test_finish_evidence_tasks_enforces_ownership_atomically(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            keys = [self._key("wayback", "v1"), self._key("arquivo", "v1")]
            store.enqueue_evidence_tasks(keys)
            store.claim_evidence_tasks(owner="worker-1", limit=1, keys=[keys[0]])
            store.claim_evidence_tasks(owner="worker-2", limit=1, keys=[keys[1]])

            results = [
                EvidenceQueryResult("example.com", 1997, CDXQueryState.PASS, key=keys[0]),
                EvidenceQueryResult("example.com", 1997, CDXQueryState.PASS, key=keys[1]),
            ]
            with self.assertRaises(KeyError):
                store.finish_evidence_tasks(results, owner="worker-1")

            self.assertEqual(store.get_evidence_task(keys[0]).state, CDXQueryState.PENDING)
            self.assertEqual(store.get_evidence_task(keys[1]).state, CDXQueryState.PENDING)
            store.close()

    def test_finish_evidence_tasks_rejects_unsupported_state_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._key()
            store.enqueue_evidence_tasks([key])
            store.claim_evidence_tasks(owner="worker-1", limit=1)

            result = EvidenceQueryResult("example.com", 1997, CDXQueryState.PENDING, key=key)
            with self.assertRaises(ValueError):
                store.finish_evidence_tasks([result], owner="worker-1")
            self.assertEqual(store.get_evidence_task(key).state, CDXQueryState.PENDING)
            store.close()

    def test_finish_evidence_task_remains_one_item_compatibility_wrapper(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._key()
            store.enqueue_evidence_tasks([key])
            store.claim_evidence_tasks(owner="worker-1", limit=1)
            store.finish_evidence_task(key, CDXQueryState.PASS, owner="worker-1")
            self.assertEqual(store.get_evidence_task(key).state, CDXQueryState.PASS)
            store.close()

    def test_finish_evidence_task_wrapper_preserves_retry_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._key()
            store.enqueue_evidence_tasks([key])
            store.claim_evidence_tasks(owner="worker-1", limit=1)
            store.finish_evidence_task(
                key,
                CDXQueryState.TRANSIENT_ERROR,
                owner="worker-1",
                retry_at=123.5,
            )
            task = store.get_evidence_task(key)
            self.assertEqual(task.state, CDXQueryState.TRANSIENT_ERROR)
            self.assertEqual(task.retry_at, 123.5)
            store.close()

    def test_completed_provider_ranges_are_reused_as_coverage_masks(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            range_key = EvidenceQueryKey(
                "example.com", TemporalScope(1996, 1998), "wayback", "v1"
            )
            invalid_key = EvidenceQueryKey(
                "invalid.example", TemporalScope(1996, 1998), "wayback", "v1"
            )
            store.enqueue_evidence_tasks([range_key, invalid_key])
            claimed = store.claim_evidence_tasks(owner="worker-1", limit=2)
            self.assertEqual(len(claimed), 2)
            store.finish_range_task(
                range_key,
                CDXQueryState.PASS,
                owner="worker-1",
            )
            store.finish_range_task(
                invalid_key,
                CDXQueryState.INVALID,
                owner="worker-1",
            )

            masks = store.resolve_provider_coverage_masks(
                ["example.com", "invalid.example"],
                provider="wayback",
                policy_version="v1",
            )

            self.assertEqual(
                masks["example.com"],
                YEAR_BITS[1996] | YEAR_BITS[1997] | YEAR_BITS[1998],
            )
            self.assertEqual(masks["invalid.example"], 0)
            store.close()

    def test_finish_range_task_atomically_fans_out_exact_followups(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            range_key = EvidenceQueryKey(
                "example.com", TemporalScope(1996, 1998), "wayback", "v1"
            )
            exact_keys = [
                EvidenceQueryKey(
                    "example.com", TemporalScope(year, year), "wayback", "v1"
                )
                for year in (1996, 1998)
            ]
            store.enqueue_evidence_tasks([range_key])
            store.claim_evidence_tasks(owner="worker-1", limit=1)

            created = store.finish_range_task(
                range_key,
                CDXQueryState.PASS,
                followup_keys=exact_keys,
                owner="worker-1",
            )

            self.assertEqual(created, 2)
            self.assertEqual(store.get_evidence_task(range_key).state, CDXQueryState.PASS)
            self.assertEqual(
                {task.key for task in store.list_evidence_tasks()},
                {range_key, *exact_keys},
            )
            store.close()


    def test_range_followups_inherit_source_origin_and_credit_host_year(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3", clock=lambda: 100.0)
            store.save_domain(self._domain())
            ready = self._ready_reservoir()
            store.save_reservoir(ready)
            lease = store.grant_fresh_lease(
                ready.reservoir_id,
                owner="source-worker",
                now=100.0,
                **self._lease_limits(),
            )
            self.assertIsNotNone(lease)
            assert lease is not None

            range_key = EvidenceQueryKey(
                "example.com",
                TemporalScope(1996, 1998),
                "wayback",
                "v1",
            )
            followup = EvidenceQueryKey(
                "example.com",
                TemporalScope(1997, 1997),
                "wayback",
                "v1",
            )
            store.enqueue_evidence_tasks([range_key])
            store.record_evidence_task_origins(
                [range_key],
                source_key="source-a",
                reservoir_id=ready.reservoir_id,
                lease_id=lease.lease_id,
            )
            store.claim_evidence_tasks(
                owner="evidence-worker",
                limit=1,
                keys=[range_key],
            )
            store.finish_range_task(
                range_key,
                CDXQueryState.PASS,
                followup_keys=[followup],
                owner="evidence-worker",
            )

            inherited = store.connection.execute(
                """
                SELECT source_key, reservoir_id, lease_id
                FROM evidence_task_origins
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                """,
                store._values(followup),
            ).fetchone()
            self.assertIsNotNone(inherited)
            self.assertEqual(inherited["source_key"], "source-a")

            self.assertEqual(
                store.attribute_task_host_years(followup, [1997]),
                1,
            )
            self.assertEqual(
                store.resolve_primary_source_origins(
                    [("example.com", 1997)]
                ),
                {("example.com", 1997): "source-a"},
            )
            store.close()


if __name__ == "__main__":
    unittest.main()
