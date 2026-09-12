from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from creeper.evidence.actions import (
    EvidenceActionKind,
    classify_evidence_action,
    posterior_host_year_yield,
)
from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_queue import DurableEvidenceQueue


class EvidenceActionValueTests(unittest.TestCase):
    @staticmethod
    def _exact(hostname: str) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1997, 1997),
            "wayback",
            "cdx-v1",
        )

    @staticmethod
    def _domain(hostname: str) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1996, 2001),
            "wayback",
            "cdx-domain-v1",
        )

    def _seed_history(self, store: ControlStore) -> None:
        exact = self._exact("history-exact.com")
        domain = self._domain("history-domain.com")
        store.enqueue_evidence_tasks([exact, domain])
        claimed = store.claim_evidence_tasks(
            owner="history-worker",
            limit=2,
            keys=[exact, domain],
        )
        self.assertEqual({task.key for task in claimed}, {exact, domain})
        store.record_evidence_task_attempt_metric(
            exact,
            attempt=1,
            state=CDXQueryState.PASS,
            provider_requests=10,
            provider_elapsed_milliseconds=100,
            pages_seen=10,
            records_seen=20,
        )
        store.record_evidence_task_attempt_metric(
            domain,
            attempt=1,
            state=CDXQueryState.DECOMPOSED,
            provider_requests=10,
            provider_elapsed_milliseconds=100,
            pages_seen=10,
            records_seen=20,
        )
        store.finish_evidence_task(
            exact,
            CDXQueryState.PASS,
            owner="history-worker",
        )
        store.finish_range_task(
            domain,
            CDXQueryState.DECOMPOSED,
            owner="history-worker",
            followup_keys=(),
        )
        store.publish_evidence_action_final_rewards(
            {
                "exact": {
                    "novel_host_years": 20,
                    "novel_eed": "20",
                },
                "domain": {
                    "novel_host_years": 1,
                    "novel_eed": "1",
                },
            },
            baseline_signature="baseline-a",
            model_signature="model-a",
        )

    def test_action_classification_matches_existing_queue_semantics(self) -> None:
        self.assertEqual(
            classify_evidence_action(self._exact("a.com")),
            EvidenceActionKind.EXACT,
        )
        self.assertEqual(
            classify_evidence_action(
                EvidenceQueryKey(
                    "b.com",
                    TemporalScope(1996, 2001),
                    "wayback",
                    "cdx-v1",
                )
            ),
            EvidenceActionKind.RANGE,
        )
        self.assertEqual(
            classify_evidence_action(self._domain("c.com")),
            EvidenceActionKind.DOMAIN,
        )
        self.assertEqual(
            classify_evidence_action(
                EvidenceQueryKey(
                    "d.com",
                    TemporalScope(1996, 2001),
                    "rdap",
                    "rdap-registration-v1",
                )
            ),
            EvidenceActionKind.RDAP,
        )

    def test_posterior_uses_attempts_as_cost_floor(self) -> None:
        # Five zero-request attempts are not allowed to look like free yield.
        value = posterior_host_year_yield(
            EvidenceActionKind.EXACT,
            final_novel_host_years=2,
            provider_requests=0,
            attempts=5,
        )
        self.assertAlmostEqual(value, (2 + 4 * 0.25) / (5 + 4))

    def test_formal_reward_can_reverse_static_domain_first_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            self._seed_history(store)
            exact = self._exact("candidate-exact.com")
            domain = self._domain("candidate-domain.com")
            store.enqueue_evidence_tasks([domain, exact])
            store.set_eed_tld_weights({"com": 1.0})

            summary = store.evidence_action_value_summary()
            self.assertGreater(
                summary["exact"].posterior_host_years_per_request,
                summary["domain"].posterior_host_years_per_request,
            )

            claimed = store.claim_evidence_tasks(
                owner="value-worker",
                limit=1,
            )

            self.assertEqual([task.key for task in claimed], [exact])
            store.close()

    def test_durable_queue_uses_same_learned_value_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            self._seed_history(store)
            exact = self._exact("queue-exact.com")
            domain = self._domain("queue-domain.com")
            store.enqueue_evidence_tasks([domain, exact])
            store.set_eed_tld_weights({"com": 1.0})
            queue = DurableEvidenceQueue(store)

            claimed = queue.claim(
                owner="queue-worker",
                limit=1,
                providers=("wayback",),
                lease_seconds=30.0,
            )

            self.assertEqual([task.key for task in claimed], [exact])
            store.close()

    def test_range_first_stays_configured_until_formal_reward_then_adapts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            self.assertAlmostEqual(
                store.recommended_range_first_fraction(0.10),
                0.10,
            )
            exact = self._exact("adapt-exact.com")
            ranged = EvidenceQueryKey(
                "adapt-range.com",
                TemporalScope(1996, 2001),
                "wayback",
                "cdx-v1",
            )
            store.enqueue_evidence_tasks([exact, ranged])
            store.record_evidence_task_attempt_metric(
                exact,
                attempt=1,
                state=CDXQueryState.PASS,
                provider_requests=25,
                provider_elapsed_milliseconds=100,
                pages_seen=25,
                records_seen=25,
            )
            store.record_evidence_task_attempt_metric(
                ranged,
                attempt=1,
                state=CDXQueryState.PASS,
                provider_requests=25,
                provider_elapsed_milliseconds=100,
                pages_seen=25,
                records_seen=25,
            )
            # Cost data alone must not mutate generation policy; only formal
            # readiness reward is allowed to close the control loop.
            self.assertAlmostEqual(
                store.recommended_range_first_fraction(0.10),
                0.10,
            )
            store.publish_evidence_action_final_rewards(
                {
                    "exact": {
                        "novel_host_years": 5,
                        "novel_eed": "5",
                    },
                    "range": {
                        "novel_host_years": 30,
                        "novel_eed": "30",
                    },
                },
                baseline_signature="baseline-a",
                model_signature="model-a",
            )

            exact_yield = (5 + 4 * 0.25) / (25 + 4)
            range_yield = (30 + 4 * 0.50) / (25 + 4)
            expected = range_yield / (exact_yield + range_yield)
            self.assertAlmostEqual(
                store.recommended_range_first_fraction(0.10),
                expected,
            )
            # Zero remains an explicit operator kill-switch.
            self.assertEqual(
                store.recommended_range_first_fraction(0.0),
                0.0,
            )
            store.close()

    def test_duplicate_attempt_metric_does_not_double_count_action_cost(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._exact("metric.com")
            store.enqueue_evidence_tasks([key])
            self.assertTrue(
                store.record_evidence_task_attempt_metric(
                    key,
                    attempt=1,
                    state=CDXQueryState.PASS,
                    provider_requests=3,
                    provider_elapsed_milliseconds=11,
                    pages_seen=2,
                    records_seen=5,
                )
            )
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

            summary = store.evidence_action_value_summary()["exact"]

            self.assertEqual(summary.attempts, 1)
            self.assertEqual(summary.provider_requests, 3)
            self.assertEqual(summary.provider_elapsed_milliseconds, 11)
            store.close()

    def test_authority_change_resets_reward_but_preserves_cost_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = ControlStore(Path(tmp) / "control.sqlite3")
            key = self._exact("authority.com")
            store.enqueue_evidence_tasks([key])
            store.record_evidence_task_attempt_metric(
                key,
                attempt=1,
                state=CDXQueryState.PASS,
                provider_requests=2,
                provider_elapsed_milliseconds=10,
                pages_seen=1,
                records_seen=1,
            )
            self.assertTrue(
                store.publish_evidence_action_final_rewards(
                    {
                        "exact": {
                            "novel_host_years": 4,
                            "novel_eed": "2",
                        }
                    },
                    baseline_signature="baseline-a",
                    model_signature="model-a",
                )
            )
            before = store.evidence_action_value_summary()["exact"]
            self.assertEqual(before.final_novel_host_years, 4)

            changed = store.publish_evidence_action_final_rewards(
                {},
                baseline_signature="baseline-b",
                model_signature="model-a",
            )
            after = store.evidence_action_value_summary()["exact"]

            self.assertTrue(changed)
            self.assertEqual(after.final_novel_host_years, 0)
            self.assertEqual(after.final_novel_eed, 0.0)
            self.assertEqual(after.provider_requests, 2)
            self.assertEqual(after.attempts, 1)
            store.close()


if __name__ == "__main__":
    unittest.main()
