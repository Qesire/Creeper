import tempfile
import unittest
from pathlib import Path

from creeper.records.candidates import (
    CandidateRecord,
    CandidateSourceScope,
    CandidateStatus,
)
from creeper.storage.candidate_store import CandidateStore


class CandidateLedgerTests(unittest.TestCase):
    def test_duplicate_observation_updates_one_current_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(Path(tmp) / "candidates.sqlite3", clock=lambda: 10.0)
            record = CandidateRecord(
                hostname=" Example.COM ",
                source_id="local-fixture",
                scope=CandidateSourceScope.LOCAL_DISCOVERY,
                source_locator="fixture://first",
                source_year=1997,
            )

            store.record_observation(record)
            store.record_observation(record)

            self.assertEqual(store.count(), 1)
            entry = store.get(
                "example.com",
                CandidateSourceScope.LOCAL_DISCOVERY,
            )
            self.assertIsNotNone(entry)
            self.assertEqual(entry.observation_count, 2)
            self.assertEqual(entry.source_locator, "fixture://first")
            self.assertEqual(store.history_count(), 1)
            store.close()

    def test_evidence_resolution_removes_active_but_preserves_audit_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(Path(tmp) / "candidates.sqlite3")
            store.record_observation(
                CandidateRecord(
                    hostname="novel.example",
                    source_id="local-fixture",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_locator="fixture://candidate",
                )
            )

            self.assertEqual(
                [row.hostname for row in store.iter_active_candidates()],
                ["novel.example"],
            )
            self.assertEqual(
                store.mark_annual_evidence_obtained("novel.example"),
                1,
            )

            self.assertEqual(list(store.iter_active_candidates()), [])
            entry = store.get(
                "novel.example",
                CandidateSourceScope.LOCAL_DISCOVERY,
            )
            self.assertEqual(
                entry.status,
                CandidateStatus.ANNUAL_EVIDENCE_OBTAINED,
            )
            self.assertEqual(store.history_count(), 2)
            store.close()

    def test_restricted_scopes_and_unparsed_values_stay_separate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(Path(tmp) / "candidates.sqlite3")
            store.record_observations([
                CandidateRecord(
                    hostname="isc.example",
                    source_id="isc-reference-1997",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_locator="fixture://isc",
                ),
                CandidateRecord(
                    hostname="cc.example",
                    source_id="common-crawl-index",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_locator="fixture://cc",
                ),
                CandidateRecord(
                    hostname="not a hostname",
                    source_id="broken-source",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                    source_locator="fixture://bad",
                ),
            ])

            self.assertEqual(list(store.iter_active_candidates()), [])
            self.assertEqual(
                [row.hostname for row in store.iter_isc_reference()],
                ["isc.example"],
            )
            excluded = store.get(
                "cc.example",
                CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
            )
            self.assertEqual(
                excluded.status,
                CandidateStatus.EXCLUDED_COMMON_CRAWL,
            )
            unparsed = list(store.iter_unparsed())
            self.assertEqual(len(unparsed), 1)
            self.assertEqual(unparsed[0].raw_value, "not a hostname")
            self.assertEqual(
                unparsed[0].reason,
                "hostname_normalization_failed",
            )
            store.close()

    def test_restart_preserves_candidate_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "candidates.sqlite3"
            first = CandidateStore(path)
            first.record_observation(
                CandidateRecord(
                    hostname="restart.example",
                    source_id="fixture",
                    scope=CandidateSourceScope.LOCAL_DISCOVERY,
                )
            )
            first.close()

            second = CandidateStore(path)
            self.assertEqual(
                [row.hostname for row in second.iter_active_candidates()],
                ["restart.example"],
            )
            second.mark_baseline_overlap("restart.example")
            second.close()

            third = CandidateStore(path)
            entry = third.get(
                "restart.example",
                CandidateSourceScope.LOCAL_DISCOVERY,
            )
            self.assertEqual(entry.status, CandidateStatus.BASELINE_OVERLAP)
            self.assertEqual(third.history_count(), 2)
            third.close()


if __name__ == "__main__":
    unittest.main()
