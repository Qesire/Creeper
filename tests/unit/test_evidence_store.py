import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS
from creeper.evidence.policies import EvidenceCapsule
from creeper.storage.evidence_store import EvidenceStore


class EvidenceStoreTests(unittest.TestCase):
    def test_put_is_idempotent_and_preserves_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            capsule = EvidenceCapsule(
                "Example.COM", 1997, "cdx", "capture_timestamp_year",
                "19970101000000", "http://example.com/", "a" * 64, "cdx-v1"
            )
            self.assertEqual(store.put_many([capsule]), 1)
            self.assertEqual(store.put_many([capsule]), 0)
            rows = store.for_hostname("example.com")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].hostname, "example.com")
            self.assertEqual(rows[0].payload_hash, "a" * 64)
            store.close()

    def test_put_many_reports_only_new_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            first = EvidenceCapsule(
                "alpha.example.com", 1997, "cdx", "capture_timestamp_year",
                "19970101000000", "http://alpha.example.com/", "a" * 64, "cdx-v1"
            )
            second = EvidenceCapsule(
                "beta.example.com", 1997, "cdx", "capture_timestamp_year",
                "19970101000000", "http://beta.example.com/", "b" * 64, "cdx-v1"
            )
            self.assertEqual(store.put_many([first]), 1)
            self.assertEqual(store.put_many([first, second]), 1)
            self.assertEqual(store.count(), 2)
            store.close()

    def test_put_many_preserves_policy_version_in_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            capsule_v1 = EvidenceCapsule(
                "example.com", 1997, "cdx", "capture_timestamp_year",
                "19970101000000", "http://example.com/", "b" * 64, "cdx-v1"
            )
            capsule_v2 = EvidenceCapsule(
                "example.com", 1997, "cdx", "capture_timestamp_year",
                "19970101000000", "http://example.com/", "b" * 64, "cdx-v2"
            )

            self.assertEqual(store.put_many([capsule_v1, capsule_v2]), 2)

            self.assertEqual(store.count(), 2)
            self.assertEqual(
                {row.policy_version for row in store.for_hostname("example.com")},
                {"cdx-v1", "cdx-v2"},
            )
            store.close()

    def test_host_year_index_deduplicates_provider_and_payload_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            store.put_many([
                EvidenceCapsule(
                    "same.example.com", 1998, "wayback", "capture_timestamp_year",
                    "19980101000000", "http://same.example.com/a", "a" * 64, "cdx-v1"
                ),
                EvidenceCapsule(
                    "same.example.com", 1998, "arquivo", "capture_timestamp_year",
                    "19980201000000", "http://same.example.com/b", "b" * 64, "archive-v1"
                ),
                EvidenceCapsule(
                    "same.example.com", 2000, "wayback", "capture_timestamp_year",
                    "20000101000000", "http://same.example.com/c", "c" * 64, "cdx-v1"
                ),
            ])

            rows = store.host_years_after(0)

            self.assertEqual(
                [(row.hostname, row.year) for row in rows],
                [("same.example.com", 1998), ("same.example.com", 2000)],
            )
            self.assertEqual(store.max_host_year_sequence(), 2)
            self.assertEqual(store.host_years_after(rows[0].sequence, limit=1)[0].year, 2000)
            store.close()

    def test_canonical_host_year_capsules_returns_one_deterministic_capsule(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            store.put_many([
                EvidenceCapsule(
                    "same.example.com", 1998, "wayback", "capture_timestamp_year",
                    "19980101000000", "http://same.example.com/a", "b" * 64, "cdx-v1"
                ),
                EvidenceCapsule(
                    "same.example.com", 1998, "arquivo", "capture_timestamp_year",
                    "19980201000000", "http://same.example.com/b", "a" * 64, "archive-v1"
                ),
            ])

            rows = store.canonical_host_year_capsules()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].hostname, "same.example.com")
            self.assertEqual(rows[0].year, 1998)
            self.assertEqual(rows[0].provider, "arquivo")
            store.close()

    def test_evidence_store_uses_wal_for_multi_process_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            mode = store.connection.execute("PRAGMA journal_mode").fetchone()[0]
            timeout = store.connection.execute("PRAGMA busy_timeout").fetchone()[0]
            self.assertEqual(str(mode).lower(), "wal")
            self.assertGreaterEqual(int(timeout), 30_000)
            store.close()

    def test_resolve_year_masks_normalizes_deduplicates_and_returns_zero_for_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            store.put_many([
                EvidenceCapsule(
                    "alpha.example.com", 1996, "cdx", "capture_timestamp_year",
                    "19960101000000", "http://alpha.example.com/", "c" * 64, "cdx-v1"
                ),
                EvidenceCapsule(
                    "alpha.example.com", 2001, "cdx", "capture_timestamp_year",
                    "20010101000000", "http://alpha.example.com/", "d" * 64, "cdx-v1"
                ),
                EvidenceCapsule(
                    "beta.example.com", 1998, "cdx", "capture_timestamp_year",
                    "19980101000000", "http://beta.example.com/", "e" * 64, "cdx-v1"
                ),
            ])

            masks = store.resolve_year_masks([
                " Alpha.Example.COM ", "alpha.example.com", "beta.example.com",
                "missing.example.com", "not-a-host", "",
            ])

            self.assertEqual(masks, {
                "alpha.example.com": YEAR_BITS[1996] | YEAR_BITS[2001],
                "beta.example.com": YEAR_BITS[1998],
                "missing.example.com": 0,
            })
            store.close()

    def test_resolve_year_masks_limits_hostname_in_chunks_to_900(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = EvidenceStore(Path(tmp) / "evidence.sqlite3")
            hostnames = [f"host-{index}.example.com" for index in range(1801)]
            store.put_many([
                EvidenceCapsule(
                    hostname, 1997, "cdx", "capture_timestamp_year",
                    "19970101000000", f"http://{hostname}/", f"{index:064x}", "cdx-v1"
                )
                for index, hostname in enumerate(hostnames)
            ])
            statements = []
            store.connection.set_trace_callback(statements.append)

            masks = store.resolve_year_masks(hostnames, chunk_size=1000)

            selects = [statement for statement in statements if "WHERE hostname IN (" in statement]
            self.assertEqual(len(selects), 3)
            self.assertTrue(all(statement.count("'") // 2 <= 900 for statement in selects))
            self.assertEqual(len(masks), len(hostnames))
            self.assertTrue(all(mask == YEAR_BITS[1997] for mask in masks.values()))
            store.close()


if __name__ == "__main__":
    unittest.main()
