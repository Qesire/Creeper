import tempfile
import unittest
from pathlib import Path

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
            store.put(capsule)
            store.put(capsule)
            rows = store.for_hostname("example.com")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].hostname, "example.com")
            self.assertEqual(rows[0].payload_hash, "a" * 64)
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

            store.put_many([capsule_v1, capsule_v2])

            self.assertEqual(store.count(), 2)
            self.assertEqual(
                {row.policy_version for row in store.for_hostname("example.com")},
                {"cdx-v1", "cdx-v2"},
            )
            store.close()


if __name__ == "__main__":
    unittest.main()
