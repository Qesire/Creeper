import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.records.candidates import CandidateSourceScope
from creeper.sources.auxiliary_audit import audit_v3_auxiliary
from creeper.sources.local.auxiliary import V3AuxiliaryURLAdapter


class AuxiliarySourceTests(unittest.TestCase):
    def test_enumerates_auxiliary_files_with_line_provenance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            merged = root / "merged260909-3"
            merged.mkdir()
            (merged / "deduplicated_urls_1996-1997.txt").write_text(
                "new.example\nnot a hostname\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_2001-2002.txt").write_text(
                "other.example\n", encoding="utf-8"
            )
            adapter = V3AuxiliaryURLAdapter(root)
            records = list(adapter.enumerate())

            self.assertEqual(len(records), 3)
            self.assertEqual(records[0].scope, CandidateSourceScope.LOCAL_DISCOVERY)
            self.assertIn("deduplicated_urls_", records[0].source_id)
            self.assertTrue(records[0].locator.endswith(":1"))
            self.assertEqual(
                [item.hostname for record in records for item in adapter.extract_hosts(record)],
                ["new.example", "other.example"],
            )

    def test_audit_deduplicates_and_separates_authority_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            merged = root / "merged260909-3"
            merged.mkdir()
            for year in range(1996, 2002):
                (merged / f"{year}.txt").write_text(
                    "annual.example\n", encoding="utf-8"
                )
            (merged / "candidate_pool.txt").write_text(
                "candidate.example\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_1996-1997.txt").write_text(
                "new.example\ncandidate.example\nnew.example\ninvalid\n", encoding="utf-8"
            )
            (merged / "deduplicated_urls_2001-2002.txt").write_text(
                "annual.example\n", encoding="utf-8"
            )
            index = BaselineIndex.build(root, root / "index.sqlite3")
            index.close()

            report = audit_v3_auxiliary(
                root, root / "index.sqlite3", total_limit=10, limit_per_file=10
            )

            self.assertEqual(report["raw_lines"], 5)
            self.assertEqual(report["valid_hostname_lines"], 4)
            self.assertEqual(report["invalid_lines"], 1)
            self.assertEqual(report["duplicate_hostname_lines"], 1)
            self.assertEqual(report["unique_hostnames"], 3)
            self.assertEqual(report["annual_authority_overlap"], 1)
            self.assertEqual(report["official_candidate_overlap"], 1)
            self.assertEqual(report["potential_active_discoveries"], 2)


if __name__ == "__main__":
    unittest.main()
