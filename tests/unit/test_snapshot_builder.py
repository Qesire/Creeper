import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.builder import build_snapshot


class SnapshotBuilderTests(unittest.TestCase):
    def test_builder_deduplicates_and_counts_baseline_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text(
                    "old.example\n" if year == 1996 else "", encoding="utf-8"
                )
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            make = lambda host, year: EvidenceCapsule(
                host, year, "wayback-cdx", "capture_timestamp_year",
                f"{year}0101000000", f"http://{host}/", "a" * 64, "evidence-v1"
            )
            snapshot = build_snapshot(
                "snap-1",
                [make("old.example", 1996), make("new.example", 1997), make("new.example", 1997)],
                index,
                {"baseline_id": "merged260909-3", "annual_file_hashes": {f"{y}.txt": "b" * 64 for y in range(1996, 2002)}},
                code_revision="c" * 64,
                source_report_set=("source.json",),
                cdx_audit_set=("audit.json",),
                eed_report={"equivalent_english_domains": "0.5"},
            )
            self.assertEqual([(x.hostname, x.year) for x in snapshot.novel_records], [("new.example", 1997)])
            self.assertEqual(snapshot.overlap_count, 1)
            self.assertEqual(snapshot.invalid_count, 0)
            index.close()


if __name__ == "__main__":
    unittest.main()
