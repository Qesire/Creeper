import hashlib
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.builder import build_snapshot


class SnapshotBuilderTests(unittest.TestCase):
    def test_builder_deduplicates_and_counts_baseline_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260912-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text(
                    "old.example\n" if year == 1996 else "", encoding="utf-8"
                )
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            annual_hashes = {
                f"{year}.txt": hashlib.sha256(
                    (annual / f"{year}.txt").read_bytes()
                ).hexdigest()
                for year in range(1996, 2002)
            }
            candidate_hash = hashlib.sha256(
                (annual / "candidate_pool.txt").read_bytes()
            ).hexdigest()
            model_hash = "d" * 64
            baseline_eed = "10"
            manifest = {
                "baseline_id": annual.name,
                "annual_file_hashes": annual_hashes,
                "candidate_file_hash": candidate_hash,
                "model_hash": model_hash,
                "baseline_eed": baseline_eed,
                "authority_digest": authority_digest(
                    baseline_id=annual.name,
                    annual_file_hashes=annual_hashes,
                    candidate_file_hash=candidate_hash,
                    model_hash=model_hash,
                    baseline_eed=baseline_eed,
                ),
            }
            make = lambda host, year: EvidenceCapsule(
                host, year, "wayback-cdx", "capture_timestamp_year",
                f"{year}0101000000", f"http://{host}/", "a" * 64, "evidence-v1"
            )
            snapshot = build_snapshot(
                "snap-1",
                [make("old.example", 1996), make("new.example", 1997), make("new.example", 1997)],
                index,
                manifest,
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
