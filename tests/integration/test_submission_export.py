import json
import tempfile
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.export import export_submission


class SubmissionExportTests(unittest.TestCase):
    def test_export_deduplicates_and_excludes_baseline_year(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text(
                    "baseline.example\n" if year == 1996 else "", encoding="utf-8"
                )
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            make = lambda host, year: EvidenceCapsule(
                host,
                year,
                "fixture",
                "capture_timestamp_year",
                f"{year}0101000000",
                f"http://{host}/",
                "a" * 64,
                "cdx-v1",
            )
            archive = export_submission(
                [make("baseline.example", 1996), make("new.example", 1997), make("new.example", 1997)],
                index,
                root / "out",
                contributor="test user",
                submission_time=datetime(2026, 9, 9, tzinfo=timezone.utc),
            )
            with zipfile.ZipFile(archive) as bundle:
                names = set(bundle.namelist())
                manifest = json.loads(bundle.read("manifest.json"))
                self.assertIn("annual/1996.txt", names)
                self.assertEqual(bundle.read("annual/1996.txt"), b"")
                self.assertEqual(bundle.read("annual/1997.txt").decode(), "new.example\n")
                self.assertEqual(manifest["records"], 1)
            index.close()


if __name__ == "__main__":
    unittest.main()
