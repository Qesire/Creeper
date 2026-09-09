import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex, novel_year_mask


class BaselineIndexTests(unittest.TestCase):
    def test_novelty_is_year_aware(self):
        evidence = 0b000011
        baseline = 0b000001
        target = 0b000011
        self.assertEqual(novel_year_mask(evidence, baseline, target), 0b000010)

    def test_build_and_resolve_annual_and_candidate_masks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text("shared.example\n", encoding="utf-8")
            (annual / "1996.txt").write_text("shared.example\nonly96.example\n", encoding="utf-8")
            (annual / "candidate_pool.txt").write_text("candidate.example\nshared.example\n", encoding="utf-8")
            (annual / "candidate_pool_unparsed_format.txt").write_text("raw\n", encoding="utf-8")
            (annual / "deduplicated_urls_1996-1997.txt").write_text("auxiliary.example\n", encoding="utf-8")

            index = BaselineIndex.build(root, root / "index.sqlite3")

            self.assertEqual(index.year_mask("shared.example"), 0b111111)
            self.assertEqual(index.year_mask("only96.example"), 0b000001)
            self.assertTrue(index.is_official_candidate("candidate.example"))
            self.assertTrue(index.is_official_candidate("shared.example"))
            self.assertFalse(index.is_official_candidate("only96.example"))
            self.assertEqual(index.year_mask("auxiliary.example"), 0)
            self.assertEqual(
                index.resolve_batch(
                    ["shared.example", "candidate.example", "absent.example", "SHARED.EXAMPLE"]
                ),
                {
                    "shared.example": (0b111111, True),
                    "candidate.example": (0, True),
                    "absent.example": (0, False),
                },
            )

            tables = {
                row[0]
                for row in index.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertIn("annual_hostnames", tables)
            self.assertIn("candidate_hostnames", tables)
            state = dict(
                index.connection.execute(
                    "SELECT stage, completed FROM import_state"
                ).fetchall()
            )
            self.assertEqual(state["year:1996"], 1)
            self.assertEqual(state["candidate_pool"], 1)


if __name__ == "__main__":
    unittest.main()
