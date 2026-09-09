import json
import tempfile
import unittest
from pathlib import Path

from creeper.sources.auxiliary_pilot import (
    hash_sample_auxiliary_file,
    sample_auxiliary_candidates,
    source_years,
)


class AuxiliaryEvidencePilotTests(unittest.TestCase):
    def test_source_years_intersect_competition_years_only(self):
        self.assertEqual(source_years("v3_auxiliary:deduplicated_urls_1996-1997"), (1996, 1997))
        self.assertEqual(source_years("v3_auxiliary:deduplicated_urls_2001-2002"), (2001,))
        self.assertEqual(source_years("v3_auxiliary:deduplicated_urls_2002-2003"), ())
        self.assertEqual(source_years("isc_reference:1996"), (1996,))
        self.assertEqual(source_years("arquivo_pt_cdxj:DEM-IST.cdxj:1998"), (1998,))

    def test_sampling_is_deterministic_and_bounded_per_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "active.jsonl"
            rows = [
                {
                    "hostname": f"host-{i}.example",
                    "source_id": "v3_auxiliary:deduplicated_urls_1996-1997",
                    "locator": f"first:{i}",
                }
                for i in range(5)
            ] + [
                {
                    "hostname": f"later-{i}.example",
                    "source_id": "v3_auxiliary:deduplicated_urls_2001-2002",
                    "locator": f"second:{i}",
                }
                for i in range(3)
            ]
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )

            first = sample_auxiliary_candidates(path, per_source=2, seed=20260909)
            second = sample_auxiliary_candidates(path, per_source=2, seed=20260909)

            self.assertEqual(first, second)
            self.assertEqual(len(first), 4)
            self.assertEqual(
                sum(item.source_id.endswith("1996-1997") for item in first), 2
            )
            self.assertEqual(
                sum(item.source_id.endswith("2001-2002") for item in first), 2
            )

    def test_hash_sampling_scans_full_file_without_duplicate_hosts(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deduplicated_urls_1996-1997.txt"
            path.write_text(
                "z.example\na.example\na.example\nb.example\nc.example\n",
                encoding="utf-8",
            )

            first = hash_sample_auxiliary_file(path, sample_size=3, seed=20260909)
            second = hash_sample_auxiliary_file(path, sample_size=3, seed=20260909)

            self.assertEqual(first, second)
            self.assertEqual(len(first), 3)
            self.assertEqual(len({item.hostname for item in first}), 3)
            self.assertTrue(all(item.source_id.endswith("1996-1997") for item in first))


if __name__ == "__main__":
    unittest.main()
