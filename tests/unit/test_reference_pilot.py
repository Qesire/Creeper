import tempfile
import unittest
from pathlib import Path

from creeper.sources.reference_pilot import hash_sample_host_file


class ReferencePilotTests(unittest.TestCase):
    def test_hash_sample_is_deterministic_and_unique(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "1996-ISC.txt"
            path.write_text(
                "z.example\na.example\na.example\nb.example\nc.example\n",
                encoding="utf-8",
            )
            first = hash_sample_host_file(
                path,
                source_id="isc_reference:1996",
                source_year=1996,
                sample_size=3,
                seed=20260909,
            )
            second = hash_sample_host_file(
                path,
                source_id="isc_reference:1996",
                source_year=1996,
                sample_size=3,
                seed=20260909,
            )
            self.assertEqual(first, second)
            self.assertEqual(len(first), 3)
            self.assertEqual(len({item.hostname for item in first}), 3)
            self.assertTrue(all(item.source_year == 1996 for item in first))


if __name__ == "__main__":
    unittest.main()
