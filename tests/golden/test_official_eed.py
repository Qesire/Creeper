import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.eed import calculate_eed


class OfficialEEDTests(unittest.TestCase):
    def test_deduplicates_and_applies_model_weights(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            domains = root / "1998.txt"
            model = root / "model.json"
            domains.write_text("A.UK\na.uk\ninvalid\nmissing.zzz\n", encoding="utf-8")
            model.write_text(
                json.dumps({
                    "tld": ["uk", "zzz"],
                    "lang": ["eng", "eng"],
                    "perc_of_tld": ["98.13", "0.0"],
                }),
                encoding="utf-8",
            )

            summary, rows = calculate_eed(domains, model)

            self.assertEqual(summary["unique_nonempty_records"], 3)
            self.assertEqual(summary["unique_valid_domains"], 2)
            self.assertEqual(summary["invalid_records"], 1)
            self.assertEqual(summary["model_matched_records"], 2)
            self.assertEqual(summary["equivalent_english_domains"], "0.9813")
            self.assertIn("method", summary)
            self.assertEqual(summary["input_file"], str(domains.resolve()))
            self.assertEqual(rows[0]["tld"], ".uk")


if __name__ == "__main__":
    unittest.main()
