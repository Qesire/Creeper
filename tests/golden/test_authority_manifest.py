import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from creeper.authority.manifest import build_manifest


class AuthorityManifestTests(unittest.TestCase):
    def _make_snapshot(self, root: Path) -> None:
        annual = root / "merged260909-3"
        isc = annual / "isc_survey_hostnames"
        model_dir = root / "equivalent_english_domain_calculator"
        annual.mkdir(parents=True)
        isc.mkdir(parents=True)
        model_dir.mkdir(parents=True)
        for year, lines in {
            1996: ["a.com\n", "b.org\n"],
            1997: ["a.com\n"],
            1998: ["c.net\n"],
            1999: ["d.uk\n"],
            2000: ["e.de\n"],
            2001: ["f.nl\n", "g.nl\n"],
        }.items():
            (annual / f"{year}.txt").write_text("".join(lines), encoding="utf-8")
        (annual / "candidate_pool.txt").write_text("candidate.example\n", encoding="utf-8")
        (annual / "candidate_pool_unparsed_format.txt").write_text("raw value\n", encoding="utf-8")
        (annual / "deduplicated_urls_1996-1997.txt").write_text("aux.example\n", encoding="utf-8")
        (isc / "1996-ISC.txt").write_text("isc.example\n", encoding="utf-8")
        (isc / "1997-ISC.txt").write_text("isc97.example\n", encoding="utf-8")
        (model_dir / "q2_tld_top_langs.json").write_text("{}\n", encoding="utf-8")

    def test_manifest_records_v3_identity_and_line_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_snapshot(root)
            output = root / "authority" / "baseline_manifest.json"

            manifest = build_manifest(root, output)

            self.assertEqual(manifest["baseline_id"], "merged260909-3")
            self.assertEqual(manifest["annual_line_counts"]["1996"], 2)
            self.assertEqual(manifest["annual_line_counts"]["2001"], 2)
            self.assertEqual(manifest["candidate_line_count"], 1)
            self.assertEqual(manifest["unparsed_line_count"], 1)
            self.assertEqual(manifest["isc_line_counts"]["1996-ISC.txt"], 1)
            self.assertEqual(manifest["auxiliary_line_counts"]["deduplicated_urls_1996-1997.txt"], 1)
            self.assertTrue(manifest["annual_file_hashes"]["1996.txt"])
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), manifest)

    def test_sha256_is_the_digest_of_the_authority_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_snapshot(root)
            output = root / "authority" / "baseline_manifest.json"

            manifest = build_manifest(root, output)
            path = root / "merged260909-3" / "1996.txt"
            expected = hashlib.sha256(path.read_bytes()).hexdigest()

            self.assertEqual(manifest["annual_file_hashes"]["1996.txt"], expected)


if __name__ == "__main__":
    unittest.main()
