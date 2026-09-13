import tempfile
import unittest
import hashlib
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import AuthoritySnapshot, authority_digest
from creeper.authority.manifest import build_manifest


class BaselinePathTests(unittest.TestCase):
    def _make_v4_snapshot(self, root: Path) -> None:
        annual = root / "merged260912-3"
        isc = annual / "isc_survey_hostnames"
        model_dir = root / "equivalent_english_domain_calculator"
        annual.mkdir(parents=True)
        isc.mkdir()
        model_dir.mkdir()
        for year in range(1996, 2002):
            (annual / f"{year}.txt").write_text(f"year{year}.example\n", encoding="utf-8")
        (annual / "candidate_pool.txt").write_text("candidate.example\n", encoding="utf-8")
        (annual / "candidate_pool_unparsed_format.txt").write_text("raw\n", encoding="utf-8")
        (annual / "deduplicated_urls_1996-1997.txt").write_text("aux.example\n", encoding="utf-8")
        (isc / "1996-ISC.txt").write_text("isc.example\n", encoding="utf-8")
        (model_dir / "q2_tld_top_langs.json").write_text("{}\n", encoding="utf-8")

    def test_v4_directory_is_discovered_for_manifest_and_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_v4_snapshot(root)

            manifest = build_manifest(root, root / "authority" / "manifest.json")
            self.assertEqual(manifest["baseline_id"], "merged260912-3")
            self.assertEqual(set(manifest["isc_line_counts"]), {"1996-ISC.txt"})

            index = BaselineIndex.build(root, root / "index.sqlite3")
            self.assertEqual(index.year_mask("year2001.example"), 1 << 5)
            index.close()

    def test_index_embeds_authority_and_refuses_cross_authority_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = root / "merged260912-3"
            baseline.mkdir()
            for year in range(1996, 2002):
                (baseline / f"{year}.txt").write_text("same.example\n", encoding="utf-8")
            (baseline / "candidate_pool.txt").write_text("candidate.example\n", encoding="utf-8")
            annual_hashes = {
                f"{year}.txt": hashlib.sha256(
                    (baseline / f"{year}.txt").read_bytes()
                ).hexdigest()
                for year in range(1996, 2002)
            }
            candidate_hash = hashlib.sha256(
                (baseline / "candidate_pool.txt").read_bytes()
            ).hexdigest()
            model_hash = "a" * 64
            digest = authority_digest(
                baseline_id=baseline.name,
                annual_file_hashes=annual_hashes,
                candidate_file_hash=candidate_hash,
                model_hash=model_hash,
                baseline_eed="10",
            )
            authority = AuthoritySnapshot(
                baseline.name, annual_hashes, candidate_hash, model_hash, "10", digest
            )
            output = root / "index.sqlite3"
            BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            ).close()
            checked = BaselineIndex(output, authority=authority)
            self.assertEqual(checked.counts()["candidate_hostnames"], 1)
            checked.close()
            replacement = AuthoritySnapshot(
                baseline.name, annual_hashes, candidate_hash, model_hash, "11",
                authority_digest(
                    baseline_id=baseline.name,
                    annual_file_hashes=annual_hashes,
                    candidate_file_hash=candidate_hash,
                    model_hash=model_hash,
                    baseline_eed="11",
                ),
            )
            with self.assertRaisesRegex(ValueError, "refusing to resume"):
                BaselineIndex.build(
                    baseline_dir=baseline,
                    output_path=output,
                    authority_manifest=replacement,
                )


if __name__ == "__main__":
    unittest.main()
