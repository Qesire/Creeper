import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import precheck_submission
from creeper.submission.snapshot import SubmissionSnapshot
from creeper.submission.exporter import build_submission_zip


class SubmissionPackageTests(unittest.TestCase):
    def test_ready_snapshot_exports_all_required_v3_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            annual = root / "merged260909-3"
            annual.mkdir()
            for year in range(1996, 2002):
                (annual / f"{year}.txt").write_text("", encoding="utf-8")
            (annual / "candidate_pool.txt").write_text("", encoding="utf-8")
            index = BaselineIndex.build(root, root / "index.sqlite3")
            documentation = root / "methods.docx"
            documentation.write_bytes(b"docx fixture")
            capsule = EvidenceCapsule(
                "new.example", 1997, "wayback-cdx", "capture_timestamp_year",
                "19970101000000", "http://new.example/", "a" * 64, "cdx-v1"
            )
            snapshot = SubmissionSnapshot(
                submission_snapshot_id="s-1",
                created_at="2026-09-09T00:00:00+00:00",
                baseline_id="merged260909-3",
                baseline_hashes={str(year): "b" * 64 for year in range(1996, 2002)},
                normalizer_version="official-calculator-regex-v1",
                evidence_policy_version="evidence-v1",
                eed_policy_version="eed-v1",
                novel_records=(capsule,),
                novel_eed="0.5000",
                growth_rate="0.050000",
                evidence_coverage="1.000000",
                invalid_count=0,
                overlap_count=0,
                source_report_set=("source-report.json",),
                cdx_audit_set=("cdx-audit.json",),
                code_revision="c" * 64,
                eed_report={"equivalent_english_domains": "0.5000"},
            )
            report = precheck_submission(snapshot)
            self.assertTrue(report.ready, report.reasons)
            archive = build_submission_zip(
                snapshot,
                "tester",
                root / "out",
                source_root=Path(__file__).parents[2],
                documentation_path=documentation,
            )
            with zipfile.ZipFile(archive) as bundle:
                names = set(bundle.namelist())
                for year in range(1996, 2002):
                    self.assertIn(f"{year}.txt", names)
                self.assertIn("active_candidates.txt", names)
                self.assertIn("isc_reference/manifest.json", names)
                self.assertIn("evidence.jsonl", names)
                self.assertIn("MANIFEST.json", names)
                manifest = json.loads(bundle.read("MANIFEST.json"))
                self.assertEqual(manifest["baseline_id"], "merged260909-3")
                self.assertEqual(manifest["policy_versions"]["eed"], "eed-v1")
                self.assertTrue(all(manifest["entry_sha256"].values()))
                self.assertIn("code/pyproject.toml", names)
                self.assertIn("code/src/creeper/cli.py", names)
                self.assertIn("documentation/methods.docx", names)
                self.assertFalse(any(name.startswith("code/.venv/") for name in names))
                self.assertFalse(any(name.endswith(".egg-info") for name in names))
            index.close()

    def test_precheck_rejects_incomplete_queries_and_common_crawl_scope(self):
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="s-2",
            created_at="2026-09-09T00:00:00+00:00",
            baseline_id="merged260909-3",
            baseline_hashes={str(year): "b" * 64 for year in range(1996, 2002)},
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(),
            novel_eed="0",
            growth_rate="0",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="c" * 64,
            eed_report={"equivalent_english_domains": "0"},
            active_candidates=("common-crawl.example",),
            active_candidate_scopes=("common_crawl_corpus_excluded",),
            incomplete_query_count=1,
        )
        report = precheck_submission(snapshot)
        self.assertFalse(report.ready)
        self.assertIn("incomplete queries cannot be submitted as negative evidence", report.reasons)
        self.assertIn("Common Crawl candidate is present in active candidates", report.reasons)
        self.assertIn("formal submission requires at least 5% EED growth", report.reasons)

    def test_precheck_accepts_dynamic_baseline_identity_and_rejects_eed_mismatch(self):
        snapshot = SubmissionSnapshot(
            submission_snapshot_id="s-3",
            created_at="2026-09-10T00:00:00+00:00",
            baseline_id="merged-next-round",
            baseline_hashes={str(year): "c" * 64 for year in range(1996, 2002)},
            normalizer_version="normalizer-v1",
            evidence_policy_version="evidence-v1",
            eed_policy_version="eed-v1",
            novel_records=(),
            novel_eed="10",
            growth_rate="0.05",
            evidence_coverage="1",
            invalid_count=0,
            overlap_count=0,
            source_report_set=("source.json",),
            cdx_audit_set=("audit.json",),
            code_revision="d" * 64,
            eed_report={"equivalent_english_domains": "9"},
        )
        report = precheck_submission(snapshot)
        self.assertFalse(report.ready)
        self.assertNotIn("baseline_id must be merged260909-3", report.reasons)
        self.assertIn("novel_eed must match the exact EED report", report.reasons)


if __name__ == "__main__":
    unittest.main()
