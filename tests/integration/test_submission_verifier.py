import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.submission.verify import verify_submission_archive


class SubmissionVerifierTests(unittest.TestCase):
    def test_verifier_checks_manifest_entries_and_annual_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "submission.zip"
            entries = {
                "1996.txt": b"",
                "1997.txt": b"new.example\n",
                "1998.txt": b"",
                "1999.txt": b"",
                "2000.txt": b"",
                "2001.txt": b"",
                "active_candidates.txt": b"candidate.example\n",
                "candidate_pool_unparsed_format.txt": b"",
                "evidence.jsonl": json.dumps({
                    "hostname": "new.example", "year": 1997, "provider": "cdx",
                    "temporal_semantics": "capture_timestamp_year",
                    "evidence_timestamp": "19970101000000",
                    "source_locator": "http://new.example/",
                    "payload_hash": "a" * 64, "policy_version": "cdx-v1",
                }).encode() + b"\n",
                "reports/eed.json": b'{"equivalent_english_domains":"0"}',
                "cdx_audit.json": b'[]',
                "source_reports.json": b'[]',
                "method_failure_summary.json": b'{"incomplete_query_count":0}',
                "isc_reference/manifest.json": b'{"records":[]}',
                "documentation/methods.docx": b"docx",
                "code/README.md": b"code",
            }
            manifest = {
                "format_version": "submission-v1",
                "baseline_id": "merged260909-3",
                "baseline_hashes": {str(y): "b" * 64 for y in range(1996, 2002)},
                "policy_versions": {"normalizer": "n", "evidence": "e", "eed": "eed"},
                "entry_sha256": {k: hashlib.sha256(v).hexdigest() for k, v in entries.items()},
                "source_files": ["README.md"],
                "documentation_file": "documentation/methods.docx",
                "active_candidate_scopes": ["official_pool"],
            }
            with zipfile.ZipFile(archive, "w") as bundle:
                for name, content in entries.items():
                    bundle.writestr(name, content)
                bundle.writestr("MANIFEST.json", json.dumps(manifest).encode())
            report = verify_submission_archive(archive)
            self.assertTrue(report.ready, report.errors)
            self.assertEqual(report.annual_records, 1)


if __name__ == "__main__":
    unittest.main()
