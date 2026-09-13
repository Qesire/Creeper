import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.identity import authority_digest
from creeper.submission.verify import verify_submission_archive


def _authority(baseline_id: str = "merged260912-3") -> dict[str, object]:
    annual = {f"{year}.txt": "b" * 64 for year in range(1996, 2002)}
    candidate = "c" * 64
    model = "d" * 64
    baseline_eed = "46483739.2890"
    return {
        "baseline_id": baseline_id,
        "annual_file_hashes": annual,
        "candidate_file_hash": candidate,
        "model_hash": model,
        "baseline_eed": baseline_eed,
        "authority_digest": authority_digest(
            baseline_id=baseline_id,
            annual_file_hashes=annual,
            candidate_file_hash=candidate,
            model_hash=model,
            baseline_eed=baseline_eed,
        ),
    }


def _entries() -> dict[str, bytes]:
    return {
        "1996.txt": b"",
        "1997.txt": b"new.example\n",
        "1998.txt": b"",
        "1999.txt": b"",
        "2000.txt": b"",
        "2001.txt": b"",
        "active_candidates.txt": b"candidate.example\n",
        "candidate_pool_unparsed_format.txt": b"",
        "evidence.jsonl": json.dumps({
            "hostname": "new.example",
            "year": 1997,
            "provider": "cdx",
            "temporal_semantics": "capture_timestamp_year",
            "evidence_timestamp": "19970101000000",
            "source_locator": "http://new.example/",
            "payload_hash": "a" * 64,
            "policy_version": "cdx-v1",
            "evidence_type": "exact_host_cdx_capture",
            "source_id": "cdx",
            "original_url": "http://new.example/",
            "record_locator": "cdx:new.example:1997:page=1:record=1",
            "extraction_method": "cdx_query_year",
        }).encode() + b"\n",
        "reports/eed.json": b'{"equivalent_english_domains":"0"}',
        "cdx_audit.json": b"[]",
        "source_reports.json": b"[]",
        "method_failure_summary.json": b'{"incomplete_query_count":0}',
        "isc_reference/manifest.json": b'{"records":[]}',
        "documentation/methods.docx": b"docx",
        "code/README.md": b"code",
    }


def _package_manifest(
    authority: dict[str, object],
    entries: dict[str, bytes],
) -> dict[str, object]:
    return {
        "format_version": "submission-v2",
        "baseline_id": authority["baseline_id"],
        "baseline_hashes": {
            name.removesuffix(".txt"): digest
            for name, digest in authority["annual_file_hashes"].items()
        },
        "candidate_file_hash": authority["candidate_file_hash"],
        "model_hash": authority["model_hash"],
        "baseline_eed": authority["baseline_eed"],
        "authority_digest": authority["authority_digest"],
        "policy_versions": {"normalizer": "n", "evidence": "e", "eed": "eed"},
        "entry_sha256": {
            key: hashlib.sha256(value).hexdigest()
            for key, value in entries.items()
        },
        "source_files": ["README.md"],
        "documentation_file": "documentation/methods.docx",
        "active_candidate_scopes": ["official_pool"],
    }


def _write_archive(
    root: Path,
    package_manifest: dict[str, object],
    entries: dict[str, bytes],
) -> Path:
    archive = root / "submission.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for name, content in entries.items():
            bundle.writestr(name, content)
        bundle.writestr("MANIFEST.json", json.dumps(package_manifest).encode())
    return archive


class SubmissionVerifierTests(unittest.TestCase):
    def test_verifier_accepts_package_matching_supplied_v4_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority = _authority()
            authority_path = root / "authority.json"
            authority_path.write_text(json.dumps(authority), encoding="utf-8")
            entries = _entries()
            archive = _write_archive(
                root,
                _package_manifest(authority, entries),
                entries,
            )

            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
            )

            self.assertTrue(report.ready, report.errors)
            self.assertEqual(report.annual_records, 1)

    def test_verifier_rejects_baseline_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority = _authority()
            authority_path = root / "authority.json"
            authority_path.write_text(json.dumps(authority), encoding="utf-8")
            entries = _entries()
            package = _package_manifest(authority, entries)
            package["baseline_hashes"] = dict(package["baseline_hashes"])
            package["baseline_hashes"]["1999"] = "e" * 64
            archive = _write_archive(root, package, entries)

            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
            )

            self.assertFalse(report.ready)
            self.assertIn(
                "manifest baseline hashes do not match supplied authority manifest",
                report.errors,
            )

    def test_legacy_v3_package_does_not_validate_against_v4(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            v4 = _authority()
            authority_path = root / "authority.json"
            authority_path.write_text(json.dumps(v4), encoding="utf-8")
            legacy = _authority("merged260909-3")
            entries = _entries()
            archive = _write_archive(
                root,
                _package_manifest(legacy, entries),
                entries,
            )

            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
            )

            self.assertFalse(report.ready)
            self.assertIn(
                "manifest baseline_id does not match supplied authority manifest",
                report.errors,
            )

    def test_verifier_fails_closed_without_supplied_authority_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority = _authority()
            entries = _entries()
            archive = _write_archive(
                root,
                _package_manifest(authority, entries),
                entries,
            )

            report = verify_submission_archive(archive)

            self.assertFalse(report.ready)
            self.assertIn("supplied authority manifest is required", report.errors)


if __name__ == "__main__":
    unittest.main()
