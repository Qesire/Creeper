import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.evidence.classification import (
    AcquisitionLane,
    classify_acquisition_lane,
)
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.verify import (
    recompute_submission_archive,
    verify_submission_archive,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _authority_fixture(
    root: Path,
    *,
    baseline_hosts: dict[int, tuple[str, ...]] | None = None,
) -> tuple[dict[str, object], Path, Path, Path]:
    baseline_hosts = baseline_hosts or {}
    baseline_dir = root / "merged260912-3"
    baseline_dir.mkdir()
    for year in range(1996, 2002):
        (baseline_dir / f"{year}.txt").write_text(
            "".join(host + "\n" for host in baseline_hosts.get(year, ())),
            encoding="utf-8",
        )
    candidate = baseline_dir / "candidate_pool.txt"
    candidate.write_text("", encoding="utf-8")
    model_path = root / "eed-model.json"
    model_path.write_text(
        json.dumps(
            {
                "tld": ["com"],
                "lang": ["eng"],
                "perc_of_tld": [100],
            }
        ),
        encoding="utf-8",
    )
    annual = {
        f"{year}.txt": _sha256(baseline_dir / f"{year}.txt")
        for year in range(1996, 2002)
    }
    baseline_eed = "10"
    authority = {
        "baseline_id": baseline_dir.name,
        "annual_file_hashes": annual,
        "candidate_file_hash": _sha256(candidate),
        "model_hash": _sha256(model_path),
        "baseline_eed": baseline_eed,
        "authority_digest": authority_digest(
            baseline_id=baseline_dir.name,
            annual_file_hashes=annual,
            candidate_file_hash=_sha256(candidate),
            model_hash=_sha256(model_path),
            baseline_eed=baseline_eed,
        ),
    }
    authority_path = root / "authority.json"
    authority_path.write_text(json.dumps(authority), encoding="utf-8")
    index_path = root / "baseline.sqlite3"
    index = BaselineIndex.build(
        baseline_dir=baseline_dir,
        output_path=index_path,
        authority_manifest=authority,
    )
    index.close()
    return authority, authority_path, index_path, model_path


def _candidate_record(
    hostname: str = "new.com",
    *,
    year: int = 1997,
    timestamp: str | None = None,
    original_url: str | None = None,
) -> dict[str, object]:
    return {
        "hostname": hostname,
        "year": year,
        "provider": "wayback",
        "temporal_semantics": "capture_timestamp_year",
        "evidence_timestamp": timestamp or f"{year}0101000000",
        "source_locator": original_url or f"http://{hostname}/",
        "payload_hash": "a" * 64,
        "policy_version": "cdx-v1",
        "evidence_type": "exact_host_cdx_capture",
        "source_id": "wayback",
        "original_url": original_url or f"http://{hostname}/",
        "record_locator": f"wayback:{hostname}:{year}:page=1:record=1",
        "extraction_method": "cdx_query_year",
    }


def _direct_record(
    hostname: str = "direct.com",
    *,
    year: int = 1998,
) -> dict[str, object]:
    source_id = "arquivo-cdxj"
    return {
        "hostname": hostname,
        "year": year,
        "provider": f"direct:{source_id}",
        "temporal_semantics": "archive_capture_timestamp",
        "evidence_timestamp": f"{year}0203040506",
        "source_locator": "https://arquivo.pt/datasets/cdxj/file.cdxj:byte:1",
        "payload_hash": "b" * 64,
        "policy_version": "evidence-v1",
        "evidence_type": "dated_archive_index",
        "source_id": source_id,
        "original_url": f"http://{hostname}/page",
        "record_locator": "https://arquivo.pt/datasets/cdxj/file.cdxj:byte:1",
        "extraction_method": (
            "CDX_CAPTURE;"
            "contract=archive-cdxj-capture-v1@archive-capture-contract-v1"
        ),
    }


def _rdap_record(
    *,
    hostname: str = "registered.com",
    year: int = 1998,
    timestamp: str = "19970101T00:00:00Z",
) -> dict[str, object]:
    return {
        "hostname": hostname,
        "year": year,
        "provider": "rdap",
        "temporal_semantics": "registration_event_year",
        "evidence_timestamp": timestamp,
        "source_locator": f"https://rdap.example/domain/{hostname}",
        "payload_hash": "c" * 64,
        "policy_version": "rdap-registration-v1",
        "evidence_type": "rdap_registration_event",
        "source_id": "rdap",
        "original_url": f"https://rdap.example/domain/{hostname}",
        "record_locator": f"rdap:{hostname}:registration",
        "extraction_method": "rdap_registration_event",
    }


def _dns_record(hostname: str = "dns.com", year: int = 1999) -> dict[str, object]:
    return {
        "hostname": hostname,
        "year": year,
        "provider": "dns",
        "temporal_semantics": "dns_observation_year",
        "evidence_timestamp": f"{year}0101000000",
        "source_locator": "dns://snapshot",
        "payload_hash": "d" * 64,
        "policy_version": "dns-v1",
        "evidence_type": "dns_observation",
        "source_id": "dns",
        "original_url": "dns://snapshot",
        "record_locator": f"dns:{hostname}:{year}",
        "extraction_method": "dns_snapshot",
    }


def _entries(
    records: list[dict[str, object]] | None = None,
    *,
    eed: str | None = None,
    growth: str | None = None,
    lane_contribution: dict[str, dict[str, object]] | None = None,
) -> dict[str, bytes]:
    records = records or [_candidate_record()]
    annual: dict[int, list[str]] = {year: [] for year in range(1996, 2002)}
    for record in records:
        annual[int(record["year"])].append(str(record["hostname"]))
    total_eed = str(len(records)) if eed is None else eed
    total_growth = str(
        __import__("decimal").Decimal(total_eed)
        / __import__("decimal").Decimal("10")
    ) if growth is None else growth
    if lane_contribution is None:
        direct = sum(str(record["provider"]).startswith("direct:") for record in records)
        verified = sum(
            not str(record["provider"]).startswith("direct:")
            and record["provider"] != "rdap"
            and "dns" not in str(record["provider"]).lower()
            for record in records
        )
        other = len(records) - direct - verified
        lane_contribution = {
            "direct_annual": {
                "novel_host_years": direct,
                "novel_eed": str(direct),
            },
            "verified_candidate": {
                "novel_host_years": verified,
                "novel_eed": str(verified),
            },
            "other_restricted": {
                "novel_host_years": other,
                "novel_eed": str(other),
            },
        }
    return {
        **{
            f"{year}.txt": "".join(sorted(set(annual[year]))).replace(
                ".com", ".com\n"
            ).encode()
            for year in range(1996, 2002)
        },
        "active_candidates.txt": b"candidate.net\n",
        "candidate_pool_unparsed_format.txt": b"",
        "evidence.jsonl": "".join(
            json.dumps(record, sort_keys=True) + "\n"
            for record in records
        ).encode(),
        "reports/eed.json": json.dumps(
            {"equivalent_english_domains": total_eed}
        ).encode(),
        "reports/baseline_reconciliation.json": json.dumps(
            {"novel_eed": total_eed, "growth_rate": total_growth}
        ).encode(),
        "reports/source_contribution.json": json.dumps(
            lane_contribution, sort_keys=True
        ).encode(),
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
    *,
    novel_eed: str | None = None,
    growth_rate: str | None = None,
) -> dict[str, object]:
    reconciliation = json.loads(entries["reports/baseline_reconciliation.json"])
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
        "novel_eed": (
            reconciliation["novel_eed"]
            if novel_eed is None
            else novel_eed
        ),
        "growth_rate": (
            reconciliation["growth_rate"]
            if growth_rate is None
            else growth_rate
        ),
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
    def _verify(
        self,
        archive: Path,
        authority_path: Path,
        index_path: Path,
        model_path: Path,
    ):
        return verify_submission_archive(
            archive,
            baseline_manifest_path=authority_path,
            baseline_index_path=index_path,
            eed_model_path=model_path,
        )

    def test_verifier_independently_accepts_valid_candidate_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries()
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertTrue(report.ready, report.errors)
            self.assertEqual(report.annual_records, 1)
            self.assertEqual(report.recomputed_novel_eed, "1")
            self.assertEqual(report.recomputed_growth_rate, "0.1")

    def test_baseline_overlap_fails_independent_verifier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(
                root, baseline_hosts={1997: ("new.com",)}
            )
            entries = _entries()
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertFalse(report.ready)
            self.assertIn("annual output overlaps target-year baseline: 1", report.errors)

    def test_tampered_eed_report_fails_even_with_matching_entry_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries(eed="9", growth="0.9")
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertFalse(report.ready)
            self.assertIn("EED report does not match independent recomputation", report.errors)

    def test_tampered_model_path_hash_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries()
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )
            tampered = root / "tampered-model.json"
            tampered.write_text(
                json.dumps(
                    {"tld": ["com"], "lang": ["eng"], "perc_of_tld": [50]}
                ),
                encoding="utf-8",
            )

            report = self._verify(archive, authority_path, index_path, tampered)

            self.assertFalse(report.ready)
            self.assertIn(
                "supplied EED model hash does not match authority manifest",
                report.errors,
            )

    def test_exact_capture_hostname_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries(
                [_candidate_record(original_url="http://other.com/")]
            )
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertFalse(report.ready)
            self.assertTrue(
                any("original URL hostname does not match" in error for error in report.errors)
            )

    def test_current_direct_cdxj_is_direct_annual_and_verifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            record = _direct_record()
            capsule = EvidenceCapsule(**record)
            self.assertEqual(
                classify_acquisition_lane(capsule),
                AcquisitionLane.DIRECT_ANNUAL,
            )
            entries = _entries([record])
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertTrue(report.ready, report.errors)

    def test_dns_observation_cannot_enter_annual_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries([_dns_record()])
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertFalse(report.ready)
            self.assertTrue(
                any("DNS reference cannot prove annual web presence" in error for error in report.errors)
            )

    def test_rdap_creation_year_cannot_prove_later_year(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            entries = _entries([_rdap_record(year=1998, timestamp="19970101T00:00:00Z")])
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = self._verify(archive, authority_path, index_path, model_path)

            self.assertFalse(report.ready)
            self.assertTrue(
                any("timestamp year does not match" in error for error in report.errors)
            )

    def test_direct_and_candidate_lane_eed_sums_exactly_to_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, index_path, model_path = _authority_fixture(root)
            records = [_candidate_record(), _direct_record()]
            entries = _entries(records)
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            recomputed = recompute_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
                baseline_index_path=index_path,
                eed_model_path=model_path,
            )

            self.assertTrue(recomputed.ready, recomputed.errors)
            self.assertEqual(recomputed.novel_eed, "2")
            self.assertEqual(
                recomputed.lane_contribution["direct_annual"]["novel_eed"],
                "1",
            )
            self.assertEqual(
                recomputed.lane_contribution["verified_candidate"]["novel_eed"],
                "1",
            )
            total = sum(
                __import__("decimal").Decimal(str(item["novel_eed"]))
                for item in recomputed.lane_contribution.values()
            )
            self.assertEqual(total, __import__("decimal").Decimal("2"))

    def test_verifier_fails_closed_without_all_independent_authority_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            authority, authority_path, _index_path, _model_path = _authority_fixture(root)
            entries = _entries()
            archive = _write_archive(
                root, _package_manifest(authority, entries), entries
            )

            report = verify_submission_archive(
                archive,
                baseline_manifest_path=authority_path,
            )

            self.assertFalse(report.ready)
            self.assertIn("supplied baseline index is required", report.errors)
            self.assertIn("supplied EED model is required", report.errors)


if __name__ == "__main__":
    unittest.main()
