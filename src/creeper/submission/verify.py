"""Independent verifier for the V3 submission archive contract."""

from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from pathlib import Path

from creeper.authority.normalizer import normalize_official


@dataclass(frozen=True)
class VerificationReport:
    ready: bool
    errors: tuple[str, ...]
    annual_records: int = 0
    evidence_records: int = 0
    active_candidates: int = 0


def verify_submission_archive(
    archive: Path,
    *,
    baseline_manifest_path: Path | None = None,
) -> VerificationReport:
    errors: list[str] = []
    annual_records = evidence_records = active_candidates = 0
    try:
        bundle = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as exc:
        return VerificationReport(False, (f"cannot open archive: {exc}",))
    with bundle:
        if bundle.testzip() is not None:
            errors.append("zip CRC check failed")
        names = set(bundle.namelist())

        def require(name: str) -> bytes | None:
            if name not in names:
                errors.append(f"missing required entry: {name}")
                return None
            return bundle.read(name)

        manifest_bytes = require("MANIFEST.json")
        if manifest_bytes is None:
            return VerificationReport(False, tuple(errors))
        try:
            manifest = json.loads(manifest_bytes)
        except json.JSONDecodeError as exc:
            return VerificationReport(False, tuple(errors + [f"invalid MANIFEST.json: {exc}"]))

        if manifest.get("baseline_id") != "merged260909-3":
            errors.append("manifest baseline_id is not merged260909-3")
        expected_years = {str(year) for year in range(1996, 2002)}
        if set(manifest.get("baseline_hashes", {})) != expected_years:
            errors.append("manifest does not contain all six baseline hashes")
        if not manifest.get("source_files"):
            errors.append("manifest source_files is empty")
        for relative in manifest.get("source_files", []):
            if f"code/{relative}" not in names:
                errors.append(f"missing source asset: code/{relative}")
        documentation = manifest.get("documentation_file")
        if not documentation or documentation not in names or not documentation.endswith(".docx"):
            errors.append("Word documentation asset is missing")
        for policy_name in ("normalizer", "evidence", "eed"):
            if not manifest.get("policy_versions", {}).get(policy_name):
                errors.append(f"missing policy version: {policy_name}")
        for name, expected_hash in manifest.get("entry_sha256", {}).items():
            if name not in names:
                errors.append(f"manifest hash entry missing from archive: {name}")
            elif hashlib.sha256(bundle.read(name)).hexdigest() != expected_hash:
                errors.append(f"entry hash mismatch: {name}")

        annual_hosts: set[tuple[str, int]] = set()
        for year in range(1996, 2002):
            content = require(f"{year}.txt")
            if content is None:
                continue
            seen: set[str] = set()
            for raw in content.decode("utf-8", errors="replace").splitlines():
                hostname = normalize_official(raw)
                if hostname is None:
                    errors.append(f"invalid hostname in {year}.txt: {raw}")
                    continue
                if hostname in seen:
                    errors.append(f"duplicate hostname in {year}.txt: {hostname}")
                seen.add(hostname)
                annual_hosts.add((hostname, year))
            annual_records += len(seen)

        evidence_bytes = require("evidence.jsonl")
        evidence_keys: set[tuple[str, int]] = set()
        if evidence_bytes is not None:
            for line_number, raw in enumerate(evidence_bytes.decode().splitlines(), 1):
                try:
                    record = json.loads(raw)
                    hostname = normalize_official(record["hostname"])
                    year = int(record["year"])
                    if hostname is None or year not in range(1996, 2002):
                        raise ValueError("invalid hostname/year")
                    for field in (
                        "evidence_type",
                        "source_id",
                        "original_url",
                        "record_locator",
                        "extraction_method",
                    ):
                        if not str(record.get(field, "")).strip():
                            raise ValueError(f"missing evidence provenance: {field}")
                    evidence_keys.add((hostname, year))
                except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                    errors.append(f"invalid evidence line {line_number}: {exc}")
            evidence_records = len(evidence_keys)
        missing_evidence = annual_hosts - evidence_keys
        if missing_evidence:
            errors.append(f"annual records without evidence: {len(missing_evidence)}")

        active_bytes = require("active_candidates.txt")
        if active_bytes is not None:
            annual_hostnames = {hostname for hostname, _ in annual_hosts}
            for raw in active_bytes.decode().splitlines():
                hostname = normalize_official(raw)
                if hostname is None:
                    errors.append(f"invalid active candidate: {raw}")
                    continue
                active_candidates += 1
                if hostname in annual_hostnames:
                    errors.append(f"active candidate overlaps annual host: {hostname}")
        scopes = manifest.get("active_candidate_scopes", [])
        if any("common_crawl" in str(scope).lower().replace("-", "_") for scope in scopes):
            errors.append("Common Crawl scope is present in active candidates")
        for required in (
            "reports/eed.json",
            "cdx_audit.json",
            "source_reports.json",
            "method_failure_summary.json",
            "isc_reference/manifest.json",
        ):
            require(required)
        if baseline_manifest_path is not None:
            try:
                external = json.loads(baseline_manifest_path.read_text(encoding="utf-8"))
                expected = {
                    name.removesuffix(".txt"): digest
                    for name, digest in external["annual_file_hashes"].items()
                }
                if manifest.get("baseline_hashes") != expected:
                    errors.append("manifest baseline hashes do not match supplied authority manifest")
            except (OSError, KeyError, json.JSONDecodeError) as exc:
                errors.append(f"cannot compare authority manifest: {exc}")
    return VerificationReport(
        not errors,
        tuple(errors),
        annual_records=annual_records,
        evidence_records=evidence_records,
        active_candidates=active_candidates,
    )
