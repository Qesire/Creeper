"""Independent semantic verifier for the submission archive contract."""

from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.eed import calculate_eed_values
from creeper.authority.identity import (
    AuthoritySnapshot,
    eed_model_authority_signature,
)
from creeper.authority.normalizer import normalize_official
from creeper.evidence.classification import (
    AcquisitionLane,
    classify_acquisition_lane,
    contribution_bucket,
    validate_evidence_semantics,
)
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import format_growth_rate


@dataclass(frozen=True)
class RecomputedSubmission:
    ready: bool
    errors: tuple[str, ...]
    annual_records: int
    evidence_records: int
    novel_eed: str
    growth_rate: str
    lane_contribution: dict[str, dict[str, object]]


@dataclass(frozen=True)
class VerificationReport:
    ready: bool
    errors: tuple[str, ...]
    annual_records: int = 0
    evidence_records: int = 0
    active_candidates: int = 0
    recomputed_novel_eed: str = "0"
    recomputed_growth_rate: str = "0"


_REQUIRED_EVIDENCE_FIELDS = (
    "hostname",
    "year",
    "provider",
    "temporal_semantics",
    "evidence_timestamp",
    "source_locator",
    "payload_hash",
    "policy_version",
    "evidence_type",
    "source_id",
    "original_url",
    "record_locator",
    "extraction_method",
)


def _decimal(value: object, name: str, errors: list[str]) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        errors.append(f"{name} must be a finite decimal")
        return None
    if not parsed.is_finite():
        errors.append(f"{name} must be a finite decimal")
        return None
    return parsed


def _read_annual_hosts(
    bundle: zipfile.ZipFile,
    errors: list[str],
) -> dict[int, set[str]]:
    names = set(bundle.namelist())
    result = {year: set() for year in YEAR_BITS}
    for year in YEAR_BITS:
        name = f"{year}.txt"
        if name not in names:
            errors.append(f"missing required entry: {name}")
            continue
        for raw in bundle.read(name).decode("utf-8", errors="replace").splitlines():
            hostname = normalize_official(raw)
            if hostname is None:
                errors.append(f"invalid hostname in {name}: {raw}")
                continue
            if hostname in result[year]:
                errors.append(f"duplicate hostname in {name}: {hostname}")
            result[year].add(hostname)
    return result


def _read_evidence(
    bundle: zipfile.ZipFile,
    errors: list[str],
) -> tuple[dict[tuple[str, int], EvidenceCapsule], int]:
    if "evidence.jsonl" not in set(bundle.namelist()):
        errors.append("missing required entry: evidence.jsonl")
        return {}, 0
    evidence: dict[tuple[str, int], EvidenceCapsule] = {}
    lines = bundle.read("evidence.jsonl").decode("utf-8", errors="replace").splitlines()
    for line_number, raw in enumerate(lines, 1):
        try:
            record = json.loads(raw)
            if not isinstance(record, dict):
                raise ValueError("evidence row must be an object")
            for field in _REQUIRED_EVIDENCE_FIELDS:
                if field not in record or not str(record[field]).strip():
                    raise ValueError(f"missing evidence provenance: {field}")
            capsule = EvidenceCapsule(
                hostname=str(record["hostname"]),
                year=int(record["year"]),
                provider=str(record["provider"]),
                temporal_semantics=str(record["temporal_semantics"]),
                evidence_timestamp=str(record["evidence_timestamp"]),
                source_locator=str(record["source_locator"]),
                payload_hash=str(record["payload_hash"]),
                policy_version=str(record["policy_version"]),
                evidence_type=str(record["evidence_type"]),
                source_id=str(record["source_id"]),
                original_url=str(record["original_url"]),
                record_locator=str(record["record_locator"]),
                extraction_method=str(record["extraction_method"]),
            )
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            errors.append(f"invalid evidence line {line_number}: {exc}")
            continue
        validation = validate_evidence_semantics(capsule)
        if not validation.accepted_for_annual:
            for error in validation.errors:
                errors.append(
                    f"invalid evidence line {line_number}: {error}"
                )
        key = (capsule.hostname, capsule.year)
        if key in evidence:
            errors.append(
                f"duplicate evidence host-year: {capsule.hostname}/{capsule.year}"
            )
        else:
            evidence[key] = capsule
    return evidence, len(evidence)


def _eed_total(values_by_year: dict[int, set[str]], model_path: Path) -> Decimal:
    total = Decimal("0")
    for year in sorted(values_by_year):
        summary, _rows = calculate_eed_values(
            values_by_year[year],
            model_path,
            input_file=f"<submission-verifier:{year}>",
        )
        total += Decimal(str(summary["equivalent_english_domains"]))
    return total


def _recompute_from_bundle(
    bundle: zipfile.ZipFile,
    *,
    authority: AuthoritySnapshot,
    baseline_index_path: Path,
    eed_model_path: Path,
) -> RecomputedSubmission:
    errors: list[str] = []
    annual = _read_annual_hosts(bundle, errors)
    evidence, evidence_records = _read_evidence(bundle, errors)
    annual_pairs = {
        (hostname, year)
        for year, hostnames in annual.items()
        for hostname in hostnames
    }

    missing = annual_pairs - set(evidence)
    if missing:
        errors.append(f"annual records without evidence: {len(missing)}")

    try:
        actual_model_hash = eed_model_authority_signature(eed_model_path)
        if actual_model_hash != authority.model_hash:
            errors.append("supplied EED model hash does not match authority manifest")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"cannot load supplied EED model: {exc}")

    baseline: BaselineIndex | None = None
    try:
        baseline = BaselineIndex(baseline_index_path)
        baseline.assert_authority(authority)
        overlap = 0
        for hostname, year in annual_pairs:
            if baseline.year_mask(hostname) & YEAR_BITS[year]:
                overlap += 1
        if overlap:
            errors.append(
                f"annual output overlaps target-year baseline: {overlap}"
            )
    except (OSError, ValueError) as exc:
        errors.append(f"cannot verify supplied baseline index authority: {exc}")
    finally:
        if baseline is not None:
            baseline.close()

    total_eed = Decimal("0")
    lane_sets = {
        "direct_annual": {year: set() for year in YEAR_BITS},
        "verified_candidate": {year: set() for year in YEAR_BITS},
        "other_restricted": {year: set() for year in YEAR_BITS},
    }
    try:
        total_eed = _eed_total(annual, eed_model_path)
        for hostname, year in annual_pairs:
            capsule = evidence.get((hostname, year))
            lane = (
                AcquisitionLane.UNKNOWN
                if capsule is None
                else classify_acquisition_lane(capsule)
            )
            lane_sets[contribution_bucket(lane)][year].add(hostname)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"cannot independently recompute EED: {exc}")

    lane_contribution: dict[str, dict[str, object]] = {}
    lane_sum = Decimal("0")
    for bucket, values_by_year in lane_sets.items():
        try:
            eed = _eed_total(values_by_year, eed_model_path)
        except (OSError, ValueError, json.JSONDecodeError):
            eed = Decimal("0")
        lane_sum += eed
        lane_contribution[bucket] = {
            "novel_host_years": sum(len(values) for values in values_by_year.values()),
            "novel_eed": format(eed, "f"),
        }
    if lane_sum != total_eed:
        errors.append("acquisition-lane EED totals do not sum to total novel EED")

    baseline_eed = Decimal(authority.baseline_eed)
    growth = "0"
    if baseline_eed <= 0:
        errors.append("authority baseline_eed must be positive")
    else:
        growth = format_growth_rate(total_eed, baseline_eed)

    return RecomputedSubmission(
        ready=not errors,
        errors=tuple(errors),
        annual_records=len(annual_pairs),
        evidence_records=evidence_records,
        novel_eed=format(total_eed, "f"),
        growth_rate=growth,
        lane_contribution=lane_contribution,
    )


def recompute_submission_archive(
    archive: Path,
    *,
    baseline_manifest_path: Path,
    baseline_index_path: Path,
    eed_model_path: Path,
) -> RecomputedSubmission:
    """Independently recompute novelty/EED from actual authority inputs."""

    try:
        authority = AuthoritySnapshot.from_manifest_path(baseline_manifest_path)
        with zipfile.ZipFile(archive) as bundle:
            return _recompute_from_bundle(
                bundle,
                authority=authority,
                baseline_index_path=Path(baseline_index_path),
                eed_model_path=Path(eed_model_path),
            )
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        return RecomputedSubmission(
            False,
            (f"cannot independently recompute submission: {exc}",),
            0,
            0,
            "0",
            "0",
            {
                "direct_annual": {"novel_host_years": 0, "novel_eed": "0"},
                "verified_candidate": {"novel_host_years": 0, "novel_eed": "0"},
                "other_restricted": {"novel_host_years": 0, "novel_eed": "0"},
            },
        )


def verify_submission_archive(
    archive: Path,
    *,
    baseline_manifest_path: Path | None = None,
    baseline_index_path: Path | None = None,
    eed_model_path: Path | None = None,
) -> VerificationReport:
    errors: list[str] = []
    active_candidates = 0
    recomputed: RecomputedSubmission | None = None
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

        if not str(manifest.get("baseline_id", "")).strip():
            errors.append("manifest baseline_id is missing")
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

        for required in (
            "reports/eed.json",
            "cdx_audit.json",
            "source_reports.json",
            "method_failure_summary.json",
            "isc_reference/manifest.json",
            "reports/baseline_reconciliation.json",
            "reports/source_contribution.json",
        ):
            require(required)

        authority: AuthoritySnapshot | None = None
        if baseline_manifest_path is None:
            errors.append("supplied authority manifest is required")
        else:
            try:
                authority = AuthoritySnapshot.from_manifest_path(
                    baseline_manifest_path
                )
                expected_hashes = {
                    name.removesuffix(".txt"): digest
                    for name, digest in authority.annual_file_hashes.items()
                }
                if manifest.get("baseline_id") != authority.baseline_id:
                    errors.append("manifest baseline_id does not match supplied authority manifest")
                if manifest.get("baseline_hashes") != expected_hashes:
                    errors.append("manifest baseline hashes do not match supplied authority manifest")
                if manifest.get("candidate_file_hash") != authority.candidate_file_hash:
                    errors.append("manifest candidate hash does not match supplied authority manifest")
                if manifest.get("model_hash") != authority.model_hash:
                    errors.append("manifest model hash does not match supplied authority manifest")
                if str(manifest.get("baseline_eed", "")) != authority.baseline_eed:
                    errors.append("manifest baseline_eed does not match supplied authority manifest")
                if manifest.get("authority_digest") != authority.authority_digest:
                    errors.append("manifest authority_digest does not match supplied authority manifest")
            except (OSError, ValueError) as exc:
                errors.append(f"cannot compare authority manifest: {exc}")

        if baseline_index_path is None:
            errors.append("supplied baseline index is required")
        if eed_model_path is None:
            errors.append("supplied EED model is required")
        if (
            authority is not None
            and baseline_index_path is not None
            and eed_model_path is not None
        ):
            recomputed = _recompute_from_bundle(
                bundle,
                authority=authority,
                baseline_index_path=Path(baseline_index_path),
                eed_model_path=Path(eed_model_path),
            )
            errors.extend(recomputed.errors)

            manifest_eed = _decimal(manifest.get("novel_eed"), "manifest novel_eed", errors)
            manifest_growth = _decimal(manifest.get("growth_rate"), "manifest growth_rate", errors)
            if manifest_eed is not None and manifest_eed != Decimal(recomputed.novel_eed):
                errors.append("manifest novel_eed does not match independent recomputation")
            if manifest_growth is not None and manifest_growth != Decimal(recomputed.growth_rate):
                errors.append("manifest growth_rate does not match independent recomputation")

            eed_bytes = require("reports/eed.json")
            if eed_bytes is not None:
                try:
                    eed_report = json.loads(eed_bytes)
                    reported = Decimal(str(eed_report["equivalent_english_domains"]))
                    if reported != Decimal(recomputed.novel_eed):
                        errors.append("EED report does not match independent recomputation")
                except (KeyError, InvalidOperation, ValueError, json.JSONDecodeError) as exc:
                    errors.append(f"invalid EED report: {exc}")

            reconciliation_bytes = require("reports/baseline_reconciliation.json")
            if reconciliation_bytes is not None:
                try:
                    reconciliation = json.loads(reconciliation_bytes)
                    if Decimal(str(reconciliation["novel_eed"])) != Decimal(recomputed.novel_eed):
                        errors.append("baseline reconciliation novel_eed does not match recomputation")
                    if Decimal(str(reconciliation["growth_rate"])) != Decimal(recomputed.growth_rate):
                        errors.append("baseline reconciliation growth_rate does not match recomputation")
                except (KeyError, InvalidOperation, ValueError, json.JSONDecodeError) as exc:
                    errors.append(f"invalid baseline reconciliation report: {exc}")

            contribution_bytes = require("reports/source_contribution.json")
            if contribution_bytes is not None:
                try:
                    contribution = json.loads(contribution_bytes)
                    for bucket, expected in recomputed.lane_contribution.items():
                        actual = contribution.get(bucket)
                        if not isinstance(actual, dict):
                            errors.append(f"source contribution missing lane: {bucket}")
                            continue
                        if int(actual.get("novel_host_years", -1)) != int(expected["novel_host_years"]):
                            errors.append(f"source contribution count mismatch: {bucket}")
                        if Decimal(str(actual.get("novel_eed", "-1"))) != Decimal(str(expected["novel_eed"])):
                            errors.append(f"source contribution EED mismatch: {bucket}")
                except (InvalidOperation, ValueError, TypeError, json.JSONDecodeError) as exc:
                    errors.append(f"invalid source contribution report: {exc}")

        active_bytes = require("active_candidates.txt")
        if active_bytes is not None:
            annual_hostnames: set[str] = set()
            for year in YEAR_BITS:
                name = f"{year}.txt"
                if name in names:
                    annual_hostnames.update(
                        value
                        for raw in bundle.read(name).decode("utf-8", errors="replace").splitlines()
                        if (value := normalize_official(raw)) is not None
                    )
            for raw in active_bytes.decode("utf-8", errors="replace").splitlines():
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

    return VerificationReport(
        not errors,
        tuple(errors),
        annual_records=0 if recomputed is None else recomputed.annual_records,
        evidence_records=0 if recomputed is None else recomputed.evidence_records,
        active_candidates=active_candidates,
        recomputed_novel_eed="0" if recomputed is None else recomputed.novel_eed,
        recomputed_growth_rate="0" if recomputed is None else recomputed.growth_rate,
    )
