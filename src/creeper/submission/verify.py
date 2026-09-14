"""Independent semantic verifier for the submission archive contract."""

from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex, YEAR_BITS
from creeper.authority.eed import load_english_weights
from creeper.authority.identity import (
    AuthoritySnapshot,
    eed_model_authority_signature,
)
from creeper.authority.normalizer import normalize_official
from creeper.evidence.classification import (
    classify_acquisition_lane,
    contribution_bucket,
    validate_evidence_semantics,
)
from creeper.evidence.contract_registry import (
    ReviewedContractRegistry,
    ReviewedContractRegistryError,
)
from creeper.evidence.policies import EvidenceCapsule
from creeper.submission.precheck import format_growth_rate
from creeper.submission.streaming_reader import iter_archive_lines


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


def _tld(hostname: str) -> str:
    return hostname.rsplit(".", 1)[-1].lower()


def _increment_tld(counts: dict[str, int], hostname: str) -> None:
    tld = _tld(hostname)
    counts[tld] = counts.get(tld, 0) + 1


def _weighted_eed(
    counts_by_year: dict[int, dict[str, int]],
    weights: dict[str, Decimal],
) -> Decimal:
    return sum(
        (
            Decimal(count) * weights.get(tld, Decimal("0"))
            for counts in counts_by_year.values()
            for tld, count in counts.items()
        ),
        Decimal("0"),
    )


def _archive_entry_digest(
    bundle: zipfile.ZipFile,
    name: str,
    *,
    chunk_size: int = 1024 * 1024,
) -> tuple[str, int]:
    if (
        isinstance(chunk_size, bool)
        or not isinstance(chunk_size, int)
        or chunk_size < 1
    ):
        raise ValueError("chunk_size must be a positive integer")
    digest = hashlib.sha256()
    size = 0
    with bundle.open(name, "r") as source:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _iter_normalized_annual_hosts(
    bundle: zipfile.ZipFile,
    names: set[str],
    year: int,
):
    name = f"{year}.txt"
    if name not in names:
        return
    for raw in iter_archive_lines(bundle, name):
        hostname = normalize_official(raw)
        if hostname is not None:
            yield hostname


def _recompute_from_bundle(
    bundle: zipfile.ZipFile,
    *,
    authority: AuthoritySnapshot,
    baseline_index_path: Path,
    eed_model_path: Path,
    reviewed_contracts: ReviewedContractRegistry | None = None,
) -> RecomputedSubmission:
    errors: list[str] = []
    names = set(bundle.namelist())

    baseline: BaselineIndex | None = None
    baseline_has_rows = False
    try:
        baseline = BaselineIndex(baseline_index_path)
        baseline.assert_authority(authority)
        baseline_has_rows = (
            baseline.connection.execute(
                "SELECT 1 FROM annual_hostnames LIMIT 1"
            ).fetchone()
            is not None
        )
    except (OSError, ValueError) as exc:
        errors.append(f"cannot verify supplied baseline index authority: {exc}")

    weights: dict[str, Decimal] = {}
    try:
        weights = load_english_weights(eed_model_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"cannot independently recompute EED: {exc}")

    try:
        actual_model_hash = eed_model_authority_signature(eed_model_path)
        if actual_model_hash != authority.model_hash:
            errors.append("supplied EED model hash does not match authority manifest")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        errors.append(f"cannot load supplied EED model: {exc}")

    annual_counts = {year: {} for year in YEAR_BITS}
    lane_counts = {
        bucket: {year: {} for year in YEAR_BITS}
        for bucket in (
            "direct_annual",
            "verified_candidate",
            "other_restricted",
        )
    }
    lane_records = {
        bucket: {year: 0 for year in YEAR_BITS}
        for bucket in lane_counts
    }
    annual_current: dict[int, str | None] = {}
    annual_previous: dict[int, str | None] = {year: None for year in YEAR_BITS}
    annual_iterators = {}
    annual_records = 0
    baseline_overlap = 0

    for year in YEAR_BITS:
        name = f"{year}.txt"
        if name not in names:
            errors.append(f"missing required entry: {name}")
            annual_iterators[year] = iter(())
        else:
            annual_iterators[year] = iter_archive_lines(bundle, name)

    def next_annual(year: int) -> str | None:
        nonlocal annual_records, baseline_overlap
        name = f"{year}.txt"
        while True:
            try:
                raw = next(annual_iterators[year])
            except StopIteration:
                return None
            hostname = normalize_official(raw)
            if hostname is None:
                errors.append(f"invalid hostname in {name}: {raw}")
                continue
            previous = annual_previous[year]
            if previous is not None and hostname == previous:
                errors.append(f"duplicate hostname in {name}: {hostname}")
                continue
            if previous is not None and hostname < previous:
                errors.append(
                    f"annual entries are not ordered in {name}: {hostname} after {previous}"
                )
                continue
            annual_previous[year] = hostname
            annual_records += 1
            _increment_tld(annual_counts[year], hostname)
            if (
                baseline is not None
                and baseline_has_rows
                and baseline.year_mask(hostname) & YEAR_BITS[year]
            ):
                baseline_overlap += 1
            return hostname

    for year in YEAR_BITS:
        annual_current[year] = next_annual(year)

    missing_annual = 0

    def assign_annual(year: int, bucket: str) -> None:
        hostname = annual_current[year]
        if hostname is None:
            return
        lane_records[bucket][year] += 1
        _increment_tld(lane_counts[bucket][year], hostname)
        annual_current[year] = next_annual(year)

    evidence_records = 0
    previous_evidence_key: tuple[str, int] | None = None
    if "evidence.jsonl" not in names:
        errors.append("missing required entry: evidence.jsonl")
    else:
        for line_number, raw in enumerate(
            iter_archive_lines(bundle, "evidence.jsonl"),
            1,
        ):
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

            key = (capsule.hostname, capsule.year)
            if previous_evidence_key is not None and key == previous_evidence_key:
                errors.append(
                    f"duplicate evidence host-year: {capsule.hostname}/{capsule.year}"
                )
                continue
            if previous_evidence_key is not None and key < previous_evidence_key:
                errors.append(
                    "evidence entries are not ordered by (hostname, year): "
                    f"{capsule.hostname}/{capsule.year} after "
                    f"{previous_evidence_key[0]}/{previous_evidence_key[1]}"
                )
                continue
            previous_evidence_key = key
            evidence_records += 1

            validation = validate_evidence_semantics(
                capsule,
                reviewed_contracts=reviewed_contracts,
            )
            if not validation.accepted_for_annual:
                for error in validation.errors:
                    errors.append(f"invalid evidence line {line_number}: {error}")

            bucket = contribution_bucket(
                classify_acquisition_lane(
                    capsule,
                    reviewed_contracts=reviewed_contracts,
                )
            )
            if capsule.year not in annual_current:
                continue
            while (
                annual_current[capsule.year] is not None
                and annual_current[capsule.year] < capsule.hostname
            ):
                missing_annual += 1
                assign_annual(capsule.year, "other_restricted")
            if annual_current[capsule.year] == capsule.hostname:
                assign_annual(capsule.year, bucket)
            else:
                errors.append(
                    f"evidence host-year is not present in annual output: "
                    f"{capsule.hostname}/{capsule.year}"
                )

    for year in YEAR_BITS:
        while annual_current[year] is not None:
            missing_annual += 1
            assign_annual(year, "other_restricted")
    if missing_annual:
        errors.append(f"annual records without evidence: {missing_annual}")
    if baseline_overlap:
        errors.append(
            f"annual output overlaps target-year baseline: {baseline_overlap}"
        )

    lane_contribution: dict[str, dict[str, object]] = {}
    total_eed = _weighted_eed(annual_counts, weights)
    lane_sum = Decimal("0")
    for bucket in lane_counts:
        eed = _weighted_eed(lane_counts[bucket], weights)
        lane_sum += eed
        lane_contribution[bucket] = {
            "novel_host_years": sum(lane_records[bucket].values()),
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

    result = RecomputedSubmission(
        ready=not errors,
        errors=tuple(errors),
        annual_records=annual_records,
        evidence_records=evidence_records,
        novel_eed=format(total_eed, "f"),
        growth_rate=growth,
        lane_contribution=lane_contribution,
    )
    if baseline is not None:
        baseline.close()
    return result


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
            reviewed_contracts: ReviewedContractRegistry | None = None
            registry_name = "artifacts/runtime/reviewed_contract_registry.json"
            if registry_name in set(bundle.namelist()):
                reviewed_contracts = ReviewedContractRegistry.from_manifest_payload(
                    json.loads(bundle.read(registry_name))
                )
            return _recompute_from_bundle(
                bundle,
                authority=authority,
                baseline_index_path=Path(baseline_index_path),
                eed_model_path=Path(eed_model_path),
                reviewed_contracts=reviewed_contracts,
            )
    except (
        OSError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ReviewedContractRegistryError,
        zipfile.BadZipFile,
    ) as exc:
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
        if not isinstance(manifest, dict):
            return VerificationReport(
                False,
                tuple(errors + ["MANIFEST.json root must be an object"]),
            )

        reviewed_contracts: ReviewedContractRegistry | None = None
        registry_name = "artifacts/runtime/reviewed_contract_registry.json"
        if registry_name in names:
            try:
                reviewed_contracts = ReviewedContractRegistry.from_manifest_payload(
                    json.loads(bundle.read(registry_name))
                )
            except (
                ReviewedContractRegistryError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as exc:
                errors.append(f"invalid packaged reviewed contract registry: {exc}")
        artifact_rows = manifest.get("artifacts", [])
        if not isinstance(artifact_rows, list):
            errors.append("manifest artifacts must be a list")
            artifact_rows = []
        registry_rows = [
            row
            for row in artifact_rows
            if isinstance(row, dict)
            and row.get("logical_role") == "reviewed_contract_registry"
        ]
        if registry_name in names and not any(
            row.get("archive_path") == registry_name for row in registry_rows
        ):
            errors.append("packaged reviewed contract registry artifact row is missing")
        for row in registry_rows:
            if row.get("archive_path") != registry_name:
                errors.append("reviewed contract registry artifact path is invalid")
                continue
            if registry_name not in names:
                errors.append("reviewed contract registry artifact is missing")
                continue
            expected_hash = row.get("sha256")
            actual_hash, actual_size = _archive_entry_digest(bundle, registry_name)
            if expected_hash != actual_hash:
                errors.append("reviewed contract registry artifact hash mismatch")
            try:
                if int(row.get("size", -1)) != actual_size:
                    errors.append("reviewed contract registry artifact size mismatch")
            except (TypeError, ValueError):
                errors.append("reviewed contract registry artifact size is invalid")

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
            else:
                actual_hash, _actual_size = _archive_entry_digest(bundle, name)
                if actual_hash != expected_hash:
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
                reviewed_contracts=reviewed_contracts,
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

        if "active_candidates.txt" not in names:
            errors.append("missing required entry: active_candidates.txt")
        else:
            annual_iters = {
                year: iter(_iter_normalized_annual_hosts(bundle, names, year))
                for year in YEAR_BITS
            }
            annual_current = {
                year: next(annual_iters[year], None) for year in YEAR_BITS
            }
            previous_active: str | None = None
            for raw in iter_archive_lines(bundle, "active_candidates.txt"):
                hostname = normalize_official(raw)
                if hostname is None:
                    errors.append(f"invalid active candidate: {raw}")
                    continue
                if previous_active is not None and hostname < previous_active:
                    errors.append(
                        "active candidates are not ordered: "
                        f"{hostname} after {previous_active}"
                    )
                previous_active = hostname
                active_candidates += 1
                for year in YEAR_BITS:
                    while (
                        annual_current[year] is not None
                        and annual_current[year] < hostname
                    ):
                        annual_current[year] = next(annual_iters[year], None)
                    if annual_current[year] == hostname:
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
