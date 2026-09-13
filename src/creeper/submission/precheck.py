"""Formal readiness checks before building a submission archive."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re

from creeper.authority.identity import authority_digest
from creeper.authority.normalizer import normalize_official
from creeper.submission.snapshot import SubmissionSnapshot


MINIMUM_GROWTH_RATE = Decimal("0.05")


@dataclass(frozen=True)
class PrecheckReport:
    ready: bool
    reasons: tuple[str, ...]


def _decimal(value: object, name: str, reasons: list[str]) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        reasons.append(f"{name} must be a finite decimal")
        return None
    if not parsed.is_finite():
        reasons.append(f"{name} must be a finite decimal")
        return None
    return parsed


def precheck_submission(snapshot: SubmissionSnapshot) -> PrecheckReport:
    reasons: list[str] = []
    if not snapshot.baseline_id:
        reasons.append("baseline_id is required")
    expected_years = {str(year) for year in range(1996, 2002)}
    if set(snapshot.baseline_hashes) != expected_years:
        reasons.append("all six annual baseline hashes are required")
    elif any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in snapshot.baseline_hashes.values()):
        reasons.append("annual baseline hashes must be SHA-256 values")
    sha256 = re.compile(r"[0-9a-f]{64}")
    if not sha256.fullmatch(snapshot.candidate_file_hash):
        reasons.append("candidate_file_hash must be a SHA-256 value")
    if not sha256.fullmatch(snapshot.model_hash):
        reasons.append("model_hash must be a SHA-256 value")
    baseline_eed = _decimal(snapshot.baseline_eed, "baseline_eed", reasons)
    if baseline_eed is not None and baseline_eed < 0:
        reasons.append("baseline_eed cannot be negative")
    if not sha256.fullmatch(snapshot.authority_digest):
        reasons.append("authority_digest must be a SHA-256 value")
    elif (
        set(snapshot.baseline_hashes) == expected_years
        and all(sha256.fullmatch(value) for value in snapshot.baseline_hashes.values())
        and sha256.fullmatch(snapshot.candidate_file_hash)
        and sha256.fullmatch(snapshot.model_hash)
        and baseline_eed is not None
        and baseline_eed >= 0
        and snapshot.baseline_id
    ):
        expected_digest = authority_digest(
            baseline_id=snapshot.baseline_id,
            annual_file_hashes={
                f"{year}.txt": digest
                for year, digest in snapshot.baseline_hashes.items()
            },
            candidate_file_hash=snapshot.candidate_file_hash,
            model_hash=snapshot.model_hash,
            baseline_eed=format(baseline_eed, "f"),
        )
        if snapshot.authority_digest != expected_digest:
            reasons.append(
                "authority_digest does not match submission authority fields"
            )

    if not snapshot.normalizer_version or not snapshot.evidence_policy_version:
        reasons.append("normalizer and evidence policy versions are required")
    if not snapshot.eed_policy_version:
        reasons.append("EED policy version is required")
    if not snapshot.code_revision:
        reasons.append("code revision is required")
    if not snapshot.source_report_set:
        reasons.append("source reports are required")
    if not snapshot.cdx_audit_set:
        reasons.append("CDX audit records are required")
    if snapshot.eed_report is None or "equivalent_english_domains" not in snapshot.eed_report:
        reasons.append("exact EED report is required")
    if snapshot.invalid_count:
        reasons.append("invalid annual records remain")
    if snapshot.overlap_count:
        reasons.append("annual baseline overlap remains")
    if snapshot.incomplete_query_count:
        reasons.append("incomplete queries cannot be submitted as negative evidence")
    if len(snapshot.active_candidates) != len(snapshot.active_candidate_scopes):
        reasons.append("every active candidate requires a source scope")

    novel_eed = _decimal(snapshot.novel_eed, "novel_eed", reasons)
    growth_rate = _decimal(snapshot.growth_rate, "growth_rate", reasons)
    if novel_eed is not None and novel_eed < 0:
        reasons.append("novel_eed cannot be negative")
    if growth_rate is not None:
        if growth_rate < 0:
            reasons.append("growth_rate cannot be negative")
        elif growth_rate < MINIMUM_GROWTH_RATE:
            reasons.append("formal submission requires at least 5% EED growth")

    if snapshot.eed_report is not None and "equivalent_english_domains" in snapshot.eed_report:
        reported_eed = _decimal(
            snapshot.eed_report["equivalent_english_domains"],
            "eed_report.equivalent_english_domains",
            reasons,
        )
        if novel_eed is not None and reported_eed is not None and reported_eed != novel_eed:
            reasons.append("novel_eed must match the exact EED report")

    for capsule in snapshot.novel_records:
        if normalize_official(capsule.hostname) is None:
            reasons.append(f"invalid evidence hostname: {capsule.hostname}")
        if capsule.year not in range(1996, 2002):
            reasons.append(f"evidence year out of range: {capsule.year}")
    for scope in snapshot.active_candidate_scopes:
        normalized_scope = scope.lower().replace("-", "_").replace(" ", "_")
        if "common_crawl" in normalized_scope:
            reasons.append("Common Crawl candidate is present in active candidates")
    return PrecheckReport(not reasons, tuple(reasons))
