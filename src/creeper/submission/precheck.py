"""Formal readiness checks before building a submission archive."""

from __future__ import annotations

from dataclasses import dataclass
import re

from creeper.authority.normalizer import normalize_official
from creeper.submission.snapshot import SubmissionSnapshot


@dataclass(frozen=True)
class PrecheckReport:
    ready: bool
    reasons: tuple[str, ...]


def precheck_submission(snapshot: SubmissionSnapshot) -> PrecheckReport:
    reasons: list[str] = []
    if snapshot.baseline_id != "merged260909-3":
        reasons.append("baseline_id must be merged260909-3")
    expected_years = {str(year) for year in range(1996, 2002)}
    if set(snapshot.baseline_hashes) != expected_years:
        reasons.append("all six annual baseline hashes are required")
    elif any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in snapshot.baseline_hashes.values()):
        reasons.append("annual baseline hashes must be SHA-256 values")
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
