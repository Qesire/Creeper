"""Thin bridge from runtime stores to the canonical submission builder."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from creeper.authority.baseline_index import YEAR_BITS, BaselineIndex
from creeper.authority.eed import calculate_eed_values
from creeper.authority.identity import AuthoritySnapshot, eed_model_authority_signature
from creeper.evidence.classification import (
    classify_acquisition_lane,
    contribution_bucket,
)
from creeper.storage.evidence_store import EvidenceStore
from creeper.submission.builder import build_snapshot
from creeper.submission.precheck import format_growth_rate
from creeper.submission.snapshot import SubmissionSnapshot


@dataclass(frozen=True)
class RuntimeSubmissionContext:
    """All immutable inputs required to build a runtime submission snapshot."""

    baseline_manifest: dict[str, object]
    code_revision: str
    source_report_set: tuple[str, ...]
    cdx_audit_set: tuple[str, ...]
    eed_report: dict[str, object]
    active_candidates: tuple[str, ...] = ()
    active_candidate_scopes: tuple[str, ...] = ()
    isc_reference: tuple[str, ...] = ()
    unparsed: tuple[str, ...] = ()
    novel_eed: str = "0"
    growth_rate: str = "0"
    eed_model_path: Path | None = None
    baseline_eed: str = "0"


def _annual_eed_report(
    capsules,
    model_path: Path,
) -> dict[str, object]:
    """Compute authoritative EED with annual host-year semantics.

    The official calculator deduplicates hostnames within one input. Competition
    results are annual, so the same hostname proven in two different years must
    contribute once in each year. Therefore each year is calculated
    independently and the six annual EED values are summed.
    """
    by_year: dict[int, set[str]] = {year: set() for year in YEAR_BITS}
    by_source: dict[str, dict[int, set[str]]] = {}
    lane_by_year: dict[str, dict[int, set[str]]] = {
        "direct_annual": {year: set() for year in YEAR_BITS},
        "verified_candidate": {year: set() for year in YEAR_BITS},
        "other_restricted": {year: set() for year in YEAR_BITS},
    }
    for capsule in capsules:
        if capsule.year in by_year:
            by_year[capsule.year].add(capsule.hostname)
            source = capsule.source_id or capsule.provider
            by_source.setdefault(source, {year: set() for year in YEAR_BITS})[
                capsule.year
            ].add(capsule.hostname)
            bucket = contribution_bucket(classify_acquisition_lane(capsule))
            lane_by_year[bucket][capsule.year].add(capsule.hostname)

    total = Decimal("0")
    annual: dict[str, object] = {}
    for year in sorted(by_year):
        summary, rows = calculate_eed_values(
            by_year[year],
            Path(model_path),
            input_file=f"<runtime-evidence-store:{year}>",
        )
        year_eed = Decimal(str(summary["equivalent_english_domains"]))
        total += year_eed
        annual[str(year)] = {
            "novel_host_years": len(by_year[year]),
            "equivalent_english_domains": format(year_eed, "f"),
            "summary": summary,
            "tld_breakdown": rows,
        }

    source_contribution: dict[str, dict[str, object]] = {}
    for source, source_years in sorted(by_source.items()):
        source_total = Decimal("0")
        source_count = 0
        for year in sorted(source_years):
            summary, _ = calculate_eed_values(
                source_years[year],
                Path(model_path),
                input_file=f"<runtime-source:{source}:{year}>",
            )
            source_total += Decimal(str(summary["equivalent_english_domains"]))
            source_count += len(source_years[year])
        source_contribution[source] = {
            "novel_host_years": source_count,
            "novel_eed": format(source_total, "f"),
        }
    source_report: dict[str, object] = {"by_source": source_contribution}
    lane_total = Decimal("0")
    for bucket, yearly in lane_by_year.items():
        bucket_total = Decimal("0")
        for year, values in yearly.items():
            summary, _ = calculate_eed_values(values, Path(model_path))
            bucket_total += Decimal(str(summary["equivalent_english_domains"]))
        lane_total += bucket_total
        source_report[bucket] = {
            "novel_host_years": sum(len(values) for values in yearly.values()),
            "novel_eed": format(bucket_total, "f"),
        }
    if lane_total != total:
        raise ValueError("acquisition-lane EED attribution does not sum to total")
    return {
        "authority": "official-calculator-v1",
        "method": (
            "Equivalent-English Domains are calculated independently for each "
            "annual result set and summed across 1996-2001; a hostname proven "
            "in multiple years contributes once per distinct year."
        ),
        "model_path": str(Path(model_path).resolve()),
        "equivalent_english_domains": format(total, "f"),
        "annual": annual,
        "source_contribution": source_report,
    }


def build_runtime_snapshot(
    *,
    context: RuntimeSubmissionContext,
    evidence_store: EvidenceStore,
    baseline: BaselineIndex,
    snapshot_id: str,
) -> SubmissionSnapshot:
    """Build a snapshot from durable runtime stores using the canonical builder."""
    authority = AuthoritySnapshot.from_manifest(context.baseline_manifest)
    baseline.assert_authority(authority)
    if (
        context.baseline_eed != "0"
        and Decimal(str(context.baseline_eed)) != Decimal(authority.baseline_eed)
    ):
        raise ValueError("submission baseline_eed conflicts with authority manifest")
    if (
        context.eed_model_path is not None
        and eed_model_authority_signature(context.eed_model_path)
        != authority.model_hash
    ):
        raise ValueError("submission EED model does not match authority manifest")
    novel_capsules = [
        capsule
        for capsule in evidence_store.canonical_host_year_capsules()
        if capsule.year in YEAR_BITS
        and not baseline.year_mask(capsule.hostname) & YEAR_BITS[capsule.year]
    ]
    if context.eed_model_path is None:
        eed_report = {
            "authority": "missing-official-eed-model",
            "equivalent_english_domains": "0",
            "method": "runtime snapshot is not submission-authoritative without the official EED model",
        }
        novel_eed = "0"
        growth_rate = "0"
    else:
        eed_report = _annual_eed_report(
            novel_capsules,
            Path(context.eed_model_path),
        )
        novel_eed = str(eed_report["equivalent_english_domains"])
        baseline_eed = Decimal(authority.baseline_eed)
        growth_rate = (
            format_growth_rate(Decimal(novel_eed), baseline_eed)
            if baseline_eed > 0
            else "0"
        )
    return build_snapshot(
        snapshot_id,
        novel_capsules,
        baseline,
        context.baseline_manifest,
        code_revision=context.code_revision,
        source_report_set=context.source_report_set,
        cdx_audit_set=context.cdx_audit_set,
        eed_report=eed_report,
        active_candidates=context.active_candidates,
        active_candidate_scopes=context.active_candidate_scopes,
        isc_reference=context.isc_reference,
        unparsed=context.unparsed,
        novel_eed=novel_eed,
        growth_rate=growth_rate,
        source_contribution=(
            eed_report.get("source_contribution")
            if isinstance(eed_report, dict)
            else None
        ),
    )
