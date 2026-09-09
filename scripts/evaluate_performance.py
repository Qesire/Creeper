#!/usr/bin/env python3
"""Evaluate local engineering measurements against the competition throughput bar."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path

from creeper.metrics.performance import PerformanceReference, build_performance_model


def _read(path: Path | None) -> dict[str, object] | None:
    return json.loads(path.read_text(encoding="utf-8")) if path else None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--lookup-report", type=Path)
    parser.add_argument("--efficiency-report", type=Path)
    parser.add_argument("--candidate-pilot", type=Path)
    parser.add_argument("--baseline-eed", default="34887095.7393")
    args = parser.parse_args()

    reference = PerformanceReference(
        observation_days=Decimal("23"),
        annual_raw=Decimal("2144570"),
        annual_eed=Decimal("1246435.78"),
        candidate_raw=Decimal("16953165"),
        candidate_eed=Decimal("9330214.38"),
        baseline_eed=Decimal(args.baseline_eed),
    )
    model = build_performance_model(reference)
    lookup = _read(args.lookup_report)
    efficiency = _read(args.efficiency_report)
    pilot = _read(args.candidate_pilot)
    batch_qps = None if lookup is None else lookup.get("batch_lookups_per_second")
    real_accepted = None
    if efficiency is not None:
        evidence = efficiency.get("evidence_pilot", {})
        if isinstance(evidence, dict):
            real_accepted = evidence.get("accepted")

    payload = {
        "report_version": "performance-gate-v1",
        "reference_provenance": {
            "source": "user-provided accepted result for teammate C",
            "observation_days": 23,
            "independently_verified": False,
            "annual_and_candidate_tracks_are_separate": True,
        },
        "model": model.to_dict(),
        "engineering_requirements": {
            "batch_resolve_target_per_second": 100000,
            "annual_eed_target_per_day": 250000,
            "annual_eed_strong_target_per_day": 500000,
            "candidate_and_annual_must_not_be_added_without_rule_confirmation": True,
        },
        "current_measurements": {
            "lookup_report": lookup,
            "efficiency_report": efficiency,
            "candidate_pilot": pilot,
            "batch_resolve_per_second": batch_qps,
            "batch_resolve_target_met": bool(batch_qps is not None and batch_qps >= 100000),
            "real_accepted_host_years_in_recorded_pilot": real_accepted,
            "annual_business_rate_status": "not_demonstrated",
        },
        "interpretation": {
            "primary_gate": "annual_novel_eed_per_day",
            "secondary_scenario": "candidate_novel_eed_per_day",
            "next_bottleneck": "bulk_source_enumeration_and_exact_year_evidence",
            "five_percent_gate_is_eta_metric": True,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
