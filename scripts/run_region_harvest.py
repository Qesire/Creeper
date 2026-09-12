#!/usr/bin/env python3
"""Plan and execute one bounded exact historical-region harvest cycle."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.source_discovery.harvest import (
    RegionHarvestExecutor,
    RegionHarvestPolicy,
)
from creeper.source_discovery.harvest_service import RegionHarvestService
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--baseline-db", type=Path, required=True)
    parser.add_argument("--evidence-db", type=Path, required=True)
    parser.add_argument("--owner", default="region-harvester")
    parser.add_argument("--max-regions", type=int, default=4)
    parser.add_argument("--byte-budget", type=int)
    parser.add_argument("--max-seconds", type=float, default=300.0)
    parser.add_argument("--max-records-per-lease", type=int, default=100_000)
    parser.add_argument("--baseline-batch-size", type=int, default=20_000)
    parser.add_argument("--claim-grace-seconds", type=float, default=60.0)
    parser.add_argument("--policy-version", default="historical-region-v1")
    parser.add_argument("--unknown-overlap-penalty", type=float, default=0.25)
    parser.add_argument("--confidence-floor", type=float, default=0.25)
    parser.add_argument("--min-marginal-fraction", type=float, default=0.01)
    parser.add_argument("--min-marginal-eed", type=float, default=0.0)
    parser.add_argument("--min-score-per-mib", type=float, default=0.0)
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="stop on the first region error instead of reporting and continuing",
    )
    args = parser.parse_args()

    control = ControlStore(args.control_db)
    baseline = BaselineIndex(args.baseline_db)
    evidence = EvidenceStore(args.evidence_db)
    try:
        registry = IndexSpaceRegistry(control)
        portfolio = RegionPortfolioPlanner(
            registry,
            policy=RegionPortfolioPolicy(
                unknown_overlap_penalty=args.unknown_overlap_penalty,
                confidence_floor=args.confidence_floor,
                min_marginal_fraction=args.min_marginal_fraction,
                min_marginal_eed=args.min_marginal_eed,
                min_score_per_mib=args.min_score_per_mib,
            ),
        )
        harvest = RegionHarvestExecutor(
            registry=registry,
            baseline=baseline,
            evidence_store=evidence,
            owner=args.owner,
            policy=RegionHarvestPolicy(
                max_seconds=args.max_seconds,
                baseline_batch_size=args.baseline_batch_size,
                max_records_per_lease=args.max_records_per_lease,
                policy_version=args.policy_version,
                claim_grace_seconds=args.claim_grace_seconds,
            ),
        )
        service = RegionHarvestService(
            registry,
            portfolio_planner=portfolio,
            harvest_executor=harvest,
        )
        report = service.run_once(
            max_regions=args.max_regions,
            byte_budget=args.byte_budget,
            continue_on_error=not args.fail_fast,
        )
        print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
        return 0 if not report.failed_regions else 2
    finally:
        evidence.close()
        baseline.close()
        control.close()


if __name__ == "__main__":
    raise SystemExit(main())
