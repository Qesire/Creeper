#!/usr/bin/env python3
"""Plan a non-mutating marginal-EED harvest portfolio."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.storage.control_store import ControlStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--max-regions", type=int, default=8)
    parser.add_argument("--byte-budget", type=int)
    parser.add_argument("--unknown-overlap-penalty", type=float, default=0.25)
    parser.add_argument("--confidence-floor", type=float, default=0.25)
    parser.add_argument("--min-marginal-fraction", type=float, default=0.01)
    parser.add_argument("--min-marginal-eed", type=float, default=0.0)
    parser.add_argument("--min-score-per-mib", type=float, default=0.0)
    args = parser.parse_args()

    control = ControlStore(args.control_db)
    try:
        registry = IndexSpaceRegistry(control)
        planner = RegionPortfolioPlanner(
            registry,
            policy=RegionPortfolioPolicy(
                unknown_overlap_penalty=args.unknown_overlap_penalty,
                confidence_floor=args.confidence_floor,
                min_marginal_fraction=args.min_marginal_fraction,
                min_marginal_eed=args.min_marginal_eed,
                min_score_per_mib=args.min_score_per_mib,
            ),
        )
        plan = planner.plan(
            max_regions=args.max_regions,
            byte_budget=args.byte_budget,
        )
        payload = {
            "candidate_regions_considered": plan.candidate_regions_considered,
            "harvested_reference_regions": plan.harvested_reference_regions,
            "total_harvest_bytes": plan.total_harvest_bytes,
            "total_marginal_eed": plan.total_marginal_eed,
            "selections": [
                {
                    "region_key": item.region.region_key,
                    "index_key": item.region.index_key,
                    "locator": item.region.locator,
                    "state": item.region.state.value,
                    "estimated_harvest_bytes": item.estimated_harvest_bytes,
                    "estimated_total_novel_items": (
                        item.estimated_total_novel_items
                    ),
                    "estimated_total_novel_eed": (
                        item.estimated_total_novel_eed
                    ),
                    "risk_adjusted_total_eed": item.risk_adjusted_total_eed,
                    "estimated_jaccard": item.estimated_jaccard,
                    "estimated_containment": item.estimated_containment,
                    "marginal_fraction": item.marginal_fraction,
                    "marginal_eed": item.marginal_eed,
                    "marginal_eed_per_mib": item.marginal_eed_per_mib,
                    "synopsis_confidence": item.synopsis_confidence,
                }
                for item in plan.selections
            ],
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        control.close()


if __name__ == "__main__":
    raise SystemExit(main())
