#!/usr/bin/env python3
"""Run one bounded adaptive tomography cycle for an activated source."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from pathlib import Path

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.eed import load_english_weights
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.region_probe import (
    RegionProbeExecutor,
    RegionProbePolicy,
)
from creeper.source_discovery.tomography import (
    RegionTomographyPlanner,
    RegionTomographyPolicy,
)
from creeper.source_discovery.tomography_service import RegionTomographyService
from creeper.storage.control_store import ControlStore


async def _run(args: argparse.Namespace) -> dict:
    control = ControlStore(args.control_db)
    baseline = BaselineIndex(args.baseline_db)
    try:
        registry = IndexSpaceRegistry(control)
        index = registry.get_index_for_source(args.source_key)
        if index is None:
            raise SystemExit(
                f"source has no activated historical index space: {args.source_key}"
            )
        weights = load_english_weights(args.eed_model)
        limits = httpx.Limits(
            max_connections=max(1, args.parallelism),
            max_keepalive_connections=max(1, args.parallelism),
        )
        async with httpx.AsyncClient(
            follow_redirects=True,
            limits=limits,
        ) as client:
            probe = RegionProbeExecutor(
                baseline,
                weights,
                client=client,
                policy=RegionProbePolicy(
                    max_sample_bytes=args.sample_bytes,
                    sample_windows=args.sample_windows,
                    timeout_seconds=args.timeout,
                    baseline_batch_size=args.baseline_batch_size,
                    minhash_width=args.minhash_width,
                ),
            )
            planner = RegionTomographyPlanner(
                registry,
                policy=RegionTomographyPolicy(
                    max_depth=args.max_depth,
                    min_child_bytes=args.min_child_bytes,
                    min_observations_to_stop=args.min_observations_to_stop,
                    zero_yield_stop_confidence=args.zero_yield_stop_confidence,
                    min_novel_fraction=args.min_novel_fraction,
                    min_novel_eed_per_mib=args.min_novel_eed_per_mib,
                    exploration_weight=args.exploration_weight,
                ),
            )
            service = RegionTomographyService(
                registry,
                probe_executor=probe,
                planner=planner,
                probe_parallelism=args.parallelism,
            )
            report = await service.run_once(
                index.index_key,
                max_probe_actions=args.max_probes,
            )
            return asdict(report)
    finally:
        baseline.close()
        control.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-db", type=Path, required=True)
    parser.add_argument("--baseline-db", type=Path, required=True)
    parser.add_argument("--source-key", required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--max-probes", type=int, default=8)
    parser.add_argument("--parallelism", type=int, default=4)
    parser.add_argument("--sample-bytes", type=int, default=512 * 1024)
    parser.add_argument("--sample-windows", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--baseline-batch-size", type=int, default=50_000)
    parser.add_argument("--minhash-width", type=int, default=64)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--min-child-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--min-observations-to-stop", type=int, default=64)
    parser.add_argument("--zero-yield-stop-confidence", type=float, default=0.01)
    parser.add_argument("--min-novel-fraction", type=float, default=0.0)
    parser.add_argument("--min-novel-eed-per-mib", type=float, default=0.0)
    parser.add_argument("--exploration-weight", type=float, default=0.25)
    args = parser.parse_args()

    report = asyncio.run(_run(args))
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
