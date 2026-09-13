#!/usr/bin/env python3
"""Run a bounded, offline candidate reconciliation pilot."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.paths import find_baseline_dir
from creeper.records.candidates import (
    CandidateRecord,
    CandidateSourceScope,
    reconcile_active_candidates,
)
from creeper.sources.sampling import stratified_hostnames


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--stratified", action="store_true")
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()
    if args.limit < 1:
        raise SystemExit("--limit must be positive")
    started = time.perf_counter()
    path = find_baseline_dir(args.task_root) / "candidate_pool.txt"
    sampling_started = time.perf_counter()
    sampled = (
        stratified_hostnames(path, args.limit, cache_path=args.cache)
        if args.stratified
        else None
    )
    sampling_seconds = time.perf_counter() - sampling_started
    records = []
    if sampled is not None:
        records = [
            CandidateRecord(
                item.hostname,
                "official_pool",
                CandidateSourceScope.OFFICIAL_POOL,
                source_locator=str(path),
            )
            for item in sampled
        ]
    else:
        with path.open("r", encoding="utf-8", errors="replace") as source:
            for line in source:
                records.append(
                    CandidateRecord(
                        line,
                        "official_pool",
                        CandidateSourceScope.OFFICIAL_POOL,
                        source_locator=str(path),
                    )
                )
                if len(records) >= args.limit:
                    break
    index = BaselineIndex(args.index)
    result = reconcile_active_candidates(records, index)
    index.close()
    report = {
        "sample_limit": args.limit,
        "sample_records": len(records),
        "active": len(result.active),
        "isc_reference": len(result.isc_reference),
        "excluded": len(result.excluded),
        "unparsed": len(result.unparsed),
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "scope_policy": "candidate-scope-v3",
        "stratified": args.stratified,
        "sampling_seconds": round(sampling_seconds, 6),
    }
    if sampled is not None:
        report["bucket_count"] = len({item.bucket for item in sampled})
        report["tld_count"] = len({item.tld for item in sampled})
        report["www_count"] = sum(item.www_prefix for item in sampled)
        report["depth_counts"] = {
            str(depth): sum(item.depth == depth for item in sampled)
            for depth in sorted({item.depth for item in sampled})
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
