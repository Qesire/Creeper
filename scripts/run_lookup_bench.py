#!/usr/bin/env python3
"""Measure representative baseline-index lookup latency and throughput."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def _sample_hosts(task_root: Path, limit: int) -> tuple[list[str], list[str]]:
    annual = task_root / "merged260909-3"
    present: list[str] = []
    with (annual / "1996.txt").open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            value = line.strip()
            if value:
                present.append(value)
            if len(present) >= limit:
                break
    absent = [f"absent-{i}.example" for i in range(limit)]
    return present, absent


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * q))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=1_000)
    args = parser.parse_args()
    present, absent = _sample_hosts(args.task_root, args.limit)
    index = BaselineIndex(args.index)
    hosts = present + absent
    timings: list[float] = []
    annual_present = 0
    absent_hits = 0
    started = time.perf_counter()
    for hostname in hosts:
        one = time.perf_counter()
        mask = index.year_mask(hostname)
        index.is_official_candidate(hostname)
        timings.append(time.perf_counter() - one)
        if mask:
            annual_present += 1
        elif hostname.startswith("absent-"):
            absent_hits += 1
    elapsed = time.perf_counter() - started
    batch_started = time.perf_counter()
    batch_result = index.resolve_batch(hosts)
    batch_elapsed = time.perf_counter() - batch_started
    if len(batch_result) != len(set(hosts)):
        raise RuntimeError("batch resolver returned an unexpected result count")
    index.close()
    count = len(timings)
    report = {
        "lookup_count": count,
        "annual_present_count": annual_present,
        "absent_confirmed_count": absent_hits,
        "elapsed_seconds": round(elapsed, 6),
        "lookups_per_second": count / elapsed if elapsed else 0.0,
        "batch_lookup_seconds": round(batch_elapsed, 6),
        "batch_lookups_per_second": count / batch_elapsed if batch_elapsed else 0.0,
        "batch_lookup_target_per_second": 100_000,
        "batch_target_met": (count / batch_elapsed >= 100_000) if batch_elapsed else False,
        "latency_ms": {
            "median": statistics.median(timings) * 1000 if timings else 0.0,
            "p95": _percentile(timings, 0.95) * 1000,
            "max": max(timings) * 1000 if timings else 0.0,
        },
        "sample_definition": "1996 annual first-N plus generated absent hostnames; each host checks annual mask and candidate status",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
