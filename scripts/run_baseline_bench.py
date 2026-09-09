#!/usr/bin/env python3
"""Benchmark a fresh or resumable baseline build."""

from __future__ import annotations

import argparse
import json
import resource
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    started = time.perf_counter()
    index = BaselineIndex.build(args.task_root, args.output, batch_size=args.batch_size)
    elapsed = time.perf_counter() - started
    report = {
        "elapsed_seconds": round(elapsed, 6),
        "maxrss_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "batch_size": args.batch_size,
        "counts": index.counts(),
        "resumable": True,
    }
    index.close()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
