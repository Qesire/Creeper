#!/usr/bin/env python3
"""Build the local V3 baseline index and print basic measurements."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    started = time.perf_counter()
    index = BaselineIndex.build(
        args.task_root, args.output, batch_size=args.batch_size
    )
    elapsed = time.perf_counter() - started
    counts = index.counts()
    index.close()
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"annual_hostnames={counts['annual_hostnames']:,}")
    print(f"official_candidate_hosts={counts['candidate_hostnames']:,}")
    print(f"index={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
