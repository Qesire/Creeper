#!/usr/bin/env python3
"""Build an authority-bound baseline index and print basic measurements."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--baseline-dir", type=Path)
    parser.add_argument(
        "--authority-manifest",
        type=Path,
        required=True,
        help="immutable authority manifest matching the selected baseline",
    )
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    started = time.perf_counter()
    index = BaselineIndex.build(
        args.task_root,
        args.output,
        baseline_dir=args.baseline_dir,
        authority_manifest=args.authority_manifest,
        batch_size=args.batch_size,
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
