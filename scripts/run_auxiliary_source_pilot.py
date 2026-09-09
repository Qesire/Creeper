#!/usr/bin/env python3
"""Run a bounded audit of the V3 auxiliary URL lists."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from creeper.sources.auxiliary_audit import audit_v3_auxiliary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--total-limit", type=int, default=100_000)
    parser.add_argument("--limit-per-file", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=900)
    parser.add_argument("--candidate-output", type=Path)
    args = parser.parse_args()
    report = audit_v3_auxiliary(
        args.task_root,
        args.index,
        total_limit=args.total_limit,
        limit_per_file=args.limit_per_file,
        batch_size=args.batch_size,
        candidate_output=args.candidate_output,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
