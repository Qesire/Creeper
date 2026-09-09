#!/usr/bin/env python3
"""Scan a complete auxiliary file and emit a bounded active-candidate sample."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.sources.auxiliary_pilot import (
    hash_sample_auxiliary_file_with_stats,
    source_years,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_file", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("candidate_output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--sample-size", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260909)
    args = parser.parse_args()
    if args.sample_size < 1:
        raise SystemExit("--sample-size must be positive")

    started = time.perf_counter()
    sampled, raw_lines, valid_lines = hash_sample_auxiliary_file_with_stats(
        args.source_file, sample_size=args.sample_size, seed=args.seed
    )
    index = BaselineIndex(args.index)
    resolved = index.resolve_batch([item.hostname for item in sampled])
    index.close()
    active = [
        item for item in sampled if resolved[item.hostname][0] == 0
    ]
    annual_overlap = len(sampled) - len(active)
    args.candidate_output.parent.mkdir(parents=True, exist_ok=True)
    with args.candidate_output.open("w", encoding="utf-8") as output:
        for item in active:
            year_mask, official_candidate = resolved[item.hostname]
            output.write(
                json.dumps(
                    {
                        "hostname": item.hostname,
                        "source_id": item.source_id,
                        "locator": item.locator,
                        "scope": "local_discovery",
                        "annual_year_mask": year_mask,
                        "official_candidate": official_candidate,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    elapsed = time.perf_counter() - started
    report = {
        "report_version": "auxiliary-full-source-sample-v1",
        "source_file": str(args.source_file),
        "source_id": f"v3_auxiliary:{args.source_file.stem}",
        "source_query_years": list(
            source_years(f"v3_auxiliary:{args.source_file.stem}")
        ),
        "seed": args.seed,
        "sample_size": args.sample_size,
        "raw_lines": raw_lines,
        "valid_hostname_lines": valid_lines,
        "sampled_unique_hostnames": len(sampled),
        "sample_annual_overlap": annual_overlap,
        "sample_potential_active": len(active),
        "elapsed_seconds": round(elapsed, 6),
        "raw_lines_per_second": raw_lines / elapsed if elapsed else 0.0,
        "candidate_output": str(args.candidate_output),
        "authority_policy": "auxiliary-discovery-only; annual overlap removed before evidence",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
