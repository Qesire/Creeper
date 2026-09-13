#!/usr/bin/env python3
"""Build a bounded, baseline-aware ISC reference sample for exact-year checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.paths import find_baseline_dir
from creeper.sources.reference_pilot import hash_sample_host_file_with_stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("candidate_output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--per-source", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20260909)
    args = parser.parse_args()
    if args.per_source < 1:
        raise SystemExit("--per-source must be positive")

    started = time.perf_counter()
    directory = find_baseline_dir(args.task_root) / "isc_survey_hostnames"
    sampled = []
    source_stats = {}
    for year in (1996, 1997):
        path = directory / f"{year}-ISC.txt"
        values, raw_lines, valid_lines = hash_sample_host_file_with_stats(
            path,
            source_id=f"isc_reference:{year}",
            source_year=year,
            sample_size=args.per_source,
            seed=args.seed,
        )
        sampled.extend(values)
        source_stats[str(year)] = {
            "source_file": str(path),
            "raw_lines": raw_lines,
            "valid_hostname_lines": valid_lines,
            "sampled_unique_hostnames": len(values),
        }

    index = BaselineIndex(args.index)
    resolved = index.resolve_batch([item.hostname for item in sampled])
    index.close()
    external = [item for item in sampled if resolved[item.hostname][0] == 0]
    overlap = len(sampled) - len(external)
    args.candidate_output.parent.mkdir(parents=True, exist_ok=True)
    with args.candidate_output.open("w", encoding="utf-8") as output:
        for item in external:
            year_mask, official_candidate = resolved[item.hostname]
            output.write(
                json.dumps(
                    {
                        "hostname": item.hostname,
                        "source_id": item.source_id,
                        "locator": item.locator,
                        "scope": "isc_reference",
                        "source_year": item.source_year,
                        "annual_year_mask": year_mask,
                        "official_candidate": official_candidate,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    elapsed = time.perf_counter() - started
    report = {
        "report_version": "isc-reference-sample-v1",
        "source_policy": "isc-reference-only; never active pool or official score without promotion review",
        "seed": args.seed,
        "per_source": args.per_source,
        "source_stats": source_stats,
        "sampled_unique_hostnames": len(sampled),
        "annual_baseline_overlap": overlap,
        "baseline_external_reference_candidates": len(external),
        "candidate_output": str(args.candidate_output),
        "elapsed_seconds": round(elapsed, 6),
        "hostnames_per_second": len(sampled) / elapsed if elapsed else 0.0,
        "query_years": [1996, 1997],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
