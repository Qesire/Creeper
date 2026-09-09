#!/usr/bin/env python3
"""Run a bounded Arquivo.pt CDX discovery probe against the authority index."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.sources.archive.arquivo import ArquivoCDXClient, ArquivoCDXSource


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("seeds", type=Path, help="one Arquivo.pt host/domain seed per line")
    parser.add_argument("index", type=Path)
    parser.add_argument("candidate_output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--from-year", type=int, default=1996)
    parser.add_argument("--to-year", type=int, default=2001)
    parser.add_argument("--match-type", choices=("exact", "prefix", "host", "domain"), default="domain")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--limit-per-seed", type=int)
    parser.add_argument("--total-limit", type=int, default=1_000)
    args = parser.parse_args()
    if args.total_limit < 1:
        raise SystemExit("--total-limit must be positive")

    seeds = [line.strip() for line in args.seeds.read_text(encoding="utf-8").splitlines() if line.strip()]
    started = time.perf_counter()
    client = ArquivoCDXClient(limit=args.limit)
    source = ArquivoCDXSource(
        client,
        seed_urls=seeds,
        from_year=args.from_year,
        to_year=args.to_year,
        match_type=args.match_type,
    )
    records = list(
        source.enumerate(
            limit_per_seed=args.limit_per_seed,
            total_limit=args.total_limit,
        )
    )
    observations = []
    seen: set[str] = set()
    for record in records:
        for observation in source.extract_hosts(record):
            if observation.hostname not in seen:
                seen.add(observation.hostname)
                observations.append(observation)

    index = BaselineIndex(args.index)
    resolved = index.resolve_batch([item.hostname for item in observations])
    index.close()
    active = [item for item in observations if resolved[item.hostname][0] == 0]
    annual_overlap = len(observations) - len(active)
    candidate_overlap = sum(1 for item in observations if resolved[item.hostname][1])

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
                        "scope": item.scope.value,
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
        "report_version": "arquivo-pt-cdx-probe-v1",
        "source_policy": "arquivo-discovery-only; captures require a separate accepted-evidence gate",
        "seeds": seeds,
        "from_year": args.from_year,
        "to_year": args.to_year,
        "match_type": args.match_type,
        "row_limit_per_request": args.limit,
        "raw_capture_rows": len(records),
        "unique_hostnames": len(observations),
        "annual_authority_overlap": annual_overlap,
        "official_candidate_overlap": candidate_overlap,
        "potential_active_discoveries": len(active),
        "elapsed_seconds": round(elapsed, 6),
        "candidate_output": str(args.candidate_output),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

