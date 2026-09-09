"""Small dependency-free command line entry point for local runs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.eed import calculate_eed
from creeper.authority.manifest import build_manifest
from creeper.evidence.providers.cdx import WaybackCDXClient, query_year
from creeper.runtime.doctor import run_doctor


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("task_root", type=Path)
    manifest.add_argument("output", type=Path)
    manifest.add_argument("--source-archive", type=Path)

    eed = subparsers.add_parser("eed")
    eed.add_argument("input", type=Path)
    eed.add_argument("model", type=Path)

    baseline = subparsers.add_parser("build-baseline")
    baseline.add_argument("task_root", type=Path)
    baseline.add_argument("output", type=Path)
    baseline.add_argument("--batch-size", type=int, default=50_000)

    evidence = subparsers.add_parser("evidence-query")
    evidence.add_argument("hostname")
    evidence.add_argument("year", type=int)
    evidence.add_argument("--endpoint", default="https://web.archive.org/cdx/search/cdx")
    evidence.add_argument("--timeout", type=float, default=30.0)
    evidence.add_argument("--max-retries", type=int, default=3)

    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("task_root", type=Path)
    doctor.add_argument("data_root", type=Path)
    doctor.add_argument("--min-free-gb", type=float, default=20.0)

    args = parser.parse_args(argv)
    if args.command == "manifest":
        print(json.dumps(build_manifest(args.task_root, args.output, source_archive_path=args.source_archive), indent=2))
        return 0
    if args.command == "eed":
        summary, rows = calculate_eed(args.input, args.model)
        print(json.dumps({"summary": summary, "tld_breakdown": rows}, indent=2))
        return 0
    if args.command == "build-baseline":
        index = BaselineIndex.build(args.task_root, args.output, batch_size=args.batch_size)
        print(json.dumps(index.counts(), indent=2))
        index.close()
        return 0
    if args.command == "evidence-query":
        client = WaybackCDXClient(
            endpoint=args.endpoint,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
        result = query_year(args.hostname, args.year, client, provider="wayback-cdx")
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0
    if args.command == "doctor":
        report = run_doctor(
            args.task_root,
            args.data_root,
            min_free_bytes=int(args.min_free_gb * 1024**3),
        )
        payload = {**asdict(report), "ready": report.ready}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if report.ready else 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
