#!/usr/bin/env python3
"""Read a bounded byte prefix of an Arquivo.pt CDXJ index."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.normalizer import normalize_official
from creeper.sources.archive.cdxj import complete_cdxj_lines, iter_cdxj_lines


def fetch_prefix(url: str, *, max_bytes: int, timeout: float) -> tuple[bytes, int, bool, str | None]:
    request = Request(
        url,
        headers={
            "User-Agent": "Creeper/2.1 (research; contact administrator)",
            "Accept-Encoding": "identity",
            "Range": f"bytes=0-{max_bytes - 1}",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        body = response.read(max_bytes + 1)
        content_range = response.headers.get("Content-Range")
        return body[:max_bytes], int(response.status), bool(content_range), content_range


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("cdxj_url")
    parser.add_argument("index", type=Path)
    parser.add_argument("candidate_output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--from-year", type=int, default=1996)
    parser.add_argument("--to-year", type=int, default=2001)
    parser.add_argument("--max-bytes", type=int, default=1_048_576)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()
    if args.max_bytes < 1 or args.max_rows < 1 or args.timeout <= 0:
        raise SystemExit("max-bytes, max-rows and timeout must be positive")
    if args.to_year < args.from_year:
        raise SystemExit("to-year must be >= from-year")

    started = time.perf_counter()
    payload, status, range_supported, content_range = fetch_prefix(
        args.cdxj_url, max_bytes=args.max_bytes, timeout=args.timeout
    )
    lines = complete_cdxj_lines(payload)
    source_id = f"arquivo_pt_cdxj:{args.cdxj_url.rsplit('/', 1)[-1]}"
    allowed_years = set(range(args.from_year, args.to_year + 1))
    records = []
    for record in iter_cdxj_lines(
        lines,
        source_id=source_id,
        locator_prefix=args.cdxj_url,
        allowed_years=allowed_years,
    ):
        records.append(record)
        if len(records) >= args.max_rows:
            break

    observations = []
    seen: set[str] = set()
    for record in records:
        try:
            raw_hostname = urlsplit(record.payload).hostname or ""
        except ValueError:
            raw_hostname = ""
        hostname = normalize_official(raw_hostname)
        if hostname and hostname not in seen:
            seen.add(hostname)
            observations.append((hostname, record))

    index = BaselineIndex(args.index)
    resolved = index.resolve_batch([hostname for hostname, _ in observations])
    index.close()
    active = [item for item in observations if resolved[item[0]][0] == 0]
    annual_overlap = len(observations) - len(active)
    candidate_overlap = sum(1 for hostname, _ in observations if resolved[hostname][1])

    args.candidate_output.parent.mkdir(parents=True, exist_ok=True)
    with args.candidate_output.open("w", encoding="utf-8") as output:
        for hostname, record in active:
            year_mask, official_candidate = resolved[hostname]
            output.write(
                json.dumps(
                    {
                        "hostname": hostname,
                        "source_id": record.source_id,
                        "locator": record.locator,
                        "scope": "local_discovery",
                        "source_year": record.source_year,
                        "annual_year_mask": year_mask,
                        "official_candidate": official_candidate,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    elapsed = time.perf_counter() - started
    report = {
        "report_version": "arquivo-pt-cdxj-prefix-sample-v1",
        "source_policy": "arquivo-discovery-only; CDXJ captures require a separate accepted-evidence gate",
        "cdxj_url": args.cdxj_url,
        "from_year": args.from_year,
        "to_year": args.to_year,
        "max_bytes": args.max_bytes,
        "max_rows": args.max_rows,
        "http_status": status,
        "range_supported": range_supported,
        "content_range": content_range,
        "bytes_received": len(payload),
        "complete_prefix_lines": len(lines),
        "matching_capture_rows": len(records),
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
