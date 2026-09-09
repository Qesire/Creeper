#!/usr/bin/env python3
"""Probe a bounded set of small Arquivo.pt CDXJ collections."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from urllib.request import Request, urlopen

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.normalizer import normalize_official
from creeper.sources.archive.catalog import parse_cdxj_catalog, select_bounded_entries
from creeper.sources.archive.cdxj import complete_cdxj_lines, iter_cdxj_lines


def fetch_bytes(url: str, *, max_bytes: int, timeout: float) -> tuple[bytes, int, bool, str | None]:
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
    parser.add_argument("index", type=Path)
    parser.add_argument("candidate_output", type=Path)
    parser.add_argument("report", type=Path)
    parser.add_argument("--catalog-url", default="https://arquivo.pt/datasets/cdxj/")
    parser.add_argument("--max-file-bytes", type=int, default=20_000_000)
    parser.add_argument("--max-files", type=int, default=10)
    parser.add_argument("--max-rows-per-file", type=int, default=100_000)
    parser.add_argument("--from-year", type=int, default=1996)
    parser.add_argument("--to-year", type=int, default=2001)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()
    if args.max_file_bytes < 1 or args.max_files < 1 or args.max_rows_per_file < 1:
        raise SystemExit("file, file-count and row limits must be positive")
    if args.to_year < args.from_year:
        raise SystemExit("to-year must be >= from-year")

    started = time.perf_counter()
    catalog_payload, catalog_status, _, _ = fetch_bytes(
        args.catalog_url, max_bytes=5_000_000, timeout=args.timeout
    )
    entries = parse_cdxj_catalog(
        catalog_payload.decode("utf-8", errors="replace"), base_url=args.catalog_url
    )
    selected = select_bounded_entries(
        entries, max_file_bytes=args.max_file_bytes, max_files=args.max_files
    )
    allowed_years = set(range(args.from_year, args.to_year + 1))
    first_observation: dict[str, object] = {}
    source_reports = []
    total_capture_rows = 0

    for entry in selected:
        file_started = time.perf_counter()
        payload, status, range_supported, content_range = fetch_bytes(
            entry.url, max_bytes=args.max_file_bytes, timeout=args.timeout
        )
        lines = complete_cdxj_lines(payload)
        source_id = f"arquivo_pt_cdxj:{entry.name}"
        records = []
        for record in iter_cdxj_lines(
            lines,
            source_id=source_id,
            locator_prefix=entry.url,
            allowed_years=allowed_years,
        ):
            records.append(record)
            if len(records) >= args.max_rows_per_file:
                break
        unique_in_file: set[str] = set()
        for record in records:
            try:
                from urllib.parse import urlsplit

                raw_hostname = urlsplit(record.payload).hostname or ""
            except ValueError:
                raw_hostname = ""
            hostname = normalize_official(raw_hostname)
            if hostname:
                unique_in_file.add(hostname)
                first_observation.setdefault(hostname, record)
        total_capture_rows += len(records)
        source_reports.append(
            {
                "name": entry.name,
                "url": entry.url,
                "catalog_size_bytes": entry.size_bytes,
                "bytes_received": len(payload),
                "http_status": status,
                "range_supported": range_supported,
                "content_range": content_range,
                "complete_lines": len(lines),
                "matching_capture_rows": len(records),
                "unique_hostnames": len(unique_in_file),
                "elapsed_seconds": round(time.perf_counter() - file_started, 6),
            }
        )

    hostnames = sorted(first_observation)
    index = BaselineIndex(args.index)
    resolved = index.resolve_batch(hostnames)
    index.close()
    active = [hostname for hostname in hostnames if resolved[hostname][0] == 0]
    annual_overlap = len(hostnames) - len(active)
    candidate_overlap = sum(1 for hostname in hostnames if resolved[hostname][1])

    args.candidate_output.parent.mkdir(parents=True, exist_ok=True)
    with args.candidate_output.open("w", encoding="utf-8") as output:
        for hostname in active:
            record = first_observation[hostname]
            year_mask, official_candidate = resolved[hostname]
            output.write(
                json.dumps(
                    {
                        "hostname": hostname,
                        "source_id": (
                            f"{record.source_id}:{record.source_year}"
                            if record.source_year is not None
                            else record.source_id
                        ),
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
        "report_version": "arquivo-pt-cdxj-catalog-probe-v1",
        "source_policy": "arquivo-discovery-only; CDXJ captures require a separate accepted-evidence gate",
        "catalog_url": args.catalog_url,
        "catalog_http_status": catalog_status,
        "catalog_entries": len(entries),
        "selected_entries": len(selected),
        "max_file_bytes": args.max_file_bytes,
        "max_files": args.max_files,
        "max_rows_per_file": args.max_rows_per_file,
        "from_year": args.from_year,
        "to_year": args.to_year,
        "total_capture_rows": total_capture_rows,
        "unique_hostnames": len(hostnames),
        "annual_authority_overlap": annual_overlap,
        "official_candidate_overlap": candidate_overlap,
        "potential_active_discoveries": len(active),
        "elapsed_seconds": round(elapsed, 6),
        "candidate_output": str(args.candidate_output),
        "source_reports": source_reports,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
