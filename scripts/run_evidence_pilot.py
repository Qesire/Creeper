#!/usr/bin/env python3
"""Run a deliberately small real Wayback CDX evidence pilot.

The pilot is single-worker and bounded. It records failures as states and never
turns a timeout/rate limit into negative evidence.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.paths import find_baseline_dir
from creeper.evidence.batch import EvidenceBatchRunner
from creeper.evidence.providers.cdx import WaybackCDXClient
from creeper.records.candidates import (
    CandidateRecord,
    CandidateSourceScope,
    reconcile_active_candidates,
)
from creeper.storage.evidence_store import EvidenceStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_root", type=Path)
    parser.add_argument("index", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--year", type=int, default=1997)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--backoff", type=float, default=1.0)
    parser.add_argument("--requests-per-second", type=float, default=1.0)
    args = parser.parse_args()
    if args.limit < 1:
        raise SystemExit("--limit must be positive")
    if args.year < 1996 or args.year > 2001:
        raise SystemExit("--year must be between 1996 and 2001")
    if args.requests_per_second < 0:
        raise SystemExit("--requests-per-second cannot be negative")

    started = time.perf_counter()
    candidate_path = find_baseline_dir(args.task_root) / "candidate_pool.txt"
    records = []
    with candidate_path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            records.append(CandidateRecord(line, "official_pool", CandidateSourceScope.OFFICIAL_POOL))
            if len(records) >= args.limit:
                break

    index = BaselineIndex(args.index)
    candidates = reconcile_active_candidates(records, index)
    client = WaybackCDXClient(
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff=args.backoff,
        requests_per_second=args.requests_per_second,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    store = EvidenceStore(args.output_dir / "evidence.sqlite3")
    tasks = [(candidate.hostname, args.year) for candidate in candidates.active]
    task_keys = set(tasks)
    audit_path = args.output_dir / "cdx_audit.jsonl"
    batch = EvidenceBatchRunner(
        client,
        store=store,
        audit_path=audit_path,
        provider="wayback-cdx",
        policy_version="cdx-v1",
    )
    batch_report = batch.run(tasks)
    results = []
    if audit_path.is_file():
        with audit_path.open("r", encoding="utf-8") as audit:
            for raw in audit:
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if (record.get("hostname"), record.get("year")) in task_keys:
                    results.append(record)
    store_count = store.count()
    store.close()
    index.close()
    report = {
        "synthetic": False,
        "provider": "wayback-cdx",
        "candidate_sample": len(records),
        "active_candidates": len(candidates.active),
        "year": args.year,
        "states": batch_report.states,
        "accepted": batch_report.accepted,
        "stored_capsules": store_count,
        "pages_seen": batch_report.pages_seen,
        "records_seen": batch_report.records_seen,
        "elapsed_seconds": round(time.perf_counter() - started, 6),
        "scheduled": batch_report.scheduled,
        "executed": batch_report.executed,
        "skipped": batch_report.skipped,
        "requests": client.http_requests,
        "requests_per_second": client.http_requests / batch_report.elapsed_seconds
        if batch_report.elapsed_seconds
        else 0.0,
        "accepted_host_year_rate": batch_report.accepted / batch_report.executed
        if batch_report.executed
        else 0.0,
        "incomplete_query_count": batch_report.states.get("incomplete", 0),
        "transient_error_count": batch_report.states.get("transient_error", 0),
        "network_policy": {
            "requests_per_second": args.requests_per_second,
            "timeout_seconds": args.timeout,
            "max_retries": args.max_retries,
            "backoff_seconds": args.backoff,
        },
        "results": results,
    }
    report_path = args.output_dir / "evidence_pilot.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
