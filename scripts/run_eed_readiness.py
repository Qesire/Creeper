#!/usr/bin/env python3
"""Build an auditable annual EED readiness report from committed result files."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from creeper.metrics.readiness import build_readiness_report


_NUMERIC_LEDGER_FIELDS = (
    "requests",
    "bytes",
    "bytes_read",
    "source_records",
    "raw_hostname_observations",
    "accepted_annual_host_years",
    "novel_eed",
    "evidence_tasks",
    "pass_tasks",
    "elapsed_seconds",
    "wall_seconds",
)


def _ledger_summary(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    records = 0
    totals = {field: 0.0 for field in _NUMERIC_LEDGER_FIELDS}
    with path.open("r", encoding="utf-8") as source:
        for line in source:
            if not line.strip():
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"{path} contains a non-object JSONL record")
            records += 1
            for field in _NUMERIC_LEDGER_FIELDS:
                value = record.get(field)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    totals[field] += float(value)
    return {"path": str(path), "records": records, "numeric_totals": totals}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--accepted-dir", type=Path, required=True)
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--baseline-eed", required=True)
    parser.add_argument("--elapsed-seconds", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-partition-seed", type=int, default=0)
    parser.add_argument("--code-revision")
    parser.add_argument("--source-ledger", type=Path)
    parser.add_argument("--lease-ledger", type=Path)
    parser.add_argument("--evidence-audit", type=Path)
    parser.add_argument("--queue-metrics", type=Path)
    parser.add_argument("--resource-metrics", type=Path)
    args = parser.parse_args()

    report = build_readiness_report(
        accepted_dir=args.accepted_dir,
        baseline_dir=args.baseline_dir,
        model_path=args.model,
        baseline_eed=args.baseline_eed,
        elapsed_seconds=args.elapsed_seconds,
        run_id=args.run_id,
        source_partition_seed=args.source_partition_seed,
        code_revision=args.code_revision,
    )
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "eed-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    elapsed = float(args.elapsed_seconds)
    ended = datetime.now(timezone.utc)
    started = ended - timedelta(seconds=elapsed)
    ledgers = {
        "source": _ledger_summary(args.source_ledger),
        "lease": _ledger_summary(args.lease_ledger),
        "evidence": _ledger_summary(args.evidence_audit),
        "queue": _ledger_summary(args.queue_metrics),
        "resource": _ledger_summary(args.resource_metrics),
    }
    run = {
        "run_id": args.run_id,
        "track": "annual",
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "elapsed_seconds": report["elapsed_seconds"],
        "source_partition_seed": args.source_partition_seed,
        "code_revision": args.code_revision,
        "baseline_eed": report["baseline_eed"],
        "annual_novel_eed": report["annual_novel_eed"],
        "annual_eed_per_day": report["annual_eed_per_day"],
        "five_percent_delta": report["five_percent_delta"],
        "eta_to_five_percent_days": report["eta_to_five_percent_days"],
        "ledgers": ledgers,
    }
    (output_dir / "run.json").write_text(
        json.dumps(run, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(run, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
