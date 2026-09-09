#!/usr/bin/env python3
"""Run bounded exact-year Wayback evidence collection for auxiliary discoveries."""

from __future__ import annotations

import argparse
from collections import Counter
from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path
import time

from creeper.authority.normalizer import normalize_official
from creeper.authority.eed import load_english_weights
from creeper.evidence.batch import EvidenceBatchRunner
from creeper.evidence.providers.cdx import WaybackCDXClient
from creeper.evidence.policies import EvidenceCapsule
from creeper.sources.auxiliary_pilot import (
    AuxiliaryCandidate,
    sample_auxiliary_candidates,
    source_years,
)
from creeper.storage.evidence_store import EvidenceStore


def _read_capsules(audit_path: Path) -> list[EvidenceCapsule]:
    capsules: list[EvidenceCapsule] = []
    seen: set[tuple[str, int, str]] = set()
    if not audit_path.is_file():
        return capsules
    with audit_path.open("r", encoding="utf-8") as audit:
        for raw in audit:
            try:
                row = json.loads(raw)
                capsule = row.get("capsule")
                if not isinstance(capsule, dict):
                    continue
                value = EvidenceCapsule(**capsule)
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                continue
            key = (value.hostname, value.year, value.payload_hash)
            if key not in seen:
                seen.add(key)
                capsules.append(value)
    return capsules


def _audit_summary(audit_path: Path) -> dict[str, object]:
    latest: dict[tuple[str, int], str] = {}
    tasks: set[tuple[str, int]] = set()
    records = 0
    if audit_path.is_file():
        with audit_path.open("r", encoding="utf-8") as audit:
            for raw in audit:
                try:
                    row = json.loads(raw)
                    task = (str(row["hostname"]), int(row["year"]))
                    tasks.add(task)
                    latest[task] = str(row["state"])
                    records += 1
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    continue
    return {
        "audit_records": records,
        "unique_tasks": len(tasks),
        "latest_states": dict(Counter(latest.values())),
    }


def _eed_summary(capsules: list[EvidenceCapsule], model_path: Path) -> dict[str, object]:
    weights = load_english_weights(model_path)
    hosts_by_year: dict[int, set[str]] = defaultdict(set)
    for capsule in capsules:
        hostname = normalize_official(capsule.hostname)
        if hostname is not None and capsule.year in range(1996, 2002):
            hosts_by_year[capsule.year].add(hostname)
    by_year: dict[str, str] = {}
    for year in sorted(hosts_by_year):
        total = Decimal("0")
        for hostname in hosts_by_year[year]:
            total += weights.get(hostname.rsplit(".", 1)[-1], Decimal("0"))
        by_year[str(year)] = format(total, "f")
    total = sum((Decimal(value) for value in by_year.values()), Decimal("0"))
    return {
        "accepted_unique_host_years": sum(len(values) for values in hosts_by_year.values()),
        "novel_eed_by_year": by_year,
        "novel_eed_total": format(total, "f"),
        "model": str(model_path.resolve()),
    }


def _write_selection(path: Path, candidates: tuple[AuxiliaryCandidate, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as output:
        for candidate in candidates:
            output.write(
                json.dumps(
                    {
                        "hostname": candidate.hostname,
                        "source_id": candidate.source_id,
                        "locator": candidate.locator,
                        "query_years": list(source_years(candidate.source_id)),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate_jsonl", type=Path)
    parser.add_argument("task_root", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--per-source", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-retries", type=int, default=0)
    parser.add_argument("--backoff", type=float, default=1.0)
    parser.add_argument("--requests-per-second", type=float, default=1.0)
    args = parser.parse_args()
    if args.per_source < 1:
        raise SystemExit("--per-source must be positive")
    if args.requests_per_second < 0:
        raise SystemExit("--requests-per-second cannot be negative")

    started = time.perf_counter()
    candidates = sample_auxiliary_candidates(
        args.candidate_jsonl, per_source=args.per_source, seed=args.seed
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    selection_path = args.output_dir / "candidate_selection.jsonl"
    _write_selection(selection_path, candidates)

    tasks = [
        (candidate.hostname, year)
        for candidate in candidates
        for year in source_years(candidate.source_id)
    ]
    client = WaybackCDXClient(
        timeout=args.timeout,
        max_retries=args.max_retries,
        backoff=args.backoff,
        requests_per_second=args.requests_per_second,
    )
    store = EvidenceStore(args.output_dir / "evidence.sqlite3")
    audit_path = args.output_dir / "cdx_audit.jsonl"
    runner = EvidenceBatchRunner(
        client,
        store=store,
        audit_path=audit_path,
        provider="wayback-cdx",
        policy_version="cdx-v1",
    )
    batch = runner.run(tasks)
    cumulative_capsules = _read_capsules(audit_path)
    cumulative_audit = _audit_summary(audit_path)
    eed = _eed_summary(
        cumulative_capsules,
        args.task_root / "equivalent_english_domain_calculator" / "q2_tld_top_langs.json",
    )
    store_count = store.count()
    store.close()
    elapsed = time.perf_counter() - started
    source_ids = sorted({candidate.source_id for candidate in candidates})
    isc_only = bool(source_ids) and all(
        source_id.startswith("isc_reference:") for source_id in source_ids
    )
    report = {
        "report_version": "auxiliary-evidence-pilot-v1",
        "synthetic": False,
        "source_kind": "isc_reference" if isc_only else "auxiliary_discovery",
        "source_policy": (
            "isc-reference-only; source year is a dated DNS observation and not automatic website evidence"
            if isc_only
            else "auxiliary-discovery-only; source year windows are query hints, not evidence"
        ),
        "candidate_jsonl": str(args.candidate_jsonl),
        "selected_candidates": len(candidates),
        "scheduled_tasks": len(tasks),
        "source_ids": source_ids,
        "source_year_schedule": {
            source_id: list(source_years(source_id))
            for source_id in source_ids
        },
        "current_run": batch.as_dict(),
        "cumulative_audit": cumulative_audit,
        "cumulative_stored_capsules": store_count,
        "cumulative_eed": eed,
        "current_run_wall_seconds": round(elapsed, 6),
        "cumulative_novel_eed_per_day_if_this_run_repeated": (
            float(Decimal(eed["novel_eed_total"]) * Decimal("86400") / Decimal(str(elapsed)))
            if elapsed
            else 0.0
        ),
        "requests": client.http_requests,
        "network_policy": {
            "requests_per_second": args.requests_per_second,
            "timeout_seconds": args.timeout,
            "max_retries": args.max_retries,
            "backoff_seconds": args.backoff,
        },
        "selection_manifest": str(selection_path),
        "audit_path": str(audit_path),
    }
    report_path = args.output_dir / "auxiliary_evidence_pilot.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
