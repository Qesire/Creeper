"""Bounded, provenance-preserving audits of V3 auxiliary URL lists."""

from __future__ import annotations

from collections import defaultdict
import json
from pathlib import Path
import time

from creeper.authority.baseline_index import BaselineIndex
from creeper.sources.local.auxiliary import V3AuxiliaryURLAdapter


def audit_v3_auxiliary(
    task_root: Path,
    index_path: Path,
    *,
    total_limit: int = 100_000,
    limit_per_file: int | None = 10_000,
    batch_size: int = 900,
    candidate_output: Path | None = None,
) -> dict:
    """Audit a bounded auxiliary-source slice against the V3 authority index.

    ``potential_active_discoveries`` means valid, unique hostnames absent from
    the annual authority. They still require historical evidence before they
    can affect an official score.
    """
    if total_limit < 1:
        raise ValueError("total_limit must be positive")
    if limit_per_file is not None and limit_per_file < 1:
        raise ValueError("limit_per_file must be positive")
    if not 1 <= batch_size <= 900:
        raise ValueError("batch_size must be between 1 and 900")

    started = time.perf_counter()
    adapter = V3AuxiliaryURLAdapter(task_root)
    index = BaselineIndex(index_path)
    output_handle = None
    if candidate_output is not None:
        candidate_output.parent.mkdir(parents=True, exist_ok=True)
        output_handle = candidate_output.open("w", encoding="utf-8")

    source_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"raw_lines": 0, "valid_lines": 0, "invalid_lines": 0}
    )
    first_observation: dict[str, object] = {}
    pending: list[str] = []
    raw_lines = valid_lines = invalid_lines = duplicate_lines = 0
    annual_overlap = candidate_overlap = potential_active = 0
    active_records = 0

    def flush() -> None:
        nonlocal annual_overlap, candidate_overlap, potential_active, active_records
        if not pending:
            return
        resolved = index.resolve_batch(pending, chunk_size=batch_size)
        for hostname in pending:
            year_mask, is_candidate = resolved[hostname]
            observation = first_observation[hostname]
            if year_mask:
                annual_overlap += 1
            if is_candidate:
                candidate_overlap += 1
            if not year_mask:
                potential_active += 1
                active_records += 1
                if output_handle is not None:
                    output_handle.write(
                        json.dumps(
                            {
                                "hostname": hostname,
                                "source_id": observation.source_id,
                                "locator": observation.locator,
                                "scope": observation.scope.value,
                                "annual_year_mask": year_mask,
                                "official_candidate": is_candidate,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
        pending.clear()

    try:
        for record in adapter.enumerate(
            limit_per_file=limit_per_file, total_limit=total_limit
        ):
            raw_lines += 1
            stats = source_stats[record.source_id]
            stats["raw_lines"] += 1
            observations = list(adapter.extract_hosts(record))
            if not observations:
                invalid_lines += 1
                stats["invalid_lines"] += 1
                continue
            valid_lines += 1
            stats["valid_lines"] += 1
            observation = observations[0]
            if observation.hostname in first_observation:
                duplicate_lines += 1
                continue
            first_observation[observation.hostname] = observation
            pending.append(observation.hostname)
            if len(pending) >= batch_size:
                flush()
        flush()
    finally:
        if output_handle is not None:
            output_handle.close()
        index.close()

    elapsed = time.perf_counter() - started
    return {
        "report_version": "auxiliary-audit-v1",
        "authority_policy": "auxiliary-discovery-only",
        "limits": {"total_limit": total_limit, "limit_per_file": limit_per_file},
        "raw_lines": raw_lines,
        "valid_hostname_lines": valid_lines,
        "invalid_lines": invalid_lines,
        "duplicate_hostname_lines": duplicate_lines,
        "unique_hostnames": len(first_observation),
        "annual_authority_overlap": annual_overlap,
        "official_candidate_overlap": candidate_overlap,
        "potential_active_discoveries": potential_active,
        "active_records_written": active_records if candidate_output else 0,
        "elapsed_seconds": round(elapsed, 6),
        "raw_lines_per_second": raw_lines / elapsed if elapsed else 0.0,
        "unique_hosts_per_second": len(first_observation) / elapsed if elapsed else 0.0,
        "source_stats": dict(sorted(source_stats.items())),
        "candidate_output": str(candidate_output) if candidate_output else None,
    }
