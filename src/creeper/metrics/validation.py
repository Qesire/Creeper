"""Bounded/soak validation snapshots for competition-facing runtime performance."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import time
from typing import Callable

from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


ENGINEERING_EED_PER_DAY_TARGETS = (
    Decimal("100000"),
    Decimal("250000"),
    Decimal("500000"),
)


def _read_json(path: Path) -> dict[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON runtime artifact: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"runtime JSON artifact must be an object: {path}")
    return value


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sqlite_family_size(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (
            path,
            Path(str(path) + "-wal"),
            Path(str(path) + "-shm"),
        )
        if candidate.exists() and candidate.is_file()
    )


def _runtime_tree_bytes(root: Path) -> int:
    total = 0
    validation_root = (root / "validation").resolve()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if resolved == validation_root or validation_root in resolved.parents:
            continue
        try:
            total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def capture_runtime_snapshot(
    runtime_data_root: Path,
    *,
    clock: Callable[[], float] = time.time,
) -> dict[str, object]:
    root = Path(runtime_data_root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(f"runtime_data_root does not exist: {root}")

    telemetry_path = root / "telemetry.sqlite3"
    if telemetry_path.exists():
        with RuntimeTelemetryStore(telemetry_path) as telemetry:
            telemetry_snapshot = telemetry.snapshot()
            counters = telemetry_snapshot.counters
            gauges = telemetry_snapshot.gauges
    else:
        counters = {}
        gauges = {}

    control_path = root / "control.sqlite3"
    if control_path.exists():
        control = ControlStore(control_path)
        try:
            evidence_states = control.evidence_task_state_counts()
            reservoir_states = control.reservoir_state_counts()
            lease_states = control.work_lease_state_counts()
            evidence_attempts = int(
                control.connection.execute(
                    "SELECT COALESCE(SUM(attempt), 0) FROM evidence_tasks"
                ).fetchone()[0]
            )
        finally:
            control.close()
    else:
        evidence_states = {}
        reservoir_states = {}
        lease_states = {}
        evidence_attempts = 0

    evidence_path = root / "evidence.sqlite3"
    if evidence_path.exists():
        evidence = EvidenceStore(evidence_path)
        try:
            evidence_capsules = evidence.count()
            evidence_host_years = evidence.host_year_count()
        finally:
            evidence.close()
    else:
        evidence_capsules = 0
        evidence_host_years = 0

    readiness = _read_json(root / "readiness" / "readiness.json")
    governor = _read_json(root / "governor" / "state.json")
    state_files = {
        "control": _sqlite_family_size(control_path),
        "evidence": _sqlite_family_size(evidence_path),
        "readiness": _sqlite_family_size(root / "readiness.sqlite3"),
        "telemetry": _sqlite_family_size(telemetry_path),
    }

    return {
        "snapshot_version": "runtime-validation-snapshot-v1",
        "runtime_data_root": str(root),
        "timestamp_unix": float(clock()),
        "telemetry_counters": counters,
        "telemetry_gauges": gauges,
        "evidence_task_states": evidence_states,
        "evidence_task_attempts": evidence_attempts,
        "reservoir_states": reservoir_states,
        "work_lease_states": lease_states,
        "evidence_capsules": evidence_capsules,
        "evidence_host_years": evidence_host_years,
        "readiness": readiness,
        "governor": governor,
        "state_file_bytes": state_files,
        "tracked_state_bytes": sum(state_files.values()),
        "runtime_tree_bytes": _runtime_tree_bytes(root),
    }


def _counter_deltas(
    start: dict[str, object],
    end: dict[str, object],
) -> tuple[dict[str, int], list[str]]:
    before = start.get("telemetry_counters", {})
    after = end.get("telemetry_counters", {})
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise ValueError("validation snapshots contain invalid telemetry counters")
    names = sorted(set(before) | set(after))
    deltas: dict[str, int] = {}
    reset: list[str] = []
    for name in names:
        left = int(before.get(name, 0))
        right = int(after.get(name, 0))
        if right < left:
            reset.append(str(name))
            continue
        deltas[str(name)] = right - left
    return deltas, reset


def _readiness_is_current(snapshot: dict[str, object]) -> tuple[bool, str | None]:
    readiness = snapshot.get("readiness")
    if not isinstance(readiness, dict):
        return False, "readiness snapshot is missing"
    try:
        cursor = int(readiness["evidence_cursor"])
        latest = int(readiness["latest_evidence_sequence"])
    except (KeyError, TypeError, ValueError):
        return False, "readiness cursor metadata is missing"
    if cursor < latest:
        return (
            False,
            f"readiness is behind evidence: cursor={cursor} latest={latest}",
        )
    return True, None


def _readiness_delta(
    start: dict[str, object],
    end: dict[str, object],
) -> tuple[Decimal | None, bool, str | None]:
    before = start.get("readiness")
    after = end.get("readiness")
    if not isinstance(before, dict) or not isinstance(after, dict):
        return None, False, "readiness snapshot missing at start or finish"
    authority_changed = (
        before.get("baseline_signature") != after.get("baseline_signature")
        or before.get("model_signature") != after.get("model_signature")
    )
    if authority_changed:
        return None, True, "baseline/model authority changed during validation window"
    left = _decimal(before.get("novel_eed"))
    right = _decimal(after.get("novel_eed"))
    if left is None or right is None:
        return None, False, "readiness novel_eed is unavailable"
    delta = right - left
    if delta < 0:
        return None, False, "readiness novel_eed decreased under unchanged authority"
    return delta, False, None


def build_validation_report(
    *,
    start: dict[str, object],
    end: dict[str, object],
    resource_summary: dict[str, object] | None = None,
    label: str = "",
    target_source_records: int | None = None,
    code_revision: str | None = None,
) -> dict[str, object]:
    start_time = float(start["timestamp_unix"])
    end_time = float(end["timestamp_unix"])
    elapsed = end_time - start_time
    if elapsed <= 0:
        raise ValueError("validation finish timestamp must be after start")

    deltas, counter_resets = _counter_deltas(start, end)
    start_current, start_current_reason = _readiness_is_current(start)
    end_current, end_current_reason = _readiness_is_current(end)
    eed_delta, authority_changed, readiness_reason = _readiness_delta(start, end)
    http_requests = deltas.get("wayback_http_requests", 0)
    source_records = deltas.get("source_records", 0)
    claimed = deltas.get("evidence_claimed_tasks", 0)
    pass_results = deltas.get("evidence_pass_results", 0)
    empty_results = deltas.get("evidence_empty_exhaustive_results", 0)
    retryable = deltas.get("evidence_retryable_tasks", 0)
    throttles = deltas.get("wayback_throttle_responses", 0)
    http_429 = deltas.get("wayback_http_429", 0)
    http_5xx = deltas.get("wayback_http_5xx", 0)
    transport_errors = deltas.get("wayback_transport_errors", 0)

    valid = (
        not counter_resets
        and not authority_changed
        and start_current
        and end_current
        and eed_delta is not None
    )
    eed_per_hour = (
        None if not valid else eed_delta * Decimal("3600") / Decimal(str(elapsed))
    )
    eed_per_day = None if eed_per_hour is None else eed_per_hour * Decimal("24")
    eed_per_1000_requests = (
        None
        if not valid or http_requests <= 0
        else eed_delta * Decimal("1000") / Decimal(http_requests)
    )

    def ratio(numerator: int, denominator: int) -> str | None:
        if denominator <= 0:
            return None
        return format(Decimal(numerator) / Decimal(denominator), "f")

    storage_start = int(start.get("runtime_tree_bytes", 0))
    storage_end = int(end.get("runtime_tree_bytes", 0))
    state_start = int(start.get("tracked_state_bytes", 0))
    state_end = int(end.get("tracked_state_bytes", 0))

    target_progress = None
    target_reached = None
    if target_source_records is not None:
        if target_source_records < 1:
            raise ValueError("target_source_records must be positive")
        target_progress = format(
            Decimal(source_records) / Decimal(target_source_records),
            "f",
        )
        target_reached = source_records >= target_source_records

    engineering_targets = []
    for target in ENGINEERING_EED_PER_DAY_TARGETS:
        engineering_targets.append(
            {
                "target_eed_per_day": format(target, "f"),
                "observed_fraction": (
                    None
                    if eed_per_day is None
                    else format(eed_per_day / target, "f")
                ),
            }
        )

    return {
        "report_version": "runtime-validation-report-v1",
        "label": label,
        "code_revision": code_revision,
        "runtime_data_root": end.get("runtime_data_root"),
        "start_timestamp_unix": start_time,
        "end_timestamp_unix": end_time,
        "elapsed_seconds": elapsed,
        "valid_for_throughput": valid,
        "invalid_reasons": [
            *(
                [f"telemetry counters reset: {', '.join(counter_resets)}"]
                if counter_resets
                else []
            ),
            *(
                [f"start {start_current_reason}"]
                if start_current_reason is not None
                else []
            ),
            *(
                [f"finish {end_current_reason}"]
                if end_current_reason is not None
                else []
            ),
            *([readiness_reason] if readiness_reason else []),
        ],
        "start_readiness_current": start_current,
        "finish_readiness_current": end_current,
        "authority_changed": authority_changed,
        "counter_deltas": deltas,
        "novel_eed_delta": None if eed_delta is None else format(eed_delta, "f"),
        "novel_eed_per_hour": (
            None if eed_per_hour is None else format(eed_per_hour, "f")
        ),
        "novel_eed_per_day": (
            None if eed_per_day is None else format(eed_per_day, "f")
        ),
        "novel_eed_per_1000_provider_requests": (
            None
            if eed_per_1000_requests is None
            else format(eed_per_1000_requests, "f")
        ),
        "source_records_per_second": format(
            Decimal(source_records) / Decimal(str(elapsed)),
            "f",
        ),
        "pass_fraction_of_claimed": ratio(pass_results, claimed),
        "empty_exhaustive_fraction_of_claimed": ratio(empty_results, claimed),
        "retryable_fraction_of_claimed": ratio(retryable, claimed),
        "provider_throttle_fraction": ratio(throttles, http_requests),
        "provider_429_fraction": ratio(http_429, http_requests),
        "provider_5xx_fraction": ratio(http_5xx, http_requests),
        "provider_transport_error_fraction": ratio(transport_errors, http_requests),
        "target_source_records": target_source_records,
        "target_source_records_progress": target_progress,
        "target_source_records_reached": target_reached,
        "storage": {
            "runtime_tree_start_bytes": storage_start,
            "runtime_tree_end_bytes": storage_end,
            "runtime_tree_growth_bytes": storage_end - storage_start,
            "tracked_state_start_bytes": state_start,
            "tracked_state_end_bytes": state_end,
            "tracked_state_growth_bytes": state_end - state_start,
            "end_state_file_bytes": end.get("state_file_bytes", {}),
        },
        "resource_window": resource_summary,
        "end_backlog": {
            "evidence_task_states": end.get("evidence_task_states", {}),
            "evidence_task_attempts": end.get("evidence_task_attempts", 0),
            "reservoir_states": end.get("reservoir_states", {}),
            "work_lease_states": end.get("work_lease_states", {}),
            "evidence_capsules": end.get("evidence_capsules", 0),
            "evidence_host_years": end.get("evidence_host_years", 0),
        },
        "end_readiness": end.get("readiness"),
        "end_governor": end.get("governor"),
        "engineering_targets": engineering_targets,
    }


def start_validation_run(
    *,
    runtime_data_root: Path,
    run_dir: Path,
    label: str,
    target_source_records: int | None = None,
    code_revision: str | None = None,
    clock: Callable[[], float] = time.time,
) -> dict[str, object]:
    if not label.strip():
        raise ValueError("validation label must be non-empty")
    run_dir = Path(run_dir)
    start_path = run_dir / "start.json"
    if start_path.exists():
        raise FileExistsError(f"validation run already started: {start_path}")
    snapshot = capture_runtime_snapshot(runtime_data_root, clock=clock)
    current, reason = _readiness_is_current(snapshot)
    if not current:
        raise RuntimeError(
            "validation start requires readiness caught up to evidence: "
            + str(reason)
        )
    payload = {
        **snapshot,
        "validation_label": label,
        "target_source_records": target_source_records,
        "code_revision": code_revision,
    }
    _atomic_json(start_path, payload)
    return payload


def finish_validation_run(
    *,
    runtime_data_root: Path,
    run_dir: Path,
    code_revision: str | None = None,
    clock: Callable[[], float] = time.time,
) -> dict[str, object]:
    run_dir = Path(run_dir)
    start_path = run_dir / "start.json"
    start = _read_json(start_path)
    if start is None:
        raise FileNotFoundError(f"validation run has no start snapshot: {start_path}")
    expected_root = str(Path(runtime_data_root).resolve())
    if start.get("runtime_data_root") != expected_root:
        raise ValueError("validation runtime_data_root does not match start snapshot")

    end = capture_runtime_snapshot(runtime_data_root, clock=clock)
    _atomic_json(run_dir / "end.json", end)

    telemetry_path = Path(runtime_data_root) / "telemetry.sqlite3"
    resource_summary = None
    if telemetry_path.exists():
        with RuntimeTelemetryStore(telemetry_path) as telemetry:
            resource_summary = telemetry.resource_summary(
                start_time=float(start["timestamp_unix"]),
                end_time=float(end["timestamp_unix"]),
            )

    revision = code_revision or (
        str(start.get("code_revision"))
        if start.get("code_revision") is not None
        else None
    )
    report = build_validation_report(
        start=start,
        end=end,
        resource_summary=resource_summary,
        label=str(start.get("validation_label", "")),
        target_source_records=(
            None
            if start.get("target_source_records") is None
            else int(start["target_source_records"])
        ),
        code_revision=revision,
    )
    _atomic_json(run_dir / "report.json", report)
    return report
