"""Long-lived incremental submission-readiness service."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import signal
from threading import Event

from creeper.records.candidates import CandidateStatus
from creeper.runtime.readiness import (
    IncrementalReadinessReport,
    IncrementalReadinessRuntime,
)
from creeper.source_discovery.production_value import ProductionValueModel
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.storage.telemetry_store import RuntimeTelemetryStore


def _write_marker(
    report: IncrementalReadinessReport,
    path: Path,
    *,
    active: bool,
) -> None:
    if not active:
        path.unlink(missing_ok=True)
        return
    IncrementalReadinessRuntime.write_report_atomic(report, path)


def _publish(
    runtime: IncrementalReadinessRuntime,
    report: IncrementalReadinessReport,
    runtime_data_root: Path,
) -> None:
    root = runtime_data_root / "readiness"
    IncrementalReadinessRuntime.write_report_atomic(
        report,
        root / "readiness.json",
    )
    IncrementalReadinessRuntime.write_payload_atomic(
        report.baseline_reconciliation or {},
        root / "baseline_reconciliation.json",
    )
    IncrementalReadinessRuntime.write_payload_atomic(
        report.source_contribution or {},
        root / "source_contribution.json",
    )
    _write_marker(
        report,
        root / "prewarm-ready.json",
        active=report.prewarm_reached,
    )
    _write_marker(
        report,
        root / "formal-gate-ready.json",
        active=report.formal_gate_reached,
    )
    _write_marker(
        report,
        root / "submission-dispatch-ready.json",
        active=report.submission_dispatch_ready,
    )


def _publish_runtime_observability(
    runtime: IncrementalReadinessRuntime,
    report: IncrementalReadinessReport,
    telemetry: RuntimeTelemetryStore,
    runtime_data_root: Path,
) -> None:
    contribution = report.source_contribution or {}
    direct = contribution.get("direct_annual", {})
    verified = contribution.get("verified_candidate", {})
    restricted = contribution.get("other_restricted", {})
    direct_eed = float(direct.get("novel_eed", 0) or 0)
    verified_eed = float(verified.get("novel_eed", 0) or 0)
    restricted_eed = float(restricted.get("novel_eed", 0) or 0)
    total_eed = float(report.novel_eed)

    gauges: dict[str, int | float] = {
        "readiness_cursor_lag": max(
            0,
            int(report.latest_evidence_sequence) - int(report.evidence_cursor),
        ),
        "candidate_active": runtime.candidates.count(
            CandidateStatus.ACTIVE_CANDIDATE
        ),
        "candidate_resolved": runtime.candidates.count(
            CandidateStatus.ANNUAL_EVIDENCE_OBTAINED
        ),
        "candidate_unparsed": runtime.candidates.unparsed_count(),
        "direct_annual_eed": direct_eed,
        "verified_candidate_eed": verified_eed,
        "other_restricted_eed": restricted_eed,
        "direct_fraction": (
            direct_eed / total_eed if total_eed > 0 else 0.0
        ),
    }

    production_rows: list[dict[str, object]] = []
    if runtime.control is not None:
        registry = SourceDiscoveryRegistry(runtime.control)
        outcomes = registry.list_source_run_outcomes(
            baseline_signature=report.baseline_signature,
            model_signature=report.model_signature,
            closed_only=True,
        )
        gauges["closed_source_runs"] = len(outcomes)
        gauges["zero_final_source_runs"] = sum(
            1 for outcome in outcomes if outcome.final_accepted_eed <= 0.0
        )

        value_model = ProductionValueModel(registry)
        for candidate in registry.list_candidates():
            estimate = value_model.estimate(
                candidate,
                baseline_signature=report.baseline_signature,
                model_signature=report.model_signature,
            )
            if estimate.closed_runs < 1:
                continue
            production_rows.append(
                {
                    "source_key": candidate.source_key,
                    "state": candidate.state.value,
                    "score": estimate.score,
                    "expected_final_eed": estimate.expected_final_eed,
                    "expected_cost": estimate.expected_cost,
                    "recent_marginal_eed_per_second": (
                        estimate.recent_marginal_eed_per_second
                    ),
                    "recent_marginal_eed_per_request": (
                        estimate.recent_marginal_eed_per_request
                    ),
                    "closed_runs": estimate.closed_runs,
                    "zero_runs": estimate.zero_runs,
                }
            )
    else:
        gauges["closed_source_runs"] = 0
        gauges["zero_final_source_runs"] = 0

    telemetry.set_gauges(gauges)
    production_rows.sort(
        key=lambda row: (
            -float(row["recent_marginal_eed_per_second"]),
            -float(row["score"]),
            str(row["source_key"]),
        )
    )
    IncrementalReadinessRuntime.write_payload_atomic(
        {
            "baseline_signature": report.baseline_signature,
            "model_signature": report.model_signature,
            "top_final_marginal_sources": production_rows[:20],
        },
        runtime_data_root / "readiness" / "production_value.json",
    )


def run_service(
    runtime_data_root: Path,
    *,
    baseline_index: Path,
    eed_model: Path,
    baseline_eed: str | None = None,
    authority_manifest: Path | None = None,
    dispatch_threshold: str = "0.0525",
    once: bool,
    batch_size: int = 50_000,
    max_batches_per_cycle: int = 20,
    poll_seconds: float = 30.0,
    stop_event: Event | None = None,
    emit=print,
) -> IncrementalReadinessReport:
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    if max_batches_per_cycle < 1:
        raise ValueError("max_batches_per_cycle must be positive")

    root = Path(runtime_data_root)
    root.mkdir(parents=True, exist_ok=True)
    telemetry = RuntimeTelemetryStore(root / "telemetry.sqlite3")
    lock_path = root / "locks" / "readiness-worker.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"readiness worker is already running: {lock_path}"
            ) from exc

        stop = stop_event or Event()
        with IncrementalReadinessRuntime(
            root,
            baseline_index=baseline_index,
            eed_model=eed_model,
            baseline_eed=baseline_eed,
            authority_manifest=authority_manifest,
            dispatch_threshold=dispatch_threshold,
            batch_size=batch_size,
        ) as runtime:
            last_emitted: tuple[object, ...] | None = None
            while True:
                report = runtime.sync_until_current(
                    max_batches=max_batches_per_cycle
                )
                _publish(runtime, report, root)
                _publish_runtime_observability(
                    runtime,
                    report,
                    telemetry,
                    root,
                )
                signature = (
                    report.evidence_cursor,
                    report.latest_evidence_sequence,
                    report.novel_eed,
                    report.prewarm_reached,
                    report.formal_gate_reached,
                    report.baseline_signature,
                    report.model_signature,
                )
                if signature != last_emitted:
                    emit(
                        json.dumps(
                            report.as_dict(),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                    last_emitted = signature

                if once or stop.is_set():
                    return report

                # Rebase/catch-up should use CPU/SSD continuously until current.
                if report.evidence_cursor < report.latest_evidence_sequence:
                    continue
                stop.wait(poll_seconds)
    finally:
        telemetry.close()
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-readiness-worker")
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("--baseline-index", type=Path, required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--authority-manifest", type=Path)
    parser.add_argument("--baseline-eed")
    parser.add_argument("--dispatch-threshold", default="0.0525")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--max-batches-per-cycle", type=int, default=20)
    parser.add_argument("--poll-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)

    stop = Event()

    def request_stop(_signum, _frame) -> None:
        stop.set()

    if not args.once:
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, request_stop)

    try:
        report = run_service(
            args.runtime_data_root,
            baseline_index=args.baseline_index,
            eed_model=args.eed_model,
            baseline_eed=args.baseline_eed,
            authority_manifest=args.authority_manifest,
            dispatch_threshold=args.dispatch_threshold,
            once=args.once,
            batch_size=args.batch_size,
            max_batches_per_cycle=args.max_batches_per_cycle,
            poll_seconds=args.poll_seconds,
            stop_event=stop,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))

    if args.once:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
