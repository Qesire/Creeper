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

from creeper.runtime.readiness import (
    IncrementalReadinessReport,
    IncrementalReadinessRuntime,
)


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


def run_service(
    runtime_data_root: Path,
    *,
    baseline_index: Path,
    eed_model: Path,
    baseline_eed: str,
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
            batch_size=batch_size,
        ) as runtime:
            last_emitted: tuple[object, ...] | None = None
            while True:
                report = runtime.sync_until_current(
                    max_batches=max_batches_per_cycle
                )
                _publish(runtime, report, root)
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
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-readiness-worker")
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("--baseline-index", type=Path, required=True)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--baseline-eed", required=True)
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
