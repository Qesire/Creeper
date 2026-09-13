"""Dedicated service for resumable platform-by-year historical enumeration.

This is intentionally a separate provider budget from the ordinary exact/range
Wayback evidence worker. It consumes only durable platform_year_harvests rows.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import signal

from creeper.evidence.platform_harvest import (
    PlatformHarvestState,
    PlatformYearHarvestWorker,
    PlatformYearHarvestWorkerReport,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


def _accumulate(
    total: PlatformYearHarvestWorkerReport,
    report: PlatformYearHarvestWorkerReport,
) -> PlatformYearHarvestWorkerReport:
    values = {
        name: int(getattr(total, name)) + int(getattr(report, name))
        for name in total.__dataclass_fields__
    }
    return PlatformYearHarvestWorkerReport(**values)


async def run_service(
    runtime_data_root: Path,
    *,
    owner: str,
    once: bool,
    endpoint: str,
    claim_batch_size: int,
    lease_seconds: float,
    requests_per_second: float,
    max_connections: int,
    max_keepalive_connections: int,
    keepalive_expiry_seconds: float,
    throttle_floor_seconds: float,
    timeout: float,
    max_retries: int,
    retry_base_seconds: float,
    retry_max_seconds: float,
    poll_seconds: float,
) -> PlatformYearHarvestWorkerReport:
    if poll_seconds <= 0:
        raise ValueError("platform harvest poll_seconds must be positive")
    runtime_data_root = Path(runtime_data_root)
    runtime_data_root.mkdir(parents=True, exist_ok=True)
    lock_path = runtime_data_root / "locks" / "platform-year-harvest.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(lock_fd)
        raise RuntimeError(
            f"platform-year harvest worker is already running: {lock_path}"
        ) from exc

    control: ControlStore | None = None
    evidence: EvidenceStore | None = None
    telemetry: RuntimeTelemetryStore | None = None
    installed_signals: list[signal.Signals] = []
    try:
        control = ControlStore(runtime_data_root / "control.sqlite3")
        evidence = EvidenceStore(runtime_data_root / "evidence.sqlite3")
        telemetry = RuntimeTelemetryStore(runtime_data_root / "telemetry.sqlite3")
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        if not once:
            for signum in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(signum, stop.set)
                    installed_signals.append(signum)
                except (NotImplementedError, RuntimeError):
                    pass

        total = PlatformYearHarvestWorkerReport()
        async with AsyncWaybackCDXClient(
            endpoint=endpoint,
            provider="wayback",
            timeout=timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry_seconds=keepalive_expiry_seconds,
            throttle_floor_seconds=throttle_floor_seconds,
        ) as provider:
            worker = PlatformYearHarvestWorker(
                control_store=control,
                evidence_store=evidence,
                providers={"wayback": provider},
                owner=owner,
                claim_batch_size=claim_batch_size,
                lease_seconds=lease_seconds,
                retry_base_seconds=retry_base_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            while True:
                report = await worker.run_once()
                total = _accumulate(total, report)
                telemetry.add_counters(
                    {
                        "platform_year_pages_committed": report.pages_committed,
                        "platform_year_complete": report.complete,
                        "platform_year_partial": report.partial,
                        "platform_year_retryable": report.retryable,
                        "platform_year_failed_invalid": report.failed_invalid,
                        "platform_year_inserted_capsules": report.inserted_capsules,
                        "platform_year_provider_requests": report.provider_requests,
                    }
                )
                state_counts = control.platform_year_harvest_state_counts()
                telemetry.set_gauges(
                    {
                        "platform_year_ready": state_counts[
                            PlatformHarvestState.READY
                        ],
                        "platform_year_running": state_counts[
                            PlatformHarvestState.RUNNING
                        ],
                        "platform_year_partial": state_counts[
                            PlatformHarvestState.PARTIAL
                        ],
                        "platform_year_retryable": state_counts[
                            PlatformHarvestState.RETRYABLE
                        ],
                        "platform_year_complete": state_counts[
                            PlatformHarvestState.COMPLETE
                        ],
                        "platform_year_failed_invalid": state_counts[
                            PlatformHarvestState.FAILED_INVALID
                        ],
                    }
                )
                if once:
                    break
                if report.claimed == 0:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                    except TimeoutError:
                        pass
                if stop.is_set():
                    break
        return total
    finally:
        if "loop" in locals():
            for signum in installed_signals:
                loop.remove_signal_handler(signum)
        if telemetry is not None:
            telemetry.close()
        if evidence is not None:
            evidence.close()
        if control is not None:
            control.close()
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-platform-harvest")
    parser.add_argument("runtime_data_root", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--watch", action="store_true")
    parser.add_argument("--owner", default=f"platform-harvest-{os.getpid()}")
    parser.add_argument(
        "--endpoint",
        default="https://web.archive.org/cdx/search/cdx",
    )
    parser.add_argument("--claim-batch-size", type=int, default=1)
    parser.add_argument("--lease-seconds", type=float, default=300.0)
    parser.add_argument("--requests-per-second", type=float, default=0.1)
    parser.add_argument("--max-connections", type=int, default=2)
    parser.add_argument("--max-keepalive-connections", type=int, default=1)
    parser.add_argument("--keepalive-expiry-seconds", type=float, default=30.0)
    parser.add_argument("--throttle-floor-seconds", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-base-seconds", type=float, default=30.0)
    parser.add_argument("--retry-max-seconds", type=float, default=3600.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    args = parser.parse_args(argv)

    try:
        report = asyncio.run(
            run_service(
                args.runtime_data_root,
                owner=args.owner,
                once=args.once,
                endpoint=args.endpoint,
                claim_batch_size=args.claim_batch_size,
                lease_seconds=args.lease_seconds,
                requests_per_second=args.requests_per_second,
                max_connections=args.max_connections,
                max_keepalive_connections=args.max_keepalive_connections,
                keepalive_expiry_seconds=args.keepalive_expiry_seconds,
                throttle_floor_seconds=args.throttle_floor_seconds,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_base_seconds=args.retry_base_seconds,
                retry_max_seconds=args.retry_max_seconds,
                poll_seconds=args.poll_seconds,
            )
        )
    except (OSError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
