"""Dedicated service for resumable platform-by-year historical enumeration.

This is intentionally a separate provider budget from the ordinary exact/range
Wayback evidence worker. It consumes only durable platform_year_harvests rows.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import time

from creeper.evidence.platform_harvest import (
    PlatformHarvestState,
    PlatformYearHarvestWorker,
    PlatformYearHarvestWorkerReport,
    platform_year_request_template_hash,
)
from creeper.evidence.platform_admission import (
    PlatformAdmissionReport,
    PlatformYearAdmission,
    PlatformYearAdmissionPolicy,
    PlatformYearObservation,
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


def _platform_authority_digest(control: ControlStore) -> str | None:
    row = control.connection.execute(
        """
        SELECT baseline_signature, model_signature
        FROM source_scout_authority
        WHERE singleton = 1
        """
    ).fetchone()
    if row is None:
        return None
    payload = json.dumps(
        {
            "baseline_signature": str(row["baseline_signature"]),
            "model_signature": str(row["model_signature"]),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def admit_platform_year_once(
    runtime_data_root: Path,
    *,
    endpoint: str,
    budget: int,
    policy_version: str = "platform-v1",
) -> PlatformAdmissionReport:
    """Admit platform work only from durable undated-host fallback evidence."""

    if budget < 1:
        raise ValueError("platform admission budget must be positive")
    root = Path(runtime_data_root)
    control = ControlStore(root / "control.sqlite3")
    try:
        authority_digest = _platform_authority_digest(control)
        if authority_digest is None:
            return PlatformAdmissionReport()
        rows = control.connection.execute(
            """
            SELECT DISTINCT
                   o.hostname, o.year, o.source_key, o.reservoir_id
            FROM evidence_host_year_origins AS o
            JOIN source_activations AS a
              ON a.source_key = o.source_key
             AND a.reservoir_id = o.reservoir_id
            WHERE a.activation_state = 'ACTIVE'
              -- Platform traversal is an optional continuation of the ordinary
              -- undated-host fallback lane only. Direct/date-bearing source
              -- evidence must never seed another Wayback traversal.
              AND o.evidence_provider = 'wayback'
              AND o.task_year_from IS NOT NULL
              AND o.task_year_to IS NOT NULL
              -- Do not recursively amplify a previous domain-amplification
              -- result; one explicit platform admission is enough.
              AND (
                  o.task_policy_version IS NULL
                  OR o.task_policy_version NOT LIKE 'cdx-domain-%'
              )
            ORDER BY o.source_key, o.reservoir_id, o.hostname, o.year
            LIMIT ?
            """,
            (max(1, int(budget) * 4),),
        ).fetchall()
        observations = []
        for row in rows:
            subject = str(row["hostname"])
            year = int(row["year"])
            observations.append(
                PlatformYearObservation(
                    provider="wayback",
                    subject=subject,
                    target_year=year,
                    request_template_hash=platform_year_request_template_hash(
                        endpoint=endpoint,
                        subject=subject,
                        target_year=year,
                        policy_version=policy_version,
                    ),
                    policy_version=policy_version,
                    source_key=str(row["source_key"]),
                    reservoir_id=str(row["reservoir_id"]),
                    authority_digest=authority_digest,
                )
            )
        return PlatformYearAdmission(
            control,
            policy=PlatformYearAdmissionPolicy(max_tasks=int(budget)),
        ).admit(observations)
    finally:
        control.close()


def run_admission_service(
    runtime_data_root: Path,
    *,
    owner: str,
    once: bool,
    endpoint: str,
    budget: int,
    poll_seconds: float,
) -> PlatformAdmissionReport:
    """Run the independent platform admission producer under its own lock."""

    if poll_seconds <= 0:
        raise ValueError("platform admission poll_seconds must be positive")
    root = Path(runtime_data_root)
    lock_path = root / "locks" / "platform-year-admission.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"platform admission producer is already running: {lock_path}"
            ) from exc
        telemetry = RuntimeTelemetryStore(root / "telemetry.sqlite3")
        total = PlatformAdmissionReport()
        try:
            while True:
                report = admit_platform_year_once(
                    root,
                    endpoint=endpoint,
                    budget=budget,
                )
                total = PlatformAdmissionReport(
                    admitted=total.admitted + report.admitted,
                    idempotent=total.idempotent + report.idempotent,
                    blocked=total.blocked + report.blocked,
                    tasks=report.tasks,
                )
                telemetry.add_counters(
                    {
                        "platform_admission_admitted": report.admitted,
                        "platform_admission_idempotent": report.idempotent,
                        "platform_admission_blocked": report.blocked,
                    }
                )
                telemetry.set_gauges(
                    {"platform_year_budget": int(budget)}
                )
                if once:
                    break
                time.sleep(poll_seconds)
        finally:
            telemetry.close()
        return total
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        finally:
            os.close(lock_fd)


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
    parser.add_argument(
        "--admission",
        action="store_true",
        help="run the durable platform-year admission producer",
    )
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
    parser.add_argument(
        "--platform-year-budget",
        "--budget",
        dest="budget",
        type=int,
        default=1,
    )
    args = parser.parse_args(argv)

    try:
        if args.admission:
            report = run_admission_service(
                args.runtime_data_root,
                owner=args.owner,
                once=args.once,
                endpoint=args.endpoint,
                budget=args.budget,
                poll_seconds=args.poll_seconds,
            )
        else:
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
