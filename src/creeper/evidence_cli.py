"""Long-lived EvidenceWorker service entry point.

The service keeps one HTTPX pool alive across batches and uses bounded idle
backoff instead of polling SQLite at a fixed high rate. SIGINT/SIGTERM stop new
claims and let the current bounded batch finish; hard crashes are recovered by
the durable visibility lease.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import signal

from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.evidence.worker import AsyncEvidenceWorker, EvidenceWorkerReport
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


async def run_service(
    runtime_data_root: Path,
    *,
    owner: str,
    once: bool,
    endpoint: str,
    claim_batch_size: int,
    lease_seconds: float,
    max_inflight: int,
    requests_per_second: float,
    max_connections: int,
    max_keepalive_connections: int,
    timeout: float,
    max_retries: int,
    retry_base_seconds: float,
    retry_max_seconds: float,
    poll_min_seconds: float,
    poll_max_seconds: float,
) -> EvidenceWorkerReport:
    if poll_min_seconds <= 0 or poll_max_seconds < poll_min_seconds:
        raise ValueError("invalid evidence worker poll bounds")

    runtime_data_root.mkdir(parents=True, exist_ok=True)
    control = ControlStore(runtime_data_root / "control.sqlite3")
    evidence = EvidenceStore(runtime_data_root / "evidence.sqlite3")
    total = EvidenceWorkerReport()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    if not once:
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop.set)
                installed_signals.append(signum)
            except (NotImplementedError, RuntimeError):
                pass

    try:
        async with AsyncWaybackCDXClient(
            endpoint=endpoint,
            provider="wayback",
            timeout=timeout,
            max_retries=max_retries,
            requests_per_second=requests_per_second,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        ) as provider:
            worker = AsyncEvidenceWorker(
                control_store=control,
                evidence_store=evidence,
                providers={"wayback": provider},
                owner=owner,
                claim_batch_size=claim_batch_size,
                lease_seconds=lease_seconds,
                provider_inflight={"wayback": max_inflight},
                retry_base_seconds=retry_base_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            idle_delay = poll_min_seconds
            while True:
                report = await worker.run_once()
                total = EvidenceWorkerReport(
                    claimed=total.claimed + report.claimed,
                    terminal=total.terminal + report.terminal,
                    retryable=total.retryable + report.retryable,
                    inserted_capsules=total.inserted_capsules + report.inserted_capsules,
                    unknown_provider=total.unknown_provider + report.unknown_provider,
                )
                if once:
                    return total
                if report.claimed:
                    idle_delay = poll_min_seconds
                    print(json.dumps(asdict(report), ensure_ascii=False), flush=True)
                    if stop.is_set():
                        return total
                    continue
                if stop.is_set():
                    return total
                try:
                    await asyncio.wait_for(stop.wait(), timeout=idle_delay)
                except TimeoutError:
                    idle_delay = min(poll_max_seconds, idle_delay * 2.0)
                else:
                    return total
    finally:
        for signum in installed_signals:
            loop.remove_signal_handler(signum)
        evidence.close()
        control.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-evidence-worker")
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--owner", default=f"evidence-worker-{os.getpid()}")
    parser.add_argument(
        "--endpoint",
        default="https://web.archive.org/cdx/search/cdx",
    )
    parser.add_argument("--claim-batch-size", type=int, default=16)
    parser.add_argument("--lease-seconds", type=float, default=300.0)
    parser.add_argument("--max-inflight", type=int, default=4)
    parser.add_argument("--requests-per-second", type=float, default=0.0)
    parser.add_argument("--max-connections", type=int, default=16)
    parser.add_argument("--max-keepalive-connections", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--retry-base-seconds", type=float, default=30.0)
    parser.add_argument("--retry-max-seconds", type=float, default=3600.0)
    parser.add_argument("--poll-min-seconds", type=float, default=0.25)
    parser.add_argument("--poll-max-seconds", type=float, default=10.0)
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
                max_inflight=args.max_inflight,
                requests_per_second=args.requests_per_second,
                max_connections=args.max_connections,
                max_keepalive_connections=args.max_keepalive_connections,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_base_seconds=args.retry_base_seconds,
                retry_max_seconds=args.retry_max_seconds,
                poll_min_seconds=args.poll_min_seconds,
                poll_max_seconds=args.poll_max_seconds,
            )
        )
    except ValueError as exc:
        parser.error(str(exc))
    if args.once:
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
