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
import fcntl
import json
import os
from pathlib import Path
import signal

from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient
from creeper.evidence.worker import AsyncEvidenceWorker, EvidenceWorkerReport
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


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
    throttle_floor_seconds: float,
    timeout: float,
    max_retries: int,
    retry_base_seconds: float,
    retry_max_seconds: float,
    poll_min_seconds: float,
    poll_max_seconds: float,
    keepalive_expiry_seconds: float = 30.0,
) -> EvidenceWorkerReport:
    if poll_min_seconds <= 0 or poll_max_seconds < poll_min_seconds:
        raise ValueError("invalid evidence worker poll bounds")

    runtime_data_root.mkdir(parents=True, exist_ok=True)
    lock_path = runtime_data_root / "locks" / "wayback-evidence-worker.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(lock_fd)
        raise RuntimeError(
            f"evidence provider worker is already running: {lock_path}"
        ) from exc

    control: ControlStore | None = None
    evidence: EvidenceStore | None = None
    telemetry: RuntimeTelemetryStore | None = None
    installed_signals: list[signal.Signals] = []

    try:
        control = ControlStore(runtime_data_root / "control.sqlite3")
        evidence = EvidenceStore(runtime_data_root / "evidence.sqlite3")
        telemetry = RuntimeTelemetryStore(runtime_data_root / "telemetry.sqlite3")
        total = EvidenceWorkerReport()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        if not once:
            for signum in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(signum, stop.set)
                    installed_signals.append(signum)
                except (NotImplementedError, RuntimeError):
                    pass

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
            telemetry.set_gauges(
                {
                    "wayback_configured_requests_per_second": requests_per_second,
                    "wayback_max_inflight": max_inflight,
                }
            )
            idle_delay = poll_min_seconds
            previous_http_requests = 0
            previous_throttle_responses = 0
            previous_transport_errors = 0
            previous_http_429 = 0
            previous_http_503 = 0
            previous_http_5xx = 0
            previous_http_elapsed_ms = 0
            previous_cooldown_wait_ms = 0
            previous_rate_limit_wait_ms = 0
            previous_retry_backoff_wait_ms = 0
            previous_host_lock_wait_ms = 0
            previous_inflight_wait_ms = 0
            previous_claim_wait_ms = 0
            previous_request_start_segments = 0
            previous_request_start_gaps = 0
            previous_request_start_gap_ms = 0
            previous_request_start_excess_gap_ms = 0
            previous_stream_refill_claims = 0
            previous_stream_refill_tasks = 0
            previous_stream_refill_empty_claims = 0
            previous_latency_buckets = {
                name: 0
                for name in (
                    "le_2s",
                    "le_4s",
                    "le_8s",
                    "le_16s",
                    "le_30s",
                    "gt_30s",
                )
            }
            previous_gap_buckets = {
                name: 0
                for name in (
                    "le_2_5s",
                    "le_4s",
                    "le_8s",
                    "le_16s",
                    "gt_16s",
                )
            }

            def record_report(report: EvidenceWorkerReport) -> None:
                nonlocal total
                nonlocal previous_http_requests, previous_throttle_responses
                nonlocal previous_transport_errors, previous_http_429
                nonlocal previous_http_503, previous_http_5xx
                nonlocal previous_http_elapsed_ms, previous_cooldown_wait_ms
                nonlocal previous_rate_limit_wait_ms
                nonlocal previous_retry_backoff_wait_ms
                nonlocal previous_host_lock_wait_ms, previous_inflight_wait_ms
                nonlocal previous_claim_wait_ms
                nonlocal previous_request_start_segments
                nonlocal previous_request_start_gaps
                nonlocal previous_request_start_gap_ms
                nonlocal previous_request_start_excess_gap_ms
                nonlocal previous_stream_refill_claims
                nonlocal previous_stream_refill_tasks
                nonlocal previous_stream_refill_empty_claims
                nonlocal previous_latency_buckets, previous_gap_buckets

                current_http_429 = int(provider.http_status_counts.get(429, 0))
                current_http_503 = int(provider.http_status_counts.get(503, 0))
                current_http_5xx = sum(
                    int(count)
                    for status, count in provider.http_status_counts.items()
                    if 500 <= int(status) <= 599
                )
                current_latency_buckets = {
                    name: int(provider.http_latency_buckets.get(name, 0))
                    for name in previous_latency_buckets
                }
                current_gap_buckets = {
                    name: int(provider.request_start_gap_buckets.get(name, 0))
                    for name in previous_gap_buckets
                }
                telemetry.add_counters(
                    {
                        "evidence_batches_with_work": int(report.claimed > 0),
                        "evidence_claimed_tasks": report.claimed,
                        "evidence_terminal_tasks": report.terminal,
                        "evidence_retryable_tasks": report.retryable,
                        "evidence_inserted_capsules": report.inserted_capsules,
                        "evidence_pass_results": report.pass_count,
                        "evidence_empty_exhaustive_results": report.empty_exhaustive_count,
                        "evidence_decomposed_results": report.decomposed_count,
                        "evidence_invalid_results": report.invalid_count,
                        "evidence_incomplete_results": report.incomplete_count,
                        "evidence_transient_error_results": report.transient_error_count,
                        "wayback_http_requests": (
                            provider.http_requests - previous_http_requests
                        ),
                        "wayback_throttle_responses": (
                            provider.throttle_responses - previous_throttle_responses
                        ),
                        "wayback_transport_errors": (
                            provider.transport_errors - previous_transport_errors
                        ),
                        "wayback_http_429": current_http_429 - previous_http_429,
                        "wayback_http_503": current_http_503 - previous_http_503,
                        "wayback_http_5xx": current_http_5xx - previous_http_5xx,
                        "wayback_http_elapsed_ms": (
                            provider.http_elapsed_milliseconds
                            - previous_http_elapsed_ms
                        ),
                        "wayback_cooldown_wait_ms": (
                            provider.cooldown_wait_milliseconds
                            - previous_cooldown_wait_ms
                        ),
                        "wayback_rate_limit_wait_ms": (
                            provider.rate_limit_wait_milliseconds
                            - previous_rate_limit_wait_ms
                        ),
                        "wayback_retry_backoff_wait_ms": (
                            provider.retry_backoff_wait_milliseconds
                            - previous_retry_backoff_wait_ms
                        ),
                        "wayback_request_start_segments": (
                            provider.request_start_segments
                            - previous_request_start_segments
                        ),
                        "wayback_request_start_gaps": (
                            provider.request_start_gaps
                            - previous_request_start_gaps
                        ),
                        "wayback_request_start_gap_ms": (
                            provider.request_start_gap_milliseconds
                            - previous_request_start_gap_ms
                        ),
                        "wayback_request_start_excess_gap_ms": (
                            provider.request_start_excess_gap_milliseconds
                            - previous_request_start_excess_gap_ms
                        ),
                        "evidence_host_lock_wait_ms": (
                            worker.host_lock_wait_milliseconds
                            - previous_host_lock_wait_ms
                        ),
                        "evidence_provider_inflight_wait_ms": (
                            worker.provider_inflight_wait_milliseconds
                            - previous_inflight_wait_ms
                        ),
                        "evidence_claim_wait_ms": (
                            worker.claim_wait_milliseconds
                            - previous_claim_wait_ms
                        ),
                        "evidence_stream_refill_claims": (
                            worker.stream_refill_claims
                            - previous_stream_refill_claims
                        ),
                        "evidence_stream_refill_tasks": (
                            worker.stream_refill_tasks
                            - previous_stream_refill_tasks
                        ),
                        "evidence_stream_refill_empty_claims": (
                            worker.stream_refill_empty_claims
                            - previous_stream_refill_empty_claims
                        ),
                        "evidence_batches_empty": int(report.claimed == 0),
                        **{
                            f"wayback_latency_{name}": (
                                current_latency_buckets[name]
                                - previous_latency_buckets[name]
                            )
                            for name in current_latency_buckets
                        },
                        **{
                            f"wayback_request_gap_{name}": (
                                current_gap_buckets[name]
                                - previous_gap_buckets[name]
                            )
                            for name in current_gap_buckets
                        },
                    }
                )
                previous_http_requests = provider.http_requests
                previous_throttle_responses = provider.throttle_responses
                previous_transport_errors = provider.transport_errors
                previous_http_429 = current_http_429
                previous_http_503 = current_http_503
                previous_http_5xx = current_http_5xx
                previous_http_elapsed_ms = provider.http_elapsed_milliseconds
                previous_cooldown_wait_ms = provider.cooldown_wait_milliseconds
                previous_rate_limit_wait_ms = provider.rate_limit_wait_milliseconds
                previous_retry_backoff_wait_ms = provider.retry_backoff_wait_milliseconds
                previous_request_start_segments = provider.request_start_segments
                previous_request_start_gaps = provider.request_start_gaps
                previous_request_start_gap_ms = (
                    provider.request_start_gap_milliseconds
                )
                previous_request_start_excess_gap_ms = (
                    provider.request_start_excess_gap_milliseconds
                )
                previous_host_lock_wait_ms = worker.host_lock_wait_milliseconds
                previous_inflight_wait_ms = worker.provider_inflight_wait_milliseconds
                previous_claim_wait_ms = worker.claim_wait_milliseconds
                previous_stream_refill_claims = worker.stream_refill_claims
                previous_stream_refill_tasks = worker.stream_refill_tasks
                previous_stream_refill_empty_claims = (
                    worker.stream_refill_empty_claims
                )
                previous_latency_buckets = current_latency_buckets
                previous_gap_buckets = current_gap_buckets
                total = EvidenceWorkerReport(
                    claimed=total.claimed + report.claimed,
                    terminal=total.terminal + report.terminal,
                    retryable=total.retryable + report.retryable,
                    inserted_capsules=total.inserted_capsules + report.inserted_capsules,
                    unknown_provider=total.unknown_provider + report.unknown_provider,
                    pass_count=total.pass_count + report.pass_count,
                    empty_exhaustive_count=(
                        total.empty_exhaustive_count
                        + report.empty_exhaustive_count
                    ),
                    decomposed_count=(
                        total.decomposed_count + report.decomposed_count
                    ),
                    invalid_count=total.invalid_count + report.invalid_count,
                    incomplete_count=total.incomplete_count + report.incomplete_count,
                    transient_error_count=(
                        total.transient_error_count
                        + report.transient_error_count
                    ),
                    provider_http_requests_total=provider.http_requests,
                    provider_throttle_responses_total=provider.throttle_responses,
                )

            if once:
                report = await worker.run_once()
                record_report(report)
                return total

            while True:
                saw_work = False
                async for report in worker.run_streaming(stop_event=stop):
                    saw_work = True
                    record_report(report)
                    idle_delay = poll_min_seconds
                    payload = asdict(report)
                    payload["provider_http_requests_total"] = provider.http_requests
                    payload["provider_throttle_responses_total"] = (
                        provider.throttle_responses
                    )
                    print(json.dumps(payload, ensure_ascii=False), flush=True)

                if stop.is_set():
                    return total
                if saw_work:
                    continue

                telemetry.add_counters({"evidence_batches_empty": 1})
                poll_started = loop.time()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=idle_delay)
                except TimeoutError:
                    telemetry.add_counters(
                        {
                            "evidence_poll_idle_ms": max(
                                0,
                                int(round((loop.time() - poll_started) * 1000.0)),
                            )
                        }
                    )
                    idle_delay = min(poll_max_seconds, idle_delay * 2.0)
                else:
                    telemetry.add_counters(
                        {
                            "evidence_poll_idle_ms": max(
                                0,
                                int(round((loop.time() - poll_started) * 1000.0)),
                            )
                        }
                    )
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
    parser = argparse.ArgumentParser(prog="creeper-evidence-worker")
    parser.add_argument("runtime_data_root", type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--owner", default=f"evidence-worker-{os.getpid()}")
    parser.add_argument(
        "--endpoint",
        default="https://web.archive.org/cdx/search/cdx",
    )
    parser.add_argument("--claim-batch-size", type=int, default=64)
    parser.add_argument("--lease-seconds", type=float, default=300.0)
    parser.add_argument("--max-inflight", type=int, default=4)
    parser.add_argument("--requests-per-second", type=float, default=0.5)
    parser.add_argument("--max-connections", type=int, default=8)
    parser.add_argument("--max-keepalive-connections", type=int, default=4)
    parser.add_argument("--keepalive-expiry-seconds", type=float, default=30.0)
    parser.add_argument("--throttle-floor-seconds", type=float, default=2.0)
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
                throttle_floor_seconds=args.throttle_floor_seconds,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_base_seconds=args.retry_base_seconds,
                retry_max_seconds=args.retry_max_seconds,
                poll_min_seconds=args.poll_min_seconds,
                poll_max_seconds=args.poll_max_seconds,
                keepalive_expiry_seconds=args.keepalive_expiry_seconds,
            )
        )
    except (ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    if args.once:
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
