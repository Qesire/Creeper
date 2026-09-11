"""Single-command supervisor for Creeper's autonomous production loop.

The supervisor intentionally keeps source discovery, source production, and
provider evidence in separate OS processes. Their durable coordination remains
SQLite/WAL + EvidenceTask state; the supervisor only owns lifecycle, restart
policy, and configuration consistency.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
from threading import Event
import time
import tomllib
from typing import Any, Callable

from creeper.source_discovery_service import load_source_discovery_config


@dataclass(frozen=True)
class SupervisorPolicy:
    poll_seconds: float = 1.0
    restart_base_seconds: float = 2.0
    restart_max_seconds: float = 60.0
    max_restarts: int = 5
    stable_reset_seconds: float = 300.0
    shutdown_grace_seconds: float = 10.0


@dataclass(frozen=True)
class EvidenceServicePolicy:
    endpoint: str = "https://web.archive.org/cdx/search/cdx"
    claim_batch_size: int = 16
    lease_seconds: float = 300.0
    max_inflight: int = 2
    requests_per_second: float = 0.5
    max_connections: int = 4
    max_keepalive_connections: int = 2
    throttle_floor_seconds: float = 2.0
    timeout: float = 30.0
    max_retries: int = 3
    retry_base_seconds: float = 30.0
    retry_max_seconds: float = 3600.0
    poll_min_seconds: float = 0.25
    poll_max_seconds: float = 10.0


@dataclass(frozen=True)
class ReadinessServicePolicy:
    eed_model: Path
    baseline_eed: str
    batch_size: int = 50_000
    max_batches_per_cycle: int = 20
    poll_seconds: float = 30.0


@dataclass(frozen=True)
class AutopilotConfig:
    source_discovery_config: Path
    source_producer_config: Path
    runtime_data_root: Path
    supervisor: SupervisorPolicy
    evidence: EvidenceServicePolicy
    baseline_index: Path | None = None
    readiness: ReadinessServicePolicy | None = None


@dataclass(frozen=True)
class ChildSpec:
    name: str
    argv: tuple[str, ...]


@dataclass
class _ChildRuntime:
    spec: ChildSpec
    process: Any | None = None
    restarts: int = 0
    next_start: float = 0.0
    started_at: float | None = None


def _resolve(value: object, *, base: Path, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path")
    path = Path(value)
    return (path if path.is_absolute() else base / path).resolve()


def _table(root: dict[str, Any], name: str) -> dict[str, Any]:
    value = root.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{name}] must be a TOML table")
    return value


def _positive_float(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _nonnegative_float(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{name} must be non-negative")
    return float(value)


def _strict_bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _decimal_string(value: object, *, name: str) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a finite non-negative decimal") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{name} must be a finite non-negative decimal")
    return format(parsed, "f")


def _positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _producer_runtime_config(config_path: Path) -> tuple[Path, Path]:
    with config_path.open("rb") as stream:
        root = tomllib.load(stream)
    if root.get("source_mode", "static") != "activated":
        raise ValueError("autopilot source producer must use source_mode='activated'")
    runtime_root = _resolve(
        root.get("runtime_data_root"),
        base=config_path.parent,
        name="source producer runtime_data_root",
    )
    baseline_index = _resolve(
        root.get("baseline_index"),
        base=config_path.parent,
        name="source producer baseline_index",
    )
    return runtime_root, baseline_index


def load_autopilot_config(config_path: Path) -> AutopilotConfig:
    config_path = Path(config_path).resolve()
    with config_path.open("rb") as stream:
        root = tomllib.load(stream)
    discovery_path = _resolve(
        root.get("source_discovery_config"),
        base=config_path.parent,
        name="source_discovery_config",
    )
    producer_path = _resolve(
        root.get("source_producer_config"),
        base=config_path.parent,
        name="source_producer_config",
    )
    discovery = load_source_discovery_config(discovery_path)
    producer_root, baseline_index = _producer_runtime_config(producer_path)
    if discovery.runtime_data_root.resolve() != producer_root.resolve():
        raise ValueError(
            "source discovery and source producer must share one runtime_data_root"
        )

    sup_raw = _table(root, "supervisor")
    sup_default = SupervisorPolicy()
    supervisor = SupervisorPolicy(
        poll_seconds=_positive_float(
            sup_raw.get("poll_seconds", sup_default.poll_seconds),
            name="supervisor.poll_seconds",
        ),
        restart_base_seconds=_positive_float(
            sup_raw.get("restart_base_seconds", sup_default.restart_base_seconds),
            name="supervisor.restart_base_seconds",
        ),
        restart_max_seconds=_positive_float(
            sup_raw.get("restart_max_seconds", sup_default.restart_max_seconds),
            name="supervisor.restart_max_seconds",
        ),
        max_restarts=_nonnegative_int(
            sup_raw.get("max_restarts", sup_default.max_restarts),
            name="supervisor.max_restarts",
        ),
        stable_reset_seconds=_nonnegative_float(
            sup_raw.get("stable_reset_seconds", sup_default.stable_reset_seconds),
            name="supervisor.stable_reset_seconds",
        ),
        shutdown_grace_seconds=_positive_float(
            sup_raw.get("shutdown_grace_seconds", sup_default.shutdown_grace_seconds),
            name="supervisor.shutdown_grace_seconds",
        ),
    )
    if supervisor.restart_max_seconds < supervisor.restart_base_seconds:
        raise ValueError(
            "supervisor.restart_max_seconds must be >= restart_base_seconds"
        )

    ev_raw = _table(root, "evidence")
    ev_default = EvidenceServicePolicy()
    endpoint = ev_raw.get("endpoint", ev_default.endpoint)
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("evidence.endpoint must be a non-empty string")
    evidence = EvidenceServicePolicy(
        endpoint=endpoint,
        claim_batch_size=_positive_int(
            ev_raw.get("claim_batch_size", ev_default.claim_batch_size),
            name="evidence.claim_batch_size",
        ),
        lease_seconds=_positive_float(
            ev_raw.get("lease_seconds", ev_default.lease_seconds),
            name="evidence.lease_seconds",
        ),
        max_inflight=_positive_int(
            ev_raw.get("max_inflight", ev_default.max_inflight),
            name="evidence.max_inflight",
        ),
        requests_per_second=_nonnegative_float(
            ev_raw.get("requests_per_second", ev_default.requests_per_second),
            name="evidence.requests_per_second",
        ),
        max_connections=_positive_int(
            ev_raw.get("max_connections", ev_default.max_connections),
            name="evidence.max_connections",
        ),
        max_keepalive_connections=_nonnegative_int(
            ev_raw.get(
                "max_keepalive_connections",
                ev_default.max_keepalive_connections,
            ),
            name="evidence.max_keepalive_connections",
        ),
        throttle_floor_seconds=_nonnegative_float(
            ev_raw.get(
                "throttle_floor_seconds",
                ev_default.throttle_floor_seconds,
            ),
            name="evidence.throttle_floor_seconds",
        ),
        timeout=_positive_float(
            ev_raw.get("timeout", ev_default.timeout),
            name="evidence.timeout",
        ),
        max_retries=_nonnegative_int(
            ev_raw.get("max_retries", ev_default.max_retries),
            name="evidence.max_retries",
        ),
        retry_base_seconds=_positive_float(
            ev_raw.get(
                "retry_base_seconds",
                ev_default.retry_base_seconds,
            ),
            name="evidence.retry_base_seconds",
        ),
        retry_max_seconds=_positive_float(
            ev_raw.get(
                "retry_max_seconds",
                ev_default.retry_max_seconds,
            ),
            name="evidence.retry_max_seconds",
        ),
        poll_min_seconds=_positive_float(
            ev_raw.get("poll_min_seconds", ev_default.poll_min_seconds),
            name="evidence.poll_min_seconds",
        ),
        poll_max_seconds=_positive_float(
            ev_raw.get("poll_max_seconds", ev_default.poll_max_seconds),
            name="evidence.poll_max_seconds",
        ),
    )
    if evidence.max_keepalive_connections > evidence.max_connections:
        raise ValueError(
            "evidence.max_keepalive_connections cannot exceed max_connections"
        )
    if evidence.retry_max_seconds < evidence.retry_base_seconds:
        raise ValueError(
            "evidence.retry_max_seconds must be >= retry_base_seconds"
        )
    if evidence.poll_max_seconds < evidence.poll_min_seconds:
        raise ValueError(
            "evidence.poll_max_seconds must be >= poll_min_seconds"
        )
    readiness = None
    readiness_raw = root.get("readiness")
    if readiness_raw is not None:
        if not isinstance(readiness_raw, dict):
            raise ValueError("[readiness] must be a TOML table")
        enabled = _strict_bool(
            readiness_raw.get("enabled", True),
            name="readiness.enabled",
        )
        if enabled:
            raw_model = readiness_raw.get("eed_model")
            if raw_model is None:
                if discovery.measurement is None:
                    raise ValueError(
                        "readiness.eed_model is required when discovery measurement "
                        "does not provide one"
                    )
                eed_model = discovery.measurement.eed_model
            else:
                eed_model = _resolve(
                    raw_model,
                    base=config_path.parent,
                    name="readiness.eed_model",
                )
            baseline_eed = _decimal_string(
                readiness_raw.get("baseline_eed"),
                name="readiness.baseline_eed",
            )
            readiness = ReadinessServicePolicy(
                eed_model=eed_model,
                baseline_eed=baseline_eed,
                batch_size=_positive_int(
                    readiness_raw.get("batch_size", 50_000),
                    name="readiness.batch_size",
                ),
                max_batches_per_cycle=_positive_int(
                    readiness_raw.get("max_batches_per_cycle", 20),
                    name="readiness.max_batches_per_cycle",
                ),
                poll_seconds=_positive_float(
                    readiness_raw.get("poll_seconds", 30.0),
                    name="readiness.poll_seconds",
                ),
            )

    return AutopilotConfig(
        source_discovery_config=discovery_path,
        source_producer_config=producer_path,
        runtime_data_root=producer_root,
        supervisor=supervisor,
        evidence=evidence,
        baseline_index=baseline_index,
        readiness=readiness,
    )


def build_child_specs(config: AutopilotConfig) -> tuple[ChildSpec, ...]:
    py = sys.executable
    evidence = config.evidence
    specs = [
        ChildSpec(
            "source-discovery",
            (
                py,
                "-m",
                "creeper.source_discovery_service",
                str(config.source_discovery_config),
                "--watch",
            ),
        ),
        ChildSpec(
            "source-producer",
            (
                py,
                "-m",
                "creeper.source_cli",
                "--watch",
                str(config.source_producer_config),
            ),
        ),
        ChildSpec(
            "evidence-worker",
            (
                py,
                "-m",
                "creeper.evidence_cli",
                str(config.runtime_data_root),
                "--claim-batch-size",
                str(evidence.claim_batch_size),
                "--lease-seconds",
                str(evidence.lease_seconds),
                "--max-inflight",
                str(evidence.max_inflight),
                "--requests-per-second",
                str(evidence.requests_per_second),
                "--max-connections",
                str(evidence.max_connections),
                "--max-keepalive-connections",
                str(evidence.max_keepalive_connections),
                "--throttle-floor-seconds",
                str(evidence.throttle_floor_seconds),
                "--timeout",
                str(evidence.timeout),
                "--max-retries",
                str(evidence.max_retries),
                "--retry-base-seconds",
                str(evidence.retry_base_seconds),
                "--retry-max-seconds",
                str(evidence.retry_max_seconds),
                "--poll-min-seconds",
                str(evidence.poll_min_seconds),
                "--poll-max-seconds",
                str(evidence.poll_max_seconds),
            ),
        ),
    ]
    if config.readiness is not None:
        if config.baseline_index is None:
            raise ValueError("readiness requires producer baseline_index")
        readiness = config.readiness
        specs.append(
            ChildSpec(
                "readiness-worker",
                (
                    py,
                    "-m",
                    "creeper.readiness_cli",
                    str(config.runtime_data_root),
                    "--baseline-index",
                    str(config.baseline_index),
                    "--eed-model",
                    str(readiness.eed_model),
                    "--baseline-eed",
                    readiness.baseline_eed,
                    "--batch-size",
                    str(readiness.batch_size),
                    "--max-batches-per-cycle",
                    str(readiness.max_batches_per_cycle),
                    "--poll-seconds",
                    str(readiness.poll_seconds),
                ),
            )
        )
    return tuple(specs)


@contextmanager
def _autopilot_lock(path: Path):
    """Prevent duplicate supervisors from launching competing producers."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"autopilot is already running for this runtime: {path}"
            ) from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _stop_child(process: Any, *, grace_seconds: float) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def run_autopilot(
    config: AutopilotConfig,
    *,
    stop_event: Event | None = None,
    popen_factory: Callable[..., Any] = subprocess.Popen,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Supervise all autonomous Creeper stages with bounded restart backoff."""
    stop_event = stop_event or Event()
    policy = config.supervisor
    children = {
        spec.name: _ChildRuntime(spec)
        for spec in build_child_specs(config)
    }
    try:
        while not stop_event.is_set():
            now = float(monotonic())
            for child in children.values():
                if child.process is None:
                    if now < child.next_start:
                        continue
                    child.process = popen_factory(child.spec.argv)
                    child.started_at = now
                    continue

                returncode = child.process.poll()
                if returncode is None:
                    continue
                ran_for = (
                    0.0
                    if child.started_at is None
                    else max(0.0, now - child.started_at)
                )
                if ran_for >= policy.stable_reset_seconds:
                    child.restarts = 0
                child.restarts += 1
                if child.restarts > policy.max_restarts:
                    raise RuntimeError(
                        f"{child.spec.name} exceeded restart budget "
                        f"after exit code {returncode}"
                    )
                delay = min(
                    policy.restart_max_seconds,
                    policy.restart_base_seconds * (2 ** (child.restarts - 1)),
                )
                child.process = None
                child.started_at = None
                child.next_start = now + delay
            sleep_fn(policy.poll_seconds)
    finally:
        for child in children.values():
            if child.process is not None:
                _stop_child(
                    child.process,
                    grace_seconds=policy.shutdown_grace_seconds,
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-autopilot")
    parser.add_argument("config", type=Path)
    args = parser.parse_args(argv)
    stop = Event()

    def request_stop(_signum, _frame) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, request_stop)
    try:
        config = load_autopilot_config(args.config)
        with _autopilot_lock(
            config.runtime_data_root / "locks" / "autopilot.lock"
        ):
            run_autopilot(config, stop_event=stop)
    except KeyboardInterrupt:
        return 130
    except (OSError, tomllib.TOMLDecodeError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
