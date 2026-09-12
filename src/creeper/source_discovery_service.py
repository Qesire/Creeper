"""Composition root for the source-discovery control-plane process."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import signal
import time
import tomllib
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.eed import load_english_weights
from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import (
    CommandAgentSearchExecutor,
    CommandAgentSearchPolicy,
)
from creeper.source_discovery.coordinator import (
    CoordinatorBusyError,
    SourceDiscoveryCoordinator,
)
from creeper.source_discovery.curated_seeds import ensure_curated_direct_catalogs
from creeper.source_discovery.intelligence import SourceIntelligenceContextBuilder
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.scout_router import SourceScoutRouter
from creeper.source_discovery.scrapy_scout import (
    ScrapyStructuralScoutExecutor,
    ScrapyStructuralScoutPolicy,
)
from creeper.source_discovery.scrapy_sidecar import ScrapyScoutLauncher
from creeper.source_discovery.triage import HttpSourceTriageExecutor, HttpTriagePolicy
from creeper.storage.control_store import ControlStore
from creeper.runtime.http import configured_http_proxy


@dataclass(frozen=True)
class CoordinatorConfig:
    triage_parallelism: int = 4
    scout_parallelism: int = 4
    search_parallelism: int = 3
    failure_retry_seconds: float = 30.0
    search_cooldown_seconds: float = 30.0
    search_ucb_exploration: float = 0.35
    stagnation_window: int = 6


@dataclass(frozen=True)
class AgentConfig:
    command: tuple[str, ...]
    backend: str
    actor: str
    cwd: Path | None
    policy: CommandAgentSearchPolicy
    admission: SearchAdmissionPolicy


@dataclass(frozen=True)
class MeasurementConfig:
    baseline_index: Path
    eed_model: Path
    policy: MeasuredYieldScoutPolicy


@dataclass(frozen=True)
class SourceDiscoveryServiceConfig:
    runtime_data_root: Path
    scrapy_project_dir: Path
    pool: SourcePoolTargets
    coordinator: CoordinatorConfig
    triage: HttpTriagePolicy
    scrapy: ScrapyStructuralScoutPolicy
    agent: AgentConfig
    measurement: MeasurementConfig | None = None


def _table(root: dict[str, Any], name: str) -> dict[str, Any]:
    value = root.get(name, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{name}] must be a TOML table")
    return value


def _resolve_path(value: Any, *, config_path: Path, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path")
    path = Path(value)
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _optional_path(value: Any, *, config_path: Path, name: str) -> Path | None:
    if value is None:
        return None
    return _resolve_path(value, config_path=config_path, name=name)


def _resolve_agent_command(
    values: list[str],
    *,
    config_path: Path,
) -> tuple[str, ...]:
    """Resolve path-like command arguments relative to the config file.

    Executable names such as python or codex remain PATH-resolved. Relative
    script/config paths containing a path separator are made absolute against
    the TOML directory, so daemon startup does not depend on shell cwd.
    """
    resolved: list[str] = []
    for value in values:
        path = Path(value)
        if (
            not path.is_absolute()
            and ("/" in value or "\\" in value)
        ):
            value = str((config_path.parent / path).resolve())
        resolved.append(value)
    return tuple(resolved)


def _nonnegative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, *, name: str) -> int:
    value = _nonnegative_int(value, name=name)
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _nonnegative_float(value: Any, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return float(value)


def _positive_float(value: Any, *, name: str) -> float:
    value = _nonnegative_float(value, name=name)
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


def _unit_float(value: Any, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number within [0, 1]")
    value = float(value)
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be within [0, 1]")
    return value


def _strict_bool(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def load_source_discovery_config(config_path: Path) -> SourceDiscoveryServiceConfig:
    config_path = Path(config_path).resolve()
    with config_path.open("rb") as stream:
        root = tomllib.load(stream)

    runtime_data_root = _resolve_path(
        root.get("runtime_data_root"), config_path=config_path, name="runtime_data_root"
    )
    scrapy_project_dir = _resolve_path(
        root.get("scrapy_project_dir"), config_path=config_path, name="scrapy_project_dir"
    )

    pool_raw = _table(root, "pool")
    pool_defaults = SourcePoolTargets()
    pool = SourcePoolTargets(
        active_min=_nonnegative_int(pool_raw.get("active_min", pool_defaults.active_min), name="pool.active_min"),
        active_target=_nonnegative_int(pool_raw.get("active_target", pool_defaults.active_target), name="pool.active_target"),
        warm_min=_nonnegative_int(pool_raw.get("warm_min", pool_defaults.warm_min), name="pool.warm_min"),
        warm_target=_nonnegative_int(pool_raw.get("warm_target", pool_defaults.warm_target), name="pool.warm_target"),
        cold_min=_nonnegative_int(pool_raw.get("cold_min", pool_defaults.cold_min), name="pool.cold_min"),
        cold_target=_nonnegative_int(pool_raw.get("cold_target", pool_defaults.cold_target), name="pool.cold_target"),
        triage_batch=_positive_int(pool_raw.get("triage_batch", pool_defaults.triage_batch), name="pool.triage_batch"),
        scout_parallelism=_positive_int(pool_raw.get("scout_parallelism", pool_defaults.scout_parallelism), name="pool.scout_parallelism"),
        max_search_directives=_positive_int(
            pool_raw.get("max_search_directives", pool_defaults.max_search_directives),
            name="pool.max_search_directives",
        ),
    )

    coordinator_raw = _table(root, "coordinator")
    coordinator = CoordinatorConfig(
        triage_parallelism=_positive_int(coordinator_raw.get("triage_parallelism", 4), name="coordinator.triage_parallelism"),
        scout_parallelism=_positive_int(
            coordinator_raw.get("scout_parallelism", pool.scout_parallelism), name="coordinator.scout_parallelism"
        ),
        search_parallelism=_positive_int(coordinator_raw.get("search_parallelism", 3), name="coordinator.search_parallelism"),
        failure_retry_seconds=_positive_float(
            coordinator_raw.get("failure_retry_seconds", 30.0), name="coordinator.failure_retry_seconds"
        ),
        search_cooldown_seconds=_nonnegative_float(
            coordinator_raw.get("search_cooldown_seconds", 30.0), name="coordinator.search_cooldown_seconds"
        ),
        search_ucb_exploration=_nonnegative_float(
            coordinator_raw.get("search_ucb_exploration", 0.35),
            name="coordinator.search_ucb_exploration",
        ),
        stagnation_window=_positive_int(
            coordinator_raw.get("stagnation_window", 6),
            name="coordinator.stagnation_window",
        ),
    )

    triage_raw = _table(root, "triage")
    triage = HttpTriagePolicy(
        timeout_seconds=_positive_float(triage_raw.get("timeout_seconds", 10.0), name="triage.timeout_seconds")
    )

    scrapy_raw = _table(root, "scrapy")
    scrapy = ScrapyStructuralScoutPolicy(
        max_pages=_positive_int(scrapy_raw.get("max_pages", 100), name="scrapy.max_pages"),
        max_depth=_nonnegative_int(scrapy_raw.get("max_depth", 2), name="scrapy.max_depth"),
        max_seconds=_positive_int(scrapy_raw.get("max_seconds", 120), name="scrapy.max_seconds"),
        max_memory_mb=_positive_int(scrapy_raw.get("max_memory_mb", 512), name="scrapy.max_memory_mb"),
        follow_query=_strict_bool(scrapy_raw.get("follow_query", False), name="scrapy.follow_query"),
    )

    agent_raw = _table(root, "agent")
    raw_command = agent_raw.get("command")
    if not isinstance(raw_command, list) or not raw_command or any(
        not isinstance(item, str) or not item for item in raw_command
    ):
        raise ValueError("agent.command must be a non-empty TOML string array")
    backend = agent_raw.get("backend")
    actor = agent_raw.get("actor")
    if not isinstance(backend, str) or not backend.strip():
        raise ValueError("agent.backend must be a non-empty string")
    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("agent.actor must be a non-empty string")

    admission_raw = _table(root, "admission")
    admission = SearchAdmissionPolicy(
        target_year_from=_nonnegative_int(admission_raw.get("target_year_from", 1996), name="admission.target_year_from"),
        target_year_to=_nonnegative_int(admission_raw.get("target_year_to", 2001), name="admission.target_year_to"),
        min_expected_volume=_positive_int(
            admission_raw.get("min_expected_volume", 100_000),
            name="admission.min_expected_volume",
        ),
        direct_min_expected_volume=_positive_int(
            admission_raw.get("direct_min_expected_volume", 10_000),
            name="admission.direct_min_expected_volume",
        ),
        min_enumerability_prior=_unit_float(
            admission_raw.get("min_enumerability_prior", 0.5), name="admission.min_enumerability_prior"
        ),
        min_confidence=_unit_float(admission_raw.get("min_confidence", 0.35), name="admission.min_confidence"),
        require_year_bounds=_strict_bool(admission_raw.get("require_year_bounds", True), name="admission.require_year_bounds"),
    )
    agent = AgentConfig(
        command=_resolve_agent_command(
            raw_command,
            config_path=config_path,
        ),
        backend=backend,
        actor=actor,
        cwd=_optional_path(agent_raw.get("cwd"), config_path=config_path, name="agent.cwd"),
        policy=CommandAgentSearchPolicy(
            timeout_seconds=_positive_float(agent_raw.get("timeout_seconds", 120.0), name="agent.timeout_seconds"),
            termination_grace_seconds=_positive_float(
                agent_raw.get("termination_grace_seconds", 2.0), name="agent.termination_grace_seconds"
            ),
            max_response_bytes=_positive_int(
                agent_raw.get("max_response_bytes", 2 * 1024 * 1024), name="agent.max_response_bytes"
            ),
            max_returned_candidates=_positive_int(
                agent_raw.get("max_returned_candidates", 2_000), name="agent.max_returned_candidates"
            ),
            max_returned_hypotheses=_positive_int(
                agent_raw.get("max_returned_hypotheses", 128),
                name="agent.max_returned_hypotheses",
            ),
            max_motif_expansions=_positive_int(
                agent_raw.get("max_motif_expansions", 256),
                name="agent.max_motif_expansions",
            ),
        ),
        admission=admission,
    )

    measurement: MeasurementConfig | None = None
    measurement_raw = root.get("measurement")
    if measurement_raw is not None:
        if not isinstance(measurement_raw, dict):
            raise ValueError("[measurement] must be a TOML table")
        defaults = MeasuredYieldScoutPolicy()
        measurement = MeasurementConfig(
            baseline_index=_resolve_path(
                measurement_raw.get("baseline_index"), config_path=config_path, name="measurement.baseline_index"
            ),
            eed_model=_resolve_path(
                measurement_raw.get("eed_model"), config_path=config_path, name="measurement.eed_model"
            ),
            policy=MeasuredYieldScoutPolicy(
                max_download_bytes=_positive_int(
                    measurement_raw.get("max_download_bytes", defaults.max_download_bytes), name="measurement.max_download_bytes"
                ),
                max_decompressed_bytes=_positive_int(
                    measurement_raw.get("max_decompressed_bytes", defaults.max_decompressed_bytes), name="measurement.max_decompressed_bytes"
                ),
                max_records=_positive_int(
                    measurement_raw.get("max_records", defaults.max_records), name="measurement.max_records"
                ),
                max_line_bytes=_positive_int(
                    measurement_raw.get("max_line_bytes", defaults.max_line_bytes), name="measurement.max_line_bytes"
                ),
                sample_windows=_positive_int(
                    measurement_raw.get("sample_windows", defaults.sample_windows), name="measurement.sample_windows"
                ),
                min_unique_hosts=_positive_int(
                    measurement_raw.get("min_unique_hosts", defaults.min_unique_hosts), name="measurement.min_unique_hosts"
                ),
                min_novel_hosts=_positive_int(
                    measurement_raw.get("min_novel_hosts", defaults.min_novel_hosts), name="measurement.min_novel_hosts"
                ),
                min_novel_fraction=_unit_float(
                    measurement_raw.get("min_novel_fraction", defaults.min_novel_fraction), name="measurement.min_novel_fraction"
                ),
                min_novel_eed=_nonnegative_float(
                    measurement_raw.get("min_novel_eed", defaults.min_novel_eed), name="measurement.min_novel_eed"
                ),
                timeout_seconds=_positive_float(
                    measurement_raw.get("timeout_seconds", defaults.timeout_seconds), name="measurement.timeout_seconds"
                ),
                progressive_initial_bytes=_positive_int(
                    measurement_raw.get(
                        "progressive_initial_bytes",
                        defaults.progressive_initial_bytes,
                    ),
                    name="measurement.progressive_initial_bytes",
                ),
                early_accept_multiplier=_positive_float(
                    measurement_raw.get(
                        "early_accept_multiplier",
                        defaults.early_accept_multiplier,
                    ),
                    name="measurement.early_accept_multiplier",
                ),
                early_reject_unseen_fraction=_unit_float(
                    measurement_raw.get(
                        "early_reject_unseen_fraction",
                        defaults.early_reject_unseen_fraction,
                    ),
                    name="measurement.early_reject_unseen_fraction",
                ),
            ),
        )
        if not measurement.baseline_index.is_file():
            raise ValueError(f"measurement.baseline_index does not exist: {measurement.baseline_index}")
        if not measurement.eed_model.is_file():
            raise ValueError(f"measurement.eed_model does not exist: {measurement.eed_model}")

    if not scrapy_project_dir.is_dir():
        raise ValueError(f"scrapy_project_dir does not exist: {scrapy_project_dir}")
    return SourceDiscoveryServiceConfig(
        runtime_data_root=runtime_data_root,
        scrapy_project_dir=scrapy_project_dir,
        pool=pool,
        coordinator=coordinator,
        triage=triage,
        scrapy=scrapy,
        agent=agent,
        measurement=measurement,
    )


@contextmanager
def _service_lock(path: Path):
    """Hold one lifecycle lock so multiple discovery daemons cannot alternate ticks."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CoordinatorBusyError(f"source discovery service is already running: {path}") from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


@asynccontextmanager
async def _open_runtime(config: SourceDiscoveryServiceConfig):
    root = config.runtime_data_root
    root.mkdir(parents=True, exist_ok=True)
    discovery_root = root / "source-discovery"
    discovery_root.mkdir(parents=True, exist_ok=True)

    with _service_lock(discovery_root / "service.lock"):
        control = ControlStore(root / "control.sqlite3")
        baseline: BaselineIndex | None = None
        try:
            registry = SourceDiscoveryRegistry(control)
            if config.measurement is not None:
                # Curated direct-evidence catalogs are only useful when the
                # deterministic baseline/EED scout authority is configured.
                ensure_curated_direct_catalogs(registry)
            manager = SourceReservoirManager(
                registry,
                targets=config.pool,
                search_cooldown_seconds=config.coordinator.search_cooldown_seconds,
                search_ucb_exploration=config.coordinator.search_ucb_exploration,
                stagnation_window=config.coordinator.stagnation_window,
            )
            max_io = max(config.coordinator.triage_parallelism, config.coordinator.scout_parallelism)
            limits = httpx.Limits(
                max_connections=max(4, max_io * 2),
                max_keepalive_connections=max(2, max_io),
            )
            async with httpx.AsyncClient(
                limits=limits,
                headers={"User-Agent": "Creeper-source-discovery/2.2"},
                proxy=configured_http_proxy(),
                trust_env=False,
            ) as client:
                triage = HttpSourceTriageExecutor(client, policy=config.triage)
                launcher = ScrapyScoutLauncher(config.scrapy_project_dir)
                structural = ScrapyStructuralScoutExecutor(
                    launcher,
                    discovery_root / "scrapy",
                    policy=config.scrapy,
                )
                measured = None
                if config.measurement is not None:
                    baseline = BaselineIndex(config.measurement.baseline_index)
                    measured = MeasuredYieldScoutExecutor(
                        client,
                        baseline,
                        load_english_weights(config.measurement.eed_model),
                        policy=config.measurement.policy,
                    )
                scout = SourceScoutRouter(
                    structural_executor=structural,
                    measured_executor=measured,
                )
                intelligence_context = SourceIntelligenceContextBuilder(registry)
                search = CommandAgentSearchExecutor(
                    config.agent.command,
                    discovery_root / "agent-invocations",
                    backend=config.agent.backend,
                    actor=config.agent.actor,
                    cwd=config.agent.cwd,
                    policy=config.agent.policy,
                    admission_policy=config.agent.admission,
                    context_builder=intelligence_context,
                )
                coordinator = SourceDiscoveryCoordinator(
                    registry,
                    manager,
                    lock_path=discovery_root / "coordinator.lock",
                    triage_executor=triage,
                    scout_executor=scout,
                    search_executor=search,
                    triage_parallelism=config.coordinator.triage_parallelism,
                    scout_parallelism=config.coordinator.scout_parallelism,
                    search_parallelism=config.coordinator.search_parallelism,
                    failure_retry_seconds=config.coordinator.failure_retry_seconds,
                )
                yield registry, coordinator
        finally:
            if baseline is not None:
                baseline.close()
            control.close()


def _report_has_progress(report: dict[str, object]) -> bool:
    """Return whether another near-immediate pipeline tick is useful."""
    progress_fields = (
        "recovered_scouts",
        "production_exhausted",
        "activated",
        "triaged_to_scout",
        "triaged_hold",
        "triaged_rejected",
        "scouted_warm",
        "scouted_hold",
        "scouted_rejected",
        "scout_children_registered",
        "scout_edges_added",
        "search_episodes",
        "search_candidates_registered",
    )
    return any(int(report.get(name, 0)) > 0 for name in progress_fields)


async def _run_cycle(
    registry: SourceDiscoveryRegistry,
    coordinator: SourceDiscoveryCoordinator,
    *,
    cycle: int,
) -> dict[str, object]:
    started = time.perf_counter()
    report = asdict(await coordinator.run_once())
    report["cycle"] = cycle
    report["elapsed_seconds"] = max(0.0, time.perf_counter() - started)
    report["inventory"] = {state.value: count for state, count in registry.inventory().items()}
    return report


async def run_source_discovery_cycles(
    config: SourceDiscoveryServiceConfig,
    *,
    cycles: int = 1,
) -> list[dict[str, object]]:
    """Run a finite number of discovery ticks without artificial sleeps."""
    if cycles < 1:
        raise ValueError("cycles must be positive")
    reports: list[dict[str, object]] = []
    async with _open_runtime(config) as (registry, coordinator):
        for cycle in range(1, cycles + 1):
            reports.append(await _run_cycle(registry, coordinator, cycle=cycle))
    return reports


async def run_source_discovery_watch(
    config: SourceDiscoveryServiceConfig,
    *,
    emit: Callable[[dict[str, object]], None],
    busy_sleep_seconds: float = 0.1,
    idle_sleep_seconds: float = 5.0,
    max_cycles: int | None = None,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    """Run a paced long-lived discovery service with one persistent runtime."""
    if busy_sleep_seconds < 0 or idle_sleep_seconds <= 0:
        raise ValueError("watch sleep intervals must be non-negative/positive")
    if max_cycles is not None and max_cycles < 1:
        raise ValueError("max_cycles must be positive when provided")

    async with _open_runtime(config) as (registry, coordinator):
        cycle = 0
        while max_cycles is None or cycle < max_cycles:
            cycle += 1
            report = await _run_cycle(registry, coordinator, cycle=cycle)
            emit(report)
            if max_cycles is not None and cycle >= max_cycles:
                break
            delay = busy_sleep_seconds if _report_has_progress(report) else idle_sleep_seconds
            await sleep(delay)


async def _run_watch_cli(
    config: SourceDiscoveryServiceConfig,
    *,
    busy_sleep_seconds: float,
    idle_sleep_seconds: float,
) -> int:
    """Run watch mode with signal-driven asyncio cancellation.

    Cancelling the coordinator task is important because Scrapy sidecar launches
    are cancellation-aware and terminate their whole process group.
    """
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    if current is None:
        raise RuntimeError("source discovery watch requires an asyncio task")
    received: list[signal.Signals] = []
    installed: list[signal.Signals] = []

    def request_stop(signum: signal.Signals) -> None:
        if not received:
            received.append(signum)
        current.cancel()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop, signum)
            installed.append(signum)
        except (NotImplementedError, RuntimeError):
            pass

    def emit(report: dict[str, object]) -> None:
        print(
            json.dumps(report, ensure_ascii=False, separators=(",", ":")),
            flush=True,
        )

    try:
        await run_source_discovery_watch(
            config,
            emit=emit,
            busy_sleep_seconds=busy_sleep_seconds,
            idle_sleep_seconds=idle_sleep_seconds,
        )
        return 0
    except asyncio.CancelledError:
        if received and received[0] is signal.SIGINT:
            return 130
        return 0
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-source-discovery")
    parser.add_argument("config", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cycles", type=int)
    mode.add_argument("--watch", action="store_true")
    parser.add_argument("--busy-sleep-seconds", type=float, default=0.1)
    parser.add_argument("--idle-sleep-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)

    try:
        config = load_source_discovery_config(args.config)
        if args.watch:
            return asyncio.run(
                _run_watch_cli(
                    config,
                    busy_sleep_seconds=args.busy_sleep_seconds,
                    idle_sleep_seconds=args.idle_sleep_seconds,
                )
            )
        else:
            reports = asyncio.run(run_source_discovery_cycles(config, cycles=args.cycles or 1))
            print(json.dumps(reports, ensure_ascii=False, indent=2))
    except KeyboardInterrupt:
        return 130
    except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
        parser.error(f"invalid source discovery configuration: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
