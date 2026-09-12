"""Autonomous optimizer for activated historical CDX/CDXJ index sources.

The service owns no new evidence authority. It compiles ACTIVE discovery
sources through SourceActivationCompiler, probes eligible index regions,
selects a marginal-EED portfolio, and commits exact evidence through the
existing EvidenceStore. Unsupported/compressed sources remain on the ordinary
source-producer path.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
import signal
import time
import tomllib
from typing import Any, Callable

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.eed import load_english_weights
from creeper.runtime.http import configured_http_proxy
from creeper.source_discovery.activation import (
    SourceActivationCompiler,
    SourceActivationError,
)
from creeper.source_discovery.harvest import (
    RegionHarvestExecutor,
    RegionHarvestPolicy,
)
from creeper.source_discovery.harvest_service import RegionHarvestService
from creeper.source_discovery.index_optimization import (
    TERMINAL_LEAF_STATES,
    index_region_optimizer_eligible,
)
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.models import SourceState
from creeper.source_discovery.portfolio import (
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.source_discovery.region_probe import (
    RegionProbeExecutor,
    RegionProbePolicy,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.tomography import (
    RegionTomographyPlanner,
    RegionTomographyPolicy,
)
from creeper.source_discovery.tomography_service import RegionTomographyService
from creeper.sources.reservoirs import ReservoirState
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore


@dataclass(frozen=True)
class HistoricalIndexOptimizerConfig:
    runtime_data_root: Path
    baseline_index: Path
    eed_model: Path
    enabled: bool = False
    max_indexes_per_cycle: int = 4
    max_probe_actions_per_index: int = 4
    probe_parallelism: int = 4
    max_harvest_regions_per_cycle: int = 4
    harvest_byte_budget: int | None = 256 * 1024 * 1024
    busy_poll_seconds: float = 0.25
    idle_poll_seconds: float = 5.0
    probe: RegionProbePolicy = RegionProbePolicy()
    tomography: RegionTomographyPolicy = RegionTomographyPolicy()
    portfolio: RegionPortfolioPolicy = RegionPortfolioPolicy()
    harvest: RegionHarvestPolicy = RegionHarvestPolicy()

    def __post_init__(self) -> None:
        if self.max_indexes_per_cycle < 1:
            raise ValueError("max_indexes_per_cycle must be positive")
        if self.max_probe_actions_per_index < 1:
            raise ValueError("max_probe_actions_per_index must be positive")
        if self.probe_parallelism < 1:
            raise ValueError("probe_parallelism must be positive")
        if self.max_harvest_regions_per_cycle < 1:
            raise ValueError("max_harvest_regions_per_cycle must be positive")
        if self.harvest_byte_budget is not None and self.harvest_byte_budget < 1:
            raise ValueError("harvest_byte_budget must be positive when supplied")
        if self.busy_poll_seconds <= 0 or self.idle_poll_seconds <= 0:
            raise ValueError("optimizer poll intervals must be positive")


@dataclass(frozen=True)
class HistoricalIndexCycleReport:
    compiled_active_sources: int
    compile_failures: int
    eligible_ready_indexes: int
    selected_probe_indexes: tuple[str, ...]
    probe_attempts: int
    probes_succeeded: int
    probes_failed: int
    probe_bytes_read: int
    probe_requests: int
    harvest_selected_regions: tuple[str, ...]
    harvest_completed_regions: tuple[str, ...]
    harvest_incomplete_regions: tuple[str, ...]
    harvest_failed_regions: tuple[str, ...]
    harvest_bytes_read: int
    harvest_requests: int
    direct_capsules_inserted: int
    exhausted_reservoirs: int
    recovered_expired_claims: int
    errors: tuple[str, ...]

    @property
    def made_progress(self) -> bool:
        return any(
            (
                self.probes_succeeded,
                len(self.harvest_completed_regions),
                len(self.harvest_incomplete_regions),
                self.direct_capsules_inserted,
                self.exhausted_reservoirs,
                self.recovered_expired_claims,
            )
        )


def _resolve(value: object, *, base: Path, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path")
    path = Path(value)
    return (path if path.is_absolute() else base / path).resolve()


def _strict_bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _optional_positive_int(value: object, *, name: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, name=name)


def _positive_float(value: object, *, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
    ):
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _nonnegative_float(value: object, *, name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value < 0
    ):
        raise ValueError(f"{name} must be a non-negative number")
    return float(value)


def _unit_float(value: object, *, name: str) -> float:
    result = _nonnegative_float(value, name=name)
    if result > 1:
        raise ValueError(f"{name} must be within [0, 1]")
    return result


def load_historical_index_optimizer_config(
    producer_config_path: Path,
    *,
    eed_model: Path,
) -> HistoricalIndexOptimizerConfig:
    """Load optimizer policy from the activated source-producer TOML."""

    producer_config_path = Path(producer_config_path).resolve()
    with producer_config_path.open("rb") as stream:
        root = tomllib.load(stream)
    if root.get("source_mode", "static") != "activated":
        raise ValueError("historical index optimizer requires source_mode='activated'")

    raw = root.get("historical_index", {})
    if not isinstance(raw, dict):
        raise ValueError("[historical_index] must be a TOML table")

    runtime_root = _resolve(
        root.get("runtime_data_root"),
        base=producer_config_path.parent,
        name="runtime_data_root",
    )
    baseline = _resolve(
        root.get("baseline_index"),
        base=producer_config_path.parent,
        name="baseline_index",
    )
    model = Path(eed_model).resolve()
    if not baseline.is_file():
        raise ValueError(f"baseline_index does not exist: {baseline}")
    if not model.is_file():
        raise ValueError(f"eed_model does not exist: {model}")

    probe_default = RegionProbePolicy()
    tomography_default = RegionTomographyPolicy()
    portfolio_default = RegionPortfolioPolicy()
    harvest_default = RegionHarvestPolicy()
    byte_budget_raw = raw.get("harvest_byte_budget", 256 * 1024 * 1024)

    return HistoricalIndexOptimizerConfig(
        runtime_data_root=runtime_root,
        baseline_index=baseline,
        eed_model=model,
        enabled=_strict_bool(
            raw.get("enabled", False),
            name="historical_index.enabled",
        ),
        max_indexes_per_cycle=_positive_int(
            raw.get("max_indexes_per_cycle", 4),
            name="historical_index.max_indexes_per_cycle",
        ),
        max_probe_actions_per_index=_positive_int(
            raw.get("max_probe_actions_per_index", 4),
            name="historical_index.max_probe_actions_per_index",
        ),
        probe_parallelism=_positive_int(
            raw.get("probe_parallelism", 4),
            name="historical_index.probe_parallelism",
        ),
        max_harvest_regions_per_cycle=_positive_int(
            raw.get("max_harvest_regions_per_cycle", 4),
            name="historical_index.max_harvest_regions_per_cycle",
        ),
        harvest_byte_budget=_optional_positive_int(
            byte_budget_raw,
            name="historical_index.harvest_byte_budget",
        ),
        busy_poll_seconds=_positive_float(
            raw.get("busy_poll_seconds", 0.25),
            name="historical_index.busy_poll_seconds",
        ),
        idle_poll_seconds=_positive_float(
            raw.get("idle_poll_seconds", 5.0),
            name="historical_index.idle_poll_seconds",
        ),
        probe=RegionProbePolicy(
            max_sample_bytes=_positive_int(
                raw.get("sample_bytes", probe_default.max_sample_bytes),
                name="historical_index.sample_bytes",
            ),
            sample_windows=_positive_int(
                raw.get("sample_windows", probe_default.sample_windows),
                name="historical_index.sample_windows",
            ),
            timeout_seconds=_positive_float(
                raw.get(
                    "probe_timeout_seconds",
                    probe_default.timeout_seconds,
                ),
                name="historical_index.probe_timeout_seconds",
            ),
            baseline_batch_size=_positive_int(
                raw.get(
                    "probe_baseline_batch_size",
                    probe_default.baseline_batch_size,
                ),
                name="historical_index.probe_baseline_batch_size",
            ),
            minhash_width=_positive_int(
                raw.get("minhash_width", probe_default.minhash_width),
                name="historical_index.minhash_width",
            ),
        ),
        tomography=RegionTomographyPolicy(
            max_depth=_nonnegative_int(
                raw.get("max_depth", tomography_default.max_depth),
                name="historical_index.max_depth",
            ),
            min_child_bytes=_positive_int(
                raw.get(
                    "min_child_bytes",
                    tomography_default.min_child_bytes,
                ),
                name="historical_index.min_child_bytes",
            ),
            min_observations_to_stop=_positive_int(
                raw.get(
                    "min_observations_to_stop",
                    tomography_default.min_observations_to_stop,
                ),
                name="historical_index.min_observations_to_stop",
            ),
            zero_yield_stop_confidence=_unit_float(
                raw.get(
                    "zero_yield_stop_confidence",
                    tomography_default.zero_yield_stop_confidence,
                ),
                name="historical_index.zero_yield_stop_confidence",
            ),
            min_novel_fraction=_unit_float(
                raw.get(
                    "min_novel_fraction",
                    tomography_default.min_novel_fraction,
                ),
                name="historical_index.min_novel_fraction",
            ),
            min_novel_eed_per_mib=_nonnegative_float(
                raw.get(
                    "min_novel_eed_per_mib",
                    tomography_default.min_novel_eed_per_mib,
                ),
                name="historical_index.min_novel_eed_per_mib",
            ),
            exploration_weight=_nonnegative_float(
                raw.get(
                    "exploration_weight",
                    tomography_default.exploration_weight,
                ),
                name="historical_index.exploration_weight",
            ),
        ),
        portfolio=RegionPortfolioPolicy(
            unknown_overlap_penalty=_unit_float(
                raw.get(
                    "unknown_overlap_penalty",
                    portfolio_default.unknown_overlap_penalty,
                ),
                name="historical_index.unknown_overlap_penalty",
            ),
            confidence_floor=_unit_float(
                raw.get(
                    "confidence_floor",
                    portfolio_default.confidence_floor,
                ),
                name="historical_index.confidence_floor",
            ),
            min_marginal_fraction=_unit_float(
                raw.get(
                    "min_marginal_fraction",
                    portfolio_default.min_marginal_fraction,
                ),
                name="historical_index.min_marginal_fraction",
            ),
            min_marginal_eed=_nonnegative_float(
                raw.get(
                    "min_marginal_eed",
                    portfolio_default.min_marginal_eed,
                ),
                name="historical_index.min_marginal_eed",
            ),
            min_score_per_mib=_nonnegative_float(
                raw.get(
                    "min_score_per_mib",
                    portfolio_default.min_score_per_mib,
                ),
                name="historical_index.min_score_per_mib",
            ),
        ),
        harvest=RegionHarvestPolicy(
            max_seconds=_positive_float(
                raw.get(
                    "harvest_max_seconds",
                    harvest_default.max_seconds,
                ),
                name="historical_index.harvest_max_seconds",
            ),
            baseline_batch_size=_positive_int(
                raw.get(
                    "harvest_baseline_batch_size",
                    harvest_default.baseline_batch_size,
                ),
                name="historical_index.harvest_baseline_batch_size",
            ),
            max_records_per_lease=_positive_int(
                raw.get(
                    "harvest_max_records_per_lease",
                    harvest_default.max_records_per_lease,
                ),
                name="historical_index.harvest_max_records_per_lease",
            ),
            policy_version=str(
                raw.get(
                    "harvest_policy_version",
                    harvest_default.policy_version,
                )
            ),
            claim_grace_seconds=_nonnegative_float(
                raw.get(
                    "claim_grace_seconds",
                    harvest_default.claim_grace_seconds,
                ),
                name="historical_index.claim_grace_seconds",
            ),
            boundary_record_max_bytes=_positive_int(
                raw.get(
                    "boundary_record_max_bytes",
                    harvest_default.boundary_record_max_bytes,
                ),
                name="historical_index.boundary_record_max_bytes",
            ),
        ),
    )


class HistoricalIndexOptimizerRuntime:
    """Long-lived runtime sharing Creeper's existing SQLite authorities."""

    CHECKPOINT = "historical-index-optimizer:last-index-key"

    def __init__(
        self,
        config: HistoricalIndexOptimizerConfig,
        *,
        owner: str = "historical-index",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not owner.strip():
            raise ValueError("optimizer owner is required")
        self.config = config
        self.owner = owner
        self.control = ControlStore(config.runtime_data_root / "control.sqlite3")
        self.evidence = EvidenceStore(config.runtime_data_root / "evidence.sqlite3")
        self.telemetry = RuntimeTelemetryStore(
            config.runtime_data_root / "telemetry.sqlite3"
        )
        self.baseline = BaselineIndex(config.baseline_index)
        self.discovery = SourceDiscoveryRegistry(self.control)
        self.compiler = SourceActivationCompiler(
            self.control,
            registry=self.discovery,
        )
        self.index_registry = IndexSpaceRegistry(self.control)
        self.weights = load_english_weights(config.eed_model)
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=max(2, config.probe_parallelism * 2),
                max_keepalive_connections=max(1, config.probe_parallelism),
            ),
            headers={"User-Agent": "Creeper-historical-index/2.2"},
            proxy=configured_http_proxy(),
            trust_env=False,
        )
        self.probe_executor = RegionProbeExecutor(
            self.baseline,
            self.weights,
            client=self.client,
            policy=config.probe,
        )
        self.tomography_planner = RegionTomographyPlanner(
            self.index_registry,
            policy=config.tomography,
        )
        self.tomography_service = RegionTomographyService(
            self.index_registry,
            probe_executor=self.probe_executor,
            planner=self.tomography_planner,
            probe_parallelism=config.probe_parallelism,
        )
        self.portfolio_planner = RegionPortfolioPlanner(
            self.index_registry,
            policy=config.portfolio,
        )
        self.harvest_executor = RegionHarvestExecutor(
            registry=self.index_registry,
            baseline=self.baseline,
            evidence_store=self.evidence,
            owner=owner,
            policy=config.harvest,
        )
        self.harvest_service = RegionHarvestService(
            self.index_registry,
            portfolio_planner=self.portfolio_planner,
            harvest_executor=self.harvest_executor,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()
        self.telemetry.close()
        self.evidence.close()
        self.baseline.close()
        self.control.close()

    async def __aenter__(self) -> "HistoricalIndexOptimizerRuntime":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def _compile_active_sources(self) -> tuple[int, list[str]]:
        compiled = 0
        errors: list[str] = []
        for candidate in sorted(
            self.discovery.list_candidates(state=SourceState.ACTIVE),
            key=lambda item: item.source_key,
        ):
            try:
                self.compiler.compile(candidate)
            except (SourceActivationError, ValueError, OSError) as exc:
                errors.append(
                    f"{candidate.source_key}: {type(exc).__name__}: {exc}"
                )
                continue
            compiled += 1
        return compiled, errors

    def _eligible_ready_indexes(self):
        active_keys = {
            candidate.source_key
            for candidate in self.discovery.list_candidates(
                state=SourceState.ACTIVE
            )
        }
        result = []
        for index in self.index_registry.list_indexes(
            direct_evidence_authority=True
        ):
            if index.source_key not in active_keys:
                continue
            if not index_region_optimizer_eligible(index):
                continue
            activation = self.control.get_activation(index.source_key)
            if activation is None:
                continue
            reservoir = self.control.get_reservoir(
                str(activation["reservoir_id"])
            )
            if reservoir is None or reservoir.state is not ReservoirState.READY:
                continue
            result.append((index, reservoir))
        result.sort(key=lambda pair: pair[0].index_key)
        return result

    def _rotate_indexes(self, pairs):
        if not pairs:
            return []
        last = self.control.get_checkpoint(self.CHECKPOINT)
        start = 0
        if last is not None:
            for pos, (index, _) in enumerate(pairs):
                if index.index_key > last:
                    start = pos
                    break
            else:
                start = 0
        ordered = pairs[start:] + pairs[:start]
        selected = ordered[: self.config.max_indexes_per_cycle]
        if selected:
            self.control.set_checkpoint(
                self.CHECKPOINT,
                selected[-1][0].index_key,
            )
        return selected

    def _finish_terminal_indexes(self, pairs) -> int:
        exhausted = 0
        for index, reservoir in pairs:
            leaves = self.index_registry.list_leaf_regions(index.index_key)
            if not leaves:
                continue
            if not all(
                leaf.state in TERMINAL_LEAF_STATES
                for leaf in leaves
            ):
                continue
            if self.control.exhaust_ready_reservoir(
                reservoir.reservoir_id
            ):
                exhausted += 1
        return exhausted

    async def run_once(self) -> HistoricalIndexCycleReport:
        cycle_started = time.perf_counter()
        compiled, compile_errors = self._compile_active_sources()
        errors = list(compile_errors)
        eligible = self._eligible_ready_indexes()
        selected = self._rotate_indexes(eligible)

        probe_attempts = probes_succeeded = probes_failed = 0
        probe_bytes = probe_requests = 0
        for index, _reservoir in selected:
            try:
                report = await self.tomography_service.run_once(
                    index.index_key,
                    max_probe_actions=self.config.max_probe_actions_per_index,
                )
            except Exception as exc:
                errors.append(
                    f"{index.index_key}: {type(exc).__name__}: {exc}"
                )
                continue
            probe_attempts += report.probe_attempts
            probes_succeeded += report.probes_succeeded
            probes_failed += report.probes_failed
            probe_bytes += report.bytes_read
            probe_requests += report.requests
            errors.extend(report.errors)

        # Re-evaluate READY ownership after probes. A legacy worker could have
        # claimed a reservoir between the initial snapshot and this point.
        harvest_pairs = self._eligible_ready_indexes()
        allowed = {index.index_key for index, _ in harvest_pairs}
        harvest = self.harvest_service.run_once(
            max_regions=self.config.max_harvest_regions_per_cycle,
            byte_budget=self.config.harvest_byte_budget,
            index_keys=allowed,
            continue_on_error=True,
        )
        errors.extend(harvest.errors)
        exhausted = self._finish_terminal_indexes(
            self._eligible_ready_indexes()
        )

        cycle = HistoricalIndexCycleReport(
            compiled_active_sources=compiled,
            compile_failures=len(compile_errors),
            eligible_ready_indexes=len(eligible),
            selected_probe_indexes=tuple(
                index.index_key for index, _ in selected
            ),
            probe_attempts=probe_attempts,
            probes_succeeded=probes_succeeded,
            probes_failed=probes_failed,
            probe_bytes_read=probe_bytes,
            probe_requests=probe_requests,
            harvest_selected_regions=harvest.selected_regions,
            harvest_completed_regions=harvest.completed_regions,
            harvest_incomplete_regions=harvest.incomplete_regions,
            harvest_failed_regions=harvest.failed_regions,
            harvest_bytes_read=harvest.bytes_read,
            harvest_requests=harvest.requests,
            direct_capsules_inserted=harvest.direct_capsules_inserted,
            exhausted_reservoirs=exhausted,
            recovered_expired_claims=harvest.recovered_expired_claims,
            errors=tuple(errors),
        )
        elapsed_ms = max(
            0,
            int(round((time.perf_counter() - cycle_started) * 1000.0)),
        )
        network_requests = cycle.probe_requests + cycle.harvest_requests
        network_bytes = cycle.probe_bytes_read + cycle.harvest_bytes_read
        self.telemetry.add_counters(
            {
                "historical_index_cycles": 1,
                "historical_index_probe_attempts": cycle.probe_attempts,
                "historical_index_probe_requests": cycle.probe_requests,
                "historical_index_probe_bytes": cycle.probe_bytes_read,
                "historical_index_probe_failures": cycle.probes_failed,
                "historical_index_harvest_requests": cycle.harvest_requests,
                "historical_index_harvest_bytes": cycle.harvest_bytes_read,
                "historical_index_harvest_failures": len(
                    cycle.harvest_failed_regions
                ),
                "historical_index_network_requests": network_requests,
                "historical_index_network_bytes": network_bytes,
                "historical_index_direct_capsules_inserted": (
                    cycle.direct_capsules_inserted
                ),
                "historical_index_exhausted_reservoirs": (
                    cycle.exhausted_reservoirs
                ),
                "historical_index_compile_failures": cycle.compile_failures,
                "historical_index_wall_milliseconds": elapsed_ms,
            }
        )
        self.telemetry.set_gauges(
            {
                "historical_index_eligible_ready_indexes": (
                    cycle.eligible_ready_indexes
                ),
            }
        )
        return cycle


class _OptimizerLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self.fd)
            self.fd = None
            raise RuntimeError(
                f"historical index optimizer is already running: {self.path}"
            ) from exc
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.fd is None:
            return
        try:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)
            self.fd = None


async def run_cycles(
    config: HistoricalIndexOptimizerConfig,
    *,
    cycles: int = 1,
    owner: str = "historical-index",
) -> list[HistoricalIndexCycleReport]:
    if cycles < 1:
        raise ValueError("cycles must be positive")
    if not config.enabled:
        return []
    reports = []
    async with HistoricalIndexOptimizerRuntime(
        config,
        owner=owner,
    ) as runtime:
        for _ in range(cycles):
            reports.append(await runtime.run_once())
    return reports


async def run_watch(
    config: HistoricalIndexOptimizerConfig,
    *,
    owner: str = "historical-index",
    emit: Callable[[dict[str, object]], None],
    stop_event: asyncio.Event | None = None,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> None:
    if not config.enabled:
        return
    stop_event = stop_event or asyncio.Event()
    async with HistoricalIndexOptimizerRuntime(
        config,
        owner=owner,
    ) as runtime:
        while not stop_event.is_set():
            started = time.perf_counter()
            report = await runtime.run_once()
            payload = asdict(report)
            payload["elapsed_seconds"] = max(
                0.0,
                time.perf_counter() - started,
            )
            emit(payload)
            if stop_event.is_set():
                break
            await sleep(
                config.busy_poll_seconds
                if report.made_progress
                else config.idle_poll_seconds
            )


async def _run_watch_cli(
    config: HistoricalIndexOptimizerConfig,
    *,
    owner: str,
) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        stop.set()

    installed = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop)
            installed.append(signum)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        await run_watch(
            config,
            owner=owner,
            stop_event=stop,
            emit=lambda payload: print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                flush=True,
            ),
        )
        return 0
    finally:
        for signum in installed:
            loop.remove_signal_handler(signum)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="creeper-historical-index")
    parser.add_argument("producer_config", type=Path)
    parser.add_argument("--eed-model", type=Path, required=True)
    parser.add_argument("--owner", default="historical-index")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--watch", action="store_true")
    mode.add_argument("--cycles", type=int, default=1)
    args = parser.parse_args(argv)

    try:
        config = load_historical_index_optimizer_config(
            args.producer_config,
            eed_model=args.eed_model,
        )
        if not config.enabled:
            return 0
        with _OptimizerLock(
            config.runtime_data_root / "locks" / "historical-index.lock"
        ):
            if args.watch:
                return asyncio.run(
                    _run_watch_cli(config, owner=args.owner)
                )
            reports = asyncio.run(
                run_cycles(
                    config,
                    cycles=args.cycles,
                    owner=args.owner,
                )
            )
            print(
                json.dumps(
                    [asdict(item) for item in reports],
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
    except KeyboardInterrupt:
        return 130
    except (
        OSError,
        tomllib.TOMLDecodeError,
        ValueError,
        RuntimeError,
    ) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
