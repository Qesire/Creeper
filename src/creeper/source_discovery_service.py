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
from creeper.authority.identity import (
    AuthoritySnapshot,
    baseline_authority_signature,
    eed_model_authority_signature,
)
from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import (
    CommandAgentSearchExecutor,
    CommandAgentSearchPolicy,
)
from creeper.source_discovery.arquivo_catalog_scout import (
    ArquivoCatalogScoutExecutor,
    is_audited_arquivo_catalog,
)
from creeper.source_discovery.coordinator import (
    CoordinatorBusyError,
    SourceDiscoveryCoordinator,
)
from creeper.source_discovery.curated_seeds import ensure_curated_direct_catalogs
from creeper.source_discovery.intelligence import SourceIntelligenceContextBuilder
from creeper.source_discovery.manager import (
    SearchDirective,
    SearchDirectiveKind,
    SourceIntelligenceTask,
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
)
from creeper.source_discovery.models import SourceState
from creeper.source_discovery.models import is_direct_evidence_entrypoint
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.research_trigger import (
    ResearchDirective,
    ResearchTriggerGate,
    ResearchTriggerSnapshot,
)
from creeper.source_discovery.saturation import (
    SaturationPolicy,
    SourceSaturationController,
)
from creeper.source_discovery.scout_router import SourceScoutRouter
from creeper.source_discovery.scrapy_scout import (
    ScrapyStructuralScoutExecutor,
    ScrapyStructuralScoutPolicy,
)
from creeper.source_discovery.scrapy_sidecar import ScrapyScoutLauncher
from creeper.source_discovery.triage import HttpSourceTriageExecutor, HttpTriagePolicy
from creeper.storage.control_store import ControlStore
from creeper.storage.telemetry_store import RuntimeTelemetryStore
from creeper.runtime.http import configured_http_proxy


@dataclass(frozen=True)
class CoordinatorConfig:
    triage_parallelism: int = 4
    scout_parallelism: int = 4
    search_parallelism: int = 3
    region_parallelism: int = 2
    nonblocking_research: bool = False
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
    max_active_calls: int = 1
    min_seconds_between_starts: float = 120.0
    same_context_failure_cooldown_seconds: float = 600.0

    def __post_init__(self) -> None:
        if self.max_active_calls != 1:
            raise ValueError("integrated L8 requires agent.max_active_calls = 1")
        if self.min_seconds_between_starts < 0:
            raise ValueError("agent.min_seconds_between_starts must be non-negative")
        if self.same_context_failure_cooldown_seconds < 0:
            raise ValueError(
                "agent.same_context_failure_cooldown_seconds must be non-negative"
            )


@dataclass(frozen=True)
class MeasurementConfig:
    baseline_index: Path
    eed_model: Path
    authority_manifest: Path | None
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
    saturation: SaturationPolicy = SaturationPolicy()
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
    script/config paths are made absolute against the TOML directory, so daemon
    startup does not depend on shell cwd.

    Only genuine filesystem paths are rewritten. Bare option values that merely
    contain a separator (for example an opencode model id like
    ``ustc-107/deepseek-flash``) must be preserved verbatim. A token is treated
    as a relative path when it starts with an explicit ``./`` or ``../`` prefix,
    or when it resolves to an existing file/directory next to the config.
    """
    resolved: list[str] = []
    for value in values:
        path = Path(value)
        if not path.is_absolute():
            explicit_relative = value.startswith(("./", "../", ".\\", "..\\"))
            exists_relative = (config_path.parent / path).exists()
            if explicit_relative or exists_relative:
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
        max_cold_credit_per_origin=_positive_int(
            pool_raw.get(
                "max_cold_credit_per_origin",
                pool_defaults.max_cold_credit_per_origin,
            ),
            name="pool.max_cold_credit_per_origin",
        ),
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
        region_parallelism=_positive_int(
            coordinator_raw.get("region_parallelism", 2),
            name="coordinator.region_parallelism",
        ),
        nonblocking_research=_strict_bool(
            coordinator_raw.get("nonblocking_research", False),
            name="coordinator.nonblocking_research",
        ),
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

    saturation_raw = _table(root, "saturation")
    saturation_defaults = SaturationPolicy()
    saturation = SaturationPolicy(
        min_measured_siblings=_positive_int(
            saturation_raw.get(
                "min_measured_siblings",
                saturation_defaults.min_measured_siblings,
            ),
            name="saturation.min_measured_siblings",
        ),
        min_total_observations=_nonnegative_int(
            saturation_raw.get(
                "min_total_observations",
                saturation_defaults.min_total_observations,
            ),
            name="saturation.min_total_observations",
        ),
        max_total_novel_eed_for_zero_class=_nonnegative_float(
            saturation_raw.get(
                "max_total_novel_eed_for_zero_class",
                saturation_defaults.max_total_novel_eed_for_zero_class,
            ),
            name="saturation.max_total_novel_eed_for_zero_class",
        ),
        suppression_ttl_seconds=_positive_float(
            saturation_raw.get(
                "suppression_ttl_seconds",
                saturation_defaults.suppression_ttl_seconds,
            ),
            name="saturation.suppression_ttl_seconds",
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
        max_active_calls=_positive_int(
            agent_raw.get("max_active_calls", 1),
            name="agent.max_active_calls",
        ),
        min_seconds_between_starts=_nonnegative_float(
            agent_raw.get("min_seconds_between_starts", 120.0),
            name="agent.min_seconds_between_starts",
        ),
        same_context_failure_cooldown_seconds=_nonnegative_float(
            agent_raw.get("same_context_failure_cooldown_seconds", 600.0),
            name="agent.same_context_failure_cooldown_seconds",
        ),
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
            authority_manifest=_optional_path(
                measurement_raw.get("authority_manifest"),
                config_path=config_path,
                name="measurement.authority_manifest",
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
        if measurement.authority_manifest is not None:
            authority = AuthoritySnapshot.from_manifest_path(measurement.authority_manifest)
            if eed_model_authority_signature(measurement.eed_model) != authority.model_hash:
                raise ValueError(
                    "measurement.eed_model does not match measurement.authority_manifest"
                )

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
        saturation=saturation,
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


def _requeue_unexpanded_audited_arquivo_catalog(
    registry: SourceDiscoveryRegistry,
) -> int:
    """Re-arm a legacy HOLD Arquivo parent exactly until catalog expansion lands.

    Pre-recovery production may already have structurally scouted the curated
    parent and left it in HOLD. The dedicated deterministic executor can only
    run from SCOUT_READY/SCOUTING, so restart performs one idempotent migration.
    A durable catalog edge is the completion marker; unrelated generic/LLM
    children do not suppress the audited catalog expansion.
    """

    requeued = 0
    for candidate in registry.list_candidates(state=SourceState.HOLD):
        if not is_audited_arquivo_catalog(candidate):
            continue
        if registry.suppression_reason(candidate) is not None:
            continue
        expanded = registry.connection.execute(
            """
            SELECT 1
            FROM source_edges
            WHERE parent_key = ?
              AND relation = 'catalog_enumerates_cdxj'
            LIMIT 1
            """,
            (candidate.source_key,),
        ).fetchone()
        if expanded is not None:
            continue
        registry.transition(candidate.source_key, SourceState.SCOUT_READY)
        requeued += 1
    return requeued


def _research_snapshot(
    registry: SourceDiscoveryRegistry,
) -> ResearchTriggerSnapshot:
    """Build a bounded scheduler snapshot without exposing authority handles."""

    live_states = (
        SourceState.DISCOVERED,
        SourceState.TRIAGED,
        SourceState.SCOUT_READY,
        SourceState.SCOUTING,
    )
    deterministic_backlog = len(registry.list_candidates_in_states(live_states))
    productive_direct = sum(
        1
        for candidate in registry.list_candidates_in_states(
            (SourceState.WARM, SourceState.ACTIVE)
        )
        if candidate.direct_evidence_prior >= 0.5
    )

    executable_regions = 0
    pending_regions = 0
    list_executable = getattr(registry, "list_executable_regions", None)
    if callable(list_executable):
        executable_regions = len(list_executable(limit=10_000))
    list_regions = getattr(registry, "list_regions", None)
    if callable(list_regions):
        for region in list_regions():
            state = getattr(region.state, "value", str(region.state))
            if state in {"PROPOSED", "VALIDATED", "RUNNING"}:
                pending_regions += 1

    context_payload = {
        "deterministic_backlog": deterministic_backlog,
        "executable_regions": executable_regions,
        "pending_regions": pending_regions,
        "productive_direct": productive_direct,
    }
    context_hash = __import__("hashlib").sha256(
        json.dumps(context_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return ResearchTriggerSnapshot(
        executable_regions=executable_regions,
        pending_region_count=pending_regions,
        deterministic_candidate_backlog=deterministic_backlog,
        productive_direct_inventory=productive_direct,
        context_hash=context_hash,
    )


def _legacy_background_research_executor(
    search: CommandAgentSearchExecutor,
    *,
    desired_candidates: int,
):
    """Compatibility adapter until L6 unified proposal envelopes are merged."""

    async def execute(directive: ResearchDirective) -> object:
        task = SourceIntelligenceTask(directive.task_type)
        kind = {
            SourceIntelligenceTask.DISCOVER_NEW_SOURCE:
                SearchDirectiveKind.DISCOVER_NEW_FAMILY,
            SourceIntelligenceTask.EXPLOIT_SUCCESS_PATTERN:
                SearchDirectiveKind.EXPLOIT_SOURCE_FAMILY,
            SourceIntelligenceTask.INTERPRET_STRUCTURE:
                SearchDirectiveKind.INTERPRET_STRUCTURE,
            SourceIntelligenceTask.INTERPRET_EVIDENCE_CONTRACT:
                SearchDirectiveKind.INTERPRET_STRUCTURE,
            SourceIntelligenceTask.RECOVER_STAGNATION:
                SearchDirectiveKind.RECOVER_STAGNATION,
        }[task]
        return await search(
            SearchDirective(
                kind=kind,
                strategy=directive.strategy,
                desired_candidates=desired_candidates,
                subject=directive.subject,
                reason=directive.reason,
                task_type=task,
            )
        )

    return execute


@asynccontextmanager
async def _open_runtime(config: SourceDiscoveryServiceConfig):
    root = config.runtime_data_root
    root.mkdir(parents=True, exist_ok=True)
    discovery_root = root / "source-discovery"
    discovery_root.mkdir(parents=True, exist_ok=True)

    with _service_lock(discovery_root / "service.lock"):
        control = ControlStore(root / "control.sqlite3")
        baseline: BaselineIndex | None = None
        scout_authority: tuple[str, str] | None = None
        coordinator: SourceDiscoveryCoordinator | None = None
        try:
            registry = SourceDiscoveryRegistry(control)
            if config.measurement is not None:
                authority = (
                    AuthoritySnapshot.from_manifest_path(config.measurement.authority_manifest)
                    if config.measurement.authority_manifest is not None
                    else None
                )
                scout_authority = (
                    authority.authority_digest
                    if authority is not None
                    else baseline_authority_signature(config.measurement.baseline_index),
                    eed_model_authority_signature(
                        config.measurement.eed_model
                    ),
                )
                registry.set_scout_authority(
                    baseline_signature=scout_authority[0],
                    model_signature=scout_authority[1],
                )
                # Curated direct-evidence catalogs are only useful when the
                # deterministic baseline/EED scout authority is configured.
                ensure_curated_direct_catalogs(registry)
                _requeue_unexpanded_audited_arquivo_catalog(registry)
            saturation = SourceSaturationController(
                registry,
                policy=config.saturation,
            )
            manager = SourceReservoirManager(
                registry,
                targets=config.pool,
                search_cooldown_seconds=config.coordinator.search_cooldown_seconds,
                search_ucb_exploration=config.coordinator.search_ucb_exploration,
                stagnation_window=config.coordinator.stagnation_window,
                trigger_gate=ResearchTriggerGate(
                    min_seconds_between_llm_starts=(
                        config.agent.min_seconds_between_starts
                    ),
                    same_context_failure_cooldown_seconds=(
                        config.agent.same_context_failure_cooldown_seconds
                    ),
                ),
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
                    authority = (
                        AuthoritySnapshot.from_manifest_path(config.measurement.authority_manifest)
                        if config.measurement.authority_manifest is not None
                        else None
                    )
                    baseline = BaselineIndex(
                        config.measurement.baseline_index,
                        authority=authority,
                    )
                    measured = MeasuredYieldScoutExecutor(
                        client,
                        baseline,
                        load_english_weights(config.measurement.eed_model),
                        policy=config.measurement.policy,
                    )
                arquivo_catalog = ArquivoCatalogScoutExecutor()
                scout = SourceScoutRouter(
                    structural_executor=structural,
                    measured_executor=measured,
                    arquivo_catalog_executor=arquivo_catalog,
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
                background_research = (
                    _legacy_background_research_executor(
                        search,
                        desired_candidates=config.agent.policy.max_returned_candidates,
                    )
                    if config.coordinator.nonblocking_research
                    else None
                )
                coordinator = SourceDiscoveryCoordinator(
                    registry,
                    manager,
                    lock_path=discovery_root / "coordinator.lock",
                    triage_executor=triage,
                    scout_executor=scout,
                    search_executor=search,
                    scout_authority=scout_authority,
                    saturation_controller=saturation,
                    triage_parallelism=config.coordinator.triage_parallelism,
                    scout_parallelism=config.coordinator.scout_parallelism,
                    search_parallelism=config.coordinator.search_parallelism,
                    region_parallelism=config.coordinator.region_parallelism,
                    failure_retry_seconds=config.coordinator.failure_retry_seconds,
                    research_snapshot_provider=(
                        (lambda: _research_snapshot(registry))
                        if background_research is not None
                        else None
                    ),
                    research_executor=background_research,
                )
                yield registry, coordinator
        finally:
            if coordinator is not None:
                await coordinator.shutdown()
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


_DISCOVERY_COUNTER_FIELDS = {
    "search_episodes": "discovery_search_episodes",
    "search_failures": "discovery_search_failures",
    "search_backoff_skipped": "discovery_search_backoff_skipped",
    "search_candidates_registered": "discovery_search_candidates_registered",
    "search_candidates_dropped": "discovery_search_candidates_dropped",
    "triaged_to_scout": "discovery_triaged_to_scout",
    "triaged_hold": "discovery_triaged_hold",
    "triaged_rejected": "discovery_triaged_rejected",
    "triage_failures": "discovery_triage_failures",
    "scouted_warm": "discovery_scouted_warm",
    "scouted_hold": "discovery_scouted_hold",
    "scouted_rejected": "discovery_scouted_rejected",
    "scout_failures": "discovery_scout_failures",
    "scout_children_registered": "discovery_scout_children_registered",
    "scout_edges_added": "discovery_scout_edges_added",
    "production_exhausted": "discovery_production_exhausted",
    "activated": "discovery_activations_started",
}


def _table_exists(registry: SourceDiscoveryRegistry, table: str) -> bool:
    row = registry.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _runtime_telemetry_path(registry: SourceDiscoveryRegistry) -> Path:
    """Resolve the shared telemetry path without widening ControlStore API."""
    row = registry.connection.execute("PRAGMA database_list").fetchone()
    if row is None or not str(row["file"]).strip():
        raise RuntimeError("source discovery requires a file-backed control database")
    return Path(str(row["file"])).resolve().parent / "telemetry.sqlite3"


def _durable_state_counts(
    registry: SourceDiscoveryRegistry,
    *,
    table: str,
    column: str,
    states: tuple[str, ...],
) -> dict[str, int]:
    """Read bounded state counts without materializing durable work rows."""
    counts = {state: 0 for state in states}
    if not _table_exists(registry, table):
        return counts
    rows = registry.connection.execute(
        f"SELECT {column} AS state, COUNT(*) AS n FROM {table} GROUP BY {column}"
    )
    for row in rows:
        state = str(row["state"])
        if state in counts:
            counts[state] = int(row["n"])
    return counts


def _publish_discovery_telemetry(
    registry: SourceDiscoveryRegistry,
    report: dict[str, object],
) -> None:
    """Persist discovery outcomes and live inventory gauges.

    ``report`` is a per-cycle observation. The telemetry database is the
    durable operational sink, so counters are added once for this completed
    cycle while gauges are recomputed from the SQLite authorities after all
    cycle mutations have committed.
    """
    inventory = registry.inventory()
    active = registry.list_candidates(state=SourceState.ACTIVE)
    source_gauges = {
        f"source_candidates_{state.value.lower()}": int(inventory[state])
        for state in SourceState
    }
    source_gauges.update(
        {
            "active_candidates": len(active),
            "active_direct_sources": sum(
                int(is_direct_evidence_entrypoint(item.canonical_entrypoint))
                for item in active
            ),
            "discovery_search_episodes_inflight": int(
                registry.connection.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM source_search_episodes
                    WHERE finished_at IS NULL
                    """
                ).fetchone()["n"]
            ),
        }
    )

    historical_states = _durable_state_counts(
        registry,
        table="source_regions_v1",
        column="state",
        states=("DISCOVERED", "PROBED", "HARVEST_READY", "HARVESTING", "HARVESTED", "DROPPED"),
    )
    source_gauges.update(
        {
            "historical_index_total": int(
                registry.connection.execute(
                    "SELECT COUNT(*) AS n FROM source_indexes_v1"
                    if _table_exists(registry, "source_indexes_v1")
                    else "SELECT 0 AS n"
                ).fetchone()["n"]
            ),
            **{
                f"historical_region_{state.lower()}": count
                for state, count in historical_states.items()
            },
        }
    )

    platform_states = {
        str(state): int(count)
        for state, count in registry.control_store.platform_year_harvest_state_counts().items()
    }
    source_gauges["platform_year_total"] = sum(platform_states.values())
    source_gauges.update(
        {
            f"platform_year_{state.lower()}": count
            for state, count in platform_states.items()
        }
    )

    counters = {"discovery_cycles": 1}
    for report_field, telemetry_name in _DISCOVERY_COUNTER_FIELDS.items():
        value = report.get(report_field, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"discovery report field {report_field} must be a non-negative integer")
        counters[telemetry_name] = value
    elapsed_ms = max(0, int(round(float(report.get("elapsed_seconds", 0.0)) * 1000.0)))
    counters["discovery_wall_milliseconds"] = elapsed_ms

    with RuntimeTelemetryStore(_runtime_telemetry_path(registry)) as telemetry:
        telemetry.add_counters(counters)
        telemetry.set_gauges(source_gauges)


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
    _publish_discovery_telemetry(registry, report)
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
