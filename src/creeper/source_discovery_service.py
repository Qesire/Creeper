"""Composition root for the finite source-discovery control-plane process."""

from __future__ import annotations

import asyncio
import json
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx

from creeper.source_discovery.agent_search import (
    CommandAgentSearchExecutor,
    CommandAgentSearchPolicy,
)
from creeper.source_discovery.coordinator import SourceDiscoveryCoordinator
from creeper.source_discovery.manager import SourcePoolTargets, SourceReservoirManager
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.scout_router import SourceScoutRouter
from creeper.source_discovery.scrapy_scout import (
    ScrapyStructuralScoutExecutor,
    ScrapyStructuralScoutPolicy,
)
from creeper.source_discovery.scrapy_sidecar import ScrapyScoutLauncher
from creeper.source_discovery.triage import HttpSourceTriageExecutor, HttpTriagePolicy
from creeper.storage.control_store import ControlStore


@dataclass(frozen=True)
class CoordinatorConfig:
    triage_parallelism: int = 4
    scout_parallelism: int = 4
    search_parallelism: int = 3
    failure_retry_seconds: float = 30.0


@dataclass(frozen=True)
class AgentConfig:
    command: tuple[str, ...]
    backend: str
    actor: str
    cwd: Path | None
    policy: CommandAgentSearchPolicy


@dataclass(frozen=True)
class SourceDiscoveryServiceConfig:
    runtime_data_root: Path
    scrapy_project_dir: Path
    pool: SourcePoolTargets
    coordinator: CoordinatorConfig
    triage: HttpTriagePolicy
    scrapy: ScrapyStructuralScoutPolicy
    agent: AgentConfig


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


def _nonnegative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, *, name: str) -> int:
    value = _nonnegative_int(value, name=name)
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _positive_float(value: Any, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return float(value)


def _strict_bool(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def load_source_discovery_config(config_path: Path) -> SourceDiscoveryServiceConfig:
    config_path = Path(config_path).resolve()
    with config_path.open("rb") as stream:
        root = tomllib.load(stream)

    runtime_data_root = _resolve_path(
        root.get("runtime_data_root"),
        config_path=config_path,
        name="runtime_data_root",
    )
    scrapy_project_dir = _resolve_path(
        root.get("scrapy_project_dir"),
        config_path=config_path,
        name="scrapy_project_dir",
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
        triage_parallelism=_positive_int(
            coordinator_raw.get("triage_parallelism", 4),
            name="coordinator.triage_parallelism",
        ),
        scout_parallelism=_positive_int(
            coordinator_raw.get("scout_parallelism", pool.scout_parallelism),
            name="coordinator.scout_parallelism",
        ),
        search_parallelism=_positive_int(
            coordinator_raw.get("search_parallelism", 3),
            name="coordinator.search_parallelism",
        ),
        failure_retry_seconds=_positive_float(
            coordinator_raw.get("failure_retry_seconds", 30.0),
            name="coordinator.failure_retry_seconds",
        ),
    )

    triage_raw = _table(root, "triage")
    triage = HttpTriagePolicy(
        timeout_seconds=_positive_float(
            triage_raw.get("timeout_seconds", 10.0),
            name="triage.timeout_seconds",
        )
    )

    scrapy_raw = _table(root, "scrapy")
    scrapy = ScrapyStructuralScoutPolicy(
        max_pages=_positive_int(scrapy_raw.get("max_pages", 100), name="scrapy.max_pages"),
        max_depth=_nonnegative_int(scrapy_raw.get("max_depth", 2), name="scrapy.max_depth"),
        max_seconds=_positive_int(scrapy_raw.get("max_seconds", 120), name="scrapy.max_seconds"),
        max_memory_mb=_positive_int(
            scrapy_raw.get("max_memory_mb", 512), name="scrapy.max_memory_mb"
        ),
        follow_query=_strict_bool(
            scrapy_raw.get("follow_query", False), name="scrapy.follow_query"
        ),
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
    agent = AgentConfig(
        command=tuple(raw_command),
        backend=backend,
        actor=actor,
        cwd=_optional_path(
            agent_raw.get("cwd"),
            config_path=config_path,
            name="agent.cwd",
        ),
        policy=CommandAgentSearchPolicy(
            timeout_seconds=_positive_float(
                agent_raw.get("timeout_seconds", 120.0),
                name="agent.timeout_seconds",
            ),
            termination_grace_seconds=_positive_float(
                agent_raw.get("termination_grace_seconds", 2.0),
                name="agent.termination_grace_seconds",
            ),
            max_response_bytes=_positive_int(
                agent_raw.get("max_response_bytes", 2 * 1024 * 1024),
                name="agent.max_response_bytes",
            ),
            max_returned_candidates=_positive_int(
                agent_raw.get("max_returned_candidates", 2_000),
                name="agent.max_returned_candidates",
            ),
        ),
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
    )


async def run_source_discovery_cycles(
    config: SourceDiscoveryServiceConfig,
    *,
    cycles: int = 1,
) -> list[dict[str, object]]:
    """Run a finite number of discovery cycles and return JSON-ready reports."""
    if cycles < 1:
        raise ValueError("cycles must be positive")
    root = config.runtime_data_root
    root.mkdir(parents=True, exist_ok=True)
    discovery_root = root / "source-discovery"
    discovery_root.mkdir(parents=True, exist_ok=True)

    control = ControlStore(root / "control.sqlite3")
    try:
        registry = SourceDiscoveryRegistry(control)
        manager = SourceReservoirManager(registry, targets=config.pool)
        limits = httpx.Limits(
            max_connections=max(4, config.coordinator.triage_parallelism * 2),
            max_keepalive_connections=max(2, config.coordinator.triage_parallelism),
        )
        async with httpx.AsyncClient(
            limits=limits,
            headers={"User-Agent": "Creeper-source-discovery/2.1"},
        ) as client:
            triage = HttpSourceTriageExecutor(client, policy=config.triage)
            launcher = ScrapyScoutLauncher(config.scrapy_project_dir)
            structural = ScrapyStructuralScoutExecutor(
                launcher,
                discovery_root / "scrapy",
                policy=config.scrapy,
            )
            scout = SourceScoutRouter(structural_executor=structural)
            search = CommandAgentSearchExecutor(
                config.agent.command,
                discovery_root / "agent-invocations",
                backend=config.agent.backend,
                actor=config.agent.actor,
                cwd=config.agent.cwd,
                policy=config.agent.policy,
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
            reports: list[dict[str, object]] = []
            for cycle in range(1, cycles + 1):
                report = asdict(await coordinator.run_once())
                report["cycle"] = cycle
                reports.append(report)
            return reports
    finally:
        control.close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="creeper-source-discovery")
    parser.add_argument("config", type=Path)
    parser.add_argument("--cycles", type=int, default=1)
    args = parser.parse_args(argv)
    try:
        config = load_source_discovery_config(args.config)
        reports = asyncio.run(run_source_discovery_cycles(config, cycles=args.cycles))
    except (OSError, tomllib.TOMLDecodeError, ValueError, KeyError) as exc:
        parser.error(f"invalid source discovery configuration: {exc}")
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
