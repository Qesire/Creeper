"""Configuration models for distributed Creeper services."""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from creeper.distributed.models import WorkerDescriptor
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


@dataclass(frozen=True)
class ProviderBudgetConfig:
    name: str
    requests_per_second: float
    max_global_inflight: int
    require_qualified_region: bool = True

    def __post_init__(self) -> None:
        if (
            not self.name.strip()
            or self.requests_per_second <= 0
            or self.max_global_inflight < 1
        ):
            raise ValueError("invalid distributed provider budget")


@dataclass(frozen=True)
class AuthorityRuntimeConfig:
    database: Path
    baseline_index: Path
    credentials_file: Path
    host: str = "127.0.0.1"
    port: int = 8088
    max_clock_skew_seconds: float = 300.0
    reconcile_interval_seconds: float = 5.0
    promotion_batch_size: int = 64
    auto_promote_source_pages: bool = False
    host_promotion_batch_size: int = 256
    host_resolution_providers: tuple[str, ...] = ()
    host_coverage_provider: str = ""
    host_resolver_version: str = "fabric-host-v1"
    provider_budgets: tuple[ProviderBudgetConfig, ...] = ()

    def __post_init__(self) -> None:
        if not self.host.strip() or not 1 <= self.port <= 65535:
            raise ValueError("invalid Authority listen address")
        if self.max_clock_skew_seconds <= 0:
            raise ValueError("max_clock_skew_seconds must be positive")
        if (
            self.reconcile_interval_seconds <= 0
            or self.promotion_batch_size < 1
            or self.host_promotion_batch_size < 1
        ):
            raise ValueError("invalid Authority reconcile configuration")
        if self.host_resolution_providers:
            if (
                any(not value.strip() for value in self.host_resolution_providers)
                or not self.host_coverage_provider.strip()
                or not self.host_resolver_version.strip()
            ):
                raise ValueError("invalid host resolution reconcile configuration")


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    coordinator_url: str
    descriptor: WorkerDescriptor
    secret_env: str
    poll_seconds: float
    lease_seconds: float
    cdx_providers: tuple[CDXProviderConfig, ...]

    def __post_init__(self) -> None:
        if not self.coordinator_url.strip() or not self.secret_env.strip():
            raise ValueError("worker coordinator_url and secret_env are required")
        if self.poll_seconds <= 0 or self.lease_seconds <= 0:
            raise ValueError("worker polling/lease intervals must be positive")
        capabilities = set(self.descriptor.capabilities)
        exploration_caps = {"WEB_DISCOVERY", "SEARCH_QUERY"} & capabilities
        if exploration_caps and "ONLINE_QUERY" not in capabilities:
            raise ValueError(
                "WEB_DISCOVERY/SEARCH_QUERY production workers require ONLINE_QUERY"
            )
        if (
            {"ONLINE_QUERY", "THIN_QUERY", "WEB_DISCOVERY", "SEARCH_QUERY"}
            & capabilities
            and not self.cdx_providers
        ):
            raise ValueError(
                "archive-aware production workers require CDX providers"
            )
        configured = {provider.name for provider in self.cdx_providers}
        allowed = set(self.descriptor.allowed_providers)
        missing = configured - allowed
        if missing:
            raise ValueError(
                "configured CDX providers are not allowed by worker policy: "
                + ",".join(sorted(missing))
            )

    def load_secret(self) -> str:
        value = os.environ.get(self.secret_env, "")
        if not value:
            raise RuntimeError(
                f"worker secret environment variable is missing: {self.secret_env}"
            )
        return value


def _load_toml(path: Path) -> dict[str, Any]:
    with Path(path).open("rb") as source:
        value = tomllib.load(source)
    if not isinstance(value, dict):
        raise ValueError("distributed config must contain a TOML table")
    return value


def load_worker_credentials(path: Path) -> dict[str, str]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("credentials file must contain a JSON object")
    result: dict[str, str] = {}
    for worker_id, secret in value.items():
        if not isinstance(worker_id, str) or not isinstance(secret, str):
            raise ValueError("worker credential entries must be string pairs")
        if not worker_id.strip() or not secret:
            raise ValueError("worker credential entries must be non-empty")
        result[worker_id] = secret
    if not result:
        raise ValueError("credentials file must contain at least one worker")
    return result


def load_authority_config(path: Path) -> AuthorityRuntimeConfig:
    raw = _load_toml(path)
    section = raw.get("authority")
    if not isinstance(section, Mapping):
        raise ValueError("[authority] table is required")

    host_resolution_raw = raw.get("host_resolution", {})
    if not isinstance(host_resolution_raw, Mapping):
        raise ValueError("[host_resolution] must be a table")
    raw_host_providers = host_resolution_raw.get("providers", ())
    if not isinstance(raw_host_providers, (list, tuple)):
        raise ValueError("host_resolution.providers must be a list")
    host_resolution_providers = tuple(
        str(value) for value in raw_host_providers
    )

    budgets_raw = raw.get("provider_budgets", {})
    if not isinstance(budgets_raw, Mapping):
        raise ValueError("[provider_budgets] must be a table")
    budgets: list[ProviderBudgetConfig] = []
    for name, spec in budgets_raw.items():
        if not isinstance(spec, Mapping):
            raise ValueError("provider budget entry must be a table")
        budgets.append(
            ProviderBudgetConfig(
                name=str(name),
                requests_per_second=float(spec["requests_per_second"]),
                max_global_inflight=int(spec["max_global_inflight"]),
                require_qualified_region=bool(
                    spec.get("require_qualified_region", True)
                ),
            )
        )

    return AuthorityRuntimeConfig(
        database=Path(str(section["database"])).expanduser(),
        baseline_index=Path(str(section["baseline_index"])).expanduser(),
        credentials_file=Path(str(section["credentials_file"])).expanduser(),
        host=str(section.get("host", "127.0.0.1")),
        port=int(section.get("port", 8088)),
        max_clock_skew_seconds=float(
            section.get("max_clock_skew_seconds", 300.0)
        ),
        reconcile_interval_seconds=float(
            section.get("reconcile_interval_seconds", 5.0)
        ),
        promotion_batch_size=int(section.get("promotion_batch_size", 64)),
        auto_promote_source_pages=bool(
            section.get("auto_promote_source_pages", False)
        ),
        host_promotion_batch_size=int(
            section.get("host_promotion_batch_size", 256)
        ),
        host_resolution_providers=host_resolution_providers,
        host_coverage_provider=str(
            host_resolution_raw.get("coverage_provider", "")
        ),
        host_resolver_version=str(
            host_resolution_raw.get(
                "resolver_version",
                "fabric-host-v1",
            )
        ),
        provider_budgets=tuple(budgets),
    )


def load_worker_config(path: Path) -> WorkerRuntimeConfig:
    raw = _load_toml(path)
    section = raw.get("worker")
    if not isinstance(section, Mapping):
        raise ValueError("[worker] table is required")
    providers_raw = raw.get("cdx_providers", [])
    if not isinstance(providers_raw, list):
        raise ValueError("[[cdx_providers]] entries must form a list")
    providers = tuple(
        CDXProviderConfig.from_mapping(spec)
        for spec in providers_raw
        if isinstance(spec, Mapping)
    )
    if len(providers) != len(providers_raw):
        raise ValueError("every cdx_providers entry must be a table")

    descriptor = WorkerDescriptor(
        worker_id=str(section["worker_id"]),
        runtime_class=str(section["runtime_class"]),
        region=str(section["region"]),
        architecture=str(section["architecture"]),
        memory_bytes=int(section["memory_bytes"]),
        cpu_count=int(section["cpu_count"]),
        network_class=str(section["network_class"]),
        capabilities=tuple(str(value) for value in section["capabilities"]),
        allowed_providers=tuple(
            str(value) for value in section.get("allowed_providers", ())
        ),
        daily_egress_budget_bytes=int(
            section.get("daily_egress_budget_bytes", 0)
        ),
    )
    return WorkerRuntimeConfig(
        coordinator_url=str(section["coordinator_url"]),
        descriptor=descriptor,
        secret_env=str(section.get("secret_env", "CREEPER_WORKER_SECRET")),
        poll_seconds=float(section.get("poll_seconds", 1.0)),
        lease_seconds=float(section.get("lease_seconds", 300.0)),
        cdx_providers=providers,
    )
