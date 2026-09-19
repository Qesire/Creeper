"""Configuration models for Creeper Fabric v2."""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from creeper.distributed.models import WorkerDescriptor


@dataclass(frozen=True, slots=True)
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
            raise ValueError("invalid provider budget")


@dataclass(frozen=True, slots=True)
class AuthorityRuntimeConfig:
    database: str
    credentials_file: Path
    host: str = "127.0.0.1"
    port: int = 8088
    max_clock_skew_seconds: float = 300.0
    provider_budgets: tuple[ProviderBudgetConfig, ...] = ()

    def __post_init__(self) -> None:
        if not self.database.strip():
            raise ValueError("authority database is required")
        if not self.host.strip() or not 1 <= self.port <= 65535:
            raise ValueError("invalid authority listen address")
        if self.max_clock_skew_seconds <= 0:
            raise ValueError("max_clock_skew_seconds must be positive")


@dataclass(frozen=True, slots=True)
class WorkerRuntimeConfig:
    coordinator_url: str
    descriptor: WorkerDescriptor
    spool_database: Path
    secret_env: str = "CREEPER_WORKER_SECRET"
    poll_seconds: float = 1.0
    claim_wait_seconds: float = 10.0
    lease_seconds: float = 300.0
    heartbeat_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.coordinator_url.strip() or not self.secret_env.strip():
            raise ValueError("worker coordinator_url and secret_env are required")
        if (
            self.poll_seconds <= 0
            or not 0 <= self.claim_wait_seconds <= 25
            or self.lease_seconds <= 0
            or self.heartbeat_seconds <= 0
        ):
            raise ValueError("invalid worker timing configuration")

    def load_secret(self) -> str:
        value=os.environ.get(self.secret_env,"")
        if not value:
            raise RuntimeError(
                f"worker secret environment variable is missing: {self.secret_env}"
            )
        return value


def _load_toml(path: Path) -> dict[str,Any]:
    with Path(path).open("rb") as source:
        value=tomllib.load(source)
    if not isinstance(value,dict):
        raise ValueError("distributed config must contain a TOML table")
    return value


def load_worker_credentials(
    path: Path,
    *,
    allow_empty: bool=False,
) -> dict[str,str]:
    value=json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value,dict):
        raise ValueError("credentials file must contain a JSON object")
    result:dict[str,str]={}
    for worker_id,secret in value.items():
        if (
            not isinstance(worker_id,str)
            or not isinstance(secret,str)
            or not worker_id.strip()
            or not secret
        ):
            raise ValueError("worker credentials must be non-empty string pairs")
        result[worker_id]=secret
    if not result and not allow_empty:
        raise ValueError("credentials file must contain at least one worker")
    return result


def load_authority_config(path: Path) -> AuthorityRuntimeConfig:
    raw=_load_toml(path)
    section=raw.get("authority")
    if not isinstance(section,Mapping):
        raise ValueError("[authority] table is required")
    budgets_raw=raw.get("provider_budgets",{})
    if not isinstance(budgets_raw,Mapping):
        raise ValueError("[provider_budgets] must be a table")
    budgets=[]
    for name,spec in budgets_raw.items():
        if not isinstance(spec,Mapping):
            raise ValueError("provider budget entry must be a table")
        budgets.append(
            ProviderBudgetConfig(
                name=str(name),
                requests_per_second=float(spec["requests_per_second"]),
                max_global_inflight=int(spec["max_global_inflight"]),
                require_qualified_region=bool(
                    spec.get("require_qualified_region",True)
                ),
            )
        )
    return AuthorityRuntimeConfig(
        database=str(section["database"]),
        credentials_file=Path(str(section["credentials_file"])).expanduser(),
        host=str(section.get("host","127.0.0.1")),
        port=int(section.get("port",8088)),
        max_clock_skew_seconds=float(
            section.get("max_clock_skew_seconds",300.0)
        ),
        provider_budgets=tuple(budgets),
    )


def load_worker_config(path: Path) -> WorkerRuntimeConfig:
    raw=_load_toml(path)
    section=raw.get("worker")
    if not isinstance(section,Mapping):
        raise ValueError("[worker] table is required")
    descriptor=WorkerDescriptor(
        worker_id=str(section["worker_id"]),
        worker_instance_id=str(section["worker_instance_id"]),
        runtime_class=str(section["runtime_class"]),
        region=str(section["region"]),
        architecture=str(section["architecture"]),
        memory_bytes=int(section["memory_bytes"]),
        cpu_count=int(section["cpu_count"]),
        network_class=str(section["network_class"]),
        capabilities=tuple(str(v) for v in section["capabilities"]),
        producers=tuple(str(v) for v in section.get("producers",())),
        allowed_providers=tuple(
            str(v) for v in section.get("allowed_providers",())
        ),
        daily_egress_budget_bytes=int(
            section.get("daily_egress_budget_bytes",0)
        ),
    )
    return WorkerRuntimeConfig(
        coordinator_url=str(section["coordinator_url"]),
        descriptor=descriptor,
        spool_database=Path(str(section["spool_database"])).expanduser(),
        secret_env=str(section.get("secret_env","CREEPER_WORKER_SECRET")),
        poll_seconds=float(section.get("poll_seconds",1.0)),
        claim_wait_seconds=float(section.get("claim_wait_seconds",10.0)),
        lease_seconds=float(section.get("lease_seconds",300.0)),
        heartbeat_seconds=float(section.get("heartbeat_seconds",30.0)),
    )
