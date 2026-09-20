"""Configuration models for Creeper Fabric v2."""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from creeper.distributed.models import WorkerDescriptor
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


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
    outbox_enabled: bool = True
    provider_budgets: tuple[ProviderBudgetConfig, ...] = ()

    def __post_init__(self) -> None:
        if not self.database.strip():
            raise ValueError("authority database is required")
        if not self.host.strip() or not 1 <= self.port <= 65535:
            raise ValueError("invalid authority listen address")
        if self.max_clock_skew_seconds <= 0:
            raise ValueError("max_clock_skew_seconds must be positive")
        if not isinstance(self.outbox_enabled, bool):
            raise ValueError("outbox_enabled must be a boolean")


@dataclass(frozen=True, slots=True)
class EvidenceBridgeRuntimeConfig:
    runtime_data_root: Path
    owner: str = "fabric:evidence"
    dispatch_limit: int = 32
    drain_limit: int = 64
    lease_seconds: float = 900.0
    poll_seconds: float = 1.0
    retry_base_seconds: float = 30.0
    retry_max_seconds: float = 3600.0
    rdap_endpoint: str = "https://rdap.org/domain"
    rdap_timeout: float = 20.0
    cdx_provider_configs: tuple[CDXProviderConfig, ...] = ()

    def __post_init__(self) -> None:
        if not self.owner.strip() or not self.rdap_endpoint.strip():
            raise ValueError("evidence bridge owner and RDAP endpoint are required")
        for name in ("dispatch_limit", "drain_limit"):
            value=getattr(self,name)
            if isinstance(value,bool) or not isinstance(value,int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if (
            self.lease_seconds <= 0
            or self.poll_seconds <= 0
            or self.retry_base_seconds < 0
            or self.retry_max_seconds < self.retry_base_seconds
            or self.rdap_timeout <= 0
        ):
            raise ValueError("invalid evidence bridge timing configuration")


@dataclass(frozen=True, slots=True)
class WorkerRuntimeConfig:
    coordinator_url: str
    descriptor: WorkerDescriptor
    spool_database: Path
    worker_instance_auto: bool = False
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


@dataclass(frozen=True, slots=True)
class EmailReportRuntimeConfig:
    runtime_data_root: Path
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    starttls: bool = True
    sender_env: str = "CREEPER_REPORT_FROM"
    recipient_env: str = "CREEPER_REPORT_TO"
    username_env: str = "CREEPER_SMTP_USERNAME"
    password_env: str = "CREEPER_SMTP_APP_PASSWORD"
    subject_prefix: str = "[Creeper]"
    timezone: str = "Asia/Singapore"
    state_file: Path | None = None

    def __post_init__(self) -> None:
        if not self.smtp_host.strip() or not 1 <= self.smtp_port <= 65535:
            raise ValueError("invalid email-report SMTP endpoint")
        for name in (
            "sender_env",
            "recipient_env",
            "username_env",
            "password_env",
            "subject_prefix",
            "timezone",
        ):
            if not str(getattr(self,name)).strip():
                raise ValueError(f"email-report {name} must be non-empty")

    @property
    def resolved_state_file(self) -> Path:
        return (
            self.state_file
            if self.state_file is not None
            else self.runtime_data_root/"reporting"/"last-email-snapshot.json"
        )

    def load_delivery_environment(self) -> tuple[str,str,str,str]:
        names=(
            self.sender_env,
            self.recipient_env,
            self.username_env,
            self.password_env,
        )
        values=tuple(os.environ.get(name,"").strip() for name in names)
        missing=[name for name,value in zip(names,values) if not value]
        if missing:
            raise RuntimeError(
                "email-report environment variable(s) missing: "
                + ",".join(missing)
            )
        sender,recipient,username,password=values
        return sender,recipient,username,password


def _resolve_local_path(value: object, *, config_path: Path, name: str) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty path")
    path=Path(value).expanduser()
    if not path.is_absolute():
        path=config_path.parent/path
    return path.resolve()


def _resolve_database(value: object, *, config_path: Path) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("authority.database must be non-empty")
    text=value.strip()
    if "://" in text:
        return text
    return str(_resolve_local_path(text,config_path=config_path,name="authority.database"))


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
    raw_outbox_enabled=section.get("outbox_enabled",True)
    if not isinstance(raw_outbox_enabled,bool):
        raise ValueError("authority.outbox_enabled must be a boolean")
    return AuthorityRuntimeConfig(
        database=_resolve_database(
            section["database"],
            config_path=Path(path).resolve(),
        ),
        credentials_file=_resolve_local_path(
            section["credentials_file"],
            config_path=Path(path).resolve(),
            name="authority.credentials_file",
        ),
        host=str(section.get("host","127.0.0.1")),
        port=int(section.get("port",8088)),
        max_clock_skew_seconds=float(
            section.get("max_clock_skew_seconds",300.0)
        ),
        outbox_enabled=raw_outbox_enabled,
        provider_budgets=tuple(budgets),
    )


def load_evidence_bridge_config(path: Path) -> EvidenceBridgeRuntimeConfig:
    raw=_load_toml(path)
    section=raw.get("evidence_bridge")
    if not isinstance(section,Mapping):
        raise ValueError("[evidence_bridge] table is required")
    providers_raw=section.get("cdx_providers",())
    if not isinstance(providers_raw,list):
        raise ValueError("evidence_bridge.cdx_providers must be an array of tables")
    provider_configs=tuple(
        CDXProviderConfig.from_mapping(item)
        for item in providers_raw
        if isinstance(item,Mapping)
    )
    if len(provider_configs) != len(providers_raw):
        raise ValueError("every evidence_bridge.cdx_providers item must be a table")
    return EvidenceBridgeRuntimeConfig(
        runtime_data_root=_resolve_local_path(
            section["runtime_data_root"],
            config_path=Path(path).resolve(),
            name="evidence_bridge.runtime_data_root",
        ),
        owner=str(section.get("owner","fabric:evidence")),
        dispatch_limit=int(section.get("dispatch_limit",32)),
        drain_limit=int(section.get("drain_limit",64)),
        lease_seconds=float(section.get("lease_seconds",900.0)),
        poll_seconds=float(section.get("poll_seconds",1.0)),
        retry_base_seconds=float(section.get("retry_base_seconds",30.0)),
        retry_max_seconds=float(section.get("retry_max_seconds",3600.0)),
        rdap_endpoint=str(
            section.get("rdap_endpoint","https://rdap.org/domain")
        ),
        rdap_timeout=float(section.get("rdap_timeout",20.0)),
        cdx_provider_configs=provider_configs,
    )


def load_worker_config(path: Path) -> WorkerRuntimeConfig:
    raw=_load_toml(path)
    section=raw.get("worker")
    if not isinstance(section,Mapping):
        raise ValueError("[worker] table is required")
    raw_instance=str(section.get("worker_instance_id","auto")).strip()
    worker_instance_auto=(
        not raw_instance or raw_instance.lower()=="auto"
    )
    worker_instance_id=(
        uuid4().hex if worker_instance_auto else raw_instance
    )
    descriptor=WorkerDescriptor(
        worker_id=str(section["worker_id"]),
        worker_instance_id=worker_instance_id,
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
        spool_database=_resolve_local_path(
            section["spool_database"],
            config_path=Path(path).resolve(),
            name="worker.spool_database",
        ),
        worker_instance_auto=worker_instance_auto,
        secret_env=str(section.get("secret_env","CREEPER_WORKER_SECRET")),
        poll_seconds=float(section.get("poll_seconds",1.0)),
        claim_wait_seconds=float(section.get("claim_wait_seconds",10.0)),
        lease_seconds=float(section.get("lease_seconds",300.0)),
        heartbeat_seconds=float(section.get("heartbeat_seconds",30.0)),
    )

def load_email_report_config(path: Path) -> EmailReportRuntimeConfig:
    raw=_load_toml(path)
    section=raw.get("email_report")
    if not isinstance(section,Mapping):
        raise ValueError("[email_report] table is required")
    config_path=Path(path).resolve()
    runtime_data_root=_resolve_local_path(
        section["runtime_data_root"],
        config_path=config_path,
        name="email_report.runtime_data_root",
    )
    state_value=section.get("state_file")
    state_file=(
        _resolve_local_path(
            state_value,
            config_path=config_path,
            name="email_report.state_file",
        )
        if state_value is not None
        else None
    )
    return EmailReportRuntimeConfig(
        runtime_data_root=runtime_data_root,
        smtp_host=str(section.get("smtp_host","smtp.gmail.com")),
        smtp_port=int(section.get("smtp_port",587)),
        starttls=bool(section.get("starttls",True)),
        sender_env=str(section.get("sender_env","CREEPER_REPORT_FROM")),
        recipient_env=str(section.get("recipient_env","CREEPER_REPORT_TO")),
        username_env=str(section.get("username_env","CREEPER_SMTP_USERNAME")),
        password_env=str(
            section.get("password_env","CREEPER_SMTP_APP_PASSWORD")
        ),
        subject_prefix=str(section.get("subject_prefix","[Creeper]")),
        timezone=str(section.get("timezone","Asia/Singapore")),
        state_file=state_file,
    )

