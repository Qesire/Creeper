"""Signed HTTP client used by replaceable distributed workers."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from secrets import token_hex
from typing import Any, Mapping

import httpx

from creeper.distributed.auth import sign_request
from creeper.distributed.models import (
    ProviderPermit,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


class CoordinatorError(RuntimeError):
    """Authority request failed."""


class StaleLeaseCoordinatorError(CoordinatorError):
    """Authority rejected an obsolete lease generation."""


@dataclass(frozen=True)
class HYProbeDecision:
    hostname: str
    year: int
    status: str


class CoordinatorClient:
    """Small signed client for the Authority pull API.

    Every request receives a fresh nonce. The client deliberately exposes
    explicit methods instead of a generic remote execution surface so workers
    cannot acquire authority semantics locally.
    """

    def __init__(
        self,
        base_url: str,
        *,
        worker_id: str,
        secret: str | bytes,
        timeout: float = 30.0,
        clock=time.time,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not base_url.strip() or not worker_id.strip() or timeout <= 0:
            raise ValueError("invalid coordinator client configuration")
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.secret = secret
        self.clock = clock
        self._owns_client = True
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            transport=transport,
            trust_env=False,
        )

    async def __aenter__(self) -> "CoordinatorClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _post(
        self,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = json.dumps(
            dict(payload or {}),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        timestamp = f"{float(self.clock()):.6f}"
        nonce = token_hex(16)
        signature = sign_request(
            self.secret,
            method="POST",
            path=path,
            body=body,
            timestamp=timestamp,
            nonce=nonce,
        )
        try:
            response = await self.client.post(
                path,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Creeper-Worker": self.worker_id,
                    "X-Creeper-Timestamp": timestamp,
                    "X-Creeper-Nonce": nonce,
                    "X-Creeper-Signature": signature,
                },
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise CoordinatorError(
                f"Authority transport unavailable: {type(exc).__name__}"
            ) from exc
        try:
            value = response.json()
        except ValueError as exc:
            raise CoordinatorError(
                f"Authority returned non-JSON HTTP {response.status_code}"
            ) from exc
        if response.status_code >= 400:
            code = value.get("error") if isinstance(value, dict) else None
            detail = value.get("detail") if isinstance(value, dict) else None
            if code == "STALE_LEASE":
                raise StaleLeaseCoordinatorError(str(detail or code))
            raise CoordinatorError(
                f"Authority HTTP {response.status_code}: {code or detail or value}"
            )
        if not isinstance(value, dict):
            raise CoordinatorError("Authority response must be a JSON object")
        return value

    async def register(self, descriptor: WorkerDescriptor) -> None:
        if descriptor.worker_id != self.worker_id:
            raise ValueError("descriptor worker_id does not match client")
        await self._post(
            "/v1/workers/register",
            {
                "worker_id": descriptor.worker_id,
                "runtime_class": descriptor.runtime_class,
                "region": descriptor.region,
                "architecture": descriptor.architecture,
                "memory_bytes": descriptor.memory_bytes,
                "cpu_count": descriptor.cpu_count,
                "network_class": descriptor.network_class,
                "capabilities": list(descriptor.capabilities),
                "producers": list(descriptor.producers),
            },
        )

    async def heartbeat(self) -> None:
        await self._post("/v1/heartbeat")

    @staticmethod
    def _parse_lease(raw: Mapping[str, Any]) -> TaskLease:
        work_raw = raw["work"]
        work = WorkDefinition(
            producer=str(work_raw["producer"]),
            task_class=TaskClass(str(work_raw["task_class"])),
            input_identity=str(work_raw["input_identity"]),
            coverage=dict(work_raw["coverage"]),
            partition=str(work_raw["partition"]),
            algorithm_version=str(work_raw["algorithm_version"]),
            required_capabilities=tuple(
                str(v) for v in work_raw["required_capabilities"]
            ),
            priority=float(work_raw.get("priority", 0.0)),
        )
        return TaskLease(
            task_id=str(raw["task_id"]),
            work_key=str(raw["work_key"]),
            worker_id=str(raw["worker_id"]),
            generation=int(raw["generation"]),
            lease_deadline=float(raw["lease_deadline"]),
            attempt=int(raw["attempt"]),
            work=work,
            cursor=None if raw.get("cursor") is None else str(raw["cursor"]),
            next_sequence_no=int(raw.get("next_sequence_no", 0)),
        )

    async def claim(self, *, lease_seconds: float = 300.0) -> TaskLease | None:
        value = await self._post(
            "/v1/tasks/claim",
            {"lease_seconds": float(lease_seconds)},
        )
        raw = value.get("task")
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise CoordinatorError("invalid task payload")
        return self._parse_lease(raw)

    async def renew(
        self,
        lease: TaskLease,
        *,
        lease_seconds: float = 300.0,
    ) -> TaskLease:
        value = await self._post(
            "/v1/tasks/renew",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "lease_seconds": float(lease_seconds),
            },
        )
        raw = value.get("task")
        if not isinstance(raw, Mapping):
            raise CoordinatorError("invalid renewed task payload")
        return self._parse_lease(raw)

    async def finish(self, lease: TaskLease) -> None:
        await self._post(
            "/v1/tasks/finish",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
            },
        )

    async def fail(
        self,
        lease: TaskLease,
        error: str,
        *,
        retryable: bool = True,
    ) -> None:
        await self._post(
            "/v1/tasks/fail",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "error": error,
                "retryable": bool(retryable),
            },
        )

    async def commit_batch(
        self,
        lease: TaskLease,
        *,
        sequence_no: int,
        results: list[Mapping[str, Any]],
        cursor_after: str | None = None,
    ) -> str:
        value = await self._post(
            "/v1/results/batch",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "sequence_no": int(sequence_no),
                "results": [dict(item) for item in results],
                "cursor_after": cursor_after,
            },
        )
        return str(value["status"])

    async def coverage_complete(
        self,
        lease: TaskLease,
        *,
        hostname: str,
        provider: str,
        scope: str,
        resolver_version: str,
        year_from: int,
        year_to: int,
    ) -> int:
        value = await self._post(
            "/v1/coverage/complete",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "hostname": hostname,
                "provider": provider,
                "scope": scope,
                "resolver_version": resolver_version,
                "year_from": int(year_from),
                "year_to": int(year_to),
            },
        )
        return int(value["year_mask"])

    async def hy_probe(
        self,
        lease: TaskLease,
        probes: list[Mapping[str, Any]],
    ) -> list[HYProbeDecision]:
        value = await self._post(
            "/v1/results/hy-probe",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "probes": [dict(item) for item in probes],
            },
        )
        raw = value.get("decisions")
        if not isinstance(raw, list):
            raise CoordinatorError("invalid HY probe response")
        return [
            HYProbeDecision(
                hostname=str(item["hostname"]),
                year=int(item["year"]),
                status=str(item["status"]),
            )
            for item in raw
            if isinstance(item, Mapping)
        ]

    async def hy_full(
        self,
        lease: TaskLease,
        evidence: list[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        value = await self._post(
            "/v1/results/hy-full",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "evidence": [dict(item) for item in evidence],
            },
        )
        raw = value.get("results")
        if not isinstance(raw, list):
            raise CoordinatorError("invalid HY full response")
        return [dict(item) for item in raw if isinstance(item, Mapping)]

    async def provider_observation(
        self,
        lease: TaskLease,
        *,
        provider: str,
        connect_success: bool,
        status_code: int | None,
        latency_ms: float,
        response_bytes: int,
        timeout: bool = False,
        policy_block: bool = False,
    ) -> str:
        value = await self._post(
            "/v1/providers/observation",
            {
                "provider": provider,
                "task_id": lease.task_id,
                "generation": lease.generation,
                "connect_success": bool(connect_success),
                "status_code": status_code,
                "latency_ms": float(latency_ms),
                "response_bytes": int(response_bytes),
                "timeout": bool(timeout),
                "policy_block": bool(policy_block),
            },
        )
        return str(value["state"])

    async def provider_permit(
        self,
        lease: TaskLease,
        provider: str,
        *,
        ttl_seconds: float = 30.0,
        retry_after_seconds: float | None = None,
    ) -> ProviderPermit | None:
        while True:
            value = await self._post(
                "/v1/providers/permit",
                {
                    "provider": provider,
                    "task_id": lease.task_id,
                    "generation": lease.generation,
                    "ttl_seconds": float(ttl_seconds),
                },
            )
            raw = value.get("permit")
            if raw is not None:
                if not isinstance(raw, Mapping):
                    raise CoordinatorError("invalid provider permit")
                return ProviderPermit(
                    permit_id=str(raw["permit_id"]),
                    provider=str(raw["provider"]),
                    worker_id=str(raw["worker_id"]),
                    task_id=str(raw["task_id"]),
                    generation=int(raw["generation"]),
                    allowed_requests=int(raw["allowed_requests"]),
                    max_inflight=int(raw["max_inflight"]),
                    expires_at=float(raw["expires_at"]),
                )
            if retry_after_seconds is None:
                return None
            await asyncio.sleep(float(retry_after_seconds))

    async def provider_report(
        self,
        permit_id: str,
        *,
        status_code: int | None = None,
        cooldown_seconds: float = 0.0,
    ) -> None:
        await self._post(
            "/v1/providers/report",
            {
                "permit_id": permit_id,
                "status_code": status_code,
                "cooldown_seconds": float(cooldown_seconds),
            },
        )
