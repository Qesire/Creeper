"""Signed Fabric v2 authority client."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from secrets import token_hex
from typing import Any, Mapping

import httpx

from creeper.distributed.auth import sign_request
from creeper.distributed.models import (
    ArtifactRef,
    ProviderPermit,
    ResultBatch,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)


class CoordinatorError(RuntimeError):
    pass


class StaleLeaseCoordinatorError(CoordinatorError):
    pass


class CoordinatorTransportError(CoordinatorError):
    pass


class CoordinatorUploadBudgetExceededError(CoordinatorError):
    pass


class CoordinatorClient:
    def __init__(
        self,
        base_url: str,
        *,
        worker_id: str,
        worker_instance_id: str,
        secret: str | bytes,
        timeout: float = 30.0,
        clock=time.time,
        transport: httpx.AsyncBaseTransport | None = None,
        upload_reserver: Callable[[int], bool] | None = None,
        upload_overhead_bytes: int = 0,
    ) -> None:
        if not all((base_url.strip(), worker_id.strip(), worker_instance_id.strip())):
            raise ValueError("coordinator URL and worker identities are required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if upload_overhead_bytes < 0:
            raise ValueError("upload_overhead_bytes must be non-negative")
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.worker_instance_id = worker_instance_id
        self.secret = secret
        self.clock = clock
        self.upload_reserver = upload_reserver
        self.upload_overhead_bytes = int(upload_overhead_bytes)
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
            worker_instance_id=self.worker_instance_id,
            timestamp=timestamp,
            nonce=nonce,
        )
        if self.upload_reserver is not None:
            reserved = self.upload_reserver(
                len(body) + self.upload_overhead_bytes
            )
            if not reserved:
                raise CoordinatorUploadBudgetExceededError(
                    "monthly coordinator upload budget exhausted"
                )
        try:
            response = await self.client.post(
                path,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Creeper-Worker": self.worker_id,
                    "X-Creeper-Instance": self.worker_instance_id,
                    "X-Creeper-Timestamp": timestamp,
                    "X-Creeper-Nonce": nonce,
                    "X-Creeper-Signature": signature,
                },
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise CoordinatorTransportError(type(exc).__name__) from exc
        # Treat retryable HTTP control-plane failures the same as a
        # dropped connection. All mutating Fabric endpoints involved here are
        # fenced/idempotent (BatchID, permit ID, lease generation), so callers
        # can safely retry without turning a short Authority restart into a
        # worker-process failure.
        if (
            response.status_code in {408, 425, 429}
            or response.status_code >= 500
        ):
            raise CoordinatorTransportError(
                f"authority HTTP {response.status_code}"
            )
        try:
            value = response.json()
        except ValueError as exc:
            raise CoordinatorError(
                f"authority returned non-JSON HTTP {response.status_code}"
            ) from exc
        if response.status_code >= 400:
            code = value.get("error") if isinstance(value, dict) else None
            detail = value.get("detail") if isinstance(value, dict) else None
            if code == "STALE_LEASE":
                raise StaleLeaseCoordinatorError(str(detail or code))
            raise CoordinatorError(
                f"authority HTTP {response.status_code}: {code or detail or value}"
            )
        if not isinstance(value, dict):
            raise CoordinatorError("authority response must be a JSON object")
        return value

    async def register(self, descriptor: WorkerDescriptor) -> None:
        if (
            descriptor.worker_id != self.worker_id
            or descriptor.worker_instance_id != self.worker_instance_id
        ):
            raise ValueError("worker descriptor does not match client identity")
        await self._post(
            "/v2/workers/register",
            {
                "worker_id": descriptor.worker_id,
                "worker_instance_id": descriptor.worker_instance_id,
                "runtime_class": descriptor.runtime_class,
                "region": descriptor.region,
                "architecture": descriptor.architecture,
                "memory_bytes": descriptor.memory_bytes,
                "cpu_count": descriptor.cpu_count,
                "network_class": descriptor.network_class,
                "capabilities": list(descriptor.capabilities),
                "producers": list(descriptor.producers),
                "allowed_providers": list(descriptor.allowed_providers),
                "daily_egress_budget_bytes": descriptor.daily_egress_budget_bytes,
                "protocol_version": descriptor.protocol_version,
                "edition_version": descriptor.edition_version,
            },
        )

    async def heartbeat(self) -> None:
        await self._post("/v2/heartbeat")

    @staticmethod
    def _parse_lease(raw: Mapping[str, Any]) -> TaskLease:
        work_raw = raw["work"]
        work = WorkDefinition(
            producer=str(work_raw["producer"]),
            task_class=TaskClass(str(work_raw["task_class"])),
            input_identity=str(work_raw["input_identity"]),
            payload=dict(work_raw["payload"]),
            partition=str(work_raw["partition"]),
            algorithm_version=str(work_raw["algorithm_version"]),
            required_capabilities=tuple(
                str(v) for v in work_raw["required_capabilities"]
            ),
            required_providers=tuple(
                str(v) for v in work_raw.get("required_providers", ())
            ),
            priority=float(work_raw.get("priority", 0.0)),
            max_attempts=int(work_raw.get("max_attempts", 8)),
            not_before=float(work_raw.get("not_before", 0.0)),
        )
        return TaskLease(
            task_id=str(raw["task_id"]),
            work_key=str(raw["work_key"]),
            worker_id=str(raw["worker_id"]),
            worker_instance_id=str(raw["worker_instance_id"]),
            generation=int(raw["generation"]),
            lease_deadline=float(raw["lease_deadline"]),
            attempt=int(raw["attempt"]),
            work=work,
            cursor=None if raw.get("cursor") is None else str(raw["cursor"]),
            next_sequence_no=int(raw.get("next_sequence_no", 0)),
        )

    async def claim(
        self,
        *,
        lease_seconds: float = 300.0,
        wait_seconds: float = 0.0,
    ) -> TaskLease | None:
        value = await self._post(
            "/v2/tasks/claim",
            {
                "lease_seconds": float(lease_seconds),
                "wait_seconds": float(wait_seconds),
            },
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
            "/v2/tasks/renew",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "lease_seconds": float(lease_seconds),
                "expected_lease_deadline": float(lease.lease_deadline),
            },
        )
        raw = value.get("task")
        if not isinstance(raw, Mapping):
            raise CoordinatorError("invalid renewed task payload")
        return self._parse_lease(raw)

    async def finish(self, lease: TaskLease) -> None:
        await self._post(
            "/v2/tasks/finish",
            {"task_id": lease.task_id, "generation": lease.generation},
        )

    async def fail(
        self,
        lease: TaskLease,
        error: str,
        *,
        retryable: bool = True,
        retry_after_seconds: float = 0.0,
    ) -> None:
        await self._post(
            "/v2/tasks/fail",
            {
                "task_id": lease.task_id,
                "generation": lease.generation,
                "error": error,
                "retryable": bool(retryable),
                "retry_after_seconds": float(retry_after_seconds),
            },
        )

    async def commit_batch(self, batch: ResultBatch) -> str:
        value = await self._post(
            "/v2/results/batch",
            {
                "task_id": batch.task_id,
                "generation": batch.generation,
                "sequence_no": batch.sequence_no,
                "results": [dict(item) for item in batch.results],
                "artifacts": [
                    {
                        "uri": item.uri,
                        "sha256": item.sha256,
                        "size_bytes": item.size_bytes,
                        "content_type": item.content_type,
                        "compression": item.compression,
                        "metadata": dict(item.metadata),
                    }
                    for item in batch.artifacts
                ],
                "cursor_after": batch.cursor_after,
                "final": batch.final,
            },
        )
        return str(value["status"])

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
            "/v2/providers/observation",
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
        request_id: str,
        ttl_seconds: float = 30.0,
        retry_after_seconds: float | None = None,
    ) -> ProviderPermit | None:
        while True:
            value = await self._post(
                "/v2/providers/permit",
                {
                    "provider": provider,
                    "task_id": lease.task_id,
                    "generation": lease.generation,
                    "permit_request_id": request_id,
                    "ttl_seconds": float(ttl_seconds),
                },
            )
            raw = value.get("permit")
            if raw is not None:
                if not isinstance(raw, Mapping):
                    raise CoordinatorError("invalid provider permit")
                return ProviderPermit(
                    permit_id=str(raw["permit_id"]),
                    request_id=str(raw["request_id"]),
                    provider=str(raw["provider"]),
                    worker_id=str(raw["worker_id"]),
                    worker_instance_id=str(raw["worker_instance_id"]),
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
        response_bytes: int = 0,
    ) -> None:
        await self._post(
            "/v2/providers/report",
            {
                "permit_id": permit_id,
                "status_code": status_code,
                "cooldown_seconds": float(cooldown_seconds),
                "response_bytes": int(response_bytes),
            },
        )
