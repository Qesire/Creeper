"""Signed HTTP client for replaceable Fabric v2 workers."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from secrets import token_hex
from typing import Any

import httpx

from .auth import sign_request
from .models import (
    FabricCapability,
    FabricWorkClass,
    LeaseToken,
    ProviderPermit,
    WorkerDescriptor,
    WorkSpec,
)


class FabricClientError(RuntimeError):
    pass


class FabricTransportError(FabricClientError):
    pass


class StaleLeaseClientError(FabricClientError):
    pass


class FabricClient:
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
            raise ValueError("invalid fabric client configuration")
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.secret = secret
        self.clock = clock
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            transport=transport,
            trust_env=False,
        )

    async def __aenter__(self) -> "FabricClient":
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
            raise FabricTransportError(
                f"fabric authority unavailable: {type(exc).__name__}"
            ) from exc
        try:
            value = response.json()
        except ValueError as exc:
            raise FabricClientError(
                f"fabric authority returned non-JSON HTTP {response.status_code}"
            ) from exc
        if response.status_code >= 400:
            code = value.get("error") if isinstance(value, dict) else None
            detail = value.get("detail") if isinstance(value, dict) else None
            if code == "STALE_LEASE":
                raise StaleLeaseClientError(str(detail or code))
            raise FabricClientError(
                f"fabric HTTP {response.status_code}: {code or detail or value}"
            )
        if not isinstance(value, dict):
            raise FabricClientError("fabric response must be a JSON object")
        return value

    async def register(self, worker: WorkerDescriptor) -> None:
        if worker.worker_id != self.worker_id:
            raise ValueError("descriptor worker_id does not match client")
        await self._post(
            "/v2/workers/register",
            {
                "worker_id": worker.worker_id,
                "region": worker.region,
                "runtime_class": worker.runtime_class,
                "architecture": worker.architecture,
                "network_class": worker.network_class,
                "cpu_count": worker.cpu_count,
                "memory_bytes": worker.memory_bytes,
                "capabilities": [item.value for item in worker.capabilities],
                "allowed_providers": list(worker.allowed_providers),
                "max_concurrency": worker.max_concurrency,
                "labels": dict(worker.labels),
                "protocol_version": worker.protocol_version,
                "edition": worker.edition,
            },
        )

    async def heartbeat(self) -> None:
        await self._post("/v2/workers/heartbeat")

    @staticmethod
    def _parse_lease(raw: Mapping[str, Any]) -> LeaseToken:
        work_raw = raw.get("work")
        if not isinstance(work_raw, Mapping):
            raise FabricClientError("invalid lease work payload")
        work = WorkSpec(
            work_class=FabricWorkClass(str(work_raw["work_class"])),
            producer=str(work_raw["producer"]),
            algorithm_version=str(work_raw["algorithm_version"]),
            partition_key=str(work_raw["partition_key"]),
            input_identity=str(work_raw["input_identity"]),
            coverage=dict(work_raw["coverage"]),
            required_capabilities=tuple(
                FabricCapability(item)
                for item in work_raw["required_capabilities"]
            ),
            priority=float(work_raw.get("priority", 0.0)),
            queue=str(work_raw.get("queue", "default")),
            max_attempts=int(work_raw.get("max_attempts", 8)),
            provider=(
                None
                if work_raw.get("provider") is None
                else str(work_raw["provider"])
            ),
            min_memory_bytes=int(work_raw.get("min_memory_bytes", 0)),
            network_class=(
                None
                if work_raw.get("network_class") is None
                else str(work_raw["network_class"])
            ),
        )
        cursor = raw.get("cursor")
        if cursor is not None and not isinstance(cursor, Mapping):
            raise FabricClientError("invalid lease cursor payload")
        return LeaseToken(
            task_id=str(raw["task_id"]),
            work_key=str(raw["work_key"]),
            worker_id=str(raw["worker_id"]),
            lease_epoch=int(raw["lease_epoch"]),
            lease_deadline=float(raw["lease_deadline"]),
            attempt=int(raw["attempt"]),
            work=work,
            cursor=None if cursor is None else dict(cursor),
            next_sequence_no=int(raw.get("next_sequence_no", 0)),
        )

    async def claim(
        self,
        *,
        queue: str = "default",
        lease_seconds: float = 60.0,
        wait_seconds: float = 20.0,
    ) -> LeaseToken | None:
        value = await self._post(
            "/v2/tasks/claim",
            {
                "queue": queue,
                "lease_seconds": float(lease_seconds),
                "wait_seconds": float(wait_seconds),
            },
        )
        raw = value.get("task")
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise FabricClientError("invalid task payload")
        return self._parse_lease(raw)

    async def renew(
        self,
        lease: LeaseToken,
        *,
        lease_seconds: float = 60.0,
    ) -> LeaseToken:
        value = await self._post(
            "/v2/tasks/renew",
            {
                "task_id": lease.task_id,
                "lease_epoch": lease.lease_epoch,
                "lease_seconds": float(lease_seconds),
            },
        )
        raw = value.get("task")
        if not isinstance(raw, Mapping):
            raise FabricClientError("invalid renewed lease payload")
        return self._parse_lease(raw)

    async def commit_batch(
        self,
        lease: LeaseToken,
        *,
        sequence_no: int,
        results: list[Mapping[str, Any]],
        cursor_after: Mapping[str, Any] | None = None,
    ) -> str:
        value = await self._post(
            "/v2/tasks/batch",
            {
                "task_id": lease.task_id,
                "lease_epoch": lease.lease_epoch,
                "sequence_no": int(sequence_no),
                "results": [dict(item) for item in results],
                "cursor_after": (
                    None if cursor_after is None else dict(cursor_after)
                ),
            },
        )
        return str(value["status"])

    async def complete(self, lease: LeaseToken) -> None:
        await self._post(
            "/v2/tasks/complete",
            {
                "task_id": lease.task_id,
                "lease_epoch": lease.lease_epoch,
            },
        )

    async def fail(
        self,
        lease: LeaseToken,
        *,
        error: str,
        retryable: bool = True,
        retry_delay_seconds: float = 0.0,
    ) -> None:
        await self._post(
            "/v2/tasks/fail",
            {
                "task_id": lease.task_id,
                "lease_epoch": lease.lease_epoch,
                "error": error,
                "retryable": bool(retryable),
                "retry_delay_seconds": float(retry_delay_seconds),
            },
        )

    async def provider_permit(
        self,
        lease: LeaseToken,
        provider: str,
        *,
        allowed_requests: int = 1,
        ttl_seconds: float = 30.0,
        retry_after_seconds: float | None = None,
    ) -> ProviderPermit | None:
        while True:
            value = await self._post(
                "/v2/providers/permit",
                {
                    "task_id": lease.task_id,
                    "lease_epoch": lease.lease_epoch,
                    "provider": provider,
                    "allowed_requests": int(allowed_requests),
                    "ttl_seconds": float(ttl_seconds),
                },
            )
            raw = value.get("permit")
            if raw is not None:
                if not isinstance(raw, Mapping):
                    raise FabricClientError("invalid provider permit")
                return ProviderPermit(
                    permit_id=str(raw["permit_id"]),
                    provider=str(raw["provider"]),
                    worker_id=str(raw["worker_id"]),
                    task_id=str(raw["task_id"]),
                    lease_epoch=int(raw["lease_epoch"]),
                    allowed_requests=int(raw["allowed_requests"]),
                    expires_at=float(raw["expires_at"]),
                )
            if retry_after_seconds is None:
                return None
            await asyncio.sleep(float(retry_after_seconds))

    async def provider_report(
        self,
        permit: ProviderPermit,
        *,
        status_code: int | None,
        response_bytes: int = 0,
        throttled: bool = False,
        cooldown_seconds: float = 0.0,
    ) -> None:
        await self._post(
            "/v2/providers/report",
            {
                "permit_id": permit.permit_id,
                "provider": permit.provider,
                "task_id": permit.task_id,
                "lease_epoch": permit.lease_epoch,
                "allowed_requests": permit.allowed_requests,
                "expires_at": permit.expires_at,
                "status_code": status_code,
                "response_bytes": int(response_bytes),
                "throttled": bool(throttled),
                "cooldown_seconds": float(cooldown_seconds),
            },
        )
