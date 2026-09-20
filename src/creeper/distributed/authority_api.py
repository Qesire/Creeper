"""HTTP control plane for Creeper Fabric v2."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

from aiohttp import web

from creeper.distributed.auth import AuthenticationError, HMACRequestAuthenticator
from creeper.distributed.authority_store import (
    BatchConflictError,
    BatchSequenceError,
    DistributedAuthorityStore,
    FabricProtocolMismatchError,
    ProviderAccessDeniedError,
    StaleLeaseError,
    WorkerEgressBudgetExceededError,
    WorkerRejectedError,
)
from creeper.distributed.edition import edition_metadata
from creeper.distributed.models import ArtifactRef, ResultBatch, WorkerDescriptor


def _lease_payload(lease) -> dict:
    work = lease.work
    return {
        "task_id": lease.task_id,
        "work_key": lease.work_key,
        "worker_id": lease.worker_id,
        "worker_instance_id": lease.worker_instance_id,
        "generation": lease.generation,
        "lease_deadline": lease.lease_deadline,
        "attempt": lease.attempt,
        "cursor": lease.cursor,
        "next_sequence_no": lease.next_sequence_no,
        "work": {
            "producer": work.producer,
            "task_class": work.task_class.value,
            "input_identity": work.input_identity,
            "payload": dict(work.payload),
            "partition": work.partition,
            "algorithm_version": work.algorithm_version,
            "required_capabilities": list(work.required_capabilities),
            "required_providers": list(work.required_providers),
            "priority": work.priority,
            "max_attempts": work.max_attempts,
            "not_before": work.not_before,
        },
    }


def create_authority_app(
    store: DistributedAuthorityStore,
    credentials: Mapping[str, str | bytes],
    *,
    max_clock_skew_seconds: float = 300.0,
    max_request_body_bytes: int = 32 * 1024 * 1024,
) -> web.Application:
    authenticator = HMACRequestAuthenticator(
        store,
        credentials,
        max_clock_skew_seconds=max_clock_skew_seconds,
    )

    @web.middleware
    async def errors(request: web.Request, handler):
        try:
            return await handler(request)
        except AuthenticationError as exc:
            return web.json_response(
                {"error": "AUTHENTICATION", "detail": str(exc)},
                status=401,
            )
        except StaleLeaseError as exc:
            return web.json_response(
                {"error": "STALE_LEASE", "detail": str(exc)},
                status=409,
            )
        except (
            BatchConflictError,
            BatchSequenceError,
            WorkerRejectedError,
            FabricProtocolMismatchError,
            ProviderAccessDeniedError,
            WorkerEgressBudgetExceededError,
            ValueError,
        ) as exc:
            return web.json_response(
                {"error": type(exc).__name__, "detail": str(exc)},
                status=400,
            )

    @web.middleware
    async def authenticate(request: web.Request, handler):
        if not request.path.startswith("/v2/"):
            return await handler(request)
        body = await request.read()
        worker_id = request.headers.get("X-Creeper-Worker", "")
        instance_id = request.headers.get("X-Creeper-Instance", "")
        verified_worker, verified_instance = authenticator.verify(
            worker_id=worker_id,
            worker_instance_id=instance_id,
            method=request.method,
            path=request.path,
            body=body,
            timestamp=request.headers.get("X-Creeper-Timestamp", ""),
            nonce=request.headers.get("X-Creeper-Nonce", ""),
            signature=request.headers.get("X-Creeper-Signature", ""),
        )
        request["worker_id"] = verified_worker
        request["worker_instance_id"] = verified_instance
        return await handler(request)

    if max_request_body_bytes < 1024 * 1024:
        raise ValueError("max_request_body_bytes must be at least 1 MiB")
    app = web.Application(
        middlewares=[errors, authenticate],
        client_max_size=max_request_body_bytes,
    )

    async def health(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def meta(_request: web.Request) -> web.Response:
        return web.json_response(edition_metadata() | store.status_snapshot())

    async def register(request: web.Request) -> web.Response:
        data = await request.json()
        descriptor = WorkerDescriptor(
            worker_id=str(data["worker_id"]),
            worker_instance_id=str(data["worker_instance_id"]),
            runtime_class=str(data["runtime_class"]),
            region=str(data["region"]),
            architecture=str(data["architecture"]),
            memory_bytes=int(data["memory_bytes"]),
            cpu_count=int(data["cpu_count"]),
            network_class=str(data["network_class"]),
            capabilities=tuple(str(v) for v in data.get("capabilities", ())),
            producers=tuple(str(v) for v in data.get("producers", ())),
            allowed_providers=tuple(
                str(v) for v in data.get("allowed_providers", ())
            ),
            daily_egress_budget_bytes=int(
                data.get("daily_egress_budget_bytes", 0)
            ),
            protocol_version=str(data["protocol_version"]),
            edition_version=str(data["edition_version"]),
        )
        if (
            descriptor.worker_id != request["worker_id"]
            or descriptor.worker_instance_id != request["worker_instance_id"]
        ):
            raise AuthenticationError("signed identity does not match descriptor")
        store.register_worker(descriptor)
        return web.json_response({"status": "REGISTERED"})

    async def heartbeat(request: web.Request) -> web.Response:
        store.heartbeat(
            str(request["worker_id"]),
            str(request["worker_instance_id"]),
        )
        return web.json_response({"status": "ALIVE"})

    async def claim(request: web.Request) -> web.Response:
        data = await request.json()
        lease_seconds = float(data.get("lease_seconds", 300.0))
        wait_seconds = float(data.get("wait_seconds", 0.0))
        if not 0 <= wait_seconds <= 25:
            raise ValueError("wait_seconds must be within [0,25]")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_seconds
        while True:
            lease = store.claim_work(
                str(request["worker_id"]),
                str(request["worker_instance_id"]),
                lease_seconds=lease_seconds,
            )
            if lease is not None or loop.time() >= deadline:
                return web.json_response(
                    {"task": None if lease is None else _lease_payload(lease)}
                )
            # Long-poll workers can remain connected while idle; one
            # PostgreSQL claim scan per second is sufficient and avoids
            # multiplying 4 Hz idle DB traffic by every free-cloud worker.
            await asyncio.sleep(min(1.0, max(0.0, deadline - loop.time())))

    async def renew(request: web.Request) -> web.Response:
        data = await request.json()
        lease = store.renew_task(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            generation=int(data["generation"]),
            lease_seconds=float(data.get("lease_seconds", 300.0)),
            expected_lease_deadline=float(data["expected_lease_deadline"]),
        )
        return web.json_response({"task": _lease_payload(lease)})

    async def fail(request: web.Request) -> web.Response:
        data = await request.json()
        store.fail_task(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            generation=int(data["generation"]),
            error=str(data.get("error", "")),
            retryable=bool(data.get("retryable", True)),
            retry_after_seconds=float(data.get("retry_after_seconds", 0.0)),
        )
        return web.json_response({"status": "FAILED_RECORDED"})

    async def finish(request: web.Request) -> web.Response:
        data = await request.json()
        store.finish_task(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            generation=int(data["generation"]),
        )
        return web.json_response({"status": "COMPLETE"})

    async def commit_batch(request: web.Request) -> web.Response:
        data = await request.json()
        raw_results = data.get("results", ())
        raw_artifacts = data.get("artifacts", ())
        if (
            not isinstance(raw_results, list)
            or any(not isinstance(item, Mapping) for item in raw_results)
            or not isinstance(raw_artifacts, list)
            or any(not isinstance(item, Mapping) for item in raw_artifacts)
        ):
            raise ValueError("results/artifacts must be arrays of objects")
        batch = ResultBatch(
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            sequence_no=int(data["sequence_no"]),
            results=tuple(dict(item) for item in raw_results),
            artifacts=tuple(
                ArtifactRef(
                    uri=str(item["uri"]),
                    sha256=str(item["sha256"]),
                    size_bytes=int(item["size_bytes"]),
                    content_type=str(
                        item.get("content_type", "application/octet-stream")
                    ),
                    compression=str(item.get("compression", "none")),
                    metadata=dict(item.get("metadata", {})),
                )
                for item in raw_artifacts
            ),
            cursor_after=(
                None if data.get("cursor_after") is None
                else str(data["cursor_after"])
            ),
            final=bool(data.get("final", False)),
        )
        committed = store.commit_result_batch(
            batch,
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
        )
        return web.json_response(
            {
                "status": "COMMITTED" if committed else "ALREADY_COMMITTED",
                "batch_id": batch.batch_id,
            }
        )

    async def provider_observation(request: web.Request) -> web.Response:
        data = await request.json()
        state = store.record_provider_region_observation(
            str(data["provider"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            connect_success=bool(data.get("connect_success", False)),
            status_code=(
                None if data.get("status_code") is None
                else int(data["status_code"])
            ),
            latency_ms=float(data.get("latency_ms", 0.0)),
            response_bytes=int(data.get("response_bytes", 0)),
            timeout=bool(data.get("timeout", False)),
            policy_block=bool(data.get("policy_block", False)),
        )
        return web.json_response({"state": state})

    async def provider_permit(request: web.Request) -> web.Response:
        data = await request.json()
        request_id = str(data.get("permit_request_id", "")).strip()
        if not request_id:
            raise ValueError("permit_request_id is required")
        permit = store.issue_provider_permit(
            str(data["provider"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            request_id=request_id,
            ttl_seconds=float(data.get("ttl_seconds", 30.0)),
        )
        if permit is None:
            return web.json_response({"permit": None, "reason": "BUDGET_WAIT"})
        return web.json_response(
            {
                "permit": {
                    "permit_id": permit.permit_id,
                    "request_id": permit.request_id,
                    "provider": permit.provider,
                    "worker_id": permit.worker_id,
                    "worker_instance_id": permit.worker_instance_id,
                    "task_id": permit.task_id,
                    "generation": permit.generation,
                    "allowed_requests": permit.allowed_requests,
                    "max_inflight": permit.max_inflight,
                    "expires_at": permit.expires_at,
                }
            }
        )

    async def provider_report(request: web.Request) -> web.Response:
        data = await request.json()
        store.report_provider_permit(
            str(data["permit_id"]),
            worker_id=str(request["worker_id"]),
            worker_instance_id=str(request["worker_instance_id"]),
            status_code=(
                None if data.get("status_code") is None
                else int(data["status_code"])
            ),
            cooldown_seconds=float(data.get("cooldown_seconds", 0.0)),
            response_bytes=int(data.get("response_bytes", 0)),
        )
        return web.json_response({"status": "RECORDED"})

    app.router.add_get("/healthz", health)
    app.router.add_get("/meta", meta)
    app.router.add_post("/v2/workers/register", register)
    app.router.add_post("/v2/heartbeat", heartbeat)
    app.router.add_post("/v2/tasks/claim", claim)
    app.router.add_post("/v2/tasks/renew", renew)
    app.router.add_post("/v2/tasks/fail", fail)
    app.router.add_post("/v2/tasks/finish", finish)
    app.router.add_post("/v2/results/batch", commit_batch)
    app.router.add_post("/v2/providers/observation", provider_observation)
    app.router.add_post("/v2/providers/permit", provider_permit)
    app.router.add_post("/v2/providers/report", provider_report)
    return app
