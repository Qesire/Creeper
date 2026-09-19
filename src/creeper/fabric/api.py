"""aiohttp worker API for Fabric v2."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from typing import Any

from aiohttp import web

from .auth import AuthenticationError, HMACRequestAuthenticator
from .models import (
    FABRIC_PROTOCOL_VERSION,
    FabricCapability,
    LeaseToken,
    ProviderPermit,
    ResultBatch,
    WorkerDescriptor,
)
from .store import (
    BatchConflictError,
    BatchSequenceError,
    StaleLeaseError,
    WorkerRejectedError,
)


def _body(request: web.Request) -> dict[str, Any]:
    raw = request.get("_fabric_body", b"")
    if not raw:
        return {}
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    return value


def _work_payload(lease: LeaseToken) -> dict[str, object]:
    work = lease.work
    return {
        "work_class": work.work_class.value,
        "producer": work.producer,
        "algorithm_version": work.algorithm_version,
        "partition_key": work.partition_key,
        "input_identity": work.input_identity,
        "coverage": dict(work.coverage),
        "required_capabilities": [
            item.value for item in work.required_capabilities
        ],
        "priority": work.priority,
        "queue": work.queue,
        "max_attempts": work.max_attempts,
        "provider": work.provider,
        "min_memory_bytes": work.min_memory_bytes,
        "network_class": work.network_class,
    }


def lease_payload(lease: LeaseToken) -> dict[str, object]:
    return {
        "task_id": lease.task_id,
        "work_key": lease.work_key,
        "worker_id": lease.worker_id,
        "lease_epoch": lease.lease_epoch,
        "lease_deadline": lease.lease_deadline,
        "attempt": lease.attempt,
        "cursor": lease.cursor,
        "next_sequence_no": lease.next_sequence_no,
        "work": _work_payload(lease),
    }


def permit_payload(permit: ProviderPermit) -> dict[str, object]:
    return {
        "permit_id": permit.permit_id,
        "provider": permit.provider,
        "worker_id": permit.worker_id,
        "task_id": permit.task_id,
        "lease_epoch": permit.lease_epoch,
        "allowed_requests": permit.allowed_requests,
        "expires_at": permit.expires_at,
    }


@web.middleware
async def _error_middleware(request: web.Request, handler):
    try:
        return await handler(request)
    except AuthenticationError as exc:
        return web.json_response(
            {"error": "AUTHENTICATION_FAILED", "detail": str(exc)},
            status=401,
        )
    except WorkerRejectedError as exc:
        return web.json_response(
            {"error": "WORKER_REJECTED", "detail": str(exc)},
            status=403,
        )
    except StaleLeaseError as exc:
        return web.json_response(
            {"error": "STALE_LEASE", "detail": str(exc)},
            status=409,
        )
    except BatchConflictError as exc:
        return web.json_response(
            {"error": "BATCH_CONFLICT", "detail": str(exc)},
            status=409,
        )
    except BatchSequenceError as exc:
        return web.json_response(
            {"error": "BATCH_SEQUENCE", "detail": str(exc)},
            status=409,
        )
    except KeyError as exc:
        return web.json_response(
            {"error": "NOT_FOUND", "detail": str(exc)},
            status=404,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        return web.json_response(
            {"error": "INVALID_REQUEST", "detail": str(exc)},
            status=400,
        )


def create_fabric_app(
    store,
    credentials: Mapping[str, str | bytes],
    *,
    clock=time.time,
    max_clock_skew_seconds: float = 300.0,
) -> web.Application:
    authenticator = HMACRequestAuthenticator(
        store,
        credentials,
        clock=clock,
        max_clock_skew_seconds=max_clock_skew_seconds,
    )

    @web.middleware
    async def authentication_middleware(request: web.Request, handler):
        if not request.path.startswith("/v2/"):
            return await handler(request)
        raw = await request.read()
        request["_fabric_body"] = raw
        worker_id = request.headers.get("X-Creeper-Worker", "")
        authenticator.verify(
            worker_id=worker_id,
            method=request.method,
            path=request.path,
            body=raw,
            timestamp=request.headers.get("X-Creeper-Timestamp", ""),
            nonce=request.headers.get("X-Creeper-Nonce", ""),
            signature=request.headers.get("X-Creeper-Signature", ""),
        )
        request["worker_id"] = worker_id
        return await handler(request)

    app = web.Application(
        middlewares=[_error_middleware, authentication_middleware]
    )
    app["fabric_store"] = store
    app["fabric_authenticator"] = authenticator

    async def health(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "protocol_version": FABRIC_PROTOCOL_VERSION,
                "role": "fabric-authority",
            }
        )

    async def register(request: web.Request) -> web.Response:
        data = _body(request)
        worker_id = str(request["worker_id"])
        if str(data.get("worker_id", "")) != worker_id:
            raise ValueError("signed worker_id does not match descriptor")
        worker = WorkerDescriptor(
            worker_id=worker_id,
            region=str(data.get("region", "")),
            runtime_class=str(data.get("runtime_class", "")),
            architecture=str(data.get("architecture", "")),
            network_class=str(data.get("network_class", "")),
            cpu_count=int(data.get("cpu_count", 0)),
            memory_bytes=int(data.get("memory_bytes", 0)),
            capabilities=tuple(
                FabricCapability(item)
                for item in data.get("capabilities", ())
            ),
            allowed_providers=tuple(
                str(item) for item in data.get("allowed_providers", ())
            ),
            max_concurrency=int(data.get("max_concurrency", 1)),
            labels={
                str(k): str(v)
                for k, v in dict(data.get("labels", {})).items()
            },
            protocol_version=str(
                data.get("protocol_version", FABRIC_PROTOCOL_VERSION)
            ),
            edition=str(data.get("edition", "unknown")),
        )
        store.register_worker(worker)
        return web.json_response(
            {"status": "REGISTERED", "worker_id": worker_id}
        )

    async def heartbeat(request: web.Request) -> web.Response:
        store.heartbeat(str(request["worker_id"]))
        return web.json_response({"status": "OK"})

    async def claim(request: web.Request) -> web.Response:
        data = _body(request)
        wait_seconds = float(data.get("wait_seconds", 0.0))
        lease_seconds = float(data.get("lease_seconds", 60.0))
        queue = str(data.get("queue", "default"))
        if not 0.0 <= wait_seconds <= 25.0:
            raise ValueError("wait_seconds must be within [0, 25]")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_seconds
        while True:
            lease = store.claim(
                str(request["worker_id"]),
                queue=queue,
                lease_seconds=lease_seconds,
            )
            if lease is not None or loop.time() >= deadline:
                return web.json_response(
                    {"task": None if lease is None else lease_payload(lease)}
                )
            await asyncio.sleep(min(0.25, max(0.0, deadline - loop.time())))

    def request_lease(request: web.Request, data: dict[str, Any]) -> LeaseToken:
        return store.current_lease(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            lease_epoch=int(data["lease_epoch"]),
        )

    async def renew(request: web.Request) -> web.Response:
        data = _body(request)
        lease = request_lease(request, data)
        renewed = store.renew(
            lease,
            lease_seconds=float(data.get("lease_seconds", 60.0)),
        )
        return web.json_response({"task": lease_payload(renewed)})

    async def commit_batch(request: web.Request) -> web.Response:
        data = _body(request)
        raw_results = data.get("results", [])
        if not isinstance(raw_results, list) or any(
            not isinstance(item, Mapping) for item in raw_results
        ):
            raise ValueError("results must be a list of JSON objects")
        lease = request_lease(request, data)
        cursor_after = data.get("cursor_after")
        if cursor_after is not None and not isinstance(cursor_after, Mapping):
            raise ValueError("cursor_after must be an object or null")
        batch = ResultBatch(
            task_id=lease.task_id,
            lease_epoch=lease.lease_epoch,
            sequence_no=int(data["sequence_no"]),
            results=tuple(dict(item) for item in raw_results),
            cursor_after=(
                None if cursor_after is None else dict(cursor_after)
            ),
        )
        committed = store.commit_batch(lease, batch)
        return web.json_response(
            {
                "status": (
                    "COMMITTED" if committed else "ALREADY_COMMITTED"
                ),
                "batch_key": batch.batch_key,
                "payload_digest": batch.payload_digest,
            }
        )

    async def complete(request: web.Request) -> web.Response:
        data = _body(request)
        lease = request_lease(request, data)
        store.complete(lease)
        return web.json_response({"status": "COMPLETE"})

    async def fail(request: web.Request) -> web.Response:
        data = _body(request)
        lease = request_lease(request, data)
        store.fail(
            lease,
            error=str(data.get("error", "")),
            retryable=bool(data.get("retryable", True)),
            retry_delay_seconds=float(data.get("retry_delay_seconds", 0.0)),
        )
        return web.json_response({"status": "FAILURE_RECORDED"})

    async def provider_permit(request: web.Request) -> web.Response:
        data = _body(request)
        lease = request_lease(request, data)
        permit = store.acquire_provider_permit(
            lease,
            str(data["provider"]),
            allowed_requests=int(data.get("allowed_requests", 1)),
            ttl_seconds=float(data.get("ttl_seconds", 30.0)),
        )
        return web.json_response(
            {
                "permit": (
                    None if permit is None else permit_payload(permit)
                )
            }
        )

    async def provider_report(request: web.Request) -> web.Response:
        data = _body(request)
        permit = ProviderPermit(
            permit_id=str(data["permit_id"]),
            provider=str(data["provider"]),
            worker_id=str(request["worker_id"]),
            task_id=str(data["task_id"]),
            lease_epoch=int(data["lease_epoch"]),
            allowed_requests=int(data.get("allowed_requests", 1)),
            expires_at=float(data["expires_at"]),
        )
        store.report_provider_permit(
            permit,
            status_code=(
                None
                if data.get("status_code") is None
                else int(data["status_code"])
            ),
            response_bytes=int(data.get("response_bytes", 0)),
            throttled=bool(data.get("throttled", False)),
            cooldown_seconds=float(data.get("cooldown_seconds", 0.0)),
        )
        return web.json_response({"status": "RECORDED"})

    app.router.add_get("/healthz", health)
    app.router.add_post("/v2/workers/register", register)
    app.router.add_post("/v2/workers/heartbeat", heartbeat)
    app.router.add_post("/v2/tasks/claim", claim)
    app.router.add_post("/v2/tasks/renew", renew)
    app.router.add_post("/v2/tasks/batch", commit_batch)
    app.router.add_post("/v2/tasks/complete", complete)
    app.router.add_post("/v2/tasks/fail", fail)
    app.router.add_post("/v2/providers/permit", provider_permit)
    app.router.add_post("/v2/providers/report", provider_report)
    return app
