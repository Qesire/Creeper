"""aiohttp Authority API for distributed Creeper workers."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any

from aiohttp import web

from creeper.distributed.auth import AuthenticationError, HMACRequestAuthenticator
from creeper.distributed.authority_store import (
    AuthorityNotReadyError,
    BatchConflictError,
    DistributedAuthorityStore,
    StaleLeaseError,
    WorkerRejectedError,
)
from creeper.distributed.models import (
    ResultBatch,
    TaskLease,
    WorkerDescriptor,
)


def _body(request: web.Request) -> dict[str, Any]:
    raw = request.get("_creeper_body", b"")
    if not raw:
        return {}
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("request body must be a JSON object")
    return value


def _lease_payload(lease: TaskLease) -> dict[str, Any]:
    return {
        "task_id": lease.task_id,
        "work_key": lease.work_key,
        "worker_id": lease.worker_id,
        "generation": lease.generation,
        "lease_deadline": lease.lease_deadline,
        "attempt": lease.attempt,
        "cursor": lease.cursor,
        "work": {
            "producer": lease.work.producer,
            "task_class": lease.work.task_class.value,
            "input_identity": lease.work.input_identity,
            "coverage": dict(lease.work.coverage),
            "partition": lease.work.partition,
            "algorithm_version": lease.work.algorithm_version,
            "required_capabilities": list(lease.work.required_capabilities),
            "priority": lease.work.priority,
        },
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
    except AuthorityNotReadyError as exc:
        return web.json_response(
            {"error": "AUTHORITY_NOT_READY", "detail": str(exc)},
            status=503,
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


def create_authority_app(
    store: DistributedAuthorityStore,
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
        if not request.path.startswith("/v1/"):
            return await handler(request)
        raw = await request.read()
        request["_creeper_body"] = raw
        worker_id = request.headers.get("X-Creeper-Worker", "")
        timestamp = request.headers.get("X-Creeper-Timestamp", "")
        nonce = request.headers.get("X-Creeper-Nonce", "")
        signature = request.headers.get("X-Creeper-Signature", "")
        authenticator.verify(
            worker_id=worker_id,
            method=request.method,
            path=request.path,
            body=raw,
            timestamp=timestamp,
            nonce=nonce,
            signature=signature,
        )
        request["worker_id"] = worker_id
        return await handler(request)

    app = web.Application(
        middlewares=[_error_middleware, authentication_middleware]
    )
    app["authority_store"] = store
    app["authority_authenticator"] = authenticator

    async def health(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "role": "local_authority"})

    async def register(request: web.Request) -> web.Response:
        data = _body(request)
        worker_id = str(request["worker_id"])
        if str(data.get("worker_id", "")) != worker_id:
            raise ValueError("signed worker_id does not match descriptor")
        worker = WorkerDescriptor(
            worker_id=worker_id,
            runtime_class=str(data.get("runtime_class", "")),
            region=str(data.get("region", "")),
            architecture=str(data.get("architecture", "")),
            memory_bytes=int(data.get("memory_bytes", 0)),
            cpu_count=int(data.get("cpu_count", 0)),
            network_class=str(data.get("network_class", "")),
            capabilities=tuple(str(v) for v in data.get("capabilities", ())),
        )
        store.register_worker(worker)
        return web.json_response({"status": "REGISTERED", "worker_id": worker_id})

    async def heartbeat(request: web.Request) -> web.Response:
        worker_id = str(request["worker_id"])
        store.heartbeat(worker_id)
        return web.json_response({"status": "OK"})

    async def claim(request: web.Request) -> web.Response:
        data = _body(request)
        worker_id = str(request["worker_id"])
        lease = store.claim_work(
            worker_id,
            lease_seconds=float(data.get("lease_seconds", 300.0)),
        )
        return web.json_response(
            {"task": None if lease is None else _lease_payload(lease)}
        )

    async def renew(request: web.Request) -> web.Response:
        data = _body(request)
        worker_id = str(request["worker_id"])
        lease = store.renew_task(
            str(data["task_id"]),
            worker_id=worker_id,
            generation=int(data["generation"]),
            lease_seconds=float(data.get("lease_seconds", 300.0)),
        )
        return web.json_response({"task": _lease_payload(lease)})

    async def fail(request: web.Request) -> web.Response:
        data = _body(request)
        store.fail_task(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            generation=int(data["generation"]),
            error=str(data.get("error", "")),
            retryable=bool(data.get("retryable", True)),
        )
        return web.json_response({"status": "FAILED_RECORDED"})

    async def finish(request: web.Request) -> web.Response:
        data = _body(request)
        store.finish_task(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            generation=int(data["generation"]),
        )
        return web.json_response({"status": "COMPLETE"})

    async def commit_batch(request: web.Request) -> web.Response:
        data = _body(request)
        raw_results = data.get("results", ())
        if not isinstance(raw_results, list):
            raise ValueError("results must be a list")
        batch = ResultBatch(
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            sequence_no=int(data["sequence_no"]),
            results=tuple(
                item for item in raw_results if isinstance(item, Mapping)
            ),
            cursor_after=(
                None
                if data.get("cursor_after") is None
                else str(data["cursor_after"])
            ),
        )
        if len(batch.results) != len(raw_results):
            raise ValueError("every result must be a JSON object")
        committed = store.commit_result_batch(
            batch,
            worker_id=str(request["worker_id"]),
        )
        return web.json_response(
            {
                "status": "COMMITTED" if committed else "ALREADY_COMMITTED",
                "batch_id": batch.batch_id,
            }
        )

    async def hy_probe(request: web.Request) -> web.Response:
        data = _body(request)
        probes = data.get("probes", ())
        if not isinstance(probes, list):
            raise ValueError("probes must be a list")
        if any(not isinstance(item, Mapping) for item in probes):
            raise ValueError("every HY probe must be a JSON object")
        decisions = store.probe_host_years(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            generation=int(data["generation"]),
            probes=[dict(item) for item in probes],
        )
        return web.json_response({"decisions": decisions})

    async def hy_full(request: web.Request) -> web.Response:
        data = _body(request)
        evidence = data.get("evidence", ())
        if not isinstance(evidence, list):
            raise ValueError("evidence must be a list")
        if any(not isinstance(item, Mapping) for item in evidence):
            raise ValueError("every HY evidence record must be a JSON object")
        results = store.commit_full_host_year_evidence(
            str(data["task_id"]),
            worker_id=str(request["worker_id"]),
            generation=int(data["generation"]),
            evidence=[dict(item) for item in evidence],
        )
        return web.json_response({"results": results})

    async def provider_observation(request: web.Request) -> web.Response:
        data = _body(request)
        state = store.record_provider_region_observation(
            str(data["provider"]),
            worker_id=str(request["worker_id"]),
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            connect_success=bool(data.get("connect_success", False)),
            status_code=(
                None
                if data.get("status_code") is None
                else int(data["status_code"])
            ),
            latency_ms=float(data.get("latency_ms", 0.0)),
            response_bytes=int(data.get("response_bytes", 0)),
            timeout=bool(data.get("timeout", False)),
            policy_block=bool(data.get("policy_block", False)),
        )
        return web.json_response({"state": state})

    async def provider_permit(request: web.Request) -> web.Response:
        data = _body(request)
        permit = store.issue_provider_permit(
            str(data["provider"]),
            worker_id=str(request["worker_id"]),
            task_id=str(data["task_id"]),
            generation=int(data["generation"]),
            ttl_seconds=float(data.get("ttl_seconds", 30.0)),
        )
        if permit is None:
            # This is Authority-side global budget backpressure, not an
            # external provider HTTP 429. Keep the two telemetry domains
            # separate.
            return web.json_response(
                {"permit": None, "reason": "BUDGET_WAIT"}
            )
        return web.json_response(
            {
                "permit": {
                    "permit_id": permit.permit_id,
                    "provider": permit.provider,
                    "worker_id": permit.worker_id,
                    "task_id": permit.task_id,
                    "generation": permit.generation,
                    "allowed_requests": permit.allowed_requests,
                    "max_inflight": permit.max_inflight,
                    "expires_at": permit.expires_at,
                }
            }
        )

    async def provider_report(request: web.Request) -> web.Response:
        data = _body(request)
        store.report_provider_permit(
            str(data["permit_id"]),
            worker_id=str(request["worker_id"]),
            status_code=(
                None
                if data.get("status_code") is None
                else int(data["status_code"])
            ),
            cooldown_seconds=float(data.get("cooldown_seconds", 0.0)),
        )
        return web.json_response({"status": "RECORDED"})

    app.router.add_get("/healthz", health)
    app.router.add_post("/v1/workers/register", register)
    app.router.add_post("/v1/heartbeat", heartbeat)
    app.router.add_post("/v1/tasks/claim", claim)
    app.router.add_post("/v1/tasks/renew", renew)
    app.router.add_post("/v1/tasks/fail", fail)
    app.router.add_post("/v1/tasks/finish", finish)
    app.router.add_post("/v1/results/batch", commit_batch)
    app.router.add_post("/v1/results/hy-probe", hy_probe)
    app.router.add_post("/v1/results/hy-full", hy_full)
    app.router.add_post("/v1/providers/observation", provider_observation)
    app.router.add_post("/v1/providers/permit", provider_permit)
    app.router.add_post("/v1/providers/report", provider_report)
    return app
