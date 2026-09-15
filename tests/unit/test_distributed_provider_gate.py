from __future__ import annotations

import unittest

import httpx

from creeper.distributed.coordinator_client import CoordinatorTransportError
from creeper.distributed.models import ProviderPermit
from creeper.distributed.provider_gate import DistributedProviderGate


class _Lease:
    task_id = "task-1"
    generation = 1


class _Keeper:
    lease = _Lease()

    def assert_owned(self) -> None:
        return None


class _Client:
    def __init__(self) -> None:
        self.permit_request_ids: list[str] = []
        self.permit_calls = 0
        self.report_calls = 0

    async def provider_permit(
        self,
        _lease,
        _provider: str,
        *,
        request_id: str,
        ttl_seconds: float,
    ) -> ProviderPermit:
        self.permit_calls += 1
        self.permit_request_ids.append(request_id)
        if self.permit_calls < 3:
            raise CoordinatorTransportError("lost permit ACK")
        return ProviderPermit(
            permit_id="permit-1",
            request_id=request_id,
            provider="internet_archive",
            worker_id="worker-a",
            task_id="task-1",
            generation=1,
            allowed_requests=1,
            max_inflight=1,
            expires_at=10_000.0,
        )

    async def provider_report(
        self,
        permit_id: str,
        *,
        status_code: int | None,
        cooldown_seconds: float,
        response_bytes: int,
    ) -> None:
        self.report_calls += 1
        self.last_report = (
            permit_id,
            status_code,
            cooldown_seconds,
            response_bytes,
        )
        if self.report_calls < 3:
            raise CoordinatorTransportError("lost report ACK")


class DistributedProviderGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_permit_transport_retry_reuses_request_identity(self) -> None:
        client = _Client()
        gate = DistributedProviderGate(
            client,  # type: ignore[arg-type]
            _Keeper(),  # type: ignore[arg-type]
            "internet_archive",
            budget_poll_seconds=0.001,
        )

        permit = await gate.acquire()

        self.assertIsInstance(permit, ProviderPermit)
        self.assertEqual(client.permit_calls, 3)
        self.assertEqual(len(set(client.permit_request_ids)), 1)
        self.assertEqual(
            permit.request_id,
            client.permit_request_ids[0],
        )

    async def test_provider_report_retries_same_permit_id(self) -> None:
        client = _Client()
        gate = DistributedProviderGate(
            client,  # type: ignore[arg-type]
            _Keeper(),  # type: ignore[arg-type]
            "internet_archive",
            budget_poll_seconds=0.001,
            throttle_floor_seconds=2.0,
        )
        permit = ProviderPermit(
            permit_id="permit-1",
            request_id="request-1",
            provider="internet_archive",
            worker_id="worker-a",
            task_id="task-1",
            generation=1,
            allowed_requests=1,
            max_inflight=1,
            expires_at=10_000.0,
        )

        await gate.report(
            permit,
            503,
            httpx.Headers({"Retry-After": "4"}),
            123,
        )

        self.assertEqual(client.report_calls, 3)
        self.assertEqual(
            client.last_report,
            ("permit-1", 503, 4.0, 123),
        )


if __name__ == "__main__":
    unittest.main()
