from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path

import httpx

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorTransportError,
    CoordinatorUploadBudgetExceededError,
)
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import (
    ProviderPermit,
    TaskClass,
    TaskLease,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.distributed.worker import DistributedWorker


class _Keeper:
    def __init__(self) -> None:
        self.lease=object()

    def assert_owned(self) -> None:
        return None


class _ObservationClient:
    def __init__(self) -> None:
        self.reports=[]
        self.observations=[]

    async def provider_report(self, permit_id, **kwargs) -> None:
        self.reports.append((permit_id,kwargs))

    async def provider_observation(self, lease, **kwargs) -> str:
        self.observations.append((lease,kwargs))
        return "QUALIFIED"


def _lease(deadline: float) -> TaskLease:
    work=WorkDefinition(
        producer="fixture",
        task_class=TaskClass.RESIDUAL_QUERY,
        input_identity="fixture-input",
        payload={},
        partition="fixture",
        algorithm_version="fixture-v1",
        required_capabilities=("TEST",),
    )
    return TaskLease(
        task_id="task-a",
        work_key=work.work_key,
        worker_id="worker-a",
        worker_instance_id="instance-a",
        generation=1,
        lease_deadline=deadline,
        attempt=1,
        work=work,
    )


class _TransientRenewClient:
    def __init__(self) -> None:
        self.calls=0

    async def renew(self, lease, *, lease_seconds):
        self.calls+=1
        if self.calls==1:
            raise CoordinatorTransportError("authority restart")
        return replace(
            lease,
            lease_deadline=time.time()+float(lease_seconds),
        )


class _FailingSettlementClient(_ObservationClient):
    async def provider_report(self, permit_id, **kwargs) -> None:
        self.reports.append((permit_id,kwargs))
        raise CoordinatorTransportError("authority unavailable")


class FabricMultihostTests(unittest.IsolatedAsyncioTestCase):
    async def test_coordinator_upload_budget_blocks_before_transport(self) -> None:
        calls=[]
        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200,json={"status":"ALIVE"})

        client=CoordinatorClient(
            "http://10.77.0.1:8088",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            secret="secret",
            transport=httpx.MockTransport(handler),
            upload_reserver=lambda _amount: False,
            upload_overhead_bytes=1536,
        )
        try:
            with self.assertRaises(CoordinatorUploadBudgetExceededError):
                await client.heartbeat()
            self.assertEqual(calls,[])
        finally:
            await client.aclose()

    async def test_coordinator_upload_accounting_includes_overhead(self) -> None:
        reserved=[]
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200,json={"status":"ALIVE"})

        client=CoordinatorClient(
            "http://10.77.0.1:8088",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            secret="secret",
            transport=httpx.MockTransport(handler),
            upload_reserver=lambda amount: reserved.append(amount) or True,
            upload_overhead_bytes=1536,
        )
        try:
            await client.heartbeat()
        finally:
            await client.aclose()
        self.assertEqual(len(reserved),1)
        self.assertGreaterEqual(reserved[0],1538)

    async def test_retryable_authority_http_status_is_transport_failure(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503,text="restarting")

        client=CoordinatorClient(
            "http://10.77.0.1:8088",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            secret="secret",
            transport=httpx.MockTransport(handler),
        )
        try:
            with self.assertRaises(CoordinatorTransportError):
                await client.heartbeat()
        finally:
            await client.aclose()

    async def test_lease_keeper_survives_one_transient_renewal_failure(self) -> None:
        client=_TransientRenewClient()
        keeper=LeaseKeeper(
            client,  # type: ignore[arg-type]
            _lease(time.time()+1.0),
            lease_seconds=1.0,
            renew_fraction=0.05,
            min_renew_interval=0.02,
        )
        async with keeper:
            await asyncio.sleep(0.4)
            keeper.assert_owned()
        self.assertGreaterEqual(client.calls,2)
        self.assertFalse(keeper.lost)

    async def test_provider_settlement_outage_does_not_fail_completed_io(self) -> None:
        client=_FailingSettlementClient()
        keeper=_Keeper()
        gate=DistributedProviderGate(
            client,  # type: ignore[arg-type]
            keeper,  # type: ignore[arg-type]
            "datacite",
            budget_poll_seconds=0.001,
            settlement_timeout_seconds=0.05,
            observation_timeout_seconds=0.05,
        )
        permit=ProviderPermit(
            permit_id="permit-settlement",
            request_id="request-settlement",
            provider="datacite",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            task_id="task-a",
            generation=1,
            allowed_requests=1,
            max_inflight=2,
            expires_at=999999.0,
        )

        await gate.report(permit,200,httpx.Headers(),123)

        self.assertEqual(len(client.reports),2)
        self.assertEqual(client.observations,[])

    async def test_provider_gate_records_region_observation_after_settlement(self) -> None:
        client=_ObservationClient()
        keeper=_Keeper()
        gate=DistributedProviderGate(client,keeper,"datacite")
        permit=ProviderPermit(
            permit_id="permit-a",
            request_id="request-a",
            provider="datacite",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            task_id="task-a",
            generation=1,
            allowed_requests=1,
            max_inflight=2,
            expires_at=999999.0,
        )

        await gate.report(
            permit,
            200,
            httpx.Headers(),
            321,
        )

        self.assertEqual(len(client.reports),1)
        self.assertEqual(len(client.observations),1)
        _lease,observation=client.observations[0]
        self.assertTrue(observation["connect_success"])
        self.assertFalse(observation["timeout"])
        self.assertFalse(observation["policy_block"])
        self.assertEqual(observation["response_bytes"],321)

    async def test_pending_spool_is_replayed_before_any_new_claim(self) -> None:
        class Spool:
            def pending_count(self) -> int:
                return 1

        class Worker(DistributedWorker):
            def __init__(self) -> None:
                descriptor=WorkerDescriptor(
                    worker_id="worker-a",
                    worker_instance_id="instance-a",
                    runtime_class="test",
                    region="test-region",
                    architecture="x86_64",
                    memory_bytes=1024,
                    cpu_count=1,
                    network_class="test",
                    capabilities=("TEST",),
                    producers=("test-producer",),
                )
                super().__init__(
                    client=object(),  # type: ignore[arg-type]
                    descriptor=descriptor,
                    producers={"test-producer":object()},  # type: ignore[dict-item]
                    spool=Spool(),  # type: ignore[arg-type]
                )
                self.replayed=False
                self.claimed=False

            async def _replay_spool(self) -> None:
                self.replayed=True

            async def run_once(self) -> bool:
                self.claimed=True
                return True

        worker=Worker()
        self.assertTrue(await worker._run_or_replay_once())
        self.assertTrue(worker.replayed)
        self.assertFalse(worker.claimed)

    async def test_generic_403_does_not_poison_region_as_policy_block(self) -> None:
        client=_ObservationClient()
        keeper=_Keeper()
        gate=DistributedProviderGate(client,keeper,"datacite")
        permit=ProviderPermit(
            permit_id="permit-403",
            request_id="request-403",
            provider="datacite",
            worker_id="worker-a",
            worker_instance_id="instance-a",
            task_id="task-a",
            generation=1,
            allowed_requests=1,
            max_inflight=2,
            expires_at=999999.0,
        )
        await gate.report(permit,403,httpx.Headers(),0)
        self.assertFalse(client.observations[-1][1]["policy_block"])



if __name__=="__main__":
    unittest.main()
