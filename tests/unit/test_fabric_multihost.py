from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from creeper.distributed.coordinator_client import (
    CoordinatorClient,
    CoordinatorUploadBudgetExceededError,
)
from creeper.distributed.models import ProviderPermit, WorkerDescriptor
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


if __name__=="__main__":
    unittest.main()
