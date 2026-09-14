from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from aiohttp.test_utils import TestServer

from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.models import (
    Capability,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.worker import DistributedWorker


class DistributedWorkerNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = DistributedAuthorityStore(
            Path(self.tmp.name) / "authority.sqlite3"
        )
        self.credentials = {
            "worker-a": "secret-a",
            "worker-b": "secret-b",
        }
        self.server = TestServer(
            create_authority_app(self.store, self.credentials)
        )
        await self.server.start_server()
        self.base_url = str(self.server.make_url("")).rstrip("/")

    async def asyncTearDown(self) -> None:
        await self.server.close()
        self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def descriptor(worker_id: str, region: str) -> WorkerDescriptor:
        return WorkerDescriptor(
            worker_id=worker_id,
            runtime_class="vm",
            region=region,
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.ONLINE_QUERY.value,),
        )

    async def test_two_network_workers_execute_one_work_key_once(self) -> None:
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="TestProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="example.com",
                coverage={"scope": "HOST", "year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="test-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        executed: list[str] = []

        async def executor(lease, client, keeper) -> None:
            keeper.assert_owned()
            executed.append(lease.worker_id)
            status = await client.commit_batch(
                lease,
                sequence_no=0,
                results=[
                    {
                        "kind": "H",
                        "hostname": "example.com",
                    }
                ],
                cursor_after="done",
            )
            self.assertEqual(status, "COMMITTED")
            await asyncio.sleep(0)

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client_a, CoordinatorClient(
            self.base_url,
            worker_id="worker-b",
            secret=self.credentials["worker-b"],
        ) as client_b:
            worker_a = DistributedWorker(
                client_a,
                self.descriptor("worker-a", "us-test"),
                {"TestProducer": executor},
                lease_seconds=30,
            )
            worker_b = DistributedWorker(
                client_b,
                self.descriptor("worker-b", "eu-test"),
                {"TestProducer": executor},
                lease_seconds=30,
            )
            reports = await asyncio.gather(
                worker_a.run_once(),
                worker_b.run_once(),
            )

        self.assertEqual(sum(report.claimed for report in reports), 1)
        self.assertEqual(sum(report.completed for report in reports), 1)
        self.assertEqual(len(executed), 1)
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], "COMPLETE")
        self.assertEqual(self.store.batch_count(task_id), 1)

    async def test_worker_without_required_capability_cannot_claim(self) -> None:
        self.store.admit_work(
            WorkDefinition(
                producer="BulkProducer",
                task_class=TaskClass.SOURCE_SHARD,
                input_identity="fixture:bulk",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="shard-0",
                algorithm_version="bulk-v1",
                required_capabilities=(Capability.STREAMING_BULK.value,),
            )
        )

        async def executor(_lease, _client, _keeper) -> None:
            raise AssertionError("ineligible worker executed bulk task")

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                self.descriptor("worker-a", "gcp-us"),
                {"BulkProducer": executor},
            )
            report = await worker.run_once()

        self.assertFalse(report.claimed)

    async def test_worker_producer_failure_returns_task_to_ready(self) -> None:
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="FailingProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="failure.example",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="test-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )

        async def executor(_lease, _client, keeper) -> None:
            keeper.assert_owned()
            raise RuntimeError("fixture failure")

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                self.descriptor("worker-a", "oci-test"),
                {"FailingProducer": executor},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.failed)
        self.assertEqual(self.store.task_row(task_id)["state"], "READY")


if __name__ == "__main__":
    unittest.main()
