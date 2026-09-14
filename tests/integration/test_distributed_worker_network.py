from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import httpx
from aiohttp.test_utils import TestServer

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.host_query import DistributedHostQueryProducer
from creeper.distributed.models import (
    Capability,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.worker import DistributedWorker
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


class DistributedWorkerNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        baseline_dir = root / "baseline-v1"
        baseline_dir.mkdir()
        for year in range(1996, 2002):
            (baseline_dir / f"{year}.txt").write_text("", encoding="utf-8")
        (baseline_dir / "candidate_pool.txt").write_text("", encoding="utf-8")
        annual_hashes = {
            f"{year}.txt": hashlib.sha256(
                (baseline_dir / f"{year}.txt").read_bytes()
            ).hexdigest()
            for year in range(1996, 2002)
        }
        candidate_hash = hashlib.sha256(
            (baseline_dir / "candidate_pool.txt").read_bytes()
        ).hexdigest()
        manifest = {
            "baseline_id": baseline_dir.name,
            "annual_file_hashes": annual_hashes,
            "candidate_file_hash": candidate_hash,
            "model_hash": "0" * 64,
            "baseline_eed": "0",
            "authority_digest": authority_digest(
                baseline_id=baseline_dir.name,
                annual_file_hashes=annual_hashes,
                candidate_file_hash=candidate_hash,
                model_hash="0" * 64,
                baseline_eed="0",
            ),
        }
        self.baseline = BaselineIndex.build(
            baseline_dir=baseline_dir,
            output_path=root / "baseline.sqlite3",
            authority_manifest=manifest,
        )
        self.store = DistributedAuthorityStore(
            root / "authority.sqlite3",
            baseline_index=self.baseline,
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
        self.baseline.close()
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

    async def test_host_query_producer_closes_authority_to_cdx_to_hy_loop(self) -> None:
        calls = []

        async def cdx_handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            payload = [
                ["urlkey", "timestamp", "original", "statuscode"],
                [
                    "com,example)/",
                    "19970102030405",
                    "http://example.com/",
                    "200",
                ],
            ]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1000.0,
            max_global_inflight=1,
        )
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="HistoricalQueryProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="example.com",
                coverage={
                    "scope": "HOST",
                    "year_from": 1997,
                    "year_to": 1997,
                },
                partition="0",
                algorithm_version="resolver-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        producer = DistributedHostQueryProducer(
            (
                CDXProviderConfig(
                    name="internet_archive",
                    endpoint="https://ia.test/cdx",
                    requests_per_second=1000.0,
                    max_inflight=1,
                    max_connections=1,
                    max_keepalive_connections=1,
                    max_retries=0,
                    row_limit=100,
                ),
            ),
            transports={
                "internet_archive": httpx.MockTransport(cdx_handler),
            },
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                self.descriptor("worker-a", "oci-test"),
                {"HistoricalQueryProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.store.accepted_host_year_count(), 1)
        self.assertEqual(self.store.task_row(task_id)["state"], "COMPLETE")
        self.assertEqual(
            self.store.uncovered_resolution_intervals(
                hostname="example.com",
                provider=producer.coverage_provider,
                scope="HOST",
                resolver_version=producer.resolver_version,
                year_from=1997,
                year_to=1997,
            ),
            (),
        )
        budget = self.store.provider_budget_snapshot("internet_archive")
        self.assertEqual(budget["active_inflight"], 0)

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
