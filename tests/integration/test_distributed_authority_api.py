from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.identity import authority_digest
from creeper.distributed.auth import sign_request
from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.authority_store import DistributedAuthorityStore
from creeper.distributed.models import Capability, TaskClass, WorkDefinition


class MutableClock:
    def __init__(self, value: float = 1_700_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class DistributedAuthorityAPITests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = MutableClock()
        root = Path(self.tmp.name)
        baseline_dir = root / "baseline-v1"
        baseline_dir.mkdir()
        for year in range(1996, 2002):
            content = "baseline.example\n" if year == 1997 else ""
            (baseline_dir / f"{year}.txt").write_text(content, encoding="utf-8")
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
            clock=self.clock,
        )
        self.credentials = {
            "worker-a": "secret-a",
            "worker-b": "secret-b",
        }
        app = create_authority_app(
            self.store,
            self.credentials,
            clock=self.clock,
            max_clock_skew_seconds=60,
        )
        self.server = TestServer(app)
        self.client = TestClient(self.server)
        await self.client.start_server()
        self.nonce_counter = 0

    async def asyncTearDown(self) -> None:
        await self.client.close()
        self.store.close()
        self.baseline.close()
        self.tmp.cleanup()

    def signed_headers(
        self,
        worker_id: str,
        path: str,
        body: bytes,
        *,
        nonce: str | None = None,
        timestamp: str | None = None,
    ) -> dict[str, str]:
        if nonce is None:
            self.nonce_counter += 1
            nonce = f"nonce-{self.nonce_counter}"
        if timestamp is None:
            timestamp = str(self.clock())
        signature = sign_request(
            self.credentials[worker_id],
            method="POST",
            path=path,
            body=body,
            timestamp=timestamp,
            nonce=nonce,
        )
        return {
            "Content-Type": "application/json",
            "X-Creeper-Worker": worker_id,
            "X-Creeper-Timestamp": timestamp,
            "X-Creeper-Nonce": nonce,
            "X-Creeper-Signature": signature,
        }

    async def post(
        self,
        worker_id: str,
        path: str,
        payload: dict,
        *,
        nonce: str | None = None,
        timestamp: str | None = None,
    ):
        body = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return await self.client.post(
            path,
            data=body,
            headers=self.signed_headers(
                worker_id,
                path,
                body,
                nonce=nonce,
                timestamp=timestamp,
            ),
        )

    async def register(self, worker_id: str) -> None:
        response = await self.post(
            worker_id,
            "/v1/workers/register",
            {
                "worker_id": worker_id,
                "runtime_class": "vm",
                "region": "test-region",
                "architecture": "x86_64",
                "memory_bytes": 1024**3,
                "cpu_count": 2,
                "network_class": "public",
                "capabilities": [Capability.ONLINE_QUERY.value],
            },
        )
        self.assertEqual(response.status, 200, await response.text())

    async def test_signed_register_claim_commit_and_replay(self) -> None:
        await self.register("worker-a")
        work = WorkDefinition(
            producer="HistoricalQueryProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity="example.com",
            coverage={"year_from": 1996, "year_to": 2001, "scope": "HOST"},
            partition="0",
            algorithm_version="resolver-v1",
            required_capabilities=(Capability.ONLINE_QUERY.value,),
        )
        self.store.admit_work(work)

        claim = await self.post(
            "worker-a",
            "/v1/tasks/claim",
            {"lease_seconds": 30},
        )
        self.assertEqual(claim.status, 200, await claim.text())
        task = (await claim.json())["task"]
        self.assertEqual(task["generation"], 1)
        self.assertEqual(task["work"]["task_class"], "HOST_BATCH")

        batch_payload = {
            "task_id": task["task_id"],
            "generation": task["generation"],
            "sequence_no": 0,
            "results": [
                {
                    "kind": "HY",
                    "hostname": "example.com",
                    "year": 1997,
                }
            ],
            "cursor_after": "page:1",
        }
        first = await self.post(
            "worker-a",
            "/v1/results/batch",
            batch_payload,
        )
        self.assertEqual(first.status, 200, await first.text())
        self.assertEqual((await first.json())["status"], "COMMITTED")

        replay = await self.post(
            "worker-a",
            "/v1/results/batch",
            batch_payload,
        )
        self.assertEqual(replay.status, 200, await replay.text())
        self.assertEqual((await replay.json())["status"], "ALREADY_COMMITTED")

    async def test_nonce_replay_is_rejected_even_for_idempotent_registration(self) -> None:
        payload = {
            "worker_id": "worker-a",
            "runtime_class": "vm",
            "region": "test-region",
            "architecture": "x86_64",
            "memory_bytes": 1024,
            "cpu_count": 1,
            "network_class": "public",
            "capabilities": [Capability.ONLINE_QUERY.value],
        }
        nonce = "fixed-nonce"
        first = await self.post(
            "worker-a",
            "/v1/workers/register",
            payload,
            nonce=nonce,
        )
        self.assertEqual(first.status, 200, await first.text())

        replay = await self.post(
            "worker-a",
            "/v1/workers/register",
            payload,
            nonce=nonce,
        )
        self.assertEqual(replay.status, 401)
        self.assertEqual(
            (await replay.json())["error"],
            "AUTHENTICATION_FAILED",
        )

    async def test_expired_signed_request_is_rejected(self) -> None:
        response = await self.post(
            "worker-a",
            "/v1/workers/register",
            {
                "worker_id": "worker-a",
                "runtime_class": "vm",
                "region": "test",
                "architecture": "x86_64",
                "memory_bytes": 1024,
                "cpu_count": 1,
                "network_class": "public",
                "capabilities": [Capability.ONLINE_QUERY.value],
            },
            timestamp=str(self.clock() - 61),
        )
        self.assertEqual(response.status, 401)

    async def test_hy_probe_filters_baseline_and_full_accepts_only_novel(self) -> None:
        await self.register("worker-a")
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="BulkHistoricalIndexProducer",
                task_class=TaskClass.SOURCE_SHARD,
                input_identity="fixture:shard",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="bulk-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        claim = await self.post(
            "worker-a",
            "/v1/tasks/claim",
            {"lease_seconds": 30},
        )
        task = (await claim.json())["task"]
        self.assertEqual(task["task_id"], task_id)

        probe = await self.post(
            "worker-a",
            "/v1/results/hy-probe",
            {
                "task_id": task_id,
                "generation": task["generation"],
                "probes": [
                    {
                        "hostname": "baseline.example",
                        "year": 1997,
                        "locator": "fixture:1",
                    },
                    {
                        "hostname": "novel.example",
                        "year": 1998,
                        "locator": "fixture:2",
                    },
                ],
            },
        )
        self.assertEqual(probe.status, 200, await probe.text())
        decisions = {
            (item["hostname"], item["year"]): item["status"]
            for item in (await probe.json())["decisions"]
        }
        self.assertEqual(
            decisions[("baseline.example", 1997)],
            "KNOWN_BASELINE",
        )
        self.assertEqual(
            decisions[("novel.example", 1998)],
            "NEED_FULL_EVIDENCE",
        )

        full = await self.post(
            "worker-a",
            "/v1/results/hy-full",
            {
                "task_id": task_id,
                "generation": task["generation"],
                "evidence": [
                    {
                        "hostname": "novel.example",
                        "year": 1998,
                        "evidence_class": "CDX_CAPTURE",
                        "source": "fixture-cdxj",
                        "timestamp": "19980102030405",
                        "locator": "fixture:2",
                        "original_url": "http://novel.example/",
                    }
                ],
            },
        )
        self.assertEqual(full.status, 200, await full.text())
        result = (await full.json())["results"][0]
        self.assertEqual(result["status"], "ACCEPTED")
        self.assertEqual(self.store.accepted_host_year_count(), 1)

        repeat_probe = await self.post(
            "worker-a",
            "/v1/results/hy-probe",
            {
                "task_id": task_id,
                "generation": task["generation"],
                "probes": [
                    {
                        "hostname": "novel.example",
                        "year": 1998,
                        "locator": "fixture:2",
                    }
                ],
            },
        )
        self.assertEqual(
            (await repeat_probe.json())["decisions"][0]["status"],
            "KNOWN_ACCEPTED",
        )

    async def test_hy_full_without_probe_is_rejected(self) -> None:
        await self.register("worker-a")
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="BulkHistoricalIndexProducer",
                task_class=TaskClass.SOURCE_SHARD,
                input_identity="fixture:unprobed",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="bulk-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        claim = await self.post("worker-a", "/v1/tasks/claim", {"lease_seconds": 30})
        task = (await claim.json())["task"]
        response = await self.post(
            "worker-a",
            "/v1/results/hy-full",
            {
                "task_id": task_id,
                "generation": task["generation"],
                "evidence": [
                    {
                        "hostname": "novel.example",
                        "year": 1999,
                        "evidence_class": "CDX_CAPTURE",
                        "source": "fixture",
                        "timestamp": "19990101000000",
                        "locator": "fixture:9",
                    }
                ],
            },
        )
        self.assertEqual(response.status, 400)

    async def test_provider_permit_requires_current_fenced_lease(self) -> None:
        await self.register("worker-a")
        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=10,
            max_global_inflight=1,
            require_qualified_region=False,
        )
        task_id = self.store.admit_work(
            WorkDefinition(
                producer="HistoricalQueryProducer",
                task_class=TaskClass.HOST_BATCH,
                input_identity="example.com",
                coverage={"year_from": 1996, "year_to": 2001},
                partition="0",
                algorithm_version="resolver-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        claim = await self.post(
            "worker-a",
            "/v1/tasks/claim",
            {"lease_seconds": 30},
        )
        task = (await claim.json())["task"]
        self.assertEqual(task["task_id"], task_id)

        stale = await self.post(
            "worker-a",
            "/v1/providers/permit",
            {
                "provider": "internet_archive",
                "task_id": task_id,
                "generation": task["generation"] + 1,
            },
        )
        self.assertEqual(stale.status, 409)
        self.assertEqual((await stale.json())["error"], "STALE_LEASE")

        permit = await self.post(
            "worker-a",
            "/v1/providers/permit",
            {
                "provider": "internet_archive",
                "task_id": task_id,
                "generation": task["generation"],
            },
        )
        self.assertEqual(permit.status, 200, await permit.text())
        self.assertIsNotNone((await permit.json())["permit"])


if __name__ == "__main__":
    unittest.main()
