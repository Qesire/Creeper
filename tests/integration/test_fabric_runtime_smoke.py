from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from aiohttp import web

from creeper.distributed.authority_api import create_authority_app
from creeper.distributed.bulk_shard import (
    BulkShardProducer,
    bulk_shard_work_definition,
)
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.evidence_query import (
    DistributedEvidenceBridge,
    EvidenceQueryProducer,
    PRODUCER_NAME as EVIDENCE_PRODUCER_NAME,
)
from creeper.distributed.models import (
    Capability,
    ResultBatch,
    TaskClass,
    WorkDefinition,
    WorkerDescriptor,
)
from creeper.distributed.postgres_store import PostgresAuthorityStore
from creeper.distributed.worker import DistributedWorker, ProducerContext
from creeper.distributed.worker_spool import WorkerResultSpool
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity
from creeper.storage.control_store import ControlStore
from creeper.storage.evidence_store import EvidenceStore


class _EchoProducer:
    async def run(self, lease, context: ProducerContext):
        context.keeper.assert_owned()
        yield ResultBatch(
            task_id=lease.task_id,
            generation=lease.generation,
            sequence_no=lease.next_sequence_no,
            results=(
                {
                    "kind":"RUNTIME_SMOKE",
                    "worker":context.descriptor.worker_id,
                    "value":"ok",
                },
            ),
            cursor_after="EOF",
            final=True,
        )


@unittest.skipUnless(
    os.environ.get("CREEPER_POSTGRES_TEST_DSN"),
    "CREEPER_POSTGRES_TEST_DSN is not configured",
)
class FabricRuntimeSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.dsn=os.environ["CREEPER_POSTGRES_TEST_DSN"]
        self.store=PostgresAuthorityStore(self.dsn,emit_outbox=False)
        with self.store.connection.transaction():
            with self.store.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname='public'
                      AND tablename LIKE 'fabric_%'
                    ORDER BY tablename
                    """
                )
                tables=[str(row["tablename"]) for row in cur.fetchall()]
                if tables:
                    cur.execute(
                        "TRUNCATE "+",".join(tables)+" CASCADE"
                    )

        self.secrets: dict[str,str]={}
        self.authority_runner: web.AppRunner | None=None
        self.extra_runners: list[web.AppRunner]=[]
        await self._restart_authority()

    async def asyncTearDown(self) -> None:
        for runner in reversed(self.extra_runners):
            await runner.cleanup()
        if self.authority_runner is not None:
            await self.authority_runner.cleanup()
        self.store.close()
        self.tmp.cleanup()

    async def _restart_authority(self) -> None:
        if self.authority_runner is not None:
            await self.authority_runner.cleanup()
        app=create_authority_app(
            self.store,
            self.secrets,
            max_request_body_bytes=32*1024*1024,
        )
        runner=web.AppRunner(app)
        await runner.setup()
        site=web.TCPSite(runner,"127.0.0.1",0)
        await site.start()
        assert site._server is not None
        port=int(site._server.sockets[0].getsockname()[1])
        self.authority_runner=runner
        self.authority_url=f"http://127.0.0.1:{port}"

    async def _serve(self, app: web.Application) -> str:
        runner=web.AppRunner(app)
        await runner.setup()
        site=web.TCPSite(runner,"127.0.0.1",0)
        await site.start()
        assert site._server is not None
        port=int(site._server.sockets[0].getsockname()[1])
        self.extra_runners.append(runner)
        return f"http://127.0.0.1:{port}"

    async def _run_worker(
        self,
        *,
        descriptor: WorkerDescriptor,
        producer,
    ) -> WorkerResultSpool:
        secret=f"secret-{uuid4().hex}"
        self.secrets[descriptor.worker_id]=secret
        # Authenticator takes a credentials mapping at app construction time.
        # Restarting the in-process Authority simulates credential deployment
        # while preserving the same PostgreSQL authority state.
        await self._restart_authority()
        spool=WorkerResultSpool(
            self.root/f"{descriptor.worker_id}.spool.sqlite3"
        )
        async with CoordinatorClient(
            self.authority_url,
            worker_id=descriptor.worker_id,
            worker_instance_id=descriptor.worker_instance_id,
            secret=secret,
            timeout=5.0,
        ) as client:
            worker=DistributedWorker(
                client,
                descriptor,
                {descriptor.producers[0]:producer},
                spool,
                poll_seconds=0.01,
                claim_wait_seconds=0.0,
                lease_seconds=5.0,
                heartbeat_seconds=1.0,
            )
            await worker.initialize()
            self.assertTrue(await worker.run_once())
        return spool

    @staticmethod
    def _payload(row) -> dict[str,object]:
        raw=row["payload_json"]
        if isinstance(raw,dict):
            return raw
        value=json.loads(str(raw))
        if not isinstance(value,dict):
            raise AssertionError("result payload is not an object")
        return value

    async def test_authority_http_worker_spool_and_result_round_trip(self) -> None:
        work=WorkDefinition(
            producer="RuntimeSmokeProducer",
            task_class=TaskClass.HOST_BATCH,
            input_identity="runtime-smoke:echo",
            payload={"value":"hello"},
            partition="runtime-smoke",
            algorithm_version="runtime-smoke-v1",
            required_capabilities=("RUNTIME_SMOKE",),
        )
        task_id,_=self.store.admit_work(work)
        descriptor=WorkerDescriptor(
            worker_id="smoke-echo-worker",
            worker_instance_id="smoke-echo-instance",
            runtime_class="ci-runtime-smoke",
            region="ci-local",
            architecture="x86_64",
            memory_bytes=512*1024*1024,
            cpu_count=1,
            network_class="loopback",
            capabilities=("RUNTIME_SMOKE",),
            producers=("RuntimeSmokeProducer",),
        )

        spool=await self._run_worker(
            descriptor=descriptor,
            producer=_EchoProducer(),
        )
        try:
            self.assertEqual(spool.pending_count(),0)
        finally:
            spool.close()

        task=self.store.task_row(task_id)
        self.assertEqual(str(task["state"]),"COMPLETE")
        rows=self.store.unconsumed_batches_for_task(task_id)
        self.assertEqual(len(rows),1)
        payload=self._payload(rows[0])
        self.assertTrue(bool(payload["final"]))
        self.assertEqual(payload["results"][0]["value"],"ok")

    async def test_evidence_bridge_worker_provider_gate_and_rdap_connect(self) -> None:
        async def rdap(request: web.Request) -> web.Response:
            self.assertEqual(request.match_info["hostname"],"example.com")
            return web.json_response(
                {
                    "ldhName":"example.com",
                    "events":[
                        {
                            "eventAction":"registration",
                            "eventDate":"1998-02-03T00:00:00Z",
                        }
                    ],
                }
            )

        provider_app=web.Application()
        provider_app.router.add_get("/domain/{hostname}",rdap)
        provider_root=await self._serve(provider_app)

        self.store.configure_provider_budget(
            "rdap",
            requests_per_second=50.0,
            max_global_inflight=2,
            require_qualified_region=False,
        )
        control=ControlStore(self.root/"control.sqlite3")
        evidence=EvidenceStore(self.root/"evidence.sqlite3")
        try:
            bridge=DistributedEvidenceBridge(
                self.store,
                control,
                evidence,
                rdap_endpoint=f"{provider_root}/domain",
                rdap_timeout=2.0,
                owner="runtime-smoke:evidence",
                lease_seconds=30.0,
                retry_base_seconds=0.0,
                retry_max_seconds=0.0,
            )
            key=EvidenceQueryKey(
                "example.com",
                TemporalScope(1996,2001),
                "rdap",
                "rdap-v1",
            )
            self.assertEqual(control.enqueue_evidence_tasks([key]),1)
            self.assertEqual(
                bridge.dispatch(limit=1,providers=("rdap",)),
                1,
            )

            descriptor=WorkerDescriptor(
                worker_id="smoke-evidence-worker",
                worker_instance_id="smoke-evidence-instance",
                runtime_class="ci-runtime-smoke",
                region="ci-local",
                architecture="x86_64",
                memory_bytes=512*1024*1024,
                cpu_count=1,
                network_class="loopback",
                capabilities=(
                    Capability.EVIDENCE_QUERY.value,
                    Capability.RDAP.value,
                ),
                producers=(EVIDENCE_PRODUCER_NAME,),
                allowed_providers=("rdap",),
            )
            spool=await self._run_worker(
                descriptor=descriptor,
                producer=EvidenceQueryProducer(),
            )
            try:
                self.assertEqual(spool.pending_count(),0)
            finally:
                spool.close()

            report=bridge.drain(limit=16)
            self.assertEqual(report.committed,1)
            self.assertEqual(report.inserted_capsules,1)
            capsules=evidence.for_hostname("example.com")
            self.assertEqual(len(capsules),1)
            self.assertEqual(capsules[0].year,1998)
            self.assertEqual(
                capsules[0].evidence_timestamp,
                "1998-02-03T00:00:00Z",
            )
            self.assertEqual(self.store.unconsumed_batches(),())

            with self.store.connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT active,status_code
                    FROM fabric_provider_permits
                    WHERE provider='rdap'
                    ORDER BY issued_at DESC
                    LIMIT 1
                    """
                )
                permit=cur.fetchone()
                cur.execute(
                    """
                    SELECT samples,successes,response_bytes
                    FROM fabric_provider_regions
                    WHERE provider='rdap' AND region='ci-local'
                    """
                )
                region=cur.fetchone()
            self.assertIsNotNone(permit)
            self.assertFalse(bool(permit["active"]))
            self.assertEqual(int(permit["status_code"]),200)
            self.assertIsNotNone(region)
            self.assertGreaterEqual(int(region["samples"]),1)
            self.assertGreaterEqual(int(region["successes"]),1)
            self.assertGreater(int(region["response_bytes"]),0)
        finally:
            evidence.close()
            control.close()

    async def test_bulk_worker_fetches_real_range_and_returns_witnesses(self) -> None:
        body=(
            b"com,example)/ 19970102123456 http://example.com/ "
            b"text/html 200 D 1 1 f.arc\n"
            b"com,example)/a 19980102123456 http://example.com/a "
            b"text/html 200 D 2 2 f.arc\n"
            b"org,other)/ 19990102123456 http://other.org/ "
            b"text/html 200 D 3 3 f.arc\n"
        )
        etag='"runtime-smoke-v1"'

        async def index(request: web.Request) -> web.Response:
            raw=request.headers.get("Range","")
            self.assertTrue(raw.startswith("bytes="))
            start_text,end_text=raw.removeprefix("bytes=").split("-",1)
            start=int(start_text)
            end=int(end_text)
            chunk=body[start:end+1]
            return web.Response(
                status=206,
                body=chunk,
                headers={
                    "Content-Range":f"bytes {start}-{end}/{len(body)}",
                    "ETag":etag,
                    "Accept-Ranges":"bytes",
                },
            )

        source_app=web.Application()
        source_app.router.add_get("/index.cdx",index)
        source_root=await self._serve(source_app)
        identity=HistoricalIndexObjectIdentity(
            kind="remote",
            content_length=len(body),
            etag=etag,
        )
        work=bulk_shard_work_definition(
            region_key="runtime-smoke-region",
            index_key="runtime-smoke-index",
            source_key="runtime-smoke-source",
            locator=f"{source_root}/index.cdx",
            index_format="CDX",
            byte_start=0,
            byte_end_exclusive=len(body),
            expected_identity=identity,
            boundary_record_max_bytes=4096,
            timeout_seconds=2.0,
            group_batch_size=16,
        )
        task_id,_=self.store.admit_work(work)
        descriptor=WorkerDescriptor(
            worker_id="smoke-bulk-worker",
            worker_instance_id="smoke-bulk-instance",
            runtime_class="ci-runtime-smoke",
            region="ci-local",
            architecture="x86_64",
            memory_bytes=512*1024*1024,
            cpu_count=1,
            network_class="loopback",
            capabilities=(
                Capability.STREAMING_BULK.value,
                Capability.ARTIFACT_FETCH.value,
            ),
            producers=("BulkShardProducer",),
        )
        spool=await self._run_worker(
            descriptor=descriptor,
            producer=BulkShardProducer(),
        )
        try:
            self.assertEqual(spool.pending_count(),0)
        finally:
            spool.close()

        task=self.store.task_row(task_id)
        self.assertEqual(str(task["state"]),"COMPLETE")
        rows=self.store.unconsumed_batches_for_task(task_id,limit=32)
        self.assertGreaterEqual(len(rows),1)
        host_years=set()
        summaries=0
        for row in rows:
            payload=self._payload(row)
            for item in payload["results"]:
                if item.get("kind")=="BULK_SUMMARY":
                    summaries+=1
                    continue
                if item.get("kind")!="BULK_WITNESS_GROUP":
                    continue
                for witness in item["witnesses"]:
                    host_years.add(
                        (str(witness["hostname"]),int(witness["year"]))
                    )
        self.assertEqual(
            host_years,
            {
                ("example.com",1997),
                ("example.com",1998),
                ("other.org",1999),
            },
        )
        self.assertEqual(summaries,1)


if __name__=="__main__":
    unittest.main()
