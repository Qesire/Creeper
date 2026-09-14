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
from creeper.distributed.bulk_index import (
    BulkChunkLimits,
    BulkHistoricalIndexProducer,
)
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.exploration import (
    ExplorationLimits,
    HistoricalCrawlerProducer,
    SeededExplorationProducer,
)
from creeper.distributed.host_query import DistributedHostQueryProducer
from creeper.distributed.region_probe import RegionProbeProducer
from creeper.distributed.search_campaign import SearchCampaign
from creeper.distributed.seeded_search import SeededSearchProducer
from creeper.distributed.source_discovery import SourceDiscoveryProducer
from creeper.distributed.thin_query import ThinHistoricalQueryProducer
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
            allowed_providers=("internet_archive", "arquivo_pt"),
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

    async def test_historical_crawler_consumes_hostnames_remotely_into_hy(self) -> None:
        web_calls: list[str] = []
        cdx_hosts: list[str] = []

        async def web_handler(request: httpx.Request) -> httpx.Response:
            web_calls.append(str(request.url))
            host = request.url.host
            if host == "root.example":
                html = (
                    '<html><body>'
                    '<a href="https://historic.example/links.html">links</a>'
                    '<a href="https://modern.example/">modern</a>'
                    '</body></html>'
                )
            else:
                html = "<html><body></body></html>"
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=html.encode(),
                request=request,
            )

        async def cdx_handler(request: httpx.Request) -> httpx.Response:
            queried = request.url.params.get("url", "")
            cdx_hosts.append(queried)
            if "historic.example" in queried:
                payload = [
                    ["timestamp", "original", "statuscode"],
                    [
                        "19980102030405",
                        "http://historic.example/",
                        "200",
                    ],
                ]
            else:
                payload = [["timestamp", "original", "statuscode"]]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        for provider in ("web_discovery", "internet_archive"):
            self.store.configure_provider_budget(
                provider,
                requests_per_second=1000.0,
                max_global_inflight=2,
                require_qualified_region=False,
            )
        task_id = self.store.admit_historical_exploration_work(
            url="https://root.example/",
            archive_providers=("internet_archive",),
            seed=7,
        )
        config = CDXProviderConfig(
            name="internet_archive",
            endpoint="https://ia.test/cdx",
            requests_per_second=1000.0,
            max_inflight=2,
            max_connections=2,
            max_keepalive_connections=1,
            max_retries=0,
            row_limit=100,
        )
        producer = HistoricalCrawlerProducer(
            (config,),
            limits=ExplorationLimits(
                max_pages=4,
                max_hosts=8,
                max_depth=2,
                max_links_per_page=16,
                max_response_bytes=1024 * 1024,
            ),
            web_transport=httpx.MockTransport(web_handler),
            cdx_transports={
                "internet_archive": httpx.MockTransport(cdx_handler),
            },
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="vm",
            region="crawler-test",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(
                Capability.WEB_DISCOVERY.value,
                Capability.ONLINE_QUERY.value,
            ),
            allowed_providers=("web_discovery", "internet_archive"),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"HistoricalCrawlerProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertGreaterEqual(len(cdx_hosts), 3)
        self.assertGreaterEqual(len(web_calls), 2)
        self.assertEqual(self.store.accepted_host_year_count(), 1)
        self.assertEqual(self.store.host_candidate_count(), 0)
        self.assertEqual(self.store.source_candidate_count(), 0)
        self.assertEqual(self.store.batch_count(task_id), 1)
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], "COMPLETE")
        self.assertEqual(row["cursor"], "EOF")

    async def test_source_discovery_worker_persists_deduplicated_candidates(self) -> None:
        html = b"""
        <html><body>
          <a href="/archives/sample.cdxj.gz">bulk</a>
          <a href="/archives/sample.cdxj.gz#duplicate">bulk duplicate</a>
          <a href="/links/page.html">page</a>
          <a href="mailto:test@example.com">mail</a>
          <a href="#local">fragment</a>
        </body></html>
        """

        async def discovery_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=html,
                request=request,
            )

        self.store.configure_provider_budget(
            "web_discovery",
            requests_per_second=1000.0,
            max_global_inflight=1,
            require_qualified_region=False,
        )
        task_id = self.store.admit_source_page_work(
            url="https://sources.test/index.html",
            max_links=16,
        )
        producer = SourceDiscoveryProducer(
            transport=httpx.MockTransport(discovery_handler)
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="vm",
            region="oci-test",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.WEB_DISCOVERY.value,),
            allowed_providers=("web_discovery",),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"SourceDiscoveryProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertEqual(self.store.source_candidate_count(), 2)
        self.assertEqual(self.store.host_candidate_count(), 1)
        self.assertEqual(
            self.store.host_candidate_rows()[0]["hostname"],
            "sources.test",
        )
        rows = self.store.source_candidate_rows()
        by_url = {str(row["canonical_url"]): row for row in rows}
        self.assertEqual(
            by_url["https://sources.test/archives/sample.cdxj.gz"][
                "candidate_type"
            ],
            "bulk_artifact",
        )
        self.assertEqual(
            by_url["https://sources.test/archives/sample.cdxj.gz"][
                "parser_kind"
            ],
            "cdxj",
        )
        self.assertEqual(
            by_url["https://sources.test/links/page.html"]["candidate_type"],
            "source_page",
        )
        self.assertEqual(self.store.batch_count(task_id), 1)
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], "COMPLETE")
        self.assertEqual(row["cursor"], SourceDiscoveryProducer.EOF_CURSOR)

    async def test_seeded_search_emits_deduplicated_hostname_candidates(self) -> None:
        requested_queries: list[str] = []
        html = b"""
        <html><body>
          <a href="https://result-one.example/a">one</a>
          <a href="https://result-one.example/b">one duplicate host</a>
          <a href="/redir?uddg=https%3A%2F%2Fresult-two.example%2Fx">two</a>
        </body></html>
        """

        async def search_handler(request: httpx.Request) -> httpx.Response:
            requested_queries.append(request.url.params.get("q", ""))
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=html,
                request=request,
            )

        campaign = SearchCampaign(
            name="fixture-search",
            templates=("{anchor} {source} {era} {topology}",),
            anchors=("personal homepage", "company page"),
            source_terms=("links", "directory"),
            era_terms=("1998", "2000"),
            topology_terms=("webring", "resources"),
        )
        expected_queries = list(
            campaign.render_slice(
                seed=42,
                slot_start=0,
                slot_count=2,
            )
        )

        self.store.configure_provider_budget(
            "web_search",
            requests_per_second=1000.0,
            max_global_inflight=1,
            require_qualified_region=False,
        )
        task_id = self.store.admit_search_slice(
            campaign=campaign,
            seed=42,
            slot_start=0,
            slot_count=2,
            search_endpoint="https://search.test/",
            provider="web_search",
        )
        producer = SeededSearchProducer(
            transport=httpx.MockTransport(search_handler)
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="vm",
            region="search-free",
            architecture="x86_64",
            memory_bytes=512 * 1024**2,
            cpu_count=1,
            network_class="public",
            capabilities=(Capability.SEARCH_QUERY.value,),
            allowed_providers=("web_search",),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"SeededSearchProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertEqual(requested_queries, expected_queries)
        self.assertEqual(self.store.host_candidate_count(), 2)
        hosts = {
            str(row["hostname"]): int(row["discovery_count"])
            for row in self.store.host_candidate_rows()
        }
        self.assertEqual(
            set(hosts),
            {"result-one.example", "result-two.example"},
        )
        self.assertEqual(hosts["result-one.example"], 1)
        self.assertEqual(self.store.batch_count(task_id), 1)

    async def test_seeded_exploration_consumes_search_hosts_remotely(self) -> None:
        requested_queries: list[str] = []

        async def search_handler(request: httpx.Request) -> httpx.Response:
            requested_queries.append(request.url.params.get("q", ""))
            html = (
                '<html><body>'
                '<a href="https://historic-search.example/links.html">old</a>'
                '<a href="https://modern-search.example/">modern</a>'
                '</body></html>'
            )
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=html.encode(),
                request=request,
            )

        async def web_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=b"<html><body></body></html>",
                request=request,
            )

        async def cdx_handler(request: httpx.Request) -> httpx.Response:
            queried = request.url.params.get("url", "")
            if "historic-search.example" in queried:
                payload = [
                    ["timestamp", "original", "statuscode"],
                    [
                        "20000102030405",
                        "http://historic-search.example/",
                        "200",
                    ],
                ]
            else:
                payload = [["timestamp", "original", "statuscode"]]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        campaign = SearchCampaign(
            name="resolved-search",
            templates=("{anchor} {source} {era} {topology}",),
            anchors=("personal homepage",),
            source_terms=("links",),
            era_terms=("2000",),
            topology_terms=("webring",),
        )
        expected = list(
            campaign.render_slice(seed=11, slot_start=0, slot_count=1)
        )

        for provider in (
            "web_search",
            "web_discovery",
            "internet_archive",
        ):
            self.store.configure_provider_budget(
                provider,
                requests_per_second=1000.0,
                max_global_inflight=2,
                require_qualified_region=False,
            )

        task_id = self.store.admit_seeded_exploration(
            campaign=campaign,
            seed=11,
            slot_start=0,
            slot_count=1,
            search_endpoint="https://search.test/",
            archive_providers=("internet_archive",),
        )
        config = CDXProviderConfig(
            name="internet_archive",
            endpoint="https://ia.test/cdx",
            requests_per_second=1000.0,
            max_inflight=2,
            max_connections=2,
            max_keepalive_connections=1,
            max_retries=0,
            row_limit=100,
        )
        producer = SeededExplorationProducer(
            (config,),
            exploration_limits=ExplorationLimits(
                max_pages=4,
                max_hosts=8,
                max_depth=1,
                max_links_per_page=16,
                max_response_bytes=1024 * 1024,
            ),
            search_transport=httpx.MockTransport(search_handler),
            web_transport=httpx.MockTransport(web_handler),
            cdx_transports={
                "internet_archive": httpx.MockTransport(cdx_handler),
            },
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="vm",
            region="search-resolve-test",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(
                Capability.SEARCH_QUERY.value,
                Capability.WEB_DISCOVERY.value,
                Capability.ONLINE_QUERY.value,
            ),
            allowed_providers=(
                "web_search",
                "web_discovery",
                "internet_archive",
            ),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"SeededExplorationProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertEqual(requested_queries, expected)
        self.assertEqual(self.store.accepted_host_year_count(), 1)
        self.assertEqual(self.store.host_candidate_count(), 0)
        self.assertEqual(self.store.source_candidate_count(), 0)
        self.assertEqual(self.store.batch_count(task_id), 1)

    async def test_region_probe_worker_qualifies_provider_region(self) -> None:
        calls = 0

        async def probe_handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(
                200,
                content=b"[]",
                request=request,
            )

        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1000.0,
            max_global_inflight=1,
            require_qualified_region=True,
        )
        probe_task = self.store.admit_work(
            WorkDefinition(
                producer="RegionProbeProducer",
                task_class=TaskClass.PROBE,
                input_identity="internet_archive",
                coverage={
                    "provider": "internet_archive",
                    "probe_hostname": "example.com",
                    "year": 2001,
                    "samples": 3,
                },
                partition="qualification",
                algorithm_version="probe-v1",
                required_capabilities=(Capability.ONLINE_QUERY.value,),
            )
        )
        producer = RegionProbeProducer(
            (
                CDXProviderConfig(
                    name="internet_archive",
                    endpoint="https://ia.test/cdx",
                    requests_per_second=1000.0,
                    max_inflight=1,
                    max_connections=1,
                    max_keepalive_connections=1,
                    max_retries=0,
                    row_limit=1,
                ),
            ),
            transports={
                "internet_archive": httpx.MockTransport(probe_handler),
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
                {"RegionProbeProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, probe_task)
        self.assertEqual(calls, 3)
        snapshot = self.store.provider_region_snapshot(
            "internet_archive",
            "oci-test",
        )
        assert snapshot is not None
        self.assertEqual(snapshot["state"], "QUALIFIED")
        self.assertEqual(snapshot["samples"], 3)

    async def test_cloudflare_thin_query_uses_one_request_and_only_positive_hy(self) -> None:
        calls = 0

        async def thin_handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://thin.example/", "200"],
            ]
            return httpx.Response(
                200,
                headers={"Content-Type": "application/json"},
                content=json.dumps(payload).encode(),
                request=request,
            )

        self.store.configure_provider_budget(
            "internet_archive",
            requests_per_second=1000.0,
            max_global_inflight=1,
            require_qualified_region=False,
        )
        task_id = self.store.admit_thin_host_probe(
            hostname="thin.example",
            provider="internet_archive",
            year=1997,
            estimated_response_bytes=64 * 1024,
        )
        config = CDXProviderConfig(
            name="internet_archive",
            endpoint="https://ia.test/cdx",
            requests_per_second=1000.0,
            max_inflight=1,
            max_connections=1,
            max_keepalive_connections=1,
            max_retries=3,
            row_limit=100,
        )
        producer = ThinHistoricalQueryProducer(
            (config,),
            transports={
                "internet_archive": httpx.MockTransport(thin_handler),
            },
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="cloudflare_worker",
            region="cf-global",
            architecture="wasm",
            memory_bytes=128 * 1024**2,
            cpu_count=1,
            network_class="edge",
            capabilities=(Capability.THIN_QUERY.value,),
            allowed_providers=("internet_archive",),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"ThinHistoricalQueryProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertEqual(calls, 1)
        self.assertEqual(self.store.accepted_host_year_count(), 1)
        self.assertEqual(self.store.task_row(task_id)["state"], "COMPLETE")
        # Thin positive probes never claim complete resolution coverage.
        self.assertEqual(
            self.store.uncovered_resolution_intervals(
                hostname="thin.example",
                provider="cdx-pool:any",
                scope="HOST",
                resolver_version="resolver-v1",
                year_from=1997,
                year_to=1997,
            ),
            ((1997, 1997),),
        )

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
            require_qualified_region=False,
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

    async def test_bulk_index_streams_probe_full_and_durable_eof_checkpoint(self) -> None:
        source = Path(self.tmp.name) / "fixture.cdxj"
        source.write_text(
            "\n".join(
                [
                    'com,example)/ 19970102030405 {"url":"http://bulk.example/a","status":"200"}',
                    'com,example)/ 19970103030405 {"url":"http://bulk.example/b","status":"200"}',
                    'com,other)/ 19980102030405 {"url":"http://other.example/","status":"200"}',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        task_id = self.store.admit_bulk_source_work(
            source_id="fixture-cdxj",
            source_locator=str(source),
            partition="all",
        )
        producer = BulkHistoricalIndexProducer(
            limits=BulkChunkLimits(
                max_records=2,
                max_bytes=1024 * 1024,
                max_seconds=2.0,
                probe_batch_size=16,
            )
        )
        descriptor = WorkerDescriptor(
            worker_id="worker-a",
            runtime_class="vm",
            region="oci-test",
            architecture="x86_64",
            memory_bytes=1024**3,
            cpu_count=2,
            network_class="public",
            capabilities=(Capability.STREAMING_BULK.value,),
            allowed_providers=(),
        )

        async with CoordinatorClient(
            self.base_url,
            worker_id="worker-a",
            secret=self.credentials["worker-a"],
        ) as client:
            worker = DistributedWorker(
                client,
                descriptor,
                {"BulkHistoricalIndexProducer": producer},
                lease_seconds=30,
            )
            report = await worker.run_once()

        self.assertTrue(report.completed, report.error)
        self.assertEqual(report.task_id, task_id)
        self.assertEqual(self.store.accepted_host_year_count(), 2)
        row = self.store.task_row(task_id)
        self.assertEqual(row["state"], "COMPLETE")
        self.assertEqual(row["cursor"], BulkHistoricalIndexProducer.EOF_CURSOR)
        self.assertGreaterEqual(int(row["next_sequence_no"]), 2)
        self.assertEqual(
            self.store.batch_count(task_id),
            int(row["next_sequence_no"]),
        )

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
