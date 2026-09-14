from __future__ import annotations

import unittest

import httpx

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.multi_cdx import (
    AsyncArquivoCDXClient,
    AsyncCDXProviderPool,
    CDXProviderConfig,
)


def capsule(hostname: str, year: int, source_id: str) -> EvidenceCapsule:
    return EvidenceCapsule(
        hostname=hostname,
        year=year,
        provider="wayback",
        temporal_semantics="capture_timestamp_year",
        evidence_timestamp=f"{year}0101000000",
        source_locator=f"http://{hostname}/",
        payload_hash=f"{source_id}-{hostname}-{year}",
        policy_version="cdx-v1",
        source_id=source_id,
        original_url=f"http://{hostname}/",
        record_locator=f"{source_id}:{hostname}:{year}",
        extraction_method="test",
    )


class FakeClient:
    provider = "wayback"

    def __init__(self) -> None:
        self.exact = {}
        self.ranges = {}
        self.calls = []

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        self.calls.append(("exact", key.hostname, key.temporal_scope.year_from))
        return self.exact[key]

    async def query_range(self, key: EvidenceQueryKey) -> RangeEvidenceQueryResult:
        self.calls.append(
            (
                "range",
                key.hostname,
                key.temporal_scope.year_from,
                key.temporal_scope.year_to,
            )
        )
        return self.ranges[key]


class ArquivoDialectTests(unittest.IsolatedAsyncioTestCase):
    async def test_arquivo_uses_native_query_shape_and_normalizes_rows(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                200,
                json=[
                    {
                        "url": "http://arquivo.example/path",
                        "timestamp": "19980102030405",
                        "status": "200",
                        "mime": "text/html",
                        "digest": "sha1:test",
                        "length": "123",
                        "offset": "10",
                        "filename": "example.arc.gz",
                    }
                ],
            )

        client = AsyncArquivoCDXClient(
            endpoint="https://arquivo.example/wayback/cdx",
            provider="wayback",
            source_id="arquivo_pt",
            limit=100,
            requests_per_second=0.0,
            transport=httpx.MockTransport(handler),
        )
        key = EvidenceQueryKey(
            "arquivo.example",
            TemporalScope(1998, 1998),
            "wayback",
            "cdx-v1",
        )
        try:
            result = await client.query_key(key)
        finally:
            await client.aclose()

        self.assertEqual(result.state, CDXQueryState.PASS)
        assert result.capsule is not None
        self.assertEqual(result.capsule.source_id, "arquivo_pt")
        self.assertEqual(len(seen), 1)
        params = seen[0].url.params
        self.assertEqual(params["from"], "1998")
        self.assertEqual(params["to"], "1998")
        self.assertEqual(params["matchType"], "host")
        self.assertIn("fields", params)
        self.assertNotIn("fl", params)
        self.assertNotIn("showResumeKey", params)

    async def test_arquivo_host_range_queries_all_target_years_at_once(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(
                200,
                json=[
                    {
                        "url": "http://arquivo.example/",
                        "timestamp": "19970102030405",
                        "status": "200",
                    },
                    {
                        "url": "http://arquivo.example/a",
                        "timestamp": "20010102030405",
                        "status": "200",
                    },
                ],
            )

        client = AsyncArquivoCDXClient(
            endpoint="https://arquivo.example/wayback/cdx",
            provider="wayback",
            source_id="arquivo_pt",
            limit=100,
            requests_per_second=0.0,
            transport=httpx.MockTransport(handler),
        )
        key = EvidenceQueryKey(
            "arquivo.example",
            TemporalScope(1996, 2001),
            "wayback",
            "cdx-v1",
        )
        try:
            result = await client.query_range(key)
        finally:
            await client.aclose()

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1997, 2001))
        self.assertEqual(len(seen), 1)
        params = seen[0].url.params
        self.assertEqual(params["matchType"], "host")
        self.assertEqual(params["from"], "1996")
        self.assertEqual(params["to"], "2001")


class ProviderConfigTests(unittest.TestCase):
    def test_row_limit_is_serialized_and_applied_to_client(self) -> None:
        config = CDXProviderConfig(
            name="internet_archive",
            endpoint="https://web.archive.org/cdx/search/cdx",
            row_limit=150_000,
        )
        self.assertEqual(config.as_dict()["row_limit"], 150_000)
        pool = AsyncCDXProviderPool.from_configs((config,))
        try:
            self.assertEqual(pool.clients["internet_archive"].limit, 150_000)
        finally:
            import asyncio
            asyncio.run(pool.aclose())


class MultiCDXProviderPoolTests(unittest.IsolatedAsyncioTestCase):
    def make_pool(self):
        first = FakeClient()
        second = FakeClient()
        pool = AsyncCDXProviderPool(
            {"first": first, "second": second},
            inflight={"first": 2, "second": 3},
            weights={"first": 1.0, "second": 2.0},
        )
        return pool, first, second

    async def test_primary_pass_never_duplicates_query(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1998, 1998),
            "wayback",
            "cdx-v1",
        )
        order = pool._provider_order(key)
        primary = {"first": first, "second": second}[order[0]]
        backup = {"first": first, "second": second}[order[1]]
        primary.exact[key] = EvidenceQueryResult(
            "example.com",
            1998,
            CDXQueryState.PASS,
            capsule=capsule("example.com", 1998, order[0]),
            provider_requests=1,
            key=key,
        )
        backup.exact[key] = EvidenceQueryResult(
            "example.com",
            1998,
            CDXQueryState.PASS,
            capsule=capsule("example.com", 1998, order[1]),
            provider_requests=1,
            key=key,
        )

        result = await pool.query_key(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(len(primary.calls), 1)
        self.assertEqual(backup.calls, [])

    async def test_empty_primary_fails_over_sequentially(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "fallback.example",
            TemporalScope(1999, 1999),
            "wayback",
            "cdx-v1",
        )
        order = pool._provider_order(key)
        clients = {"first": first, "second": second}
        clients[order[0]].exact[key] = EvidenceQueryResult(
            key.hostname,
            1999,
            CDXQueryState.EMPTY_EXHAUSTIVE,
            provider_requests=1,
            key=key,
        )
        clients[order[1]].exact[key] = EvidenceQueryResult(
            key.hostname,
            1999,
            CDXQueryState.PASS,
            capsule=capsule(key.hostname, 1999, order[1]),
            provider_requests=1,
            key=key,
        )

        result = await pool.query_key(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.provider_requests, 2)
        self.assertEqual(pool.failover_attempts, 1)
        self.assertEqual(sum(len(client.calls) for client in clients.values()), 2)

    async def test_pool_negative_requires_every_provider_exhaustive(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "absent.example",
            TemporalScope(2000, 2000),
            "wayback",
            "cdx-v1",
        )
        for client in (first, second):
            client.exact[key] = EvidenceQueryResult(
                key.hostname,
                2000,
                CDXQueryState.EMPTY_EXHAUSTIVE,
                provider_requests=1,
                key=key,
            )

        result = await pool.query_key(key)

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(result.provider_requests, 2)

    async def test_range_combines_disjoint_provider_years(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "years.example",
            TemporalScope(1998, 1999),
            "wayback",
            "cdx-v1",
        )
        order = pool._provider_order(key)
        clients = {"first": first, "second": second}
        clients[order[0]].ranges[key] = RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.PASS,
            candidate_years=(1998,),
            capsules=(capsule(key.hostname, 1998, order[0]),),
            provider_requests=1,
        )
        clients[order[1]].ranges[key] = RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.PASS,
            candidate_years=(1999,),
            capsules=(capsule(key.hostname, 1999, order[1]),),
            provider_requests=1,
        )

        result = await pool.query_range(key)

        self.assertIsInstance(result, RangeEvidenceQueryResult)
        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1998, 1999))
        self.assertEqual(result.followup_years, ())
        self.assertEqual(result.provider_requests, 2)

    async def test_exhaustive_range_treats_missing_years_as_proven_empty(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "sparse-years.example",
            TemporalScope(1998, 1999),
            "wayback",
            "cdx-v1",
        )
        clients = {"first": first, "second": second}
        for name, client in clients.items():
            client.ranges[key] = RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.PASS,
                candidate_years=(1998,),
                capsules=(capsule(key.hostname, 1998, name),),
                provider_requests=1,
            )

        result = await pool.query_range(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1998,))
        self.assertEqual(result.followup_years, ())
        self.assertEqual(result.provider_requests, 2)
        self.assertFalse(
            any(call[0] == "exact" for client in clients.values() for call in client.calls)
        )

    async def test_partial_range_closes_missing_year_inside_same_host_task(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "partial.example",
            TemporalScope(1998, 1999),
            "wayback",
            "cdx-v1",
        )
        order = pool._provider_order(key)
        clients = {"first": first, "second": second}
        clients[order[0]].ranges[key] = RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.DECOMPOSED,
            candidate_years=(1998,),
            followup_years=(1999,),
            capsules=(capsule(key.hostname, 1998, order[0]),),
            provider_requests=1,
        )
        clients[order[1]].ranges[key] = RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.TRANSIENT_ERROR,
            provider_requests=1,
            error="offline",
        )
        exact = EvidenceQueryKey(
            key.hostname,
            TemporalScope(1999, 1999),
            key.provider,
            key.policy_version,
        )
        for name, client in clients.items():
            client.exact[exact] = EvidenceQueryResult(
                key.hostname,
                1999,
                CDXQueryState.PASS,
                capsule=capsule(key.hostname, 1999, name),
                provider_requests=1,
                key=exact,
            )

        result = await pool.query_range(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1998, 1999))
        self.assertEqual(result.followup_years, ())
        self.assertGreaterEqual(result.provider_requests, 3)

    async def test_unresolved_exact_fallback_retries_parent_without_children(self) -> None:
        pool, first, second = self.make_pool()
        key = EvidenceQueryKey(
            "still-partial.example",
            TemporalScope(1998, 1999),
            "wayback",
            "cdx-v1",
        )
        clients = {"first": first, "second": second}
        for client in clients.values():
            client.ranges[key] = RangeEvidenceQueryResult(
                hostname=key.hostname,
                key=key,
                state=CDXQueryState.DECOMPOSED,
                candidate_years=(1998,),
                capsules=(capsule(key.hostname, 1998, "range"),),
                provider_requests=1,
            )
        exact = EvidenceQueryKey(
            key.hostname,
            TemporalScope(1999, 1999),
            key.provider,
            key.policy_version,
        )
        for client in clients.values():
            client.exact[exact] = EvidenceQueryResult(
                key.hostname,
                1999,
                CDXQueryState.INCOMPLETE,
                provider_requests=1,
                key=exact,
            )

        result = await pool.query_range(key)

        self.assertEqual(result.state, CDXQueryState.INCOMPLETE)
        self.assertEqual(result.candidate_years, (1998,))
        self.assertEqual(result.followup_years, ())


if __name__ == "__main__":
    unittest.main()
