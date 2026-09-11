import json
import unittest
from unittest.mock import patch

import httpx

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient


class AsyncWaybackCDXClientTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def key(hostname="example.com"):
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1997, 1997),
            "wayback",
            "cdx-v1",
        )

    async def test_http_429_is_retried_by_tenacity_then_passes(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, request=request)
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
            ]
            return httpx.Response(200, content=json.dumps(payload).encode(), request=request)

        transport = httpx.MockTransport(handler)
        async with AsyncWaybackCDXClient(
            transport=transport,
            max_retries=1,
            backoff=0,
            requests_per_second=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(calls, 2)
        self.assertEqual(client.http_requests, 2)

    async def test_retry_after_extends_provider_wide_cooldown(self):
        request = httpx.Request("GET", "https://example.invalid/cdx")
        response = httpx.Response(
            429,
            headers={"Retry-After": "3"},
            request=request,
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, request=request)
            ),
            max_retries=0,
        ) as client:
            loop = __import__("asyncio").get_running_loop()
            before = loop.time()
            await client._register_throttle(response)
            self.assertEqual(client.throttle_responses, 1)
            self.assertGreaterEqual(client._cooldown_until - before, 2.9)

    async def test_503_without_retry_after_uses_shared_floor_cooldown(self):
        request = httpx.Request("GET", "https://example.invalid/cdx")
        response = httpx.Response(503, request=request)
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, request=request)
            ),
            max_retries=0,
            backoff=0,
            throttle_floor_seconds=1.5,
        ) as client:
            loop = __import__("asyncio").get_running_loop()
            before = loop.time()
            await client._register_throttle(response)
            self.assertGreaterEqual(client._cooldown_until - before, 1.4)

    async def test_resume_key_pages_are_exhausted_with_one_reused_client(self):
        calls = []

        async def handler(request):
            calls.append(str(request.url))
            if len(calls) == 1:
                payload = [
                    ["timestamp", "original", "statuscode"],
                    ["19970101000000", "http://other.example/", "200"],
                    ["resume-token!"],
                ]
            else:
                payload = [["timestamp", "original", "statuscode"]]
            return httpx.Response(200, content=json.dumps(payload).encode(), request=request)

        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(result.pages_seen, 2)
        self.assertIn("showResumeKey=true", calls[0])
        self.assertIn("resumeKey=resume-token%21", calls[1])

    async def test_non_retryable_http_error_is_invalid_without_retry(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(404, request=request)

        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=3,
            backoff=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.INVALID)
        self.assertEqual(calls, 1)

    async def test_transport_timeout_exhaustion_is_transient(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            raise httpx.ReadTimeout("timeout", request=request)

        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=1,
            backoff=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(calls, 2)

    async def test_range_probe_reuses_positive_rows_as_capsules(self):
        async def handler(request):
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
                ["19990102030405", "http://example.com/", "200"],
            ]
            return httpx.Response(200, content=json.dumps(payload).encode(), request=request)

        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler), max_retries=0
        ) as client:
            result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1997, 1999))
        self.assertEqual(tuple(capsule.year for capsule in result.capsules), (1997, 1999))
        self.assertTrue(
            all(capsule.extraction_method == "cdx_query_range" for capsule in result.capsules)
        )
        self.assertEqual(result.key, range_key)

    async def test_incomplete_range_keeps_positive_capsule_without_negative_claim(self):
        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
            max_retries=0,
        ) as client:
            async def partial_pages(hostname, year_from, year_to):
                yield (
                    [
                        {
                            "timestamp": "19970102030405",
                            "original": "http://example.com/",
                            "statuscode": "200",
                        }
                    ],
                    False,
                )
                raise ConnectionError("later page unavailable")

            with patch.object(client, "iter_range_pages", partial_pages):
                result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(result.candidate_years, ())
        self.assertEqual(tuple(capsule.year for capsule in result.capsules), (1997,))

    async def test_sub_one_request_per_second_limit_allows_single_request(self):
        async def handler(request):
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
            ]
            return httpx.Response(200, content=json.dumps(payload).encode(), request=request)

        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=0,
            requests_per_second=0.5,
        ) as client:
            result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1997,))

    async def test_range_probe_empty_requires_complete_final_page(self):
        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
            max_retries=0,
        ) as client:
            async def incomplete_pages(hostname, year_from, year_to):
                yield ([], False)

            with patch.object(client, "iter_range_pages", incomplete_pages):
                result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.INCOMPLETE)
        self.assertEqual(result.candidate_years, ())


if __name__ == "__main__":
    unittest.main()
