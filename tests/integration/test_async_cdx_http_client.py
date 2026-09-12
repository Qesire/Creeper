import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

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
        self.assertEqual(result.provider_requests, 2)
        self.assertGreaterEqual(result.provider_elapsed_milliseconds, 0)
        self.assertEqual(client.throttle_responses, 1)
        self.assertEqual(client.http_status_counts[429], 1)
        self.assertEqual(client.http_status_counts[200], 1)
        self.assertEqual(client.transport_errors, 0)
        self.assertGreaterEqual(client.http_elapsed_milliseconds, 0)
        self.assertEqual(sum(client.http_latency_buckets.values()), 2)

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
        first_query = parse_qs(urlsplit(calls[0]).query)
        second_query = parse_qs(urlsplit(calls[1]).query)
        self.assertEqual(first_query["showResumeKey"], ["true"])
        self.assertIn("urlkey", first_query["fl"][0].split(","))
        self.assertEqual(first_query["filter"], ["statuscode:[23][0-9][0-9]"])
        self.assertEqual(second_query["resumeKey"], ["resume-token!"])

    async def test_exact_year_uses_one_row_pages_but_range_keeps_bulk_limit(self):
        seen_limits = []
        seen_collapse = []

        async def handler(request):
            query = parse_qs(request.url.query.decode())
            seen_limits.append(query["limit"][0])
            seen_collapse.append(query.get("collapse", []))
            payload = [
                ["urlkey", "timestamp", "original", "statuscode"],
                ["com,example)/", "19970102030405", "http://example.com/", "200"],
            ]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 2000), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=0,
            limit=1000,
        ) as client:
            exact = await client.query_key(self.key())
            ranged = await client.query_range(range_key)

        self.assertEqual(exact.state, CDXQueryState.PASS)
        self.assertEqual(ranged.state, CDXQueryState.PASS)
        self.assertEqual(seen_limits, ["1", "1000"])
        self.assertEqual(seen_collapse, [[], ["timestamp:4"]])

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
        self.assertEqual(client.http_requests, 1)
        self.assertEqual(client.http_status_counts[404], 1)
        self.assertEqual(client.transport_errors, 0)

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
        self.assertEqual(client.http_requests, 2)
        self.assertEqual(client.transport_errors, 2)
        self.assertEqual(sum(client.http_status_counts.values()), 0)

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
            all(
                capsule.extraction_method == "cdx_query_range_bounded"
                for capsule in result.capsules
            )
        )
        self.assertEqual(result.key, range_key)

    async def test_range_stops_when_every_year_is_already_proven(self):
        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, request=request)
            ),
            max_retries=0,
        ) as client:
            async def dense_pages(hostname, year_from, year_to, **_kwargs):
                yield (
                    [
                        {
                            "timestamp": "19960102030405",
                            "original": "http://example.com/a",
                            "statuscode": "200",
                        },
                        {
                            "timestamp": "19970102030405",
                            "original": "http://example.com/b",
                            "statuscode": "200",
                        },
                    ],
                    False,
                )
                yield (
                    [
                        {
                            "timestamp": "19980102030405",
                            "original": "http://example.com/c",
                            "statuscode": "200",
                        }
                    ],
                    False,
                )
                raise AssertionError("range probe consumed unnecessary later page")

            with patch.object(client, "iter_range_pages", dense_pages):
                result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.DECOMPOSED)
        self.assertEqual(result.candidate_years, (1996, 1997))
        self.assertEqual(result.followup_years, (1998,))
        self.assertEqual(result.pages_seen, 1)
        self.assertEqual(
            tuple(capsule.year for capsule in result.capsules),
            (1996, 1997),
        )

    async def test_incomplete_range_keeps_positive_capsule_without_negative_claim(self):
        range_key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
            max_retries=0,
        ) as client:
            async def partial_pages(hostname, year_from, year_to, **_kwargs):
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

        self.assertEqual(result.state, CDXQueryState.DECOMPOSED)
        self.assertEqual(result.candidate_years, (1997,))
        self.assertEqual(result.followup_years, (1996, 1998))
        self.assertEqual(tuple(capsule.year for capsule in result.capsules), (1997,))


    async def test_bounded_range_uses_exactly_one_provider_request_with_resume_key(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
                ["resume-token!"],
            ]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        key = EvidenceQueryKey(
            "example.com", TemporalScope(1996, 1998), "wayback", "cdx-v1"
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=0,
        ) as client:
            result = await client.query_range(key)

        self.assertEqual(calls, 1)
        self.assertEqual(result.provider_requests, 1)
        self.assertEqual(result.state, CDXQueryState.DECOMPOSED)
        self.assertEqual(result.candidate_years, (1997,))
        self.assertEqual(result.followup_years, (1996, 1998))


    async def test_domain_probe_returns_many_exact_host_years_in_one_request(self):
        seen_query = None

        async def handler(request):
            nonlocal seen_query
            seen_query = parse_qs(request.url.query.decode())
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
                ["19980102030405", "http://www.example.com/a", "200"],
                ["19990102030405", "http://shop.example.com/b", "302"],
                ["20000102030405", "http://outside.test/", "200"],
                ["resume-token!"],
            ]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1996, 2001),
            "wayback",
            "domain-amplification-v1",
        )
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=0,
        ) as client:
            result = await client.query_domain(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.provider_requests, 1)
        self.assertEqual(
            [(capsule.hostname, capsule.year) for capsule in result.capsules],
            [
                ("example.com", 1997),
                ("shop.example.com", 1999),
                ("www.example.com", 1998),
            ],
        )
        self.assertEqual(seen_query["matchType"], ["domain"])
        self.assertEqual(seen_query["showResumeKey"], ["true"])
        self.assertEqual(seen_query["collapse"], ["timestamp:4"])

    async def test_rate_limiter_strictly_spaces_requests_above_one_rps(self):
        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b"[]", request=request)
            ),
            max_retries=0,
            requests_per_second=4.0,
        ) as client:
            self.assertIsNotNone(client._limiter)
            self.assertEqual(client._limiter.max_rate, 1)
            self.assertAlmostEqual(client._limiter.time_period, 0.25)

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
            async def incomplete_pages(hostname, year_from, year_to, **_kwargs):
                yield ([], False)

            with patch.object(client, "iter_range_pages", incomplete_pages):
                result = await client.query_range(range_key)

        self.assertEqual(result.state, CDXQueryState.DECOMPOSED)
        self.assertEqual(result.candidate_years, ())
        self.assertEqual(result.followup_years, (1996, 1997, 1998))



    async def test_request_start_gap_telemetry_tracks_real_request_timeline(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, content=b"[]", request=request)

        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=1,
            backoff=0,
            throttle_floor_seconds=0,
            requests_per_second=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(client.http_requests, 2)
        self.assertEqual(client.request_start_segments, 1)
        self.assertEqual(client.request_start_gaps, 1)
        self.assertGreaterEqual(client.request_start_gap_milliseconds, 0)
        self.assertEqual(
            sum(client.request_start_gap_buckets.values()),
            client.request_start_gaps,
        )
        self.assertEqual(client.request_start_excess_gap_milliseconds, 0)

    async def test_wait_state_counters_measure_retry_and_shared_cooldown(self):
        calls = 0

        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, content=b"[]", request=request)

        async with AsyncWaybackCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=1,
            backoff=0.005,
            max_backoff=0.005,
            throttle_floor_seconds=0.03,
            requests_per_second=0,
        ) as client:
            result = await client.query_key(self.key())

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(calls, 2)
        self.assertGreater(client.retry_backoff_wait_milliseconds, 0)
        self.assertGreater(client.cooldown_wait_milliseconds, 0)


if __name__ == "__main__":
    unittest.main()
