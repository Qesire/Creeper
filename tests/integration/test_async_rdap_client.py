import json
import unittest

import httpx

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.evidence.providers.async_rdap import AsyncRDAPClient


class AsyncRDAPClientTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def key(hostname: str = "example.com") -> EvidenceQueryKey:
        return EvidenceQueryKey(
            hostname,
            TemporalScope(1996, 2001),
            "rdap",
            "rdap-registration-v1",
        )

    async def test_registration_event_in_target_period_is_positive_evidence(self):
        async def handler(request):
            payload = {
                "ldhName": "EXAMPLE.COM",
                "events": [
                    {
                        "eventAction": "registration",
                        "eventDate": "1998-04-05T00:00:00Z",
                    }
                ],
            }
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.candidate_years, (1998,))
        self.assertEqual(result.provider_requests, 1)
        self.assertEqual(len(result.capsules), 1)
        capsule = result.capsules[0]
        self.assertEqual(capsule.hostname, "example.com")
        self.assertEqual(capsule.year, 1998)
        self.assertEqual(capsule.evidence_type, "rdap_registration_event")
        self.assertEqual(capsule.extraction_method, "rdap_registration_event")

    async def test_registration_outside_period_is_provider_specific_empty(self):
        async def handler(request):
            payload = {
                "ldhName": "example.com",
                "events": [
                    {
                        "eventAction": "registration",
                        "eventDate": "1992-01-01T00:00:00Z",
                    }
                ],
            }
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(result.capsules, ())

    async def test_missing_registration_event_is_invalid_not_negative(self):
        async def handler(request):
            payload = {
                "ldhName": "example.com",
                "events": [
                    {
                        "eventAction": "last changed",
                        "eventDate": "2000-01-01T00:00:00Z",
                    }
                ],
            }
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.INVALID)
        self.assertIn("no registration event", result.error or "")

    async def test_mismatched_domain_is_rejected(self):
        async def handler(request):
            payload = {
                "ldhName": "other.example",
                "events": [
                    {
                        "eventAction": "registration",
                        "eventDate": "1999-01-01T00:00:00Z",
                    }
                ],
            }
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.INVALID)

    async def test_429_reduces_effective_rate_and_records_retry_after(self):
        async def handler(request):
            return httpx.Response(
                429,
                headers={"Retry-After": "7"},
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=1.0,
            min_requests_per_second=0.1,
            decrease_factor=0.5,
            throttle_floor_seconds=2.0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(client.throttle_events, 1)
        self.assertEqual(client.adaptive_rate_decreases, 1)
        self.assertEqual(client.effective_requests_per_second, 0.5)

    async def test_success_streak_recovers_effective_rate(self):
        async def handler(request):
            payload = {
                "ldhName": "example.com",
                "events": [{
                    "eventAction": "registration",
                    "eventDate": "1998-04-05T00:00:00Z",
                }],
            }
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=1.0,
            min_requests_per_second=0.1,
            recovery_successes=2,
            recovery_step_fraction=0.2,
        ) as client:
            client.effective_requests_per_second = 0.5
            await client.query_range(self.key())
            await client.query_range(self.key())

        self.assertEqual(client.adaptive_rate_increases, 1)
        self.assertAlmostEqual(client.effective_requests_per_second, 0.7)

    async def test_server_error_is_retryable(self):
        async def handler(request):
            return httpx.Response(503, request=request)

        async with AsyncRDAPClient(
            transport=httpx.MockTransport(handler),
            requests_per_second=0,
        ) as client:
            result = await client.query_range(self.key())

        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(result.provider_requests, 1)


if __name__ == "__main__":
    unittest.main()
