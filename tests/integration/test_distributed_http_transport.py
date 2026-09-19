from __future__ import annotations

import json
import unittest

import httpx

from creeper.distributed.http_transport import AuthorityPermitTransport
from creeper.evidence.policies import CDXQueryState, EvidenceQueryKey, TemporalScope
from creeper.evidence.providers.async_cdx import AsyncWaybackCDXClient


class _TwoChunkStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"a"
        yield b"b"

    async def aclose(self) -> None:
        return None


class DistributedAuthorityTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_each_real_cdx_retry_consumes_one_authority_permit(self) -> None:
        calls = 0
        permits: list[str] = []
        reports: list[tuple[str, int | None, int]] = []

        async def acquire() -> object:
            token = f"permit-{len(permits) + 1}"
            permits.append(token)
            return token

        async def report(
            token: object,
            status_code: int | None,
            _headers: httpx.Headers | None,
            response_bytes: int,
        ) -> None:
            reports.append((str(token), status_code, response_bytes))

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, request=request)
            payload = [
                ["timestamp", "original", "statuscode"],
                ["19970102030405", "http://example.com/", "200"],
            ]
            return httpx.Response(
                200,
                content=json.dumps(payload).encode(),
                request=request,
            )

        transport = AuthorityPermitTransport(
            httpx.MockTransport(handler),
            acquire=acquire,
            report=report,
        )
        key = EvidenceQueryKey(
            "example.com",
            TemporalScope(1997, 1997),
            "wayback",
            "cdx-v1",
        )
        async with AsyncWaybackCDXClient(
            transport=transport,
            max_retries=1,
            backoff=0,
            throttle_floor_seconds=0,
            requests_per_second=0,
        ) as client:
            result = await client.query_key(key)

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(calls, 2)
        self.assertEqual(permits, ["permit-1", "permit-2"])
        self.assertEqual(
            reports,
            [
                ("permit-1", 503, 0),
                ("permit-2", 200, len(json.dumps([
                    ["timestamp", "original", "statuscode"],
                    ["19970102030405", "http://example.com/", "200"],
                ]).encode())),
            ],
        )

    async def test_permit_is_held_until_response_stream_is_consumed(self) -> None:
        reports: list[tuple[str, int | None, int]] = []

        async def acquire() -> object:
            return "permit-stream"

        async def report(
            token: object,
            status_code: int | None,
            _headers: httpx.Headers | None,
            response_bytes: int,
        ) -> None:
            reports.append((str(token), status_code, response_bytes))

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                stream=_TwoChunkStream(),
                request=request,
            )

        transport = AuthorityPermitTransport(
            httpx.MockTransport(handler),
            acquire=acquire,
            report=report,
        )
        request = httpx.Request("GET", "https://example.invalid/")
        response = await transport.handle_async_request(request)

        self.assertEqual(reports, [])
        body = b"".join([chunk async for chunk in response.stream])
        self.assertEqual(body, b"ab")
        self.assertEqual(reports, [("permit-stream", 200, 2)])

        # Closing after complete consumption must not double-report/release.
        await response.aclose()
        self.assertEqual(reports, [("permit-stream", 200, 2)])


if __name__ == "__main__":
    unittest.main()
