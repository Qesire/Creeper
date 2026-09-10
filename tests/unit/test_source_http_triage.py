from __future__ import annotations

import unittest

import httpx

from creeper.source_discovery.coordinator import TriageDisposition
from creeper.source_discovery.models import SourceCandidate, SourceLevel
from creeper.source_discovery.triage import HttpSourceTriageExecutor, TriageTransientError


class HttpSourceTriageExecutorTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def candidate() -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint="https://example.com/catalog/",
            source_family="RESOURCE_CATALOG",
            level=SourceLevel.METASOURCE,
            discovered_by="test",
            discovery_strategy="test",
            confidence=0.5,
        )

    async def test_head_success_admits_scout_without_get(self) -> None:
        methods: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            methods.append(request.method)
            return httpx.Response(200, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await HttpSourceTriageExecutor(client)(self.candidate())

        self.assertEqual(result.disposition, TriageDisposition.SCOUT)
        self.assertEqual(methods, ["HEAD"])

    async def test_head_rejection_falls_back_to_streaming_range_get(self) -> None:
        requests: list[tuple[str, str | None]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append((request.method, request.headers.get("Range")))
            if request.method == "HEAD":
                return httpx.Response(405, request=request)
            return httpx.Response(206, request=request, content=b"x")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await HttpSourceTriageExecutor(client)(self.candidate())

        self.assertEqual(result.disposition, TriageDisposition.SCOUT)
        self.assertEqual(requests, [("HEAD", None), ("GET", "bytes=0-0")])

    async def test_permanent_missing_entrypoint_is_held_not_rejected(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await HttpSourceTriageExecutor(client)(self.candidate())

        self.assertEqual(result.disposition, TriageDisposition.HOLD)
        self.assertIn("HTTP 404", result.reason)

    async def test_server_error_is_transient_for_coordinator_retry(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaisesRegex(TriageTransientError, "HTTP 503"):
                await HttpSourceTriageExecutor(client)(self.candidate())

    async def test_network_error_is_transient_for_coordinator_retry(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with self.assertRaisesRegex(TriageTransientError, "ConnectError"):
                await HttpSourceTriageExecutor(client)(self.candidate())


if __name__ == "__main__":
    unittest.main()
