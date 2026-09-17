from __future__ import annotations

import asyncio
import unittest

import httpx

from creeper.source_discovery.deterministic_search import (
    DeterministicSearchExecutor,
    InternetArchiveSearchProvider,
)
from creeper.source_discovery.residual_search import QueryPlan, SearchCell


class ResidualProviderCompletenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = QueryPlan(
            cell=SearchCell(
                mechanism="proxy_access",
                institution="university",
                period="1998",
                artifact="trace",
            ),
            query='"1998" "proxy" "university" "trace"',
            exclusions=(),
            variant=0,
            mechanism_phrase="proxy",
            include_institution=True,
        )

    def test_executor_fails_closed_when_any_configured_provider_fails(self) -> None:
        class EmptyProvider:
            name = "empty"

            async def search(self, plan, *, limit):
                return ()

        class FailingProvider:
            name = "failing"

            async def search(self, plan, *, limit):
                raise RuntimeError("temporary provider outage")

        async def run():
            executor = DeterministicSearchExecutor(
                (EmptyProvider(), FailingProvider())
            )
            return await executor(self.plan)

        with self.assertRaisesRegex(
            RuntimeError,
            r"configured deterministic search providers incomplete.*failing",
        ):
            asyncio.run(run())

    def test_internet_archive_fails_closed_on_partial_item_expansion(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/advancedsearch.php":
                return httpx.Response(
                    200,
                    json={
                        "response": {
                            "docs": [
                                {
                                    "identifier": "item-ok",
                                    "title": "1998 University Proxy Trace",
                                    "description": "proxy access log trace",
                                    "date": "1998",
                                },
                                {
                                    "identifier": "item-fails",
                                    "title": "1998 University Proxy Trace",
                                    "description": "proxy access log trace",
                                    "date": "1998",
                                },
                            ]
                        }
                    },
                )
            if request.url.path == "/metadata/item-ok/files":
                return httpx.Response(
                    200,
                    json={
                        "result": [
                            {
                                "name": "proxy.log",
                                "source": "original",
                                "format": "Text",
                                "size": "10",
                            }
                        ]
                    },
                )
            if request.url.path == "/metadata/item-fails/files":
                raise httpx.ConnectError(
                    "temporary metadata outage",
                    request=request,
                )
            raise AssertionError(f"unexpected request: {request.url}")

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = InternetArchiveSearchProvider(
                    client,
                    max_items=2,
                    files_per_item=1,
                )
                return await provider.search(self.plan, limit=10)

        with self.assertRaisesRegex(
            RuntimeError,
            r"Internet Archive bounded item expansion incomplete",
        ):
            asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
