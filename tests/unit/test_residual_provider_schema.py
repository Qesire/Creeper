from __future__ import annotations

import asyncio
import unittest

import httpx

from creeper.source_discovery.deterministic_search import (
    DataCiteSearchProvider,
    DeterministicSearchExecutor,
    HarvardDataverseSearchProvider,
    InternetArchiveSearchProvider,
    ZenodoSearchProvider,
)
from creeper.source_discovery.residual_search import QueryPlan, SearchCell


class ResidualProviderSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = QueryPlan(
            cell=SearchCell(
                mechanism="proxy_access",
                institution="university",
                period="1998",
                artifact="trace",
            ),
            query='"1998" "proxy" "university" "trace"',
            variant=0,
            exclusions=(),
            score=1.0,
            mechanism_phrase="proxy",
            include_institution=True,
            query_shape="STRICT_4D",
        )

    def _run_provider(self, provider_factory, payload):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=payload)

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = provider_factory(client)
                return await provider.search(self.plan, limit=4)

        return asyncio.run(run())

    def test_datacite_schema_drift_is_not_empty_result(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "DataCite response schema incomplete"):
            self._run_provider(DataCiteSearchProvider, {"meta": {"total": 0}})
        self.assertEqual(
            self._run_provider(DataCiteSearchProvider, {"data": []}),
            (),
        )

    def test_zenodo_schema_drift_is_not_empty_result(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Zenodo response schema incomplete"):
            self._run_provider(ZenodoSearchProvider, {"hits": {"total": 0}})
        self.assertEqual(
            self._run_provider(ZenodoSearchProvider, {"hits": {"hits": []}}),
            (),
        )

    def test_dataverse_schema_drift_is_not_empty_result(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError,
            "Harvard Dataverse response schema incomplete",
        ):
            self._run_provider(
                HarvardDataverseSearchProvider,
                {"status": "OK", "data": {"total_count": 0}},
            )
        self.assertEqual(
            self._run_provider(
                HarvardDataverseSearchProvider,
                {"status": "OK", "data": {"items": []}},
            ),
            (),
        )

    def test_internet_archive_search_schema_drift_is_not_empty_result(self) -> None:
        with self.assertRaisesRegex(
            RuntimeError,
            "Internet Archive response schema incomplete",
        ):
            self._run_provider(
                InternetArchiveSearchProvider,
                {"responseHeader": {"status": 0}},
            )
        self.assertEqual(
            self._run_provider(
                InternetArchiveSearchProvider,
                {"response": {"docs": []}},
            ),
            (),
        )

    def test_internet_archive_metadata_schema_drift_fails_whole_provider(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/advancedsearch.php":
                return httpx.Response(
                    200,
                    json={
                        "response": {
                            "docs": [
                                {
                                    "identifier": "proxy98",
                                    "title": "1998 University Proxy Trace",
                                }
                            ]
                        }
                    },
                )
            if request.url.path == "/metadata/proxy98/files":
                return httpx.Response(200, json={"count": 0})
            raise AssertionError(f"unexpected request: {request.url}")

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = InternetArchiveSearchProvider(client, max_items=1)
                return await provider.search(self.plan, limit=4)

        with self.assertRaisesRegex(
            RuntimeError,
            "Internet Archive bounded item expansion incomplete",
        ):
            asyncio.run(run())

    def test_executor_rejects_duplicate_physical_provider_names(self) -> None:
        class Provider:
            name = "same"

            async def search(self, plan, *, limit):
                return ()

        with self.assertRaisesRegex(ValueError, "provider names must be unique"):
            DeterministicSearchExecutor((Provider(), Provider()))


if __name__ == "__main__":
    unittest.main()
