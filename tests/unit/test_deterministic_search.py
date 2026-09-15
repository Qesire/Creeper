from __future__ import annotations

import asyncio
import unittest

import httpx

from creeper.source_discovery.deterministic_search import (
    DataCiteSearchProvider,
    DeterministicSearchExecutor,
    DeterministicSearchPolicy,
    HarvardDataverseSearchProvider,
    InternetArchiveSearchProvider,
    ZenodoSearchProvider,
    candidate_from_result,
    classify_result,
    relevance_score,
)
from creeper.source_discovery.models import SourceLevel
from creeper.source_discovery.residual_search import QueryPlan, SearchCell
from creeper.source_discovery.search_identity import RawSearchResult


class DeterministicSearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cell = SearchCell(
            mechanism="proxy_access",
            institution="university",
            period="1998",
            artifact="trace",
        )
        self.plan = QueryPlan(
            cell=self.cell,
            query='"1998" "proxy trace" university "trace dataset"',
            variant=0,
            exclusions=(),
            score=1.0,
        )
        self.policy = DeterministicSearchPolicy(
            results_per_provider=10,
            max_total_results=20,
            min_relevance_score=0.55,
            timeout_seconds=5.0,
        )

    def test_relevance_requires_target_mechanism_not_dataset_popularity(self) -> None:
        relevant = RawSearchResult(
            provider="datacite",
            provider_result_id="10.1234/proxy",
            url="https://data.example/proxy-1998.zip",
            title="1998 University HTTP Proxy Trace Dataset",
            resource_type="Dataset",
        )
        unrelated = RawSearchResult(
            provider="datacite",
            provider_result_id="10.1234/modern",
            url="https://data.example/modern.zip",
            title="Modern genomics dataset",
            resource_type="Dataset",
            publication_year=2026,
        )

        self.assertGreaterEqual(relevance_score(self.cell, relevant), 0.55)
        self.assertLess(relevance_score(self.cell, unrelated), 0.55)
        self.assertTrue(
            classify_result(self.plan, relevant, policy=self.policy).qualified
        )
        self.assertFalse(
            classify_result(self.plan, unrelated, policy=self.policy).qualified
        )

    def test_common_crawl_result_is_rejected_before_identity_registration(self) -> None:
        result = RawSearchResult(
            provider="datacite",
            provider_result_id="10.1234/cc",
            url="https://example.org/common-crawl-1998.zip",
            title="1998 Common Crawl proxy trace dataset",
            resource_type="Dataset",
        )
        self.assertIsNone(classify_result(self.plan, result, policy=self.policy))

    def test_datacite_provider_prefers_direct_content_url(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/dois")
            self.assertIn("query", request.url.params)
            query = request.url.params["query"]
            self.assertIn('"1998"', query)
            self.assertIn('"proxy"', query)
            self.assertIn('"university"', query)
            self.assertIn('"trace"', query)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "10.1234/proxy98",
                            "attributes": {
                                "doi": "10.1234/proxy98",
                                "titles": [{"title": "1998 University Proxy Trace"}],
                                "publisher": "Example University",
                                "publicationYear": 2004,
                                "types": {"resourceTypeGeneral": "Dataset"},
                                "url": "https://repo.example/record/1",
                                "contentUrl": [
                                    "https://repo.example/files/proxy98.zip"
                                ],
                                "creators": [{"name": "Research Group"}],
                                "descriptions": [
                                    {"description": "HTTP access log proxy dataset"}
                                ],
                            },
                        }
                    ]
                },
            )

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = DataCiteSearchProvider(client)
                return await provider.search(self.plan, limit=10)

        results = asyncio.run(run())
        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0].url,
            "https://repo.example/files/proxy98.zip",
        )
        self.assertEqual(results[0].provider_result_id, "10.1234/proxy98")

    def test_datacite_query_rotates_mechanism_variant_without_losing_cell_anchors(self) -> None:
        variant_plan = QueryPlan(
            cell=self.cell,
            query='"1998" "access log" university "trace dataset"',
            variant=2,
            exclusions=("famous proxy trace",),
            score=1.0,
        )

        seen_query = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal seen_query
            seen_query = request.url.params["query"]
            return httpx.Response(200, json={"data": []})

        async def run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = DataCiteSearchProvider(client)
                await provider.search(variant_plan, limit=10)

        asyncio.run(run())
        self.assertIsNotNone(seen_query)
        self.assertIn('"access log"', seen_query)
        self.assertIn('"university"', seen_query)
        self.assertIn('"trace"', seen_query)
        self.assertIn('-"famous proxy trace"', seen_query)

    def test_harvard_dataverse_provider_returns_direct_file_source(self) -> None:
        observed = {}

        def handler(request: httpx.Request) -> httpx.Response:
            observed["path"] = request.url.path
            observed["q"] = request.url.params["q"]
            observed["type"] = request.url.params["type"]
            observed["per_page"] = int(request.url.params["per_page"])
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": {
                        "items": [
                            {
                                "name": "proxy98.log.gz",
                                "type": "file",
                                "url": (
                                    "https://dataverse.harvard.edu/"
                                    "api/access/datafile/12345"
                                ),
                                "file_id": "12345",
                                "description": "HTTP proxy access log trace",
                                "published_at": "2015-02-03T04:05:06Z",
                                "file_type": "Gzip Archive",
                                "file_content_type": "application/gzip",
                                "size_in_bytes": 987654,
                                "file_persistent_id": (
                                    "doi:10.7910/DVN/PROXY98/FILE1"
                                ),
                                "dataset_name": (
                                    "1998 University Web Proxy Trace Dataset"
                                ),
                                "dataset_persistent_id": (
                                    "doi:10.7910/DVN/PROXY98"
                                ),
                                "dataset_citation": (
                                    "Example Group, 2015, "
                                    "\"1998 University Web Proxy Trace Dataset\""
                                ),
                            }
                        ]
                    },
                },
            )

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = HarvardDataverseSearchProvider(client)
                return await provider.search(self.plan, limit=100)

        results = asyncio.run(run())

        self.assertEqual(observed["path"], "/api/search")
        self.assertEqual(observed["type"], "file")
        self.assertEqual(observed["per_page"], 100)
        self.assertIn('"1998"', observed["q"])
        self.assertIn('"proxy"', observed["q"])
        self.assertIn('"university"', observed["q"])
        self.assertIn('"trace"', observed["q"])
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result.provider, "harvard_dataverse")
        self.assertEqual(result.provider_result_id, "12345")
        self.assertEqual(
            result.url,
            "https://dataverse.harvard.edu/api/access/datafile/12345",
        )
        self.assertEqual(result.content_length, 987654)
        self.assertEqual(
            result.identifiers,
            (
                "10.7910/DVN/PROXY98",
                "10.7910/DVN/PROXY98/FILE1",
            ),
        )
        classified = classify_result(self.plan, result, policy=self.policy)
        self.assertIsNotNone(classified)
        assert classified is not None
        candidate = candidate_from_result(self.plan, classified)
        self.assertEqual(candidate.level, SourceLevel.SOURCE)
        self.assertEqual(candidate.discovered_by, "deterministic:harvard_dataverse")

    def test_internet_archive_provider_expands_bounded_original_files(self) -> None:
        requests: list[tuple[str, dict[str, str]]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(
                (
                    request.url.path,
                    dict(request.url.params.multi_items()),
                )
            )
            if request.url.path == "/advancedsearch.php":
                self.assertEqual(request.url.params["rows"], "2")
                self.assertEqual(request.url.params["output"], "json")
                query = request.url.params["q"]
                self.assertIn('"1998"', query)
                self.assertIn('"proxy"', query)
                self.assertIn('"university"', query)
                self.assertIn('"trace"', query)
                return httpx.Response(
                    200,
                    json={
                        "response": {
                            "numFound": 1,
                            "start": 0,
                            "docs": [
                                {
                                    "identifier": "proxy-trace-1998",
                                    "title": "1998 University HTTP Proxy Trace",
                                    "description": (
                                        "Historical proxy access log dataset"
                                    ),
                                    "creator": ["Example Network Lab"],
                                    "date": "1998-09-01",
                                    "mediatype": "software",
                                    "collection": ["opensource"],
                                    "files_count": 300,
                                }
                            ],
                        }
                    },
                )
            if request.url.path == "/metadata/proxy-trace-1998/files":
                self.assertEqual(request.url.params["start"], "0")
                self.assertEqual(request.url.params["count"], "7")
                return httpx.Response(
                    200,
                    json={
                        "result": [
                            {
                                "name": "proxy-trace-1998_meta.xml",
                                "source": "original",
                                "format": "Metadata",
                            },
                            {
                                "name": "derived.txt",
                                "source": "derivative",
                                "format": "Text",
                            },
                            {
                                "name": "proxy access 1998.log",
                                "source": "original",
                                "format": "Text",
                                "size": "12345",
                            },
                            {
                                "name": "hosts.csv",
                                "source": "original",
                                "format": "CSV",
                                "size": "23456",
                            },
                        ]
                    },
                )
            raise AssertionError(f"unexpected request: {request.url}")

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = InternetArchiveSearchProvider(
                    client,
                    max_items=2,
                    files_per_item=2,
                    file_slice_count=7,
                )
                return await provider.search(self.plan, limit=10)

        results = asyncio.run(run())

        self.assertEqual(len(requests), 2)
        self.assertEqual(len(results), 2)
        self.assertEqual(
            {item.provider_result_id for item in results},
            {
                "proxy-trace-1998:hosts.csv",
                "proxy-trace-1998:proxy access 1998.log",
            },
        )
        self.assertEqual(
            {item.content_length for item in results},
            {12345, 23456},
        )
        log_result = next(
            item for item in results
            if item.provider_result_id.endswith("proxy access 1998.log")
        )
        self.assertEqual(
            log_result.url,
            (
                "https://archive.org/download/proxy-trace-1998/"
                "proxy%20access%201998.log"
            ),
        )
        self.assertEqual(log_result.publication_year, 1998)
        classified = [
            classify_result(self.plan, item, policy=self.policy)
            for item in results
        ]
        self.assertTrue(all(item is not None for item in classified))
        canonical = [item for item in classified if item is not None]
        self.assertEqual(
            len({item.dataset_key for item in canonical}),
            1,
        )
        candidate = candidate_from_result(self.plan, canonical[0])
        self.assertEqual(candidate.level, SourceLevel.SOURCE)
        self.assertEqual(
            candidate.discovered_by,
            "deterministic:internet_archive",
        )

    def test_internet_archive_provider_caps_item_expansion(self) -> None:
        metadata_calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal metadata_calls
            if request.url.path == "/advancedsearch.php":
                self.assertEqual(request.url.params["rows"], "2")
                return httpx.Response(
                    200,
                    json={
                        "response": {
                            "docs": [
                                {
                                    "identifier": f"item-{index}",
                                    "title": f"1998 Proxy Trace {index}",
                                    "description": "proxy access log trace",
                                    "date": "1998",
                                }
                                for index in range(5)
                            ]
                        }
                    },
                )
            if request.url.path.startswith("/metadata/item-"):
                metadata_calls += 1
                identifier = request.url.path.split("/")[2]
                return httpx.Response(
                    200,
                    json={
                        "result": [
                            {
                                "name": f"{identifier}.log",
                                "source": "original",
                                "format": "Text",
                                "size": "10",
                            }
                        ]
                    },
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
                    file_slice_count=3,
                )
                return await provider.search(self.plan, limit=100)

        results = asyncio.run(run())

        self.assertEqual(metadata_calls, 2)
        self.assertEqual(len(results), 2)

    def test_zenodo_provider_prefers_direct_file_and_bounds_page_size(self) -> None:
        observed_size = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal observed_size
            self.assertEqual(request.url.path, "/api/records")
            observed_size = int(request.url.params["size"])
            query = request.url.params["q"]
            self.assertIn('"1998"', query)
            self.assertIn('"proxy"', query)
            self.assertIn('"university"', query)
            self.assertIn('"trace"', query)
            return httpx.Response(
                200,
                json={
                    "hits": {
                        "hits": [
                            {
                                "id": 42,
                                "pids": {
                                    "doi": {
                                        "identifier": "10.5281/zenodo.42"
                                    }
                                },
                                "metadata": {
                                    "title": "1998 University Proxy Trace",
                                    "description": "HTTP access log dataset",
                                    "publication_date": "2004-03-01",
                                    "resource_type": {
                                        "id": "dataset",
                                        "title": "Dataset",
                                    },
                                    "creators": [
                                        {
                                            "person_or_org": {
                                                "name": "Example Group"
                                            }
                                        }
                                    ],
                                    "keywords": ["proxy", "HTTP", "trace"],
                                },
                                "files": {
                                    "entries": {
                                        "proxy98.zip": {
                                            "size": 123456,
                                            "checksum": (
                                                "sha256:"
                                                + "a" * 64
                                            ),
                                            "links": {
                                                "content": (
                                                    "https://zenodo.org/api/records/"
                                                    "42/files/proxy98.zip/content"
                                                )
                                            },
                                        }
                                    }
                                },
                                "links": {
                                    "self_html": "https://zenodo.org/records/42"
                                },
                            }
                        ]
                    }
                },
            )

        async def run():
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                provider = ZenodoSearchProvider(client)
                return await provider.search(self.plan, limit=100)

        results = asyncio.run(run())
        self.assertEqual(observed_size, 25)
        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0].url,
            "https://zenodo.org/api/records/42/files/proxy98.zip/content",
        )
        self.assertEqual(results[0].content_length, 123456)
        self.assertEqual(results[0].checksum_sha256, "a" * 64)
        self.assertEqual(results[0].identifiers, ("10.5281/zenodo.42",))
        classified = classify_result(self.plan, results[0], policy=self.policy)
        self.assertIsNotNone(classified)
        candidate = candidate_from_result(self.plan, classified)
        self.assertEqual(candidate.level, SourceLevel.SOURCE)

    def test_harvard_dataverse_and_datacite_doi_share_dataset_identity(self) -> None:
        datacite = RawSearchResult(
            provider="datacite",
            provider_result_id="10.7910/DVN/PROXY98",
            url="https://doi.org/10.7910/DVN/PROXY98",
            title="1998 University Web Proxy Trace Dataset",
            publisher="Harvard Dataverse",
        )
        dataverse = RawSearchResult(
            provider="harvard_dataverse",
            provider_result_id="12345",
            url="https://dataverse.harvard.edu/api/access/datafile/12345",
            title=(
                "1998 University Web Proxy Trace Dataset — proxy98.log.gz"
            ),
            publisher="Harvard Dataverse",
            resource_type="file",
            identifiers=("10.7910/DVN/PROXY98",),
        )

        first = classify_result(self.plan, datacite, policy=self.policy)
        second = classify_result(self.plan, dataverse, policy=self.policy)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual(first.dataset_key, second.dataset_key)

    def test_zenodo_and_datacite_same_doi_collapse_to_one_dataset(self) -> None:
        datacite = RawSearchResult(
            provider="datacite",
            provider_result_id="10.5281/zenodo.42",
            url="https://doi.org/10.5281/zenodo.42",
            title="1998 University Proxy Trace",
            publisher="Zenodo",
        )
        zenodo = RawSearchResult(
            provider="zenodo",
            provider_result_id="42",
            url="https://zenodo.org/api/records/42/files/proxy98.zip/content",
            title="1998 University Proxy Trace",
            publisher="Zenodo",
            identifiers=("10.5281/zenodo.42",),
        )

        first = classify_result(self.plan, datacite, policy=self.policy)
        second = classify_result(self.plan, zenodo, policy=self.policy)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(first.dataset_key, second.dataset_key)

    def test_executor_returns_pure_canonical_results(self) -> None:
        class Provider:
            name = "fixture"

            async def search(self, plan, *, limit):
                return (
                    RawSearchResult(
                        provider=self.name,
                        provider_result_id="fixture-1",
                        url="https://example.edu/proxy98.txt",
                        title="1998 Proxy Access Log Trace",
                        publication_year=1998,
                        resource_type="Dataset",
                    ),
                )

        batch = asyncio.run(
            DeterministicSearchExecutor(
                (Provider(),),
                policy=self.policy,
            )(self.plan)
        )
        self.assertEqual(batch.backend, "fixture")
        self.assertEqual(len(batch.results), 1)
        self.assertTrue(batch.results[0].qualified)

        candidate = candidate_from_result(self.plan, batch.results[0])
        self.assertEqual(candidate.expected_year_from, 1998)
        self.assertEqual(candidate.expected_year_to, 1998)
        self.assertEqual(candidate.discovery_strategy, "RESIDUAL_CELL_SEARCH")
        self.assertTrue(candidate.source_family.startswith("RESIDUAL_PROXY_ACCESS:"))
        self.assertGreater(len(candidate.source_family), len("RESIDUAL_PROXY_ACCESS:"))


if __name__ == "__main__":
    unittest.main()
