from __future__ import annotations

import unittest
import os
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

import httpx

from creeper.records.candidates import CandidateSourceScope
from creeper.sources.archive.arquivo import ArquivoCDXClient, ArquivoCDXSource


class ArquivoSourceTests(unittest.TestCase):
    def test_default_client_ignores_unsupported_socks_all_proxy(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HTTPS_PROXY": "http://127.0.0.1:7897",
                "HTTP_PROXY": "http://127.0.0.1:7897",
                "ALL_PROXY": "socks://127.0.0.1:7897",
            },
            clear=True,
        ):
            client = ArquivoCDXClient(timeout=1, max_retries=0)
            client.close()

    def test_arquivo_cdx_client_builds_bounded_year_filtered_json_query(self) -> None:
        captured: list[str] = []

        def fetch(url: str, timeout: float, headers: dict[str, str]) -> bytes:
            captured.append(url)
            return b'[{"url":"http://old.example.pt/","timestamp":"19991231120000","status":"200"}]'

        client = ArquivoCDXClient(fetch=fetch, limit=7)
        rows = client.query("*.example.pt", from_year=1999, to_year=2000)

        self.assertEqual(
            rows,
            [{"url": "http://old.example.pt/", "timestamp": "19991231120000", "status": "200"}],
        )
        query = parse_qs(urlsplit(captured[0]).query)
        self.assertEqual(query["url"], ["*.example.pt"])
        self.assertEqual(query["matchType"], ["domain"])
        self.assertEqual(query["from"], ["1999"])
        self.assertEqual(query["to"], ["2000"])
        self.assertEqual(query["output"], ["json"])
        self.assertEqual(query["limit"], ["7"])

    def test_httpx_transport_retries_429_and_reuses_client(self) -> None:
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(429, request=request)
            return httpx.Response(
                200,
                request=request,
                json=[{
                    "url": "http://retry.example.pt/",
                    "timestamp": "19990101000000",
                    "status": "200",
                }],
            )

        transport = httpx.MockTransport(handler)
        with ArquivoCDXClient(
            transport=transport,
            max_retries=1,
            backoff=0,
            limit=3,
        ) as client:
            first = client.query("*.example.pt", from_year=1999, to_year=1999)
            second = client.query("*.example.pt", from_year=1999, to_year=1999)

        self.assertEqual(calls, 3)
        self.assertEqual(client.http_requests, 3)
        self.assertEqual(first[0]["url"], "http://retry.example.pt/")
        self.assertEqual(second[0]["url"], "http://retry.example.pt/")

    def test_non_retryable_4xx_is_rejected_once(self) -> None:
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(404, request=request)

        with ArquivoCDXClient(
            transport=httpx.MockTransport(handler),
            max_retries=3,
            backoff=0,
        ) as client:
            with self.assertRaisesRegex(ValueError, "HTTP 404"):
                client.query("*.missing.pt", from_year=1999, to_year=1999)

        self.assertEqual(calls, 1)

    def test_arquivo_source_preserves_capture_year_and_exact_row_locator(self) -> None:
        def fetch(url: str, timeout: float, headers: dict[str, str]) -> bytes:
            return (
                b'[{"url":"https://a.example.pt/a","timestamp":"19970102030405","status":"200"},'
                b'{"url":"https://b.example.pt/b","timestamp":"20020102030405","status":"200"}]'
            )

        source = ArquivoCDXSource(
            ArquivoCDXClient(fetch=fetch, limit=10),
            seed_urls=["*.example.pt"],
            from_year=1996,
            to_year=2001,
        )
        records = list(source.enumerate())

        self.assertEqual(
            [record.payload for record in records],
            ["https://a.example.pt/a", "https://b.example.pt/b"],
        )
        self.assertEqual([record.source_year for record in records], [1997, 2002])
        self.assertTrue(all(record.scope is CandidateSourceScope.LOCAL_DISCOVERY for record in records))
        self.assertIn("row=1", records[0].locator)
        self.assertEqual(list(source.extract_hosts(records[0]))[0].hostname, "a.example.pt")


if __name__ == "__main__":
    unittest.main()
