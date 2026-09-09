from __future__ import annotations

import unittest
from urllib.parse import parse_qs, urlsplit

from creeper.records.candidates import CandidateSourceScope
from creeper.sources.archive.arquivo import ArquivoCDXClient, ArquivoCDXSource


class ArquivoSourceTests(unittest.TestCase):
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
