from __future__ import annotations

import unittest

from creeper.source_research.adapters.base import RootQuery, RootRequestError
from creeper.source_research.adapters.datacite import DataCiteAdapter


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, url, params=None, headers=None):
        self.calls.append((url, params or {}, headers or {}))
        return self.responses.pop(0)


class Response:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self):
        return self._payload


class DataCiteRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self, **kwargs):
        values = dict(
            query_id="q1",
            root_id="datacite",
            query_text="web archive",
            max_pages=10,
            max_wall_seconds=120.0,
            page_size=1000,
        )
        values.update(kwargs)
        return RootQuery(**values)

    async def test_follows_links_next_exactly_and_resumes_without_rebuilding_params(self):
        next_url = "https://api.datacite.org/dois?page%5Bcursor%5D=opaque-token&page%5Bsize%5D=1000"
        transport = FakeTransport([
            Response(200, {
                "data": [{
                    "id": "doi:10.1234/ABC",
                    "attributes": {"titles": [{"title": "x"}], "publicationYear": 1999},
                }],
                "links": {"next": next_url},
            }),
            Response(200, {"data": [{"id": "10.1234/def", "attributes": {}}], "links": {"next": None}}),
        ])
        adapter = DataCiteAdapter(transport=transport)

        first = await adapter.search(self.query(), None)
        self.assertFalse(first.terminal)
        self.assertEqual(first.next_checkpoint.next_url, next_url)
        self.assertEqual(first.next_checkpoint.page, 2)
        self.assertEqual(transport.calls[0][1]["page[cursor]"], "1")
        self.assertEqual(transport.calls[0][1]["page[size]"], 1000)
        self.assertIn("fields[dois]", transport.calls[0][1])

        second = await adapter.search(self.query(), first.next_checkpoint)
        self.assertTrue(second.terminal)
        self.assertEqual(transport.calls[1][0], next_url)
        self.assertEqual(transport.calls[1][1], {})

    async def test_page_budget_stops_at_bound_without_manufacturing_negative_evidence(self):
        transport = FakeTransport([Response(200, {
            "data": [{"id": "10.1/a", "attributes": {}}],
            "links": {"next": "https://api.datacite.org/dois?page%5Bcursor%5D=more"},
        })])
        page = await DataCiteAdapter(transport=transport).search(self.query(max_pages=1), None)
        self.assertTrue(page.terminal)
        self.assertIsNone(page.next_checkpoint)
        self.assertEqual(len(page.hits), 1)

    async def test_rate_limit_and_server_failures_preserve_checkpoint(self):
        for status, headers, expected in (
            (429, {"Retry-After": "7"}, 7.0),
            (503, {}, 5.0),
        ):
            with self.subTest(status=status):
                transport = FakeTransport([Response(status, headers=headers)])
                page = await DataCiteAdapter(transport=transport).search(self.query(), None)
                self.assertFalse(page.terminal)
                self.assertEqual(page.retry_after, expected)
                self.assertEqual(page.next_checkpoint.page, 1)

    async def test_non_retryable_http_failure_is_not_silently_treated_as_empty(self):
        adapter = DataCiteAdapter(transport=FakeTransport([Response(400, {"errors": []})]))
        with self.assertRaises(RootRequestError):
            await adapter.search(self.query(), None)

    async def test_doi_normalization_dedup_and_deterministic_metadata_pivots(self):
        payload = {
            "data": [
                {
                    "id": "https://doi.org/10.1234/ABC",
                    "attributes": {
                        "publisher": "Example Publisher",
                        "publicationYear": 1998,
                        "creators": [{
                            "name": "Ada Example",
                            "nameIdentifiers": [{"nameIdentifier": "https://orcid.org/0000-0000"}],
                        }],
                        "relatedIdentifiers": [{"relatedIdentifier": "10.9999/REL"}],
                        "contentUrl": ["https://objects.example/a.warc.gz"],
                    },
                    "relationships": {
                        "client": {"data": {"id": "repo.client"}},
                        "provider": {"data": {"id": "provider.member"}},
                    },
                },
                {"id": "doi:10.1234/abc", "attributes": {}},
            ],
            "links": {"next": None},
        }
        page = await DataCiteAdapter(transport=FakeTransport([Response(200, payload)])).search(self.query(), None)
        self.assertEqual(len(page.hits), 1)
        hit = page.hits[0]
        self.assertEqual(hit.provider_native_id, "10.1234/abc")
        self.assertEqual(hit.metadata["publication_year"], 1998)
        self.assertFalse(hasattr(hit, "evidence_year"))
        pivots = {(pivot["kind"], pivot["value"]) for pivot in hit.metadata["pivots"]}
        self.assertIn(("client", "repo.client"), pivots)
        self.assertIn(("provider", "provider.member"), pivots)
        self.assertIn(("publisher", "Example Publisher"), pivots)
        self.assertIn(("creator", "Ada Example"), pivots)
        self.assertIn(("creator", "https://orcid.org/0000-0000"), pivots)
        self.assertIn(("related_identifier", "10.9999/REL"), pivots)

        leads = await DataCiteAdapter(transport=FakeTransport([])).resolve(hit)
        self.assertEqual(len(leads), 1)
        self.assertEqual(leads[0].locator, "https://objects.example/a.warc.gz")
        self.assertIsNone(leads[0].evidence_year)

    async def test_native_filters_cannot_override_cursor_controls(self):
        query = self.query(native_filters={"page[size]": "1"})
        with self.assertRaises(ValueError):
            await DataCiteAdapter(transport=FakeTransport([])).search(query, None)

    async def test_probe_reports_retryable_state_without_throwing(self):
        report = await DataCiteAdapter(
            transport=FakeTransport([Response(429, headers={"Retry-After": "1"})])
        ).probe_capabilities()
        self.assertFalse(report.available)
        self.assertEqual(report.status_code, 429)
        self.assertEqual(report.reason, "retryable_status:429")


if __name__ == "__main__":
    unittest.main()
