import unittest

from creeper.source_research.adapters.base import RootQuery, SearchCheckpoint
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
    def query(self):
        return RootQuery("q1", "datacite", "web archive", 10, 120.0, 1000)

    async def test_follows_links_next_and_resumes_cursor(self):
        transport = FakeTransport([
            Response(200, {"data": [{"id": "doi:10.1234/ABC", "attributes": {
                "titles": [{"title": "x"}], "publicationYear": 1999,
                "publisher": "P", "types": {"resourceTypeGeneral": "Dataset"},
            }}], "links": {"next": "https://api.datacite.org/dois?page[cursor]=NEXT"}}),
            Response(200, {"data": [{"id": "10.1234/abc"}], "links": {"next": None}}),
        ])
        adapter = DataCiteAdapter(transport=transport)
        first = await adapter.search(self.query(), None)
        self.assertFalse(first.terminal)
        self.assertEqual(first.next_checkpoint.next_url, "https://api.datacite.org/dois?page[cursor]=NEXT")
        second = await adapter.search(self.query(), first.next_checkpoint)
        self.assertTrue(second.terminal)
        self.assertEqual(transport.calls[1][0], "https://api.datacite.org/dois?page[cursor]=NEXT")

    async def test_rate_limit_is_retryable_and_doi_deduplicates(self):
        transport = FakeTransport([Response(429, headers={"Retry-After": "7"})])
        page = await DataCiteAdapter(transport=transport).search(self.query(), None)
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 7.0)

        transport = FakeTransport([Response(200, {"data": [
            {"id": "10.1234/ABC", "attributes": {}},
            {"id": "doi:10.1234/abc", "attributes": {}},
        ], "links": {"next": None}})])
        page = await DataCiteAdapter(transport=transport).search(self.query(), None)
        self.assertEqual(len(page.hits), 1)
        self.assertNotIn("publicationYear", page.hits[0].metadata)

    async def test_metadata_never_becomes_annual_evidence(self):
        transport = FakeTransport([Response(200, {"data": [{"id": "10.1/x", "attributes": {
            "publicationYear": 1996
        }}], "links": {"next": None}})])
        hit = (await DataCiteAdapter(transport=transport).search(self.query(), None)).hits[0]
        self.assertIsNone(getattr(hit, "evidence_year", None))


if __name__ == "__main__":
    unittest.main()
