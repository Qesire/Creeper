import unittest

from creeper.source_research.adapters.archiveit import ArchiveItAdapter
from creeper.source_research.adapters.base import RootQuery, SearchCheckpoint


class Response:
    def __init__(self, status, payload=None, text="", headers=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def __call__(self, url, params=None, headers=None):
        self.calls.append((url, params or {}, headers or {}))
        return self.responses.pop(0)


class ArchiveItRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self):
        return RootQuery("a1", "archiveit", "early web", 5, 60.0, 100)

    async def test_429_is_retryable_and_limit_is_bounded(self):
        transport = FakeTransport([Response(429, headers={"Retry-After": "11"})])
        page = await ArchiveItAdapter(transport=transport).search(self.query(), None)
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 11.0)
        self.assertEqual(transport.calls[0][1]["limit"], 100)
        self.assertNotEqual(transport.calls[0][1]["limit"], -1)

    async def test_seed_resource_and_native_filters_stay_bounded(self):
        payload = {"seeds": [{"id": "s1", "seed_url": "https://example.org/"}]}
        transport = FakeTransport([Response(200, payload)])
        query = RootQuery(
            "a2", "archiveit", "", 2, 30.0, 500,
            native_filters={"resource": "seed", "sort": "id", "pluck": "id", "state": "ACTIVE", "limit": "-1"},
        )
        page = await ArchiveItAdapter(transport=transport, max_limit=100).search(query, None)
        self.assertEqual(transport.calls[0][0], "https://partner.archive-it.org/api/seed")
        self.assertEqual(transport.calls[0][1]["limit"], 100)
        self.assertEqual(transport.calls[0][1]["sort"], "id")
        self.assertEqual(transport.calls[0][1]["pluck"], "id")
        self.assertEqual(transport.calls[0][1]["state"], "ACTIVE")
        self.assertEqual(page.hits[0].provider_type, "SEED")
        self.assertEqual(ArchiveItAdapter.max_inflight, 1)
        self.assertEqual(ArchiveItAdapter.requests_per_second, 0.1)

    async def test_api_failure_switches_to_public_explore_fallback(self):
        transport = FakeTransport([
            Response(404),
            Response(200, text='''<html><body>
              <a href="/collections/1234">Brazilian Web Engines (1997–2013)</a>
            </body></html>'''),
        ])
        adapter = ArchiveItAdapter(transport=transport)
        first = await adapter.search(self.query(), None)
        self.assertFalse(first.terminal)
        self.assertEqual(first.next_checkpoint.query_variant, "explore")
        second = await adapter.search(self.query(), first.next_checkpoint)
        self.assertTrue(second.terminal)
        self.assertEqual(second.hits[0].provider_native_id, "1234")
        self.assertTrue(second.hits[0].metadata["target_period_signal"])
        self.assertTrue(second.hits[0].metadata["ia_derived_overlap_penalty"])
        self.assertFalse(second.hits[0].metadata["annual_evidence_authority"])
        self.assertEqual(second.artifact_leads, ())

    async def test_collection_metadata_never_becomes_evidence(self):
        payload = {"collections": [{"id": 7, "name": "Collection 1999", "archived_since": "1999-01-01"}]}
        page = await ArchiveItAdapter(transport=FakeTransport([Response(200, payload)])).search(self.query(), None)
        self.assertEqual(len(page.hits), 1)
        self.assertFalse(page.hits[0].metadata["annual_evidence_authority"])
        self.assertIsNone(getattr(page.hits[0], "evidence_year", None))


if __name__ == "__main__":
    unittest.main()
