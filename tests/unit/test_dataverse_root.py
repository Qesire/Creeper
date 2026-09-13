import unittest

from creeper.source_research.adapters.base import RootQuery, SearchCheckpoint
from creeper.source_research.adapters.dataverse import DataverseAdapter


class Response:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self._payload, self.headers = status, payload or {}, headers or {}

    def json(self):
        return self._payload


class FakeTransport:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    async def __call__(self, url, params=None, headers=None):
        self.calls.append((url, params or {}, headers or {}))
        return self.responses.pop(0)


class DataverseRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self):
        return RootQuery("d1", "dataverse", "web archive", 10, 120.0, 1000)

    async def test_dataset_and_file_queries_are_distinct_and_file_resolves(self):
        payload = {"status": "OK", "data": {"items": [
            {"type": "file", "dataFile": {"id": 7, "filename": "part.tar.gz",
             "filesize": 100, "md5": "abc", "persistentId": "doi:10/x",
             "datasetPersistentId": "doi:10/d"}},
            {"type": "dataset", "global_id": "doi:10/d", "name": "dataset"}
        ], "total_count": 2}}
        adapter = DataverseAdapter("https://dv.example", transport=FakeTransport([Response(200, payload)]))
        page = await adapter.search(self.query(), None)
        self.assertTrue(page.terminal)
        self.assertEqual(len(page.artifact_leads), 1)
        self.assertEqual(page.artifact_leads[0].size, 100)
        self.assertEqual(page.artifact_leads[0].persistent_id, "doi:10/x")
        self.assertEqual(page.hits[1].provider_type, "DATASET")

    async def test_start_checkpoint_and_retry(self):
        transport = FakeTransport([Response(503, headers={"Retry-After": "3"})])
        page = await DataverseAdapter("https://dv.example", transport=transport).search(self.query(), SearchCheckpoint(start=100))
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 3.0)
        self.assertEqual(transport.calls[0][1]["start"], 100)


if __name__ == "__main__":
    unittest.main()
