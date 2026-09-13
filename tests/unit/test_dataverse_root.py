from __future__ import annotations

import unittest

from creeper.source_research.adapters.base import RootQuery, SearchCheckpoint
from creeper.source_research.adapters.dataverse import DataverseAdapter


class Response:
    def __init__(self, status, payload=None, headers=None):
        self.status_code = status
        self._payload = payload or {}
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


class DataverseRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self, **kwargs):
        values = dict(
            query_id="d1",
            root_id="dataverse",
            query_text="web archive",
            max_pages=10,
            max_wall_seconds=120.0,
            page_size=1000,
        )
        values.update(kwargs)
        return RootQuery(**values)

    async def test_flat_file_search_emits_direct_artifact_lead(self):
        payload = {"status": "OK", "data": {"items": [{
            "type": "file",
            "file_id": 7,
            "name": "part.tar.gz",
            "size_in_bytes": 100,
            "md5": "abc",
            "file_persistent_id": "doi:10/x",
            "dataset_persistent_id": "doi:10/d",
            "file_content_type": "application/gzip",
        }], "total_count": 1}}
        transport = FakeTransport([Response(200, payload)])
        adapter = DataverseAdapter("https://dv.example", transport=transport)
        page = await adapter.search(self.query(native_filters={"type": "file"}), None)

        self.assertTrue(page.terminal)
        self.assertEqual(transport.calls[0][1]["type"], "file")
        self.assertEqual(len(page.artifact_leads), 1)
        lead = page.artifact_leads[0]
        self.assertEqual(lead.locator, "https://dv.example/api/access/datafile/7")
        self.assertEqual(lead.size, 100)
        self.assertEqual(lead.checksum, "abc")
        self.assertEqual(lead.persistent_id, "doi:10/x")
        self.assertEqual(lead.parent_persistent_id, "doi:10/d")
        self.assertIsNone(lead.evidence_year)

    async def test_dataset_hit_stays_node_until_artifact_resolution(self):
        payload = {"status": "OK", "data": {"items": [{
            "type": "dataset",
            "global_id": "doi:10/d",
            "name": "dataset",
            "published_at": "1999-01-01T00:00:00Z",
            "url": "https://dv.example/dataset.xhtml?persistentId=doi:10/d",
        }], "total_count": 1}}
        adapter = DataverseAdapter("https://dv.example", transport=FakeTransport([Response(200, payload)]))
        page = await adapter.search(self.query(), None)
        self.assertEqual(len(page.hits), 1)
        self.assertEqual(page.hits[0].provider_type, "DATASET")
        self.assertEqual(page.artifact_leads, ())
        self.assertFalse(hasattr(page.hits[0], "evidence_year"))
        self.assertEqual(await adapter.resolve(page.hits[0]), ())

    async def test_legacy_nested_file_shape_is_supported(self):
        payload = {"status": "OK", "data": {"items": [{
            "type": "file",
            "dataFile": {
                "id": 7,
                "filename": "part.tar.gz",
                "filesize": 100,
                "md5": "abc",
                "persistentId": "doi:10/x",
                "datasetPersistentId": "doi:10/d",
            },
        }], "total_count": 1}}
        page = await DataverseAdapter(
            "https://dv.example", transport=FakeTransport([Response(200, payload)])
        ).search(self.query(native_filters={"type": "file"}), None)
        self.assertEqual(page.artifact_leads[0].provider_native_id, "file:7")

    async def test_start_checkpoint_retry_and_pagination_resume(self):
        retry_transport = FakeTransport([Response(503, headers={"Retry-After": "3"})])
        retry_page = await DataverseAdapter(
            "https://dv.example", transport=retry_transport
        ).search(self.query(native_filters={"type": "file"}), SearchCheckpoint(start=100, page=2, query_variant="file"))
        self.assertFalse(retry_page.terminal)
        self.assertEqual(retry_page.retry_after, 3.0)
        self.assertEqual(retry_transport.calls[0][1]["start"], 100)

        payload = {"status": "OK", "data": {
            "items": [{"type": "dataset", "global_id": "doi:10/a", "name": "a"}],
            "total_count": 3,
        }}
        transport = FakeTransport([Response(200, payload)])
        page = await DataverseAdapter("https://dv.example", transport=transport).search(
            self.query(page_size=1),
            SearchCheckpoint(start=1, page=2, query_variant="dataset"),
        )
        self.assertFalse(page.terminal)
        self.assertEqual(page.next_checkpoint.start, 2)
        self.assertEqual(page.next_checkpoint.page, 3)

    async def test_dynamic_capability_probe_and_optional_token(self):
        payload = {"status": "OK", "data": {"items": [], "total_count": 0}}
        transport = FakeTransport([Response(200, payload)])
        report = await DataverseAdapter(
            "https://dv.example", transport=transport, token="token"
        ).probe_capabilities()
        self.assertTrue(report.available)
        self.assertIn("file_search", report.capabilities)
        self.assertEqual(transport.calls[0][2]["X-Dataverse-key"], "token")

    async def test_query_variant_validation_and_reserved_filter_protection(self):
        with self.assertRaises(ValueError):
            await DataverseAdapter("https://dv.example", transport=FakeTransport([])).search(
                self.query(native_filters={"type": "dataverse"}), None
            )
        with self.assertRaises(ValueError):
            await DataverseAdapter("https://dv.example", transport=FakeTransport([])).search(
                self.query(native_filters={"type": "file", "start": "999"}), None
            )


if __name__ == "__main__":
    unittest.main()
