from __future__ import annotations

import unittest

from creeper.source_research.adapters.base import RootQuery
from creeper.source_research.adapters.zenodo import ZenodoAdapter, recognize_archives_unleashed


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


class ZenodoRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self, **kwargs):
        values = dict(
            query_id="z1",
            root_id="zenodo",
            query_text="web archive",
            max_pages=10,
            max_wall_seconds=120.0,
            page_size=100,
        )
        values.update(kwargs)
        return RootQuery(**values)

    async def test_links_next_pagination_version_identity_and_file_leads(self):
        next_url = "https://zenodo.org/api/records/?page=2&size=25&q=web+archive"
        payload = {
            "hits": {
                "hits": [{
                    "id": 12,
                    "conceptrecid": "9",
                    "conceptdoi": "10.5281/zenodo.9",
                    "metadata": {
                        "doi": "10.5281/zenodo.12",
                        "title": "General web archive collection derivatives",
                        "description": "crawl_date src dest anchor",
                        "version": "2",
                    },
                    "files": [{
                        "key": "cul-1716-auk.tar.gz",
                        "size": 10,
                        "checksum": "md5:abc",
                        "links": {"self": "https://zenodo.org/api/files/a/content"},
                    }],
                }],
                "total": 26,
            },
            "links": {"next": next_url},
        }
        transport = FakeTransport([Response(200, payload)])
        page = await ZenodoAdapter(transport=transport).search(self.query(), None)

        self.assertFalse(page.terminal)
        self.assertEqual(page.next_checkpoint.next_url, next_url)
        self.assertEqual(len(page.artifact_leads), 1)
        lead = page.artifact_leads[0]
        self.assertEqual(lead.checksum, "md5:abc")
        self.assertEqual(lead.persistent_id, "10.5281/zenodo.12")
        self.assertEqual(lead.parent_persistent_id, "10.5281/zenodo.9")
        hit = page.hits[0]
        self.assertEqual(hit.metadata["version"], "2")
        self.assertEqual(hit.metadata["concept_record_id"], "9")
        self.assertTrue(hit.metadata["family_prior"])
        self.assertFalse(hasattr(hit, "evidence_year"))

    async def test_anonymous_and_authenticated_page_size_caps(self):
        payload = {"hits": {"hits": [], "total": 0}, "links": {}}
        anonymous = FakeTransport([Response(200, payload)])
        await ZenodoAdapter(transport=anonymous).search(self.query(page_size=1000), None)
        self.assertEqual(anonymous.calls[0][1]["size"], 25)
        self.assertNotIn("Authorization", anonymous.calls[0][2])

        authenticated = FakeTransport([Response(200, payload)])
        await ZenodoAdapter(transport=authenticated, token="secret").search(self.query(page_size=1000), None)
        self.assertEqual(authenticated.calls[0][1]["size"], 100)
        self.assertEqual(authenticated.calls[0][2]["Authorization"], "Bearer secret")

    async def test_inveniordm_file_entries_mapping_is_supported(self):
        payload = {
            "hits": {
                "hits": [{
                    "id": "99",
                    "pids": {"doi": {"identifier": "10.5281/zenodo.99"}},
                    "metadata": {"title": "x"},
                    "files": {"entries": {
                        "part.parquet": {
                            "size": 123,
                            "checksum": {"algorithm": "md5", "value": "deadbeef"},
                            "mimetype": "application/octet-stream",
                            "links": {"content": "https://zenodo.org/api/records/99/files/part.parquet/content"},
                        }
                    }},
                }],
                "total": {"value": 1},
            },
            "links": {},
        }
        page = await ZenodoAdapter(transport=FakeTransport([Response(200, payload)])).search(self.query(), None)
        self.assertTrue(page.terminal)
        self.assertEqual(page.artifact_leads[0].checksum, "md5:deadbeef")
        self.assertEqual(page.artifact_leads[0].size, 123)


    async def test_duplicate_files_are_deduplicated_and_zero_size_is_preserved(self):
        payload = {
            "hits": {
                "hits": [{
                    "id": "101",
                    "metadata": {"title": "x", "doi": "10.5281/zenodo.101"},
                    "files": [
                        {
                            "key": "empty.cdxj",
                            "size": 0,
                            "checksum": "md5:0",
                            "links": {"self": "https://zenodo.org/api/files/empty"},
                        },
                        {
                            "key": "empty.cdxj",
                            "size": 0,
                            "checksum": "md5:0",
                            "links": {"self": "https://zenodo.org/api/files/empty"},
                        },
                    ],
                }],
                "total": 1,
            },
            "links": {},
        }
        page = await ZenodoAdapter(transport=FakeTransport([Response(200, payload)])).search(self.query(), None)
        self.assertEqual(len(page.artifact_leads), 1)
        self.assertEqual(page.artifact_leads[0].size, 0)

    async def test_retry_preserves_checkpoint(self):
        page = await ZenodoAdapter(
            transport=FakeTransport([Response(503)])
        ).search(self.query(), None)
        self.assertFalse(page.terminal)
        self.assertEqual(page.retry_after, 5.0)
        self.assertEqual(page.next_checkpoint.page, 1)

    def test_archives_unleashed_recognizer_is_strict_scheduling_prior(self):
        self.assertTrue(
            recognize_archives_unleashed(
                "General web archive collection derivatives",
                ["x-auk.tar.gz"],
                "",
            )
        )
        self.assertTrue(
            recognize_archives_unleashed(
                "other",
                ["x-parquet.tar.gz"],
                "crawl_date src dest anchor",
            )
        )
        self.assertFalse(
            recognize_archives_unleashed(
                "General web archive collection derivatives",
                ["readme.txt"],
                "crawl_date src dest anchor",
            )
        )


if __name__ == "__main__":
    unittest.main()
