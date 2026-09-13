import unittest

from creeper.source_research.adapters.base import RootQuery
from creeper.source_research.adapters.zenodo import ZenodoAdapter, recognize_archives_unleashed


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


class ZenodoRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self):
        return RootQuery("z1", "zenodo", "web archive", 10, 120.0, 25)

    async def test_pagination_and_file_artifact_leads(self):
        payload = {"hits": {"hits": [{
            "id": 12, "conceptrecid": "9", "metadata": {"doi": "10.5281/zenodo.12",
            "title": "General web archive collection derivatives"},
            "files": [{"key": "cul-1716-auk.tar.gz", "size": 10, "checksum": "md5:abc",
                        "links": {"self": "https://zenodo.org/api/files/a"}}]
        }], "total": 1}}
        page = await ZenodoAdapter(transport=FakeTransport([Response(200, payload)])).search(self.query(), None)
        self.assertTrue(page.terminal)
        self.assertEqual(len(page.artifact_leads), 1)
        self.assertEqual(page.artifact_leads[0].checksum, "md5:abc")
        self.assertEqual(page.hits[0].provider_native_id, "12")

    def test_family_recognizer_is_only_a_prior(self):
        result = recognize_archives_unleashed(
            "General web archive collection derivatives",
            ["x-auk.tar.gz"], "crawl_date,src,dest,anchor")
        self.assertTrue(result)
