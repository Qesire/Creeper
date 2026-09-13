import unittest

from creeper.source_research.adapters.base import RootQuery
from creeper.source_research.adapters.github_code import GitHubCodeAdapter, GitHubHitClass, classify_code_hit


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


class GitHubCodeRootTests(unittest.IsolatedAsyncioTestCase):
    def query(self):
        return RootQuery("g1", "github-code", '"webbase-2001"', 5, 60.0, 100)

    async def test_no_auth_is_disabled_not_failure(self):
        transport = FakeTransport([])
        adapter = GitHubCodeAdapter(transport=transport, token=None)
        report = await adapter.probe_capabilities()
        self.assertFalse(report.available)
        self.assertEqual(report.reason, "DISABLED_AUTH")
        page = await adapter.search(self.query(), None)
        self.assertTrue(page.terminal)
        self.assertEqual(page.requests, 0)
        self.assertEqual(transport.calls, [])

    def test_schema_family_classification_does_not_create_artifact_class(self):
        cls = classify_code_hit("docs/schema.md", "crawl_date src dest anchor")
        self.assertEqual(cls, GitHubHitClass.SCHEMA_DOC)
        self.assertNotEqual(cls, GitHubHitClass.DATA_ARTIFACT)

    async def test_hardcoded_download_fossil_becomes_artifact_and_mirrors_dedupe(self):
        url = "https://data.example.org/webbase-2001.tar.gz"
        payload = {"total_count": 2, "incomplete_results": False, "items": [
            {"path": "Makefile", "sha": "abc", "html_url": "https://github.com/a/r/blob/abc/Makefile", "repository": {"full_name": "a/r"}, "text_matches": [{"fragment": f"wget {url}"}]},
            {"path": "scripts/download.sh", "sha": "def", "html_url": "https://github.com/b/r/blob/def/scripts/download.sh", "repository": {"full_name": "b/r"}, "text_matches": [{"fragment": f"curl -O {url}"}]},
        ]}
        page = await GitHubCodeAdapter(transport=FakeTransport([Response(200, payload)]), token="t").search(self.query(), None)
        self.assertEqual(len(page.hits), 2)
        self.assertEqual(len(page.artifact_leads), 1)
        self.assertEqual(page.artifact_leads[0].locator, url)
        self.assertEqual({h.metadata["classification"] for h in page.hits}, {"DOWNLOAD_FOSSIL"})

    async def test_version_pinned_manifest_is_artifact_lead(self):
        payload = {"total_count": 1, "items": [
            {"path": "data/manifest.json", "sha": "abc123",
             "repository": {"full_name": "owner/repo"},
             "text_matches": [{"fragment": "archive dataset manifest"}]},
        ]}
        page = await GitHubCodeAdapter(
            transport=FakeTransport([Response(200, payload)]), token="t"
        ).search(self.query(), None)
        self.assertEqual(page.hits[0].metadata["classification"], "MANIFEST")
        self.assertEqual(
            page.artifact_leads[0].locator,
            "https://raw.githubusercontent.com/owner/repo/abc123/data/manifest.json",
        )
        self.assertFalse(page.hits[0].metadata["annual_evidence_authority"])

    async def test_noise_and_schema_do_not_consume_candidate_pool(self):
        payload = {"total_count": 2, "items": [
            {"path": "vendor/x.min.js", "sha": "a", "repository": {"full_name": "a/r"}, "text_matches": [{"fragment": "cdxj"}]},
            {"path": "docs/schema.txt", "sha": "b", "repository": {"full_name": "a/r"}, "text_matches": [{"fragment": "urlkey timestamp original"}]},
        ]}
        page = await GitHubCodeAdapter(transport=FakeTransport([Response(200, payload)]), token="t").search(self.query(), None)
        self.assertEqual(page.artifact_leads, ())
        self.assertEqual({h.metadata["classification"] for h in page.hits}, {"NOISE", "SCHEMA_DOC"})


if __name__ == "__main__":
    unittest.main()
