from __future__ import annotations

import unittest
from datetime import datetime, timezone

from creeper.source_research.adapters.base import (
    ArtifactLead,
    RootQuery,
    SearchCheckpoint,
    retry_after_seconds,
    safe_next_url,
)


class Response:
    def __init__(self, value):
        self.headers = {"Retry-After": value}


class RepositoryRootBaseTests(unittest.TestCase):
    def test_contract_validation(self):
        with self.assertRaises(ValueError):
            RootQuery("", "x", "q", 1, 1.0)
        with self.assertRaises(ValueError):
            RootQuery("q", "x", "q", 0, 1.0)
        with self.assertRaises(ValueError):
            SearchCheckpoint(page=0)
        with self.assertRaises(ValueError):
            ArtifactLead("x", "id", "file:///tmp/a")

    def test_artifact_lead_cannot_be_constructed_with_evidence_year(self):
        with self.assertRaises(TypeError):
            ArtifactLead("x", "id", "https://example.test/a", evidence_year=1999)  # type: ignore[call-arg]

    def test_safe_next_url_rejects_origin_escape(self):
        self.assertEqual(
            safe_next_url("https://api.example.test/root", "/next"),
            "https://api.example.test/next",
        )
        with self.assertRaises(ValueError):
            safe_next_url("https://api.example.test/root", "https://evil.example/next")

    def test_retry_after_http_date(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(
            retry_after_seconds(Response("Thu, 01 Jan 2026 00:00:10 GMT"), now=now),
            10.0,
        )


if __name__ == "__main__":
    unittest.main()
