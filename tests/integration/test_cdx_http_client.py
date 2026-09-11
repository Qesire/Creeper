import gzip
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from creeper.evidence.policies import CDXQueryState
from creeper.evidence.providers.cdx import WaybackCDXClient, query_year


class WaybackCDXClientTests(unittest.TestCase):
    def test_query_range_uses_inclusive_year_bounds(self):
        urls = []

        def fetch(url, timeout, headers):
            urls.append(url)
            return json.dumps([["timestamp", "original", "statuscode"]]).encode()

        client = WaybackCDXClient(fetch=fetch, max_retries=0)
        pages = list(client.query_range("example.com", 1996, 1998))

        self.assertEqual(pages, [([], True)])
        query = parse_qs(urlsplit(urls[0]).query)
        self.assertEqual(query["from"], ["19960101000000"])
        self.assertEqual(query["to"], ["19981231235959"])
        self.assertIn("urlkey", query["fl"][0].split(","))
        self.assertEqual(query["filter"], ["statuscode:[23][0-9][0-9]"])

    def test_empty_resume_key_page_followed_by_complete_empty_page_is_exhaustive(self):
        responses = [
            json.dumps([
                ["timestamp", "original", "statuscode"],
                ["19970101000000", "http://other.example/", "200"],
                [],
                ["resume-token!"]
            ]).encode(),
            json.dumps([
                ["timestamp", "original", "statuscode"],
            ]).encode(),
        ]

        def fetch(url, timeout, headers):
            return responses.pop(0)

        client = WaybackCDXClient(fetch=fetch, max_retries=0)
        result = query_year("example.com", 1997, client)
        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(result.pages_seen, 2)

    def test_gzip_http_body_is_decoded_before_json_parsing(self):
        payload = json.dumps([
            ["timestamp", "original", "statuscode"],
            ["19970101000000", "http://example.com/", "200"],
        ]).encode()
        client = WaybackCDXClient(
            fetch=lambda url, timeout, headers: gzip.compress(payload),
            max_retries=0,
        )
        result = query_year("example.com", 1997, client)
        self.assertEqual(result.state, CDXQueryState.PASS)

    def test_resume_key_pages_are_consumed_before_exhaustive_empty(self):
        responses = [
            json.dumps([
                ["timestamp", "original", "statuscode"],
                ["19970101000000", "http://other.example/", "200"],
                [],
                ["resume-token!"]
            ]).encode(),
            json.dumps([
                ["timestamp", "original", "statuscode"],
                ["19971231000000", "http://example.com/", "200"]
            ]).encode(),
        ]
        urls = []

        def fetch(url, timeout, headers):
            urls.append(url)
            return responses.pop(0)

        client = WaybackCDXClient(fetch=fetch, max_retries=0)
        result = query_year("example.com", 1997, client)
        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(result.pages_seen, 2)
        self.assertIn("showResumeKey=true", urls[0])
        self.assertIn("resumeKey=resume-token%21", urls[1])

    def test_rate_limit_becomes_transient_after_bounded_retries(self):
        calls = []

        def fetch(url, timeout, headers):
            calls.append(url)
            raise HTTPError(url, 429, "rate limited", {}, None)

        client = WaybackCDXClient(fetch=fetch, max_retries=1, sleep=lambda seconds: None)
        result = query_year("example.com", 1997, client)
        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
