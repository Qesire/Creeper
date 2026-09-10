from __future__ import annotations

import unittest

import scrapy
from scrapy.http import HtmlResponse

from creeper_scrapy import settings
from creeper_scrapy.spiders.bounded_links import BoundedLinksSpider


class BoundedLinksSpiderTests(unittest.TestCase):
    @staticmethod
    def response(html: str, *, url: str = "http://example.com/start/") -> HtmlResponse:
        request = scrapy.Request(url, meta={"depth": 1})
        return HtmlResponse(
            url=url,
            request=request,
            body=html.encode("utf-8"),
            encoding="utf-8",
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    def test_emits_all_http_links_but_follows_only_same_site_html_like_paths(self) -> None:
        spider = BoundedLinksSpider(
            start_url="http://example.com/start/",
            source_key="src:test",
        )
        response = self.response(
            """
            <a href="/inside.html#section"> Inside page </a>
            <a href="https://external.example/a">External</a>
            <a href="/archive/data.zip">Dataset</a>
            <a href="/search?q=oldweb">Query trap</a>
            <a href="/directory/">Directory</a>
            """
        )

        outputs = list(spider.parse(response))
        items = [item for item in outputs if isinstance(item, dict)]
        requests = [item for item in outputs if isinstance(item, scrapy.Request)]

        self.assertEqual(len(items), 5)
        self.assertEqual(
            {item["discovered_url"] for item in items},
            {
                "http://example.com/inside.html",
                "https://external.example/a",
                "http://example.com/archive/data.zip",
                "http://example.com/search?q=oldweb",
                "http://example.com/directory/",
            },
        )
        self.assertEqual(
            {request.url for request in requests},
            {"http://example.com/inside.html", "http://example.com/directory/"},
        )
        external = next(item for item in items if "external.example" in item["discovered_url"])
        self.assertFalse(external["same_site"])
        self.assertEqual(external["depth"], 1)

    def test_follow_query_is_explicit_opt_in(self) -> None:
        spider = BoundedLinksSpider(
            start_url="https://example.com/",
            source_key="src:test",
            follow_query="true",
        )
        outputs = list(spider.parse(self.response('<a href="/index.php?year=1998">1998</a>', url="https://example.com/")))
        requests = [item for item in outputs if isinstance(item, scrapy.Request)]
        self.assertEqual([request.url for request in requests], ["https://example.com/index.php?year=1998"])

    def test_default_port_http_to_https_redirect_family_remains_same_site(self) -> None:
        spider = BoundedLinksSpider(
            start_url="http://example.com/",
            source_key="src:test",
        )
        self.assertTrue(spider._same_site("https://example.com/archive/"))
        self.assertFalse(spider._same_site("https://example.com:8443/archive/"))
        self.assertFalse(spider._same_site("https://sub.example.com/archive/"))

    def test_non_html_response_is_not_link_parsed(self) -> None:
        spider = BoundedLinksSpider(
            start_url="https://example.com/",
            source_key="src:test",
        )
        request = scrapy.Request("https://example.com/blob")
        response = HtmlResponse(
            url=request.url,
            request=request,
            body=b'<a href="/should-not-follow">x</a>',
            encoding="utf-8",
            headers={"Content-Type": "application/octet-stream"},
        )
        self.assertEqual(list(spider.parse(response)), [])

    def test_project_does_not_override_scrapy_scheduler_or_static_jobdir(self) -> None:
        self.assertFalse(hasattr(settings, "JOBDIR"))
        self.assertFalse(hasattr(settings, "SCHEDULER"))
        self.assertTrue(settings.ROBOTSTXT_OBEY)
        self.assertTrue(settings.AUTOTHROTTLE_ENABLED)
        self.assertGreater(settings.DOWNLOAD_MAXSIZE, 0)


if __name__ == "__main__":
    unittest.main()
