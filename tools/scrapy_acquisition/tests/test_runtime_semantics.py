from __future__ import annotations

from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest


class _SiteHandler(BaseHTTPRequestHandler):
    hits: Counter[str] = Counter()

    pages = {
        "/": b'<a href="/a.html">a</a><a href="/shared.html">shared</a><a href="/deep/1.html">deep</a>',
        "/a.html": b'<a href="/shared.html">shared again</a><a href="/b.html">b</a>',
        "/b.html": b"done",
        "/shared.html": b"shared",
        "/deep/1.html": b'<a href="/deep/2.html">two</a>',
        "/deep/2.html": b'<a href="/deep/3.html">three</a>',
        "/deep/3.html": b"three",
    }

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        type(self).hits[self.path] += 1
        body = type(self).pages.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ScrapyRuntimeSemanticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        _SiteHandler.hits = Counter()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _SiteHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address
        cls.base_url = f"http://{host}:{port}"
        cls.project_dir = Path(__file__).resolve().parents[1]

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _crawl(
        self,
        *,
        jobdir: Path,
        output: Path,
        page_count: int,
        depth_limit: int,
        start_path: str = "/",
    ) -> None:
        command = [
            sys.executable,
            "-m",
            "scrapy",
            "crawl",
            "bounded_links",
            "-a",
            f"start_url={self.base_url}{start_path}",
            "-a",
            "source_key=src:runtime-test",
            "-O",
            str(output),
            "-s",
            f"JOBDIR={jobdir}",
            "-s",
            "ROBOTSTXT_OBEY=False",
            "-s",
            "LOG_ENABLED=False",
            "-s",
            "CONCURRENT_REQUESTS=1",
            "-s",
            "CONCURRENT_REQUESTS_PER_DOMAIN=1",
            "-s",
            "DOWNLOAD_DELAY=0",
            "-s",
            "AUTOTHROTTLE_ENABLED=False",
            "-s",
            f"CLOSESPIDER_PAGECOUNT={page_count}",
            "-s",
            f"DEPTH_LIMIT={depth_limit}",
        ]
        subprocess.run(command, cwd=self.project_dir, check=True, timeout=30)

    def test_jobdir_resume_and_native_dupefilter(self) -> None:
        _SiteHandler.hits = Counter()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobdir = root / "job"
            self._crawl(
                jobdir=jobdir,
                output=root / "first.jsonl",
                page_count=2,
                depth_limit=4,
            )
            first_root_hits = _SiteHandler.hits["/"]
            self.assertEqual(first_root_hits, 1)

            self._crawl(
                jobdir=jobdir,
                output=root / "second.jsonl",
                page_count=50,
                depth_limit=4,
            )

            # JOBDIR persists the request fingerprint set, so the spider's
            # start URL is filtered on restart instead of being downloaded again.
            self.assertEqual(_SiteHandler.hits["/"], 1)
            # /shared.html is discovered from two different pages; Scrapy's
            # scheduler/dupefilter owns this deduplication, not Creeper.
            self.assertEqual(_SiteHandler.hits["/shared.html"], 1)
            self.assertGreaterEqual(sum(_SiteHandler.hits.values()), 4)

    def test_depth_limit_is_enforced_by_scrapy_middleware(self) -> None:
        _SiteHandler.hits = Counter()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._crawl(
                jobdir=root / "depth-job",
                output=root / "depth.jsonl",
                page_count=50,
                depth_limit=1,
                start_path="/deep/1.html",
            )

            self.assertEqual(_SiteHandler.hits["/deep/1.html"], 1)
            self.assertEqual(_SiteHandler.hits["/deep/2.html"], 1)
            self.assertEqual(_SiteHandler.hits["/deep/3.html"], 0)

    def test_close_spider_pagecount_is_a_real_fetch_bound_with_single_inflight(self) -> None:
        _SiteHandler.hits = Counter()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._crawl(
                jobdir=root / "bound-job",
                output=root / "bound.jsonl",
                page_count=2,
                depth_limit=4,
            )
            self.assertLessEqual(sum(_SiteHandler.hits.values()), 2)


if __name__ == "__main__":
    unittest.main()
