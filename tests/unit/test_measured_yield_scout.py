from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.source_discovery.coordinator import ScoutDisposition
from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel


def streamed_response(
    status: int,
    body: bytes,
    *,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status,
        stream=httpx.ByteStream(body),
        headers=headers,
    )


class MeasuredYieldScoutTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        task = root / "task" / "merged260909-3"
        task.mkdir(parents=True)
        for year in range(1996, 2002):
            values = "known.com\n" if year == 1996 else ""
            (task / f"{year}.txt").write_text(values, encoding="utf-8")
        (task / "candidate_pool.txt").write_text("candidate-only.net\n", encoding="utf-8")
        self.baseline = BaselineIndex.build(root / "task", root / "baseline.sqlite3")

    def tearDown(self) -> None:
        self.baseline.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate(url: str) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=url,
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_year_from=1996,
            expected_year_to=2001,
            expected_volume=100_000,
            enumerability_prior=0.9,
            confidence=0.8,
        )

    def policy(self, **overrides) -> MeasuredYieldScoutPolicy:
        values = dict(
            max_download_bytes=64 * 1024,
            max_decompressed_bytes=128 * 1024,
            max_records=100,
            max_line_bytes=4096,
            min_unique_hosts=2,
            min_novel_hosts=1,
            min_novel_fraction=0.4,
            min_novel_eed=0.5,
            timeout_seconds=2.0,
        )
        values.update(overrides)
        return MeasuredYieldScoutPolicy(**values)

    async def test_cdxj_sample_measures_baseline_external_eed_and_warms(self) -> None:
        body = (
            'com,known)/ 19970101000000 {"url":"http://known.com/a"}\n'
            'com,novel)/ 19980101000000 {"url":"https://novel.com/b"}\n'
            'com,late)/ 20040101000000 {"url":"https://late.com/"}\n'
        ).encode()

        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertIn("Range", request.headers)
            return streamed_response(206, body, headers={"content-type": "text/plain"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            result = await scout(self.candidate("https://archive.example/index.cdxj"))

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.sampled_records, 3)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.novel_hosts, 1)
        self.assertEqual(measurement.novel_eed, 1.0)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_csv_sample_is_measured_but_low_novelty_holds(self) -> None:
        body = b"hostname,other\nknown.com,1\nknown.com,2\nnovel.org,3\n"

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(200, body, headers={"content-type": "text/csv"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(min_novel_eed=0.75),
            )
            result = await scout(self.candidate("https://data.example/hosts.csv"))

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNotNone(result.measurement)
        assert result.measurement is not None
        self.assertEqual(result.measurement.unique_hosts, 2)
        self.assertEqual(result.measurement.novel_hosts, 1)
        self.assertEqual(result.measurement.novel_eed, 0.5)

    async def test_gzipped_jsonl_uses_bounded_mature_stream_path(self) -> None:
        raw = b'\n'.join(
            [
                json.dumps({"url": "https://known.com/a"}).encode(),
                json.dumps({"hostname": "novel.com"}).encode(),
            ]
        ) + b"\n"
        body = gzip.compress(raw)

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                206,
                body,
                headers={"content-type": "application/gzip"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            result = await scout(self.candidate("https://data.example/hosts.jsonl.gz"))

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertEqual(result.measurement.novel_hosts if result.measurement else None, 1)

    async def test_gzip_content_encoding_is_not_double_decoded(self) -> None:
        raw = b'{"hostname":"known.com"}\n{"hostname":"novel.com"}\n'
        body = gzip.compress(raw)

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                206,
                body,
                headers={
                    "content-type": "application/gzip",
                    "content-encoding": "gzip",
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            result = await scout(self.candidate("https://data.example/hosts.jsonl.gz"))

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertEqual(result.measurement.novel_hosts if result.measurement else None, 1)

    async def test_invalid_warc_prefix_fails_closed_without_source_promotion(self) -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return streamed_response(206, b"WARC/1.0\r\n")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            result = await scout(self.candidate("https://data.example/crawl.warc.gz"))

        self.assertEqual(calls, 1)
        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNotNone(result.measurement)
        assert result.measurement is not None
        self.assertEqual(result.measurement.unique_hosts, 0)
        self.assertIn("too few unique hostnames", result.reason)


if __name__ == "__main__":
    unittest.main()
