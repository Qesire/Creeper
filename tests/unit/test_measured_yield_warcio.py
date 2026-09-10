from __future__ import annotations

import io
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import httpx
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from creeper.authority.baseline_index import BaselineIndex
from creeper.source_discovery.coordinator import ScoutDisposition
from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
)
from creeper.source_discovery.models import SourceCandidate, SourceLevel


def build_warc() -> bytes:
    output = io.BytesIO()
    writer = WARCWriter(output, gzip=True)
    for url, date in (
        ("https://known.com/a", "1998-01-01T00:00:00Z"),
        ("https://novel.com/b", "1999-02-02T00:00:00Z"),
        ("https://late.com/c", "2004-03-03T00:00:00Z"),
    ):
        headers = StatusAndHeaders(
            "200 OK",
            [("Content-Type", "text/html")],
            protocol="HTTP/1.0",
        )
        record = writer.create_warc_record(
            url,
            "response",
            payload=io.BytesIO(b"<html></html>"),
            http_headers=headers,
            warc_headers_dict={"WARC-Date": date},
        )
        writer.write_record(record)
    return output.getvalue()


class MeasuredYieldWarcioTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        task = root / "task" / "merged260909-3"
        task.mkdir(parents=True)
        for year in range(1996, 2002):
            (task / f"{year}.txt").write_text(
                "known.com\n" if year == 1996 else "",
                encoding="utf-8",
            )
        (task / "candidate_pool.txt").write_text("", encoding="utf-8")
        self.baseline = BaselineIndex.build(root / "task", root / "baseline.sqlite3")

    def tearDown(self) -> None:
        self.baseline.close()
        self.tmp.cleanup()

    @staticmethod
    def candidate() -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint="https://archive.example/sample.warc.gz",
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

    @staticmethod
    def policy() -> MeasuredYieldScoutPolicy:
        return MeasuredYieldScoutPolicy(
            max_download_bytes=256 * 1024,
            max_decompressed_bytes=512 * 1024,
            max_records=20,
            max_line_bytes=4096,
            min_unique_hosts=2,
            min_novel_hosts=1,
            min_novel_fraction=0.4,
            min_novel_eed=0.5,
            timeout_seconds=2.0,
        )

    async def run_scout(self, body: bytes):
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertIn("Range", request.headers)
            return httpx.Response(
                206,
                content=body,
                headers={"content-type": "application/warc"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            return await scout(self.candidate())

    async def test_record_compressed_warc_uses_target_years_only(self) -> None:
        result = await self.run_scout(build_warc())

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.sampled_records, 3)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.novel_hosts, 1)
        self.assertEqual(measurement.novel_eed, 1.0)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_truncated_final_gzip_member_keeps_complete_prefix_sample(self) -> None:
        body = build_warc()
        result = await self.run_scout(body[:-24])

        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertGreaterEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.novel_hosts, 1)


if __name__ == "__main__":
    unittest.main()
