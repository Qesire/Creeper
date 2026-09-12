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
from creeper.source_discovery.models import MeasurementMode, SourceCandidate, SourceLevel


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
    def candidate(url: str, *, exact_year: int | None = None) -> SourceCandidate:
        return SourceCandidate(
            canonical_entrypoint=url,
            source_family="BULK_ARTIFACT",
            level=SourceLevel.SOURCE,
            discovered_by="test",
            discovery_strategy="META_SOURCE_SEARCH",
            expected_year_from=1996 if exact_year is None else exact_year,
            expected_year_to=2001 if exact_year is None else exact_year,
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
            self.assertEqual(request.headers.get("Accept-Encoding"), "identity")
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
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.observed_host_year_pairs, 2)
        self.assertEqual(measurement.novel_host_year_pairs, 2)
        self.assertEqual(measurement.novel_pair_eed, 2.0)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_dated_source_keeps_new_year_for_partially_known_hostname(self) -> None:
        body = b'com,known)/ 19980101000000 {"url":"https://known.com/new"}\n'

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(206, body, headers={"content-type": "text/plain"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(min_unique_hosts=1, min_novel_hosts=1),
            )
            result = await scout(self.candidate("https://archive.example/index.cdxj"))

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.novel_hosts, 0)
        self.assertEqual(measurement.novel_host_year_pairs, 1)
        self.assertEqual(measurement.novel_pair_eed, 1.0)
        self.assertEqual(measurement.novel_eed_for_ranking, 1.0)

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
        self.assertEqual(result.measurement.singleton_observations, 1)
        self.assertEqual(result.measurement.doubleton_observations, 1)
        self.assertAlmostEqual(
            result.measurement.estimated_unseen_fraction,
            1 / 3,
        )
        self.assertEqual(len(result.measurement.minhash_values), 64)

    async def test_progressive_scout_early_accepts_strong_low_fidelity_sample(self) -> None:
        body = (
            b"https://novel-a.org/a\n"
            b"https://novel-b.org/b\n"
            b"https://novel-c.org/c\n"
            + b"x" * 2048
        )
        calls: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            raw_range = request.headers["Range"]
            calls.append(raw_range)
            start_text, end_text = raw_range.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = min(int(end_text), len(body) - 1)
            chunk = body[start : end + 1]
            return streamed_response(
                206,
                chunk,
                headers={
                    "content-type": "text/plain",
                    "content-range": f"bytes {start}-{end}/{len(body)}",
                    "content-length": str(len(chunk)),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"org": Decimal("1")},
                policy=self.policy(
                    max_download_bytes=1024,
                    progressive_initial_bytes=128,
                    sample_windows=4,
                    min_unique_hosts=1,
                    min_novel_hosts=1,
                    min_novel_fraction=0.01,
                    min_novel_eed=0.1,
                    early_accept_multiplier=1.0,
                ),
            )
            result = await scout(
                self.candidate("https://data.example/strong.urls")
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIn("early-accepted", result.reason)
        self.assertEqual(len(calls), 1)
        self.assertLessEqual(
            result.measurement.bytes_read if result.measurement else 0,
            128,
        )

    async def test_large_cdxj_stratifies_fixed_byte_budget_across_file(self) -> None:
        lines = []
        for index in range(24):
            hostname = "known.com" if index < 6 else f"novel-{index}.com"
            year = 1997 + (index % 4)
            lines.append(
                f"com,{hostname.split('.')[0]})/ {year}0101000000 "
                f'{{"url":"http://{hostname}/page-{index:02d}"}}\n'
            )
        body = "".join(lines).encode()
        ranges: list[tuple[int, int]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            raw_range = request.headers["Range"]
            span = raw_range.removeprefix("bytes=")
            start_text, end_text = span.split("-", 1)
            start = int(start_text)
            requested_end = int(end_text)
            end = min(requested_end, len(body) - 1)
            ranges.append((start, end))
            chunk = body[start : end + 1]
            return streamed_response(
                206,
                chunk,
                headers={
                    "content-type": "text/plain",
                    "content-range": f"bytes {start}-{end}/{len(body)}",
                    "content-length": str(len(chunk)),
                },
            )

        policy = self.policy(
            max_download_bytes=480,
            sample_windows=4,
            max_records=100,
            min_unique_hosts=2,
            min_novel_hosts=1,
            min_novel_fraction=0.01,
            min_novel_eed=0.1,
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=policy,
            )
            result = await scout(
                self.candidate("https://archive.example/large-index.cdxj")
            )

        self.assertEqual(len(ranges), 4)
        self.assertEqual(ranges[0][0], 0)
        self.assertGreater(ranges[-1][0], len(body) // 2)
        self.assertLessEqual(sum(end - start + 1 for start, end in ranges), 480)
        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.requests, 4)
        self.assertLessEqual(measurement.bytes_read, 480)
        self.assertGreater(measurement.novel_hosts, 0)
        self.assertGreater(measurement.novel_host_year_pairs, 0)

    async def test_single_year_url_list_ranks_partially_known_host_as_novel_pair(self) -> None:
        raw = b"https://known.com/from-webbase\n"
        body = gzip.compress(raw)

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                206,
                body,
                headers={"content-type": "application/gzip"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(
                    min_unique_hosts=1,
                    min_novel_hosts=1,
                    min_novel_fraction=0.0,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate(
                    "https://data.example/webbase-2001.urls.gz",
                    exact_year=2001,
                )
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.novel_hosts, 0)
        self.assertEqual(measurement.observed_host_year_pairs, 1)
        self.assertEqual(measurement.novel_host_year_pairs, 1)
        self.assertEqual(measurement.novel_pair_eed, 1.0)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_gzipped_jsonl_uses_bounded_mature_stream_path(self) -> None:
        calls = 0
        raw = b'\n'.join(
            [
                json.dumps({"url": "https://known.com/a"}).encode(),
                json.dumps({"hostname": "novel.com"}).encode(),
            ]
        ) + b"\n"
        body = gzip.compress(raw)

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return streamed_response(
                206,
                body,
                headers={
                    "content-type": "application/gzip",
                    "content-range": f"bytes 0-{len(body) - 1}/{len(body) + 1000}",
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
        self.assertEqual(calls, 1)
        self.assertEqual(result.measurement.requests if result.measurement else None, 1)

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
