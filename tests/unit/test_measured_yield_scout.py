from __future__ import annotations

import gzip
import io
import json
import tempfile
import unittest
import zipfile
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
from creeper.sources.ftp_sitelist import AUDITED_FTP_SITELIST_LOCATORS


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

    async def test_monthly_mailbox_scout_uses_message_dates_for_host_years(self) -> None:
        body = (
            b"From sender@example.net Wed Mar 25 10:38:28 1998\n"
            b"From: Person <person@example.net>\n"
            b"Date: Wed, 25 Mar 1998 10:38:28 -0600\n"
            b"Subject: first\n"
            b"\n"
            b"See http://known.com/a and https://novel.com/b\n"
            b"From sender@example.net Thu Mar 26 11:00:00 1998\n"
            b"From: Person <person@example.net>\n"
            b"Date: Thu, 26 Mar 1998 11:00:00 -0600\n"
            b"Subject: second\n"
            b"\n"
            b"Reference: https://other.org/c\n"
        )

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                200,
                body,
                headers={
                    "content-type": "text/plain",
                    "content-length": str(len(body)),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(
                    min_unique_hosts=2,
                    min_novel_hosts=1,
                    min_novel_fraction=0.1,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate(
                    "https://lists.gnu.org/archive/mbox/lynx-dev/1998-03",
                    exact_year=1998,
                )
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.sampled_records, 2)
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.unique_hosts, 3)
        self.assertEqual(measurement.novel_hosts, 2)
        self.assertEqual(measurement.observed_host_year_pairs, 3)
        # known.com exists only in the 1996 baseline, so 1998 remains novel.
        self.assertEqual(measurement.novel_host_year_pairs, 3)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_dmoz_scout_uses_exact_dump_year_as_ranking_hint(self) -> None:
        raw = (
            b'<RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#">\n'
            b'<link r:resource="http://duplicate.example/ignored"/>\n'
            b'<ExternalPage about="http://known.com/from-directory">\n'
            b'<ExternalPage about="https://novel.org/from-directory">\n'
        )
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
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(
                    min_unique_hosts=2,
                    min_novel_hosts=1,
                    min_novel_fraction=0.1,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate(
                    "https://mirror.example/dmoz/2001-01-22/content.rdf.u8.gz",
                    exact_year=2001,
                )
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.sampled_records, 2)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.observed_host_year_pairs, 2)
        self.assertEqual(measurement.novel_host_year_pairs, 2)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_ftp_sitelist_scout_measures_record_level_dates(self) -> None:
        raw = io.BytesIO()
        with zipfile.ZipFile(
            raw,
            "w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.writestr(
                "part01.txt",
                (
                    "Site: ignored-old.example\nDate: 10-Nov-95\n"
                    "Site: known.com\nDate: 19-Nov-97\n"
                    "Site: novel.org\nDate: 03-Jan-97\n"
                ),
            )
        body = raw.getvalue()

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                200,
                body,
                headers={"content-type": "application/zip"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(
                    min_unique_hosts=2,
                    min_novel_hosts=1,
                    min_novel_fraction=0.1,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate(
                    next(iter(AUDITED_FTP_SITELIST_LOCATORS))
                )
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.sampled_records, 2)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.observed_host_year_pairs, 2)
        self.assertEqual(measurement.novel_host_year_pairs, 2)
        self.assertEqual(measurement.novel_hosts, 1)

    async def test_squid_scout_uses_access_year_and_drops_off_window_rows(self) -> None:
        body = (
            b"915148800.000 1 192.0.2.1 TCP_MISS/200 10 GET "
            b"http://known.com/a - DIRECT/x text/html\n"
            b"915148801.000 1 192.0.2.2 TCP_MISS/200 10 GET "
            b"https://novel.org/b - DIRECT/x text/html\n"
            b"1072915200.000 1 192.0.2.3 TCP_MISS/200 10 GET "
            b"http://late.example/c - DIRECT/x text/html\n"
        )

        async def handler(request: httpx.Request) -> httpx.Response:
            return streamed_response(
                206,
                body,
                headers={"content-type": "text/plain"},
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(
                    min_unique_hosts=2,
                    min_novel_hosts=1,
                    min_novel_fraction=0.1,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate("https://trace.example/data/old.squid.log")
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.observed_host_year_pairs, 2)
        self.assertEqual(measurement.novel_host_year_pairs, 2)
        self.assertEqual(measurement.direct_host_years, 0)

    async def test_ircache_sanitized_access_name_uses_squid_parser(self) -> None:
        raw = (
            b"952819200.000 1 192.0.2.1 TCP_MISS/200 10 GET "
            b"http://known.com/a - DIRECT/x text/html\n"
            b"952819201.000 1 192.0.2.2 TCP_MISS/200 10 GET "
            b"https://novel.org/b - DIRECT/x text/html\n"
        )
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
                {"com": Decimal("1"), "org": Decimal("0.5")},
                policy=self.policy(
                    min_unique_hosts=2,
                    min_novel_hosts=1,
                    min_novel_fraction=0.1,
                    min_novel_eed=0.5,
                ),
            )
            result = await scout(
                self.candidate(
                    "https://mirror.example/Traces/"
                    "uc.sanitized-access.20000312.gz"
                )
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        self.assertEqual(measurement.measurement_mode, MeasurementMode.HOST_YEAR)
        self.assertEqual(measurement.unique_hosts, 2)
        self.assertEqual(measurement.observed_host_year_pairs, 2)
        self.assertEqual(measurement.novel_host_year_pairs, 2)
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

    async def test_progressive_scout_uses_all_fidelity_stages_within_total_budget(self) -> None:
        total_size = 8 * 1024 * 1024
        requested: list[tuple[int, int]] = []
        record = b"https://known.com/repeated\n"

        async def handler(request: httpx.Request) -> httpx.Response:
            span = request.headers["Range"].removeprefix("bytes=")
            start_text, end_text = span.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            requested.append((start, end))
            size = end - start + 1
            repetitions = (size // len(record)) + 1
            chunk = (record * repetitions)[:size]
            return streamed_response(
                206,
                chunk,
                headers={
                    "content-type": "text/plain",
                    "content-range": f"bytes {start}-{end}/{total_size}",
                    "content-length": str(len(chunk)),
                },
            )

        budget = 3 * 1024 * 1024
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"com": Decimal("1")},
                policy=self.policy(
                    max_download_bytes=budget,
                    progressive_initial_bytes=64 * 1024,
                    sample_windows=4,
                    max_records=100,
                    min_unique_hosts=1000,
                    min_novel_hosts=1000,
                    min_novel_fraction=0.9,
                    min_novel_eed=1000.0,
                ),
            )
            result = await scout(
                self.candidate("https://data.example/progressive.urls")
            )

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNotNone(result.measurement)
        measurement = result.measurement
        assert measurement is not None
        # Four fidelity targets use 1+2+3+4 distributed range requests.
        self.assertEqual(measurement.requests, 10)
        self.assertEqual(len(requested), 10)
        self.assertLessEqual(measurement.bytes_read, budget)
        self.assertGreater(measurement.bytes_read, 2 * 1024 * 1024)

    async def test_ignored_nonzero_ranges_do_not_duplicate_prefix_samples(self) -> None:
        lines = [
            f"https://prefix-{index}.example/path\n".encode()
            for index in range(20)
        ]
        body = b"".join(lines)
        requested_starts: list[int] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            span = request.headers["Range"].removeprefix("bytes=")
            start_text, end_text = span.split("-", 1)
            start = int(start_text)
            requested_starts.append(start)
            if start == 0:
                end = min(int(end_text), len(body) - 1)
                chunk = body[: end + 1]
                return streamed_response(
                    206,
                    chunk,
                    headers={
                        "content-type": "text/plain",
                        "content-range": f"bytes 0-{end}/{len(body)}",
                        "content-length": str(len(chunk)),
                    },
                )
            # Simulate an origin/CDN that ignores later Range requests and
            # sends the object from byte zero with HTTP 200.
            return streamed_response(
                200,
                body,
                headers={
                    "content-type": "text/plain",
                    "content-length": str(len(body)),
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        ) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                self.baseline,
                {"example": Decimal("1")},
                policy=self.policy(max_download_bytes=240, sample_windows=3),
            )
            download = await scout._download_sample(
                "https://data.example/ignored-range.urls",
                max_download_bytes=240,
                sample_windows=3,
            )

        self.assertEqual(len(requested_starts), 3)
        self.assertEqual(download.requests, 3)
        self.assertEqual(download.bytes_read, 240)
        self.assertEqual(download.payload.count(lines[1]), 1)

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
