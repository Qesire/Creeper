from __future__ import annotations

import gzip
import io
import unittest
from unittest.mock import patch
from decimal import Decimal
from types import SimpleNamespace

import httpx
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from creeper.source_discovery.coordinator import ScoutDisposition
from creeper.source_discovery.measured_scout import (
    _explicitly_truncated,
    _parse_content_range,
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
    _enforce_gzip_expansion_budget,
    _extract_hosts,
    _extract_warc_hosts,
)
from creeper.sources.archive.warc import WarcFormatError, WarcTargetRecord


class _BaselineStub:
    def resolve_batch(self, hosts):
        return {
            hostname: ((1 if hostname == "known.com" else 0), False)
            for hostname in hosts
        }


def _warc_bytes(*, gzip: bool) -> bytes:
    output = io.BytesIO()
    writer = WARCWriter(output, gzip=gzip)
    http_headers = StatusAndHeaders(
        "200 OK",
        [("Content-Type", "text/html")],
        protocol="HTTP/1.1",
    )
    for url, date in (
        ("http://known.com/a", "1999-01-01T00:00:00Z"),
        ("https://novel.org/b", "2001-12-31T23:59:59Z"),
        ("https://outside.net/c", "2005-01-01T00:00:00Z"),
    ):
        record = writer.create_warc_record(
            url,
            "response",
            payload=io.BytesIO(b"payload"),
            http_headers=http_headers,
            warc_headers_dict={"WARC-Date": date},
        )
        writer.write_record(record)
    return output.getvalue()


class MeasuredYieldWarcTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def policy() -> MeasuredYieldScoutPolicy:
        return MeasuredYieldScoutPolicy(
            max_download_bytes=1024 * 1024,
            max_decompressed_bytes=4 * 1024 * 1024,
            max_records=100,
            max_line_bytes=64 * 1024,
            min_unique_hosts=1,
            min_novel_hosts=1,
            min_novel_fraction=0.0,
            min_novel_eed=0.0,
            timeout_seconds=2.0,
        )

    def test_uncompressed_warc_filters_to_target_years(self) -> None:
        sampled, hosts = _extract_hosts(
            _warc_bytes(gzip=False),
            url="https://data.example/sample.warc",
            content_type="application/warc",
            policy=self.policy(),
        )

        self.assertEqual(sampled, 3)
        self.assertEqual(hosts, {"known.com", "novel.org"})

    def test_record_gzip_warc_is_parsed_by_warcio_not_generic_gzip(self) -> None:
        sampled, hosts = _extract_hosts(
            _warc_bytes(gzip=True),
            url="https://data.example/sample.warc.gz",
            content_type="application/gzip",
            policy=self.policy(),
        )

        self.assertEqual(sampled, 3)
        self.assertEqual(hosts, {"known.com", "novel.org"})

    def test_content_type_can_route_an_opaque_warc_url(self) -> None:
        sampled, hosts = _extract_hosts(
            _warc_bytes(gzip=False),
            url="https://data.example/download?id=123",
            content_type="application/warc; charset=binary",
            policy=self.policy(),
        )

        self.assertEqual(sampled, 3)
        self.assertEqual(hosts, {"known.com", "novel.org"})

    def test_warc_gzip_expansion_budget_rejects_compression_bomb_prefix(self) -> None:
        with self.assertRaisesRegex(ValueError, "decompressed budget"):
            _enforce_gzip_expansion_budget(
                gzip.compress(b"x" * 4096),
                1024,
                allow_truncated=False,
            )

    def test_short_truncated_gzip_fails_closed(self) -> None:
        truncated = gzip.compress(b"record")[:-4]
        with self.assertRaisesRegex(ValueError, "truncated gzip"):
            _enforce_gzip_expansion_budget(
                truncated,
                1024,
                allow_truncated=False,
            )

    def test_range_probe_preserves_complete_records_before_truncated_tail(self) -> None:
        def partial_records(_stream):
            yield WarcTargetRecord(
                record_type="response",
                target_uri="https://novel.org/a",
                source_year=1999,
                offset=0,
                length=128,
            )
            raise WarcFormatError("truncated final gzip member")

        with patch(
            "creeper.source_discovery.measured_scout.iter_warc_target_records",
            partial_records,
        ):
            sampled, hosts = _extract_warc_hosts(
                b"bounded-prefix",
                policy=self.policy(),
                allow_truncated_tail=True,
            )
        self.assertEqual(sampled, 1)
        self.assertEqual(hosts, {"novel.org"})

    def test_non_probe_warc_truncation_remains_fail_closed(self) -> None:
        def partial_records(_stream):
            yield WarcTargetRecord(
                record_type="response",
                target_uri="https://novel.org/a",
                source_year=1999,
                offset=0,
                length=128,
            )
            raise WarcFormatError("corrupt archive")

        with patch(
            "creeper.source_discovery.measured_scout.iter_warc_target_records",
            partial_records,
        ):
            with self.assertRaisesRegex(WarcFormatError, "corrupt archive"):
                _extract_warc_hosts(
                    b"short-complete-response",
                    policy=self.policy(),
                    allow_truncated_tail=False,
                )

    def test_content_range_proves_probe_truncation(self) -> None:
        response = httpx.Response(
            206,
            headers={"Content-Range": "bytes 0-1023/4096", "Content-Length": "1024"},
            request=httpx.Request("GET", "https://data.example/archive.warc.gz"),
        )
        self.assertEqual(_parse_content_range(response.headers.get("content-range")), (0, 1023, 4096))
        self.assertTrue(
            _explicitly_truncated(response, bytes_read=1024, overflowed_chunk=False)
        )

    def test_complete_content_range_does_not_grant_tail_tolerance(self) -> None:
        response = httpx.Response(
            206,
            headers={"Content-Range": "bytes 0-1023/1024", "Content-Length": "1024"},
            request=httpx.Request("GET", "https://data.example/archive.warc.gz"),
        )
        self.assertFalse(
            _explicitly_truncated(response, bytes_read=1024, overflowed_chunk=False)
        )

    def test_ignored_range_content_length_can_prove_truncation(self) -> None:
        response = httpx.Response(
            200,
            headers={"Content-Length": "8192"},
            request=httpx.Request("GET", "https://data.example/archive.warc.gz"),
        )
        self.assertTrue(
            _explicitly_truncated(response, bytes_read=1024, overflowed_chunk=False)
        )

    def test_exact_budget_without_metadata_fails_closed_as_not_truncated(self) -> None:
        response = httpx.Response(
            206,
            request=httpx.Request("GET", "https://data.example/archive.warc.gz"),
        )
        self.assertFalse(
            _explicitly_truncated(response, bytes_read=1024, overflowed_chunk=False)
        )

    def test_discarded_chunk_bytes_prove_truncation(self) -> None:
        response = httpx.Response(
            200,
            request=httpx.Request("GET", "https://data.example/archive.warc.gz"),
        )
        self.assertTrue(
            _explicitly_truncated(response, bytes_read=1024, overflowed_chunk=True)
        )

    async def test_malformed_warc_is_held_without_retrying_as_network_failure(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                206,
                stream=httpx.ByteStream(b"not a WARC stream"),
                headers={"content-type": "application/warc"},
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                _BaselineStub(),
                {"com": Decimal("1")},
                policy=self.policy(),
            )
            result = await scout(
                SimpleNamespace(canonical_entrypoint="https://data.example/broken.warc")
            )

        self.assertEqual(result.disposition, ScoutDisposition.HOLD)
        self.assertIsNone(result.measurement)
        self.assertIn("parse failed closed", result.reason)

    async def test_warc_scout_measures_baseline_external_eed_without_granting_evidence(self) -> None:
        body = _warc_bytes(gzip=True)

        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers.get("Range"), "bytes=0-1048575")
            return httpx.Response(
                206,
                stream=httpx.ByteStream(body),
                headers={
                    "content-type": "application/gzip",
                    "content-range": f"bytes 0-{len(body) - 1}/{len(body)}",
                    "content-length": str(len(body)),
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            scout = MeasuredYieldScoutExecutor(
                client,
                _BaselineStub(),
                {"com": Decimal("1"), "org": Decimal("0.8")},
                policy=self.policy(),
            )
            result = await scout(
                SimpleNamespace(canonical_entrypoint="https://data.example/sample.warc.gz")
            )

        self.assertEqual(result.disposition, ScoutDisposition.WARM)
        self.assertIsNotNone(result.measurement)
        assert result.measurement is not None
        self.assertEqual(result.measurement.unique_hosts, 2)
        self.assertEqual(result.measurement.novel_hosts, 1)
        self.assertAlmostEqual(result.measurement.novel_eed, 0.8)
        self.assertEqual(result.measurement.direct_host_years, 0)


if __name__ == "__main__":
    unittest.main()
