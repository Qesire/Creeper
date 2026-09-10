from __future__ import annotations

import io
import unittest

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from creeper.sources.archive.warc import (
    WarcCursorError,
    decode_warc_cursor,
    read_warc_metadata_lease,
)


def _archive_bytes(*, gzip: bool) -> bytes:
    output = io.BytesIO()
    writer = WARCWriter(output, gzip=gzip)
    http_headers = StatusAndHeaders(
        "200 OK",
        [("Content-Type", "text/html")],
        protocol="HTTP/1.1",
    )
    for url, date in (
        ("http://one.example/a", "1998-01-01T00:00:00Z"),
        ("http://two.example/b", "1999-01-01T00:00:00Z"),
        ("http://late.example/c", "2005-01-01T00:00:00Z"),
        ("http://three.example/d", "2001-01-01T00:00:00Z"),
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


class WarcMetadataLeaseTests(unittest.TestCase):
    def _assert_resume(self, *, gzip: bool) -> None:
        stream = io.BytesIO(_archive_bytes(gzip=gzip))
        first = read_warc_metadata_lease(
            stream,
            max_scanned_records=2,
            max_archive_bytes=10 * 1024 * 1024,
        )
        self.assertFalse(first.exhausted)
        self.assertEqual(first.scanned_records, 2)
        self.assertEqual(
            [record.target_uri for record in first.records],
            ["http://one.example/a", "http://two.example/b"],
        )
        self.assertIsNotNone(first.next_cursor)
        assert first.next_cursor is not None
        self.assertGreater(decode_warc_cursor(first.next_cursor), 0)

        second = read_warc_metadata_lease(
            stream,
            cursor=first.next_cursor,
            max_scanned_records=20,
            max_archive_bytes=10 * 1024 * 1024,
        )
        self.assertTrue(second.exhausted)
        # The 2005 record is scanned (and therefore consumes lease budget) but
        # cannot become a target-year discovery observation.
        self.assertEqual(second.scanned_records, 2)
        self.assertEqual(
            [record.target_uri for record in second.records],
            ["http://three.example/d"],
        )
        self.assertIsNone(second.next_cursor)

    def test_uncompressed_warc_resumes_at_public_record_offset(self) -> None:
        self._assert_resume(gzip=False)

    def test_record_gzip_warc_resumes_at_gzip_member_offset(self) -> None:
        self._assert_resume(gzip=True)

    def test_cursor_beyond_eof_fails_closed(self) -> None:
        stream = io.BytesIO(_archive_bytes(gzip=False))
        with self.assertRaises(WarcCursorError):
            read_warc_metadata_lease(
                stream,
                cursor="warc-byte:99999999",
                max_scanned_records=1,
                max_archive_bytes=1024,
            )


if __name__ == "__main__":
    unittest.main()
