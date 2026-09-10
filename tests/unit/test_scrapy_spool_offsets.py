from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from creeper.source_discovery.models import source_key
from creeper.source_discovery.scrapy_sidecar import (
    iter_scrapy_link_discoveries,
    prepare_append_spool,
)


class ScrapySpoolOffsetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source_key = source_key("https://example.com/archive/")
        self.path = self.root / "links.jsonl"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def row(self, discovered_url: str) -> bytes:
        return (
            json.dumps(
                {
                    "record_type": "LINK_DISCOVERY",
                    "source_key": self.source_key,
                    "page_url": "https://example.com/archive/",
                    "discovered_url": discovered_url,
                    "anchor_text": "x",
                    "depth": 0,
                    "same_site": False,
                },
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    def test_prepare_append_spool_truncates_only_uncommitted_tail(self) -> None:
        committed = self.row("https://one.example/")
        self.path.write_bytes(committed + b'{"record_type":"LINK_DISCOVERY"')

        offset = prepare_append_spool(self.path, scan_chunk_bytes=7)

        self.assertEqual(offset, len(committed))
        self.assertEqual(self.path.read_bytes(), committed)

    def test_byte_range_reads_only_new_committed_records(self) -> None:
        first = self.row("https://first.example/")
        second = self.row("https://second.example/")
        self.path.write_bytes(first)
        start = prepare_append_spool(self.path)
        with self.path.open("ab") as stream:
            stream.write(second)
        end = self.path.stat().st_size

        rows = list(
            iter_scrapy_link_discoveries(
                self.path,
                expected_source_key=self.source_key,
                start_offset=start,
                end_offset=end,
            )
        )

        self.assertEqual([row.discovered_url for row in rows], ["https://second.example/"])

    def test_unterminated_but_valid_json_is_not_committed(self) -> None:
        raw = self.row("https://partial.example/").rstrip(b"\n")
        self.path.write_bytes(raw)

        rows = list(
            iter_scrapy_link_discoveries(
                self.path,
                expected_source_key=self.source_key,
            )
        )

        self.assertEqual(rows, [])
        self.assertEqual(prepare_append_spool(self.path), 0)
        self.assertEqual(self.path.read_bytes(), b"")

    def test_start_offset_must_be_a_line_boundary(self) -> None:
        self.path.write_bytes(self.row("https://one.example/") + self.row("https://two.example/"))
        with self.assertRaisesRegex(ValueError, "line boundary"):
            list(
                iter_scrapy_link_discoveries(
                    self.path,
                    expected_source_key=self.source_key,
                    start_offset=3,
                )
            )


if __name__ == "__main__":
    unittest.main()
