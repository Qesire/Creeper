from __future__ import annotations

import unittest
from unittest.mock import patch

from creeper.sources.archive.warc import WarcMetadataLease, WarcTargetRecord
from creeper.sources.archive.warc_source import (
    WarcSourceLeaseExecutor,
    WarcSourceObservation,
)


class WarcSourceLeaseExecutorTests(unittest.TestCase):
    def test_observation_exposes_hint_but_never_direct_authority(self) -> None:
        observation = WarcSourceObservation(
            source_id="archive-1",
            locator="warc-byte:128:64",
            target_uri="https://example.org/path",
            year_hint=1999,
            record_type="response",
        )
        self.assertEqual(observation.year_hint_mask, 1 << (1999 - 1996))
        self.assertEqual(observation.direct_year_mask, 0)

    def test_out_of_competition_year_cannot_set_hint_mask(self) -> None:
        observation = WarcSourceObservation(
            source_id="archive-1",
            locator="warc-byte:128:64",
            target_uri="https://example.org/path",
            year_hint=2005,
            record_type="response",
        )
        self.assertEqual(observation.year_hint_mask, 0)
        self.assertEqual(observation.direct_year_mask, 0)

    def test_executor_accepts_explicit_remote_read_ahead_size(self) -> None:
        metadata = WarcMetadataLease(
            records=(),
            scanned_records=0,
            start_offset=0,
            end_offset=0,
            next_offset=None,
            exhausted=True,
        )
        with patch(
            "creeper.sources.archive.warc_source.read_warc_source_lease",
            return_value=metadata,
        ) as read:
            WarcSourceLeaseExecutor(
                "https://archive.example/big.warc.gz",
                source_id="archive-1",
                remote_block_size=1024 * 1024,
            ).execute(
                cursor=None,
                max_scanned_records=1,
                max_archive_bytes=1024,
            )
        self.assertEqual(read.call_args.kwargs["remote_block_size"], 1024 * 1024)
        self.assertEqual(read.call_args.kwargs["max_record_content_bytes"], 64 * 1024 * 1024)

    def test_executor_projects_bounded_metadata_lease(self) -> None:
        metadata = WarcMetadataLease(
            records=(
                WarcTargetRecord(
                    record_type="response",
                    target_uri="https://one.example/a",
                    source_year=1998,
                    offset=100,
                    length=55,
                ),
                WarcTargetRecord(
                    record_type="resource",
                    target_uri="https://two.example/b",
                    source_year=2001,
                    offset=155,
                    length=70,
                ),
            ),
            scanned_records=3,
            start_offset=100,
            end_offset=225,
            next_offset=225,
            exhausted=False,
        )
        with patch(
            "creeper.sources.archive.warc_source.read_warc_source_lease",
            return_value=metadata,
        ) as read:
            result = WarcSourceLeaseExecutor(
                "https://archive.example/big.warc.gz",
                source_id="archive-1",
            ).execute(
                cursor="warc-byte:100",
                max_scanned_records=3,
                max_archive_bytes=1_000_000,
            )

        read.assert_called_once_with(
            "https://archive.example/big.warc.gz",
            cursor="warc-byte:100",
            max_scanned_records=3,
            max_archive_bytes=1_000_000,
            target_year_from=1996,
            target_year_to=2001,
            remote_block_size=4 * 1024 * 1024,
            max_record_content_bytes=64 * 1024 * 1024,
        )
        self.assertFalse(result.exhausted)
        self.assertEqual(result.next_cursor, "warc-byte:225")
        self.assertEqual(result.scanned_records, 3)
        self.assertEqual(result.bytes_advanced, 125)
        self.assertEqual(
            [item.locator for item in result.observations],
            ["warc-byte:100:55", "warc-byte:155:70"],
        )
        self.assertEqual(
            [item.year_hint_mask for item in result.observations],
            [1 << 2, 1 << 5],
        )
        self.assertTrue(all(item.direct_year_mask == 0 for item in result.observations))

    def test_executor_forwards_single_record_content_limit(self) -> None:
        metadata = WarcMetadataLease(
            records=(),
            scanned_records=0,
            start_offset=0,
            end_offset=0,
            next_offset=None,
            exhausted=True,
        )
        with patch(
            "creeper.sources.archive.warc_source.read_warc_source_lease",
            return_value=metadata,
        ) as read:
            WarcSourceLeaseExecutor(
                "https://archive.example/big.warc.gz",
                source_id="archive-1",
                max_record_content_bytes=2 * 1024 * 1024,
            ).execute(
                cursor=None,
                max_scanned_records=1,
                max_archive_bytes=1024,
            )
        self.assertEqual(read.call_args.kwargs["max_record_content_bytes"], 2 * 1024 * 1024)

    def test_executor_rejects_invalid_single_record_content_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_record_content_bytes"):
            WarcSourceLeaseExecutor(
                "https://archive.example/big.warc.gz",
                source_id="archive-1",
                max_record_content_bytes=0,
            )


if __name__ == "__main__":
    unittest.main()
