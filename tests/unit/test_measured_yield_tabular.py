from __future__ import annotations

import unittest

from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutPolicy,
    _extract_hosts,
)


class MeasuredYieldTabularTests(unittest.TestCase):
    @staticmethod
    def policy(**overrides) -> MeasuredYieldScoutPolicy:
        values = dict(
            max_download_bytes=64 * 1024,
            max_decompressed_bytes=256 * 1024,
            max_records=100,
            max_line_bytes=16 * 1024,
            min_unique_hosts=1,
            min_novel_hosts=1,
            min_novel_fraction=0.0,
            min_novel_eed=0.0,
            timeout_seconds=2.0,
        )
        values.update(overrides)
        return MeasuredYieldScoutPolicy(**values)

    def test_headerless_csv_does_not_drop_first_record(self) -> None:
        parsed = _extract_hosts(
            b"https://first.example/a,first\nhttps://second.example/b,second\n",
            url="https://data.example/hosts.csv",
            content_type="text/csv",
            policy=self.policy(),
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        sampled, hosts = parsed
        self.assertEqual(sampled, 2)
        self.assertEqual(hosts, {"first.example", "second.example"})

    def test_named_url_column_remains_preferred(self) -> None:
        parsed = _extract_hosts(
            b"label,url,notes\nnot-a-host,https://first.example/a,x\nother,https://second.example/b,y\n",
            url="https://data.example/hosts.csv",
            content_type="text/csv",
            policy=self.policy(),
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        sampled, hosts = parsed
        self.assertEqual(sampled, 2)
        self.assertEqual(hosts, {"first.example", "second.example"})

    def test_headerless_tsv_is_sampled_as_scalar_fields(self) -> None:
        parsed = _extract_hosts(
            b"row1\thttps://one.example/a\nrow2\thttps://two.example/b\n",
            url="https://data.example/hosts.tsv",
            content_type="text/tab-separated-values",
            policy=self.policy(),
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        sampled, hosts = parsed
        self.assertEqual(sampled, 2)
        self.assertEqual(hosts, {"one.example", "two.example"})

    def test_invalid_target_year_bounds_fail_closed_at_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "target year bounds"):
            self.policy(target_year_from=2001, target_year_to=1996)


if __name__ == "__main__":
    unittest.main()
