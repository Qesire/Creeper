import unittest

from creeper.authority.baseline_index import YEAR_BITS
from creeper.sources.archive.cdx import parse_cdx_line


class CdxSourceParserTests(unittest.TestCase):
    def test_jisc_style_row_is_exact_year_direct_evidence(self):
        record = parse_cdx_line(
            "com,example)/ 19970102123456 http://example.com/ text/html 200 DIGEST 12 34 file.arc",
            source_id="jisc_ukwa_cdx:1997",
            locator="1997.cdx:42",
        )

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.payload, "http://example.com/")
        self.assertEqual(record.source_year, 1997)
        self.assertEqual(record.source_time, "19970102123456")
        self.assertEqual(record.direct_year_mask, YEAR_BITS[1997])
        self.assertEqual(record.year_hint_mask, 0)
        self.assertEqual(record.record_type, "CDX_CAPTURE")

    def test_cdx_row_with_non_success_status_is_not_annual_evidence(self):
        record = parse_cdx_line(
            "com,example)/ 19970102123456 http://example.com/ text/html 404 DIGEST 12 34 file.arc",
            source_id="jisc_ukwa_cdx:1997",
            locator="1997.cdx:42",
        )
        self.assertIsNone(record)


if __name__ == "__main__":
    unittest.main()
