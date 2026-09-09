from __future__ import annotations

import unittest

from creeper.sources.archive.cdxj import (
    complete_cdxj_lines,
    iter_cdxj_lines,
    parse_cdxj_line,
)


class ArquivoCDXJTests(unittest.TestCase):
    def test_parses_cdxj_key_timestamp_and_json_payload(self) -> None:
        line = (
            'com,example)/ 19991231120000 '
            '{"url":"http://example.com/","mime":"text/html","status":"200"}'
        )
        record = parse_cdxj_line(line, source_id="arquivo_pt_cdxj:Dinis", locator="Dinis.cdxj:7")

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.payload, "http://example.com/")
        self.assertEqual(record.source_year, 1999)
        self.assertEqual(record.locator, "Dinis.cdxj:7")

    def test_stream_parser_filters_to_competition_years_and_skips_bad_rows(self) -> None:
        lines = [
            'com,a)/ 19960101000000 {"url":"http://a.com/"}\n',
            'not a cdxj row\n',
            'com,b)/ 20020101000000 {"url":"http://b.com/"}\n',
            'com,c)/ 19970101000000 {"url":"http://c.com/"}\n',
        ]

        records = list(
            iter_cdxj_lines(
                lines,
                source_id="arquivo_pt_cdxj:test",
                locator_prefix="test.cdxj",
                allowed_years={1996, 1997},
            )
        )

        self.assertEqual([record.payload for record in records], ["http://a.com/", "http://c.com/"])
        self.assertEqual([record.source_year for record in records], [1996, 1997])
        self.assertTrue(records[1].locator.endswith(":4"))

    def test_prefix_parser_discards_a_partial_trailing_row(self) -> None:
        complete = b'com,a)/ 19960101000000 {"url":"http://a.com/"}\n'
        partial = b'com,b)/ 19970101000000 {"url":"http://b.com/"'

        self.assertEqual(complete_cdxj_lines(complete + partial), [complete.decode().rstrip("\n")])


if __name__ == "__main__":
    unittest.main()
