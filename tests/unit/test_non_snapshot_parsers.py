from __future__ import annotations

import unittest

from creeper.sources.non_snapshot import (
    extract_http_urls,
    is_dmoz_content_locator,
    is_mailbox_url_locator,
    is_squid_access_locator,
    iter_mbox_messages,
    mailbox_year_from_locator,
    parse_dmoz_external_page_line,
    parse_mbox_message,
    parse_squid_access_line,
)


class NonSnapshotParserTests(unittest.TestCase):
    def test_mailbox_url_extraction_keeps_only_http_urls(self) -> None:
        text = (
            "From: Person <person@example.net>\n"
            "See http://old.example/a and https://www.example.org/b?q=1. "
            "Contact person@example.net; duplicate http://old.example/a.\n"
        )

        urls = extract_http_urls(text)

        self.assertEqual(
            urls,
            (
                "http://old.example/a",
                "https://www.example.org/b?q=1",
            ),
        )
        self.assertTrue(all("person@" not in item for item in urls))

    def test_mbox_message_binds_body_urls_to_message_date(self) -> None:
        message = (
            b"From sender@example.test Fri Oct 16 04:31:23 1998\n"
            b"Date: Fri, 16 Oct 1998 04:31:23 -0400\n"
            b"From: Person <person@example.net>\n"
            b"Subject: historical link\n"
            b"Content-Type: text/plain; charset=utf-8\n"
            b"\n"
            b"See http://expect.nist.gov/ and https://old.example/path.\n"
            b">From this body line must not split the mbox message.\n"
        )

        urls, year, source_time = parse_mbox_message(message)

        self.assertEqual(
            urls,
            (
                "http://expect.nist.gov/",
                "https://old.example/path",
            ),
        )
        self.assertEqual(year, 1998)
        self.assertEqual(source_time, "1998-10-16T08:31:23+00:00")
        self.assertTrue(all("person@" not in value for value in urls))

    def test_mbox_parser_excludes_attachment_urls(self) -> None:
        message = (
            b"From sender@example.test Fri Oct 16 04:31:23 1998\n"
            b"Date: Fri, 16 Oct 1998 04:31:23 -0400\n"
            b"Content-Type: multipart/mixed; boundary=BOUNDARY\n"
            b"\n"
            b"--BOUNDARY\n"
            b"Content-Type: text/plain; charset=utf-8\n\n"
            b"http://body.example/\n"
            b"--BOUNDARY\n"
            b"Content-Type: text/plain; charset=utf-8\n"
            b"Content-Disposition: attachment; filename=links.txt\n\n"
            b"http://attachment.example/\n"
            b"--BOUNDARY--\n"
        )

        urls, year, _source_time = parse_mbox_message(message)

        self.assertEqual(urls, ("http://body.example/",))
        self.assertEqual(year, 1998)

    def test_mbox_boundaries_do_not_split_quoted_from_or_keep_partial_tail(self) -> None:
        first = (
            b"From first@example.test Fri Oct 16 04:31:23 1998\n"
            b"Date: Fri, 16 Oct 1998 04:31:23 -0400\n\n"
            b"http://first.example/\n"
            b">From quoted body text\n"
        )
        second = (
            b"From second@example.test Sat Oct 17 04:31:23 1998\n"
            b"Date: Sat, 17 Oct 1998 04:31:23 -0400\n\n"
            b"http://second.example/\n"
        )
        payload = first + second

        complete = tuple(iter_mbox_messages(payload))
        truncated = tuple(
            iter_mbox_messages(
                payload[:-8],
                include_truncated_tail=False,
            )
        )

        self.assertEqual(len(complete), 2)
        self.assertEqual(complete[0][0], 0)
        self.assertEqual(complete[1][0], len(first))
        self.assertEqual(len(truncated), 1)

    def test_mbox_missing_date_keeps_urls_without_direct_year(self) -> None:
        message = (
            b"From sender@example.test Fri Oct 16 04:31:23 1998\n"
            b"Subject: no date header\n\n"
            b"http://undated.example/\n"
        )

        urls, year, source_time = parse_mbox_message(message)

        self.assertEqual(urls, ("http://undated.example/",))
        self.assertIsNone(year)
        self.assertIsNone(source_time)

    def test_gnu_extensionless_target_month_is_mailbox_source(self) -> None:
        url = "https://lists.gnu.org/archive/mbox/lynx-dev/1998-03"

        self.assertTrue(is_mailbox_url_locator(url))
        self.assertEqual(mailbox_year_from_locator(url), 1998)
        self.assertFalse(
            is_mailbox_url_locator(
                "https://lists.gnu.org/archive/mbox/lynx-dev/2002-03"
            )
        )

    def test_generic_target_month_filename_is_not_a_mailbox(self) -> None:
        self.assertFalse(
            is_mailbox_url_locator("https://data.example/releases/1998-03")
        )

    def test_explicit_mbox_suffix_is_supported(self) -> None:
        self.assertTrue(
            is_mailbox_url_locator("https://example.test/archive/1999-04.mbox")
        )
        self.assertEqual(
            mailbox_year_from_locator(
                "https://example.test/archive/1999-04.mbox.gz"
            ),
            1999,
        )

    def test_dmoz_content_locator_is_specific_to_content_dumps(self) -> None:
        self.assertTrue(
            is_dmoz_content_locator(
                "https://mirror.example/dmoz/2001-01-22/content.rdf.u8.gz"
            )
        )
        self.assertTrue(
            is_dmoz_content_locator(
                "https://mirror.example/dmoz/kt-content.rdf.u8"
            )
        )
        self.assertFalse(
            is_dmoz_content_locator(
                "https://mirror.example/dmoz/structure.rdf.u8.gz"
            )
        )
        self.assertFalse(
            is_dmoz_content_locator(
                "https://mirror.example/data/arbitrary.rdf.gz"
            )
        )

    def test_dmoz_parser_extracts_only_external_page_url(self) -> None:
        self.assertEqual(
            parse_dmoz_external_page_line(
                '<ExternalPage about="http://old.example/path?a=1&amp;b=2">'
            ),
            "http://old.example/path?a=1&b=2",
        )
        self.assertIsNone(
            parse_dmoz_external_page_line(
                '<link r:resource="http://old.example/path"/>'
            )
        )
        self.assertIsNone(
            parse_dmoz_external_page_line(
                '<RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
            )
        )

    def test_squid_access_parser_keeps_url_and_target_year_only(self) -> None:
        line = (
            "915148800.123 42 192.0.2.9 TCP_MISS/200 1234 GET "
            "http://old.example/path - DIRECT/203.0.113.8 text/html"
        )

        parsed = parse_squid_access_line(line)

        self.assertEqual(parsed, ("http://old.example/path", 1999))

    def test_squid_access_parser_rejects_off_window_rows(self) -> None:
        line = (
            "1072915200.000 42 192.0.2.9 TCP_MISS/200 1234 GET "
            "http://late.example/path - DIRECT/203.0.113.8 text/html"
        )
        self.assertIsNone(parse_squid_access_line(line))

    def test_squid_locator_detection_is_specific(self) -> None:
        self.assertTrue(
            is_squid_access_locator(
                "https://trace.example/cache/squid/rawlogs/1999-05-21.log"
            )
        )
        self.assertTrue(
            is_squid_access_locator(
                "https://trace.example/data/old.squid.log.gz"
            )
        )
        self.assertTrue(
            is_squid_access_locator(
                "https://mirror.example/Traces/uc.sanitized-access.20000312.gz"
            )
        )
        self.assertTrue(
            is_squid_access_locator(
                "https://mirror.example/Traces/sanitized-access.20010827"
            )
        )
        self.assertFalse(
            is_squid_access_locator(
                "https://mirror.example/Traces/uc.sanitized-access.20070109.gz"
            )
        )
        self.assertFalse(
            is_squid_access_locator("https://trace.example/data/server.log")
        )


if __name__ == "__main__":
    unittest.main()
