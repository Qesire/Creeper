from __future__ import annotations

import unittest

from creeper.sources.non_snapshot import (
    extract_http_urls,
    is_dmoz_content_locator,
    is_mailbox_url_locator,
    is_squid_access_locator,
    is_target_mailbox_shard,
    mailbox_year_from_locator,
    parse_dmoz_external_page_line,
    parse_squid_access_line,
)


class NonSnapshotParserTests(unittest.TestCase):
    def test_mailbox_url_extraction_rejects_lossy_bound(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive integer"):
            extract_http_urls("http://example.test/", max_urls=1.5)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            extract_http_urls("http://example.test/", max_urls=True)

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

    def test_gnu_extensionless_target_month_is_mailbox_source(self) -> None:
        url = "https://lists.gnu.org/archive/mbox/lynx-dev/1998-03"

        self.assertTrue(is_mailbox_url_locator(url))
        self.assertEqual(mailbox_year_from_locator(url), 1998)
        self.assertFalse(
            is_mailbox_url_locator(
                "https://lists.gnu.org/archive/mbox/lynx-dev/2002-03"
            )
        )

    def test_ietf_mail_archive_months_are_recognized_without_false_variants(self) -> None:
        extensionless = (
            "https://www.ietf.org/ietf-ftp/ietf-mail-archive/ietf/1996-01"
        )
        mail_suffix = (
            "https://www.ietf.org/ietf-ftp/ietf-mail-archive/ietf/2001-07.mail"
        )
        compressed = mail_suffix + ".gz"

        self.assertTrue(is_mailbox_url_locator(extensionless))
        self.assertEqual(mailbox_year_from_locator(extensionless), 1996)
        self.assertTrue(is_mailbox_url_locator(mail_suffix))
        self.assertEqual(mailbox_year_from_locator(mail_suffix), 2001)
        self.assertTrue(is_mailbox_url_locator(compressed))
        self.assertEqual(mailbox_year_from_locator(compressed), 2001)
        self.assertFalse(
            is_target_mailbox_shard(
                "https://www.ietf.org/ietf-ftp/ietf-mail-archive/ietf/"
                "2001-07.mail.1"
            )
        )
        self.assertFalse(
            is_target_mailbox_shard(
                "https://www.ietf.org/ietf-ftp/ietf-mail-archive/ietf/2002-01"
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
