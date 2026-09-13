from __future__ import annotations

import unittest

from creeper.sources.archive.catalog import parse_cdxj_catalog, select_bounded_entries


class ArquivoCatalogTests(unittest.TestCase):
    def test_parses_apache_listing_sizes_and_urls(self) -> None:
        html = """
        <pre>
        <a href="Dinis.cdxj">Dinis.cdxj</a> 2026-09-10 11:00 1.4M
        <a href="Tiny.cdxj">Tiny.cdxj</a> 2026-09-10 11:01 11K
        <a href="AWP1.cdxj">AWP1.cdxj</a> 2026-09-10 11:02 18G
        <a href="../">Parent Directory</a>
        </pre>
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual(
            [entry.name for entry in entries],
            ["Dinis.cdxj", "Tiny.cdxj", "AWP1.cdxj"],
        )
        self.assertEqual(entries[0].size_bytes, 1_400_000)
        self.assertEqual(entries[2].size_bytes, 18_000_000_000)
        self.assertEqual(entries[1].url, "https://arquivo.pt/datasets/cdxj/Tiny.cdxj")

    def test_table_listing_uses_row_local_size_and_nested_link_text(self) -> None:
        html = """
        <table>
          <tr><td><a href="one.cdxj"><span>one.cdxj</span></a></td><td>2026-09-10 12:44</td><td>2.5M</td></tr>
          <tr><td><a href="two.cdxj">two.cdxj</a></td><td>2026-09-10 12:45</td><td>17K</td></tr>
        </table>
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual([entry.name for entry in entries], ["one.cdxj", "two.cdxj"])
        self.assertEqual(entries[0].size_bytes, 2_500_000)
        self.assertEqual(entries[1].size_bytes, 17_000)

    def test_lexbor_recovers_malformed_markup_without_cross_link_size_leak(self) -> None:
        html = """
        <pre>
        <a href='broken.cdxj'><b>broken.cdxj</a> 4M
        <a href='second.cdxj'>second.cdxj</a> 9K
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual([entry.name for entry in entries], ["broken.cdxj", "second.cdxj"])
        self.assertEqual([entry.size_bytes for entry in entries], [4_000_000, 9_000])

    def test_rejects_external_nested_and_non_cdxj_links(self) -> None:
        html = """
        <pre>
        <a href="https://evil.example/x.cdxj">external.cdxj</a> 1K
        <a href="nested/x.cdxj">nested.cdxj</a> 2K
        <a href="notes.txt">notes.txt</a> 3K
        <a href="ok.cdxj?download=1">ok.cdxj</a> 4K
        </pre>
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual([entry.name for entry in entries], ["ok.cdxj"])
        self.assertEqual(entries[0].size_bytes, 4_000)

    def test_duplicate_resource_links_keep_first_occurrence(self) -> None:
        html = """
        <pre>
        <a href="same.cdxj">same-first.cdxj</a> 1M
        <a href="./same.cdxj">same-second.cdxj</a> 9G
        <a href="other.cdxj">other.cdxj</a> 2M
        </pre>
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual(
            [(entry.name, entry.url, entry.size_bytes) for entry in entries],
            [
                ("same-first.cdxj", "https://arquivo.pt/datasets/cdxj/same.cdxj", 1_000_000),
                ("other.cdxj", "https://arquivo.pt/datasets/cdxj/other.cdxj", 2_000_000),
            ],
        )

    def test_bounded_selection_is_deterministic_and_size_ordered(self) -> None:
        html = """
        <pre>
        <a href="large.cdxj">large.cdxj</a> 3M
        <a href="small.cdxj">small.cdxj</a> 1K
        <a href="medium.cdxj">medium.cdxj</a> 2M
        <a href="zero.cdxj">zero.cdxj</a> 0
        </pre>
        """
        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        selected = select_bounded_entries(entries, max_file_bytes=2_000_000, max_files=2)

        self.assertEqual([entry.name for entry in selected], ["small.cdxj", "medium.cdxj"])


if __name__ == "__main__":
    unittest.main()
