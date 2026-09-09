from __future__ import annotations

import unittest

from creeper.sources.archive.catalog import parse_cdxj_catalog, select_bounded_entries


class ArquivoCatalogTests(unittest.TestCase):
    def test_parses_apache_listing_sizes_and_urls(self) -> None:
        html = """
        <a href="Dinis.cdxj">Dinis.cdxj</a> 1.4M
        <a href="Tiny.cdxj">Tiny.cdxj</a> 11K
        <a href="AWP1.cdxj">AWP1.cdxj</a> 18G
        <a href="../">Parent Directory</a>
        """

        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        self.assertEqual([entry.name for entry in entries], ["Dinis.cdxj", "Tiny.cdxj", "AWP1.cdxj"])
        self.assertEqual(entries[0].size_bytes, 1_400_000)
        self.assertEqual(entries[2].size_bytes, 18_000_000_000)
        self.assertEqual(entries[1].url, "https://arquivo.pt/datasets/cdxj/Tiny.cdxj")

    def test_bounded_selection_is_deterministic_and_size_ordered(self) -> None:
        html = """
        <a href="large.cdxj">large.cdxj</a> 3M
        <a href="small.cdxj">small.cdxj</a> 1K
        <a href="medium.cdxj">medium.cdxj</a> 2M
        <a href="zero.cdxj">zero.cdxj</a> 0
        """
        entries = parse_cdxj_catalog(html, base_url="https://arquivo.pt/datasets/cdxj/")

        selected = select_bounded_entries(entries, max_file_bytes=2_000_000, max_files=2)

        self.assertEqual([entry.name for entry in selected], ["small.cdxj", "medium.cdxj"])


if __name__ == "__main__":
    unittest.main()
