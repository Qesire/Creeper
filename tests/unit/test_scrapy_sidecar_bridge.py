from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from creeper.source_discovery.models import source_key
from creeper.source_discovery.scrapy_sidecar import (
    ScrapyScoutLauncher,
    ScrapyScoutSpec,
    iter_scrapy_link_discoveries,
    prepare_jobdir_binding,
)


class ScrapySidecarBridgeTests(unittest.TestCase):
    def _spec(self, root: Path, *, entrypoint: str = "https://example.com/archive/") -> ScrapyScoutSpec:
        return ScrapyScoutSpec(
            source_key=source_key(entrypoint),
            start_url=entrypoint,
            jobdir=root / "job",
            spool_path=root / "links.jsonl",
            max_pages=23,
            max_depth=3,
            max_seconds=47,
            max_memory_mb=384,
        )

    def test_argv_uses_locked_sidecar_and_append_feed_with_hard_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            spec = self._spec(Path(tmp))
            argv = spec.argv()

        self.assertEqual(argv[:5], ["uv", "run", "--locked", "scrapy", "crawl"])
        self.assertIn("bounded_links", argv)
        self.assertIn("-o", argv)
        self.assertNotIn("-O", argv)
        self.assertTrue(any(value.endswith(":jsonlines") for value in argv))
        self.assertIn("DEPTH_LIMIT=3", argv)
        self.assertIn("CLOSESPIDER_PAGECOUNT=23", argv)
        self.assertIn("CLOSESPIDER_TIMEOUT=47", argv)
        self.assertIn("MEMUSAGE_LIMIT_MB=384", argv)

    def test_jobdir_binding_is_idempotent_and_fails_closed_on_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self._spec(root)
            first = prepare_jobdir_binding(spec)
            second = prepare_jobdir_binding(spec)
            self.assertEqual(first, second)

            other = ScrapyScoutSpec(
                source_key=source_key("https://other.example/archive/"),
                start_url="https://other.example/archive/",
                jobdir=spec.jobdir,
                spool_path=spec.spool_path,
            )
            with self.assertRaises(ValueError):
                prepare_jobdir_binding(other)

    def test_nonempty_unbound_jobdir_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self._spec(root)
            spec.jobdir.mkdir(parents=True)
            (spec.jobdir / "requests.queue").write_text("existing", encoding="utf-8")
            with self.assertRaises(ValueError):
                prepare_jobdir_binding(spec)

    def test_jsonl_stream_accepts_valid_rows_and_ignores_only_partial_tail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self._spec(root)
            rows = [
                {
                    "record_type": "LINK_DISCOVERY",
                    "source_key": spec.source_key,
                    "page_url": "https://example.com/archive/",
                    "discovered_url": "https://example.net/catalog/",
                    "anchor_text": "catalog",
                    "depth": 0,
                    "same_site": False,
                },
                {
                    "record_type": "LINK_DISCOVERY",
                    "source_key": spec.source_key,
                    "page_url": "https://example.com/archive/",
                    "discovered_url": "https://example.com/data.zip",
                    "anchor_text": "data",
                    "depth": 0,
                    "same_site": True,
                },
            ]
            with spec.spool_path.open("wb") as stream:
                for row in rows:
                    stream.write((json.dumps(row) + "\n").encode("utf-8"))
                stream.write(b'{"record_type":"LINK_DISCOVERY"')

            parsed = list(
                iter_scrapy_link_discoveries(
                    spec.spool_path,
                    expected_source_key=spec.source_key,
                )
            )
            self.assertEqual(len(parsed), 2)
            self.assertEqual(parsed[0].discovered_url, "https://example.net/catalog/")
            self.assertTrue(parsed[1].same_site)

    def test_jsonl_committed_corruption_or_wrong_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec = self._spec(root)
            spec.spool_path.write_bytes(b"{not-json}\n")
            with self.assertRaises(ValueError):
                list(
                    iter_scrapy_link_discoveries(
                        spec.spool_path,
                        expected_source_key=spec.source_key,
                    )
                )

            wrong = {
                "record_type": "LINK_DISCOVERY",
                "source_key": source_key("https://wrong.example/"),
                "page_url": "https://example.com/",
                "discovered_url": "https://example.net/",
                "anchor_text": "x",
                "depth": 0,
                "same_site": False,
            }
            spec.spool_path.write_text(json.dumps(wrong) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                list(
                    iter_scrapy_link_discoveries(
                        spec.spool_path,
                        expected_source_key=spec.source_key,
                    )
                )

    def test_launcher_requires_a_locked_isolated_project(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                ScrapyScoutLauncher(root)
            (root / "pyproject.toml").write_text("[project]\nname='x'\nversion='0'\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                ScrapyScoutLauncher(root)
            (root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
            launcher = ScrapyScoutLauncher(root)
            self.assertEqual(launcher.project_dir, root.resolve())


if __name__ == "__main__":
    unittest.main()
