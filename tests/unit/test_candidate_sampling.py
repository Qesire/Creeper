import tempfile
import unittest
import json
from pathlib import Path

from creeper.sources.sampling import stratified_hostnames


class CandidateSamplingTests(unittest.TestCase):
    def test_sampling_is_deterministic_and_covers_multiple_buckets(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "candidate_pool.txt"
            path.write_text(
                "a.com\nwww.a.com\nlong-name.example.org\nsub.deep.example.net\n"
                "short.io\nother.co.uk\n", encoding="utf-8"
            )
            first = stratified_hostnames(path, 4)
            second = stratified_hostnames(path, 4)
            self.assertEqual(first, second)
            self.assertEqual(len(first), 4)
            self.assertGreaterEqual(len({item.tld for item in first}), 2)
            self.assertTrue(all(item.hostname for item in first))

    def test_cache_reuse_avoids_reparsing_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "candidate_pool.txt"
            cache = Path(tmp) / "sample-cache.json"
            path.write_text("a.com\nb.org\nc.net\n", encoding="utf-8")
            first = stratified_hostnames(path, 2, cache_path=cache)
            self.assertTrue(cache.is_file())
            second = stratified_hostnames(path, 2, cache_path=cache)
            self.assertEqual(first, second)
            self.assertGreaterEqual(json.loads(cache.read_text())["bucket_cap"], 32)


if __name__ == "__main__":
    unittest.main()
