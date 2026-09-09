import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import BaselineIndex


def build_test_index(root: Path) -> BaselineIndex:
    baseline = root / "merged260909-3"
    baseline.mkdir()
    for year in range(1996, 2002):
        (baseline / f"{year}.txt").write_text(
            "present.example\n", encoding="utf-8"
        )
    (baseline / "candidate_pool.txt").write_text(
        "candidate.example\n", encoding="utf-8"
    )
    return BaselineIndex.build(root, root / "index.sqlite3")


class BaselineStreamingTests(unittest.TestCase):
    def test_yields_first_batch_before_consuming_remaining_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = build_test_index(Path(tmp))

            def guarded_hosts():
                yield "present.example"
                yield "absent.example"
                raise AssertionError("source was consumed past the first batch")

            first = next(
                index.iter_resolve_batches(guarded_hosts(), input_batch_size=2)
            )

            self.assertEqual(
                first,
                {
                    "present.example": (0b111111, False),
                    "absent.example": (0, False),
                },
            )

    def test_invalid_inputs_are_omitted_and_duplicates_resolve_once_per_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = build_test_index(Path(tmp))

            batches = list(
                index.iter_resolve_batches(
                    [
                        "",
                        "not-a-host",
                        " PRESENT.EXAMPLE ",
                        "present.example",
                        "absent.example",
                    ],
                    input_batch_size=5,
                )
            )

            self.assertEqual(
                batches,
                [
                    {
                        "present.example": (0b111111, False),
                        "absent.example": (0, False),
                    }
                ],
            )

    def test_union_of_streaming_batches_matches_resolve_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = build_test_index(Path(tmp))
            hosts = [
                "present.example",
                "candidate.example",
                "absent.example",
                " PRESENT.EXAMPLE ",
                "also-absent.example",
            ]

            streamed = {}
            for batch in index.iter_resolve_batches(
                (host for host in hosts), input_batch_size=2, chunk_size=1
            ):
                streamed.update(batch)

            self.assertEqual(
                streamed,
                index.resolve_batch(hosts, chunk_size=1),
            )

    def test_input_batch_size_must_be_positive(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = build_test_index(Path(tmp))

            with self.assertRaises(ValueError):
                list(index.iter_resolve_batches([], input_batch_size=0))


if __name__ == "__main__":
    unittest.main()
