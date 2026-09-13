import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from creeper.authority.baseline_index import (
    BASELINE_INDEX_SCHEMA_VERSION,
    BaselineIndex,
)
from creeper.authority.identity import AuthoritySnapshot, authority_digest


def _authority(baseline: Path, *, baseline_eed: str = "10") -> AuthoritySnapshot:
    annual_hashes = {
        f"{year}.txt": hashlib.sha256(
            (baseline / f"{year}.txt").read_bytes()
        ).hexdigest()
        for year in range(1996, 2002)
    }
    candidate_hash = hashlib.sha256(
        (baseline / "candidate_pool.txt").read_bytes()
    ).hexdigest()
    model_hash = "a" * 64
    digest = authority_digest(
        baseline_id=baseline.name,
        annual_file_hashes=annual_hashes,
        candidate_file_hash=candidate_hash,
        model_hash=model_hash,
        baseline_eed=baseline_eed,
    )
    return AuthoritySnapshot(
        baseline.name,
        annual_hashes,
        candidate_hash,
        model_hash,
        baseline_eed,
        digest,
    )


def _make_baseline(root: Path, name: str = "merged260912-3") -> Path:
    baseline = root / name
    baseline.mkdir()
    for year in range(1996, 2002):
        (baseline / f"{year}.txt").write_text(
            f"year{year}.example\n",
            encoding="utf-8",
        )
    (baseline / "candidate_pool.txt").write_text(
        "candidate.example\n",
        encoding="utf-8",
    )
    return baseline


class BaselinePathTests(unittest.TestCase):
    def test_build_v4_from_explicit_baseline_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline)
            output = root / "index.sqlite3"

            index = BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            )
            try:
                self.assertEqual(index.year_mask("year2001.example"), 1 << 5)
                metadata = dict(
                    index.connection.execute(
                        "SELECT key, value FROM authority_metadata"
                    )
                )
                self.assertEqual(metadata["baseline_id"], "merged260912-3")
                self.assertEqual(
                    metadata["index_schema_version"],
                    BASELINE_INDEX_SCHEMA_VERSION,
                )
                self.assertEqual(
                    metadata["authority_digest"],
                    authority.authority_digest,
                )
            finally:
                index.close()

    def test_future_baseline_name_requires_no_version_specific_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root, "merged-future-round")
            authority = _authority(baseline)

            index = BaselineIndex.build(
                baseline_dir=baseline,
                output_path=root / "index.sqlite3",
                authority_manifest=authority,
            )
            try:
                metadata = dict(index.connection.execute(
                    "SELECT key, value FROM authority_metadata"
                ))
                self.assertEqual(metadata["baseline_id"], "merged-future-round")
            finally:
                index.close()

    def test_same_authority_resume_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline)
            output = root / "index.sqlite3"
            BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            ).close()

            resumed = BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            )
            try:
                self.assertEqual(resumed.counts()["candidate_hostnames"], 1)
            finally:
                resumed.close()

    def test_resume_different_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline, baseline_eed="10")
            output = root / "index.sqlite3"
            BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            ).close()

            replacement = _authority(baseline, baseline_eed="11")
            with self.assertRaisesRegex(ValueError, "authority mismatch"):
                BaselineIndex.build(
                    baseline_dir=baseline,
                    output_path=output,
                    authority_manifest=replacement,
                )

    def test_partial_same_authority_resumes_from_durable_offset(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            (baseline / "1996.txt").write_text(
                "first.example\nsecond.example\nthird.example\n",
                encoding="utf-8",
            )
            authority = _authority(baseline)
            output = root / "index.sqlite3"
            BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
                batch_size=1,
            ).close()

            with sqlite3.connect(output) as connection:
                connection.execute(
                    "DELETE FROM annual_hostnames WHERE hostname IN (?, ?)",
                    ("second.example", "third.example"),
                )
                connection.execute(
                    "UPDATE import_state SET line_offset=1, completed=0 "
                    "WHERE stage='year:1996'"
                )
                connection.commit()

            resumed = BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
                batch_size=1,
            )
            try:
                self.assertNotEqual(resumed.year_mask("second.example"), 0)
                self.assertNotEqual(resumed.year_mask("third.example"), 0)
                state = resumed.connection.execute(
                    "SELECT line_offset, completed FROM import_state "
                    "WHERE stage='year:1996'"
                ).fetchone()
                self.assertEqual(tuple(state), (3, 1))
            finally:
                resumed.close()

    def test_populated_unbound_index_cannot_be_retroactively_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline)
            output = root / "legacy.sqlite3"
            with sqlite3.connect(output) as connection:
                connection.execute(
                    "CREATE TABLE annual_hostnames("
                    "hostname TEXT PRIMARY KEY, year_mask INTEGER NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO annual_hostnames VALUES ('legacy.example', 1)"
                )
                connection.commit()

            with self.assertRaisesRegex(ValueError, "populated unbound"):
                BaselineIndex.bind_authority(output, authority)

    def test_nonresume_rebuild_never_mutates_existing_index_in_place(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline)
            output = root / "index.sqlite3"
            BaselineIndex.build(
                baseline_dir=baseline,
                output_path=output,
                authority_manifest=authority,
            ).close()
            before = output.read_bytes()

            with self.assertRaisesRegex(ValueError, "new output path"):
                BaselineIndex.build(
                    baseline_dir=baseline,
                    output_path=output,
                    authority_manifest=authority,
                    resume=False,
                )

            self.assertEqual(output.read_bytes(), before)

    def test_source_hash_mismatch_is_detected_before_output_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            baseline = _make_baseline(root)
            authority = _authority(baseline)
            (baseline / "1999.txt").write_text(
                "tampered.example\n",
                encoding="utf-8",
            )
            output = root / "index.sqlite3"

            with self.assertRaisesRegex(ValueError, "1999.txt"):
                BaselineIndex.build(
                    baseline_dir=baseline,
                    output_path=output,
                    authority_manifest=authority,
                )

            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
