from __future__ import annotations

import io
import sys
import tempfile
import types
import unittest
from pathlib import Path

from creeper.sources.archive import warc


class _ContextBytesIO(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _NonSeekable(io.BytesIO):
    def seekable(self) -> bool:
        return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _SeekRaisesValueError(io.BytesIO):
    def seek(self, *args):  # type: ignore[override]
        raise ValueError("range requests unavailable")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class _FakeWarcioUtils:
    def __init__(self, stream_factory):
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.stream_factory = stream_factory

    def install(self):
        fake_utils = types.ModuleType("warcio.utils")

        def fake_fsspec_open(source: str, mode: str, **kwargs):
            self.calls.append((source, mode, dict(kwargs)))
            return self.stream_factory()

        fake_utils.fsspec_open = fake_fsspec_open
        fake_warcio = types.ModuleType("warcio")
        old_warcio = sys.modules.get("warcio")
        old_utils = sys.modules.get("warcio.utils")
        sys.modules["warcio"] = fake_warcio
        sys.modules["warcio.utils"] = fake_utils
        return old_warcio, old_utils

    @staticmethod
    def restore(old_warcio, old_utils) -> None:
        if old_warcio is None:
            sys.modules.pop("warcio", None)
        else:
            sys.modules["warcio"] = old_warcio
        if old_utils is None:
            sys.modules.pop("warcio.utils", None)
        else:
            sys.modules["warcio.utils"] = old_utils


class WarcRemoteOpenTests(unittest.TestCase):
    def test_https_missing_fsspec_has_install_hint(self) -> None:
        fake_utils = types.ModuleType("warcio.utils")

        def missing_fsspec(*args, **kwargs):
            raise ModuleNotFoundError("No module named 'fsspec'", name="fsspec")

        fake_utils.fsspec_open = missing_fsspec
        fake_warcio = types.ModuleType("warcio")
        old_warcio = sys.modules.get("warcio")
        old_utils = sys.modules.get("warcio.utils")
        sys.modules["warcio"] = fake_warcio
        sys.modules["warcio.utils"] = fake_utils
        try:
            with self.assertRaisesRegex(warc.WarcCursorError, "install fsspec"):
                warc._open_warc_source("https://archive.example/big.warc.gz")
        finally:
            _FakeWarcioUtils.restore(old_warcio, old_utils)

    def test_s3_missing_backend_has_s3fs_install_hint(self) -> None:
        fake_utils = types.ModuleType("warcio.utils")

        def missing_s3fs(*args, **kwargs):
            raise ModuleNotFoundError("No module named 's3fs'", name="s3fs")

        fake_utils.fsspec_open = missing_s3fs
        fake_warcio = types.ModuleType("warcio")
        old_warcio = sys.modules.get("warcio")
        old_utils = sys.modules.get("warcio.utils")
        sys.modules["warcio"] = fake_warcio
        sys.modules["warcio.utils"] = fake_utils
        try:
            with self.assertRaisesRegex(warc.WarcCursorError, "install s3fs"):
                warc._open_warc_source("s3://bucket/path/big.warc.gz")
        finally:
            _FakeWarcioUtils.restore(old_warcio, old_utils)

    def test_local_path_uses_native_open_without_remote_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.warc"
            path.write_bytes(b"WARC/1.0\r\n")
            with warc._open_warc_source(path) as stream:
                self.assertEqual(stream.read(), b"WARC/1.0\r\n")

    def test_https_source_delegates_to_warcio_fsspec_open(self) -> None:
        fake = _FakeWarcioUtils(lambda: _ContextBytesIO(b"remote"))
        old_warcio, old_utils = fake.install()
        try:
            with warc._open_warc_source("https://archive.example/big.warc.gz") as stream:
                self.assertEqual(stream.read(), b"remote")
        finally:
            fake.restore(old_warcio, old_utils)

        self.assertEqual(
            fake.calls,
            [(
                "https://archive.example/big.warc.gz",
                "rb",
                {"block_size": 4 * 1024 * 1024, "cache_type": "readahead"},
            )],
        )

    def test_s3_source_delegates_to_warcio_fsspec_open(self) -> None:
        fake = _FakeWarcioUtils(lambda: _ContextBytesIO(b"remote"))
        old_warcio, old_utils = fake.install()
        try:
            with warc._open_warc_source("s3://bucket/path/big.warc.gz") as stream:
                self.assertEqual(stream.read(), b"remote")
        finally:
            fake.restore(old_warcio, old_utils)
        self.assertEqual(
            fake.calls,
            [(
                "s3://bucket/path/big.warc.gz",
                "rb",
                {"block_size": 4 * 1024 * 1024, "cache_type": "readahead"},
            )],
        )

    def test_file_url_stays_on_local_native_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.arc"
            path.write_bytes(b"ARC")
            with warc._open_warc_source(path.as_uri()) as stream:
                self.assertEqual(stream.read(), b"ARC")

    def test_percent_encoded_file_url_is_decoded_before_native_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample archive.warc"
            path.write_bytes(b"WARC")
            with warc._open_warc_source(path.as_uri()) as stream:
                self.assertEqual(stream.read(), b"WARC")

    def test_nonlocal_file_url_fails_closed(self) -> None:
        with self.assertRaisesRegex(warc.WarcCursorError, "local host"):
            warc._open_warc_source("file://remote-host/tmp/sample.warc")

    def test_unknown_fsspec_scheme_is_not_agent_extensible(self) -> None:
        for source in (
            "ftp://archive.example/sample.warc",
            "memory://sample.warc",
            "ssh://archive.example/sample.warc",
        ):
            with self.subTest(source=source):
                with self.assertRaisesRegex(warc.WarcCursorError, "unsupported .* scheme"):
                    warc._open_warc_source(source)

    def test_remote_block_size_is_configurable_without_changing_cache_policy(self) -> None:
        fake = _FakeWarcioUtils(lambda: _ContextBytesIO(b"remote"))
        old_warcio, old_utils = fake.install()
        try:
            with warc._open_warc_source(
                "https://archive.example/big.warc.gz",
                remote_block_size=1024 * 1024,
            ) as stream:
                self.assertEqual(stream.read(), b"remote")
        finally:
            fake.restore(old_warcio, old_utils)
        self.assertEqual(
            fake.calls,
            [(
                "https://archive.example/big.warc.gz",
                "rb",
                {"block_size": 1024 * 1024, "cache_type": "readahead"},
            )],
        )

    def test_invalid_remote_block_size_is_rejected_before_open(self) -> None:
        with self.assertRaisesRegex(ValueError, "remote_block_size"):
            warc._open_warc_source(
                "https://archive.example/big.warc.gz",
                remote_block_size=0,
            )

    def test_nonseekable_remote_source_fails_before_archive_parse(self) -> None:
        fake = _FakeWarcioUtils(lambda: _NonSeekable(b"WARC/1.0\r\n"))
        old_warcio, old_utils = fake.install()
        try:
            with self.assertRaisesRegex(warc.WarcCursorError, "seekable stream"):
                warc.read_warc_source_lease(
                    "https://archive.example/no-range.warc",
                    max_scanned_records=1,
                    max_archive_bytes=1024,
                )
        finally:
            fake.restore(old_warcio, old_utils)

    def test_remote_seek_failure_is_wrapped_as_cursor_error(self) -> None:
        fake = _FakeWarcioUtils(lambda: _SeekRaisesValueError(b"WARC/1.0\r\n"))
        old_warcio, old_utils = fake.install()
        try:
            with self.assertRaisesRegex(warc.WarcCursorError, "unable to seek"):
                warc.read_warc_source_lease(
                    "https://archive.example/no-range.warc",
                    max_scanned_records=1,
                    max_archive_bytes=1024,
                )
        finally:
            fake.restore(old_warcio, old_utils)


if __name__ == "__main__":
    unittest.main()
