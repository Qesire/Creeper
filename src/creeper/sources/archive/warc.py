"""Streaming WARC/ARC metadata primitives backed by :mod:`warcio`.

This module is deliberately format-only. It does **not** decide competition
novelty, evidence acceptance, or source quality. It exposes historical target
metadata plus stable byte offsets so bounded scouting and production leases can
share one mature WARC/ARC parser.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import gzip
import io
import os
import zlib
from os import PathLike
from typing import BinaryIO, Final
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

_TARGET_RECORD_TYPES: Final[frozenset[str]] = frozenset(
    {"response", "resource", "revisit"}
)
_REMOTE_SOURCE_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https", "s3"})
_DEFAULT_REMOTE_BLOCK_SIZE: Final[int] = 4 * 1024 * 1024
_DEFAULT_MAX_RECORD_CONTENT_BYTES: Final[int] = 64 * 1024 * 1024


class WarcFormatError(ValueError):
    """The input could not be consumed as a WARC/ARC stream."""


class WarcCursorError(ValueError):
    """A persisted WARC byte cursor is malformed or outside the stream."""


class WarcResourceLimitError(ValueError):
    """A WARC/ARC record violates an explicit production resource bound."""


@dataclass(frozen=True)
class WarcTargetRecord:
    """Host-bearing metadata from one WARC/ARC record."""

    record_type: str
    target_uri: str | None
    source_year: int | None
    offset: int
    length: int

    @property
    def next_offset(self) -> int:
        return self.offset + self.length


@dataclass(frozen=True)
class WarcMetadataLease:
    """Result of one bounded sequential WARC/ARC metadata lease."""

    records: tuple[WarcTargetRecord, ...]
    scanned_records: int
    start_offset: int
    end_offset: int
    next_offset: int | None
    exhausted: bool

    @property
    def next_cursor(self) -> str | None:
        return None if self.next_offset is None else encode_warc_cursor(self.next_offset)

    @property
    def bytes_advanced(self) -> int:
        return max(0, self.end_offset - self.start_offset)


def _capture_year(value: object) -> int | None:
    if not isinstance(value, str) or len(value) < 4 or not value[:4].isdigit():
        return None
    year = int(value[:4])
    return year if 1000 <= year <= 9999 else None


def encode_warc_cursor(offset: int) -> str:
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise WarcCursorError("WARC cursor offset must be a non-negative integer")
    return f"warc-byte:{offset}"


def decode_warc_cursor(cursor: str | None) -> int:
    if cursor in {None, "", "0"}:
        return 0
    if not isinstance(cursor, str) or not cursor.startswith("warc-byte:"):
        raise WarcCursorError("invalid WARC cursor; expected warc-byte:<offset>")
    raw = cursor.removeprefix("warc-byte:")
    if not raw.isdigit():
        raise WarcCursorError("invalid WARC cursor offset")
    return int(raw)


def _skip_record_separators(stream: BinaryIO) -> int:
    """Skip only inter-record CR/LF bytes after an uncompressed record."""

    start = stream.tell()
    while True:
        position = stream.tell()
        byte = stream.read(1)
        if byte in {b"\r", b"\n"}:
            continue
        if byte:
            stream.seek(position)
        break
    return stream.tell() - start


def _known_stream_size(stream: BinaryIO) -> int | None:
    """Return a cheap stream size when one is already available.

    Production must never probe the end of an arbitrary remote fsspec stream
    merely to validate a cursor. HTTP/S3 file objects normally expose ``size``
    from provider metadata; native files use ``fstat``; in-memory test streams
    expose their buffer length. Unknown-size streams remain valid and are
    exhausted by ArchiveIterator naturally.
    """

    advertised = getattr(stream, "size", None)
    if isinstance(advertised, int) and not isinstance(advertised, bool) and advertised >= 0:
        return advertised

    try:
        fileno = stream.fileno()  # type: ignore[attr-defined]
    except (AttributeError, OSError, ValueError, io.UnsupportedOperation):
        fileno = None
    if isinstance(fileno, int):
        try:
            return int(os.fstat(fileno).st_size)
        except OSError:
            pass

    if isinstance(stream, io.BytesIO):
        return len(stream.getbuffer())
    return None


def _archive_iterator(stream: BinaryIO):
    # Lazy import keeps cursor-only unit tests independent of the optional
    # parser import while production environments declare warcio in uv.lock.
    from warcio.archiveiterator import ArchiveIterator

    return ArchiveIterator(
        stream,
        no_record_parse=True,
        arc2warc=True,
        verify_http=False,
    )


def _archive_load_error_type():
    from warcio.exceptions import ArchiveLoadFailed

    return ArchiveLoadFailed


def _wrap_format_error(exc: Exception) -> None:
    """Normalize mature-parser corruption failures into one source error type."""

    try:
        archive_load_failed = _archive_load_error_type()
    except Exception:
        archive_load_failed = ()
    known_format_errors: tuple[type[BaseException], ...] = (
        EOFError,
        gzip.BadGzipFile,
        zlib.error,
    )
    if (archive_load_failed and isinstance(exc, archive_load_failed)) or isinstance(
        exc, known_format_errors
    ):
        raise WarcFormatError(str(exc).strip() or "invalid WARC/ARC stream") from exc
    raise exc


def _target_from_record(record, *, offset: int, length: int) -> WarcTargetRecord | None:
    if record.rec_type not in _TARGET_RECORD_TYPES:
        return None
    return WarcTargetRecord(
        record_type=record.rec_type,
        target_uri=record.rec_headers.get_header("WARC-Target-URI"),
        source_year=_capture_year(record.rec_headers.get_header("WARC-Date")),
        offset=offset,
        length=length,
    )


def iter_warc_target_records(stream: BinaryIO) -> Iterator[WarcTargetRecord]:
    """Yield target metadata from the stream's current byte position.

    Every record is fully consumed by warcio's public offset/length accessors;
    archived HTTP payload bytes are never materialized by Creeper.
    """

    try:
        iterator = _archive_iterator(stream)
        for record in iterator:
            offset = int(iterator.get_record_offset())
            length = int(iterator.get_record_length())
            target = _target_from_record(record, offset=offset, length=length)
            if target is not None:
                yield target
    except Exception as exc:
        _wrap_format_error(exc)


def read_warc_metadata_lease(
    stream: BinaryIO,
    *,
    cursor: str | None = None,
    max_scanned_records: int,
    max_archive_bytes: int,
    target_year_from: int = 1996,
    target_year_to: int = 2001,
    max_record_content_bytes: int = _DEFAULT_MAX_RECORD_CONTENT_BYTES,
) -> WarcMetadataLease:
    """Consume one bounded, resumable WARC/ARC metadata lease.

    The budget counts *all* archive records, not only response/resource/revisit
    records, preventing request- or metadata-heavy files from evading the work
    bound. ``max_archive_bytes`` is measured in source archive bytes.

    If a lease ends exactly on a budget boundary, the next lease may be an empty
    EOF-confirmation lease. This avoids peeking into the next potentially large
    record simply to predict exhaustion.
    """

    if max_scanned_records < 1:
        raise ValueError("max_scanned_records must be positive")
    if max_archive_bytes < 1:
        raise ValueError("max_archive_bytes must be positive")
    if (
        not isinstance(max_record_content_bytes, int)
        or isinstance(max_record_content_bytes, bool)
        or max_record_content_bytes < 1
    ):
        raise ValueError("max_record_content_bytes must be a positive integer")
    if target_year_from > target_year_to:
        raise ValueError("target_year_from must not exceed target_year_to")
    try:
        seekable = stream.seekable()
    except (OSError, ValueError) as exc:
        raise WarcCursorError(f"unable to inspect WARC stream seekability: {exc}") from exc
    if not seekable:
        raise WarcCursorError("production WARC leases require a seekable stream")

    requested_offset = decode_warc_cursor(cursor)
    size = _known_stream_size(stream)
    if size is not None and requested_offset > size:
        raise WarcCursorError(
            f"WARC cursor offset {requested_offset} exceeds stream size {size}"
        )
    try:
        # fsspec-backed HTTP/S3 handles resolve this through range/seek support.
        # Unknown-size remote streams are not probed at EOF: forcing seek(0, 2)
        # can defeat bounded remote access on weak HTTP servers.
        stream.seek(requested_offset)
    except (OSError, ValueError) as exc:
        raise WarcCursorError(f"unable to seek WARC stream: {exc}") from exc

    if requested_offset:
        _skip_record_separators(stream)
    actual_start = stream.tell()
    if size is not None and actual_start >= size:
        return WarcMetadataLease(
            records=(),
            scanned_records=0,
            start_offset=actual_start,
            end_offset=actual_start,
            next_offset=None,
            exhausted=True,
        )

    targets: list[WarcTargetRecord] = []
    scanned = 0
    end_offset = actual_start
    hit_budget = False

    try:
        iterator = _archive_iterator(stream)
        for record in iterator:
            declared_length = getattr(record, "length", None)
            if isinstance(declared_length, int) and not isinstance(declared_length, bool):
                if declared_length < 0:
                    raise WarcFormatError("WARC/ARC record has a negative declared length")
                if declared_length > max_record_content_bytes:
                    raise WarcResourceLimitError(
                        "WARC/ARC record declared content length "
                        f"{declared_length} exceeds limit {max_record_content_bytes}"
                    )
            offset = int(iterator.get_record_offset())
            length = int(iterator.get_record_length())
            if offset < actual_start or offset < end_offset or length <= 0:
                raise WarcFormatError(
                    "warcio returned a non-monotonic or non-positive record offset/length"
                )
            scanned += 1
            end_offset = offset + length

            target = _target_from_record(record, offset=offset, length=length)
            if (
                target is not None
                and target.source_year is not None
                and target_year_from <= target.source_year <= target_year_to
            ):
                targets.append(target)

            if (
                scanned >= max_scanned_records
                or end_offset - actual_start >= max_archive_bytes
            ):
                hit_budget = True
                break
    except Exception as exc:
        if isinstance(exc, (WarcFormatError, WarcResourceLimitError)):
            raise
        _wrap_format_error(exc)

    if not hit_budget:
        return WarcMetadataLease(
            records=tuple(targets),
            scanned_records=scanned,
            start_offset=actual_start,
            end_offset=end_offset,
            next_offset=None,
            exhausted=True,
        )

    # For uncompressed WARC/ARC, warcio's record length excludes blank record
    # separators. Resume deliberately skips only CR/LF before constructing the
    # next ArchiveIterator. For canonical record-gzip, end_offset is already the
    # next gzip-member boundary and separator skipping is a no-op.
    return WarcMetadataLease(
        records=tuple(targets),
        scanned_records=scanned,
        start_offset=actual_start,
        end_offset=end_offset,
        next_offset=end_offset,
        exhausted=False,
    )


def _source_scheme(source: str | PathLike[str]) -> str:
    if isinstance(source, PathLike):
        return ""
    return urlsplit(str(source)).scheme.lower()


def _is_remote_source(source: str | PathLike[str]) -> bool:
    return _source_scheme(source) in _REMOTE_SOURCE_SCHEMES


def _open_warc_source(
    source: str | PathLike[str],
    *,
    remote_block_size: int = _DEFAULT_REMOTE_BLOCK_SIZE,
):
    """Open local or remote WARC/ARC using mature warcio/fsspec support.

    Local files deliberately stay on Python's native ``open`` path. Remote
    HTTP/S3 sources are delegated to :func:`warcio.utils.fsspec_open`, added
    for this purpose in warcio 1.8. The returned handle must be seekable;
    ``read_warc_metadata_lease`` enforces that before consuming records.
    """

    if (
        not isinstance(remote_block_size, int)
        or isinstance(remote_block_size, bool)
        or remote_block_size < 1
    ):
        raise ValueError("remote_block_size must be a positive integer")

    scheme = _source_scheme(source)
    if scheme and scheme not in {"file", *_REMOTE_SOURCE_SCHEMES}:
        raise WarcCursorError(
            f"unsupported WARC/ARC source scheme {scheme!r}; "
            "allowed schemes are file, http, https, and s3"
        )

    if not _is_remote_source(source):
        path = str(source)
        if scheme == "file":
            parsed = urlsplit(path)
            if parsed.netloc not in {"", "localhost"}:
                raise WarcCursorError(
                    "file:// WARC/ARC sources must reference the local host"
                )
            path = url2pathname(unquote(parsed.path))
        return open(path, "rb")

    try:
        from warcio.utils import fsspec_open
    except (ImportError, ModuleNotFoundError) as exc:
        raise WarcCursorError(
            "remote WARC/ARC access requires warcio remote filesystem support "
            "(install the project-locked warcio/fsspec dependencies)"
        ) from exc

    try:
        return fsspec_open(
            str(source),
            "rb",
            block_size=remote_block_size,
            cache_type="readahead",
        )
    except ModuleNotFoundError as exc:
        missing = getattr(exc, "name", None) or str(exc)
        if scheme == "s3" and ("s3fs" in missing or "s3fs" in str(exc)):
            raise WarcCursorError(
                "install s3fs for S3 WARC/ARC access; "
                "use the project s3 extra (pip install 'creeper[s3]')"
            ) from exc
        if "fsspec" in missing or "fsspec" in str(exc):
            raise WarcCursorError(
                "install fsspec for remote WARC/ARC access; "
                "use the project's remote filesystem dependencies"
            ) from exc
        raise WarcCursorError(
            f"unable to open remote WARC/ARC source {source!s}: {exc}"
        ) from exc
    except Exception as exc:
        raise WarcCursorError(
            f"unable to open remote WARC/ARC source {source!s}: {exc}"
        ) from exc


def read_warc_source_lease(
    source: str | PathLike[str],
    *,
    cursor: str | None = None,
    max_scanned_records: int,
    max_archive_bytes: int,
    target_year_from: int = 1996,
    target_year_to: int = 2001,
    remote_block_size: int = _DEFAULT_REMOTE_BLOCK_SIZE,
    max_record_content_bytes: int = _DEFAULT_MAX_RECORD_CONTENT_BYTES,
) -> WarcMetadataLease:
    """Open a local/HTTP/S3 archive and execute one resumable lease.

    Remote seeking/range transport is intentionally owned by warcio/fsspec.
    Creeper only owns archive cursor semantics and lease accounting. Servers
    that cannot provide a seekable remote file fail closed rather than forcing
    an accidental whole-object download.
    """

    try:
        with _open_warc_source(source, remote_block_size=remote_block_size) as stream:
            return read_warc_metadata_lease(
                stream,
                cursor=cursor,
                max_scanned_records=max_scanned_records,
                max_archive_bytes=max_archive_bytes,
                target_year_from=target_year_from,
                target_year_to=target_year_to,
                max_record_content_bytes=max_record_content_bytes,
            )
    except WarcCursorError:
        raise
    except OSError as exc:
        raise WarcCursorError(
            f"unable to open WARC/ARC source {source!s}: {exc}"
        ) from exc


def read_warc_path_lease(
    path: str | PathLike[str],
    *,
    cursor: str | None = None,
    max_scanned_records: int,
    max_archive_bytes: int,
    target_year_from: int = 1996,
    target_year_to: int = 2001,
    max_record_content_bytes: int = _DEFAULT_MAX_RECORD_CONTENT_BYTES,
) -> WarcMetadataLease:
    """Backward-compatible local-path wrapper."""

    return read_warc_source_lease(
        path,
        cursor=cursor,
        max_scanned_records=max_scanned_records,
        max_archive_bytes=max_archive_bytes,
        target_year_from=target_year_from,
        target_year_to=target_year_to,
        max_record_content_bytes=max_record_content_bytes,
    )
