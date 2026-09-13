"""Bounded-memory readers for submission archive entries."""

from __future__ import annotations

from collections.abc import Iterator
import zipfile


def iter_archive_lines(
    bundle: zipfile.ZipFile,
    name: str,
    *,
    chunk_size: int = 64 * 1024,
) -> Iterator[str]:
    """Yield decoded logical lines from one ZIP entry.

    Only the current compressed chunk and the current unterminated line are
    retained.  UTF-8 decoding uses replacement semantics to match the
    verifier's historical behavior, and CRLF input is normalized to a line
    without the carriage return.
    """

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    pending = b""
    with bundle.open(name, "r") as source:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            lines = (pending + chunk).split(b"\n")
            pending = lines.pop()
            for line in lines:
                if line.endswith(b"\r"):
                    line = line[:-1]
                yield line.decode("utf-8", errors="replace")

    if pending:
        line = bytes(pending)
        if line.endswith(b"\r"):
            line = line[:-1]
        yield line.decode("utf-8", errors="replace")
