"""Transport-neutral locator helpers shared by source pipelines."""

from __future__ import annotations

from urllib.parse import urlsplit


_WRAPPER_SEGMENTS = frozenset({"content", "download"})


def format_path_from_locator(value: str) -> str:
    """Return the path segment that carries the artifact filename.

    Repository APIs often expose a file through a transport wrapper such as
    .../files/example.csv/content or .../example.cdx.gz/download.
    Network I/O must continue to use the original locator, while parser/format
    selection should inspect the embedded artifact filename.

    This function is syntax-only. It does not infer provenance, temporal
    semantics, or evidence authority.
    """

    if not isinstance(value, str) or not value.strip():
        raise ValueError("locator must be a non-empty string")
    path = urlsplit(value.strip()).path.lower()
    stripped = path.rstrip("/")
    head, separator, tail = stripped.rpartition("/")
    if separator and tail in _WRAPPER_SEGMENTS:
        _parent, parent_separator, candidate = head.rpartition("/")
        if parent_separator and "." in candidate:
            return head
    return path
