"""Exact hostname normalization compatible with the supplied calculator."""

from __future__ import annotations

import re


HOST_RE = re.compile(
    r"(?=.{1,253}\Z)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z]{2,63}\Z"
)


def normalize_official(raw: str) -> str | None:
    """Return the official lowercase hostname or ``None`` when invalid."""

    value = raw.strip().lower()
    if not value or not HOST_RE.fullmatch(value):
        return None
    return value


def normalize_lines(path):
    """Yield unique official hostnames from a UTF-8 line file."""

    values: set[str] = set()
    with path.open("r", encoding="utf-8", errors="replace", newline="") as source:
        for line in source:
            value = normalize_official(line)
            if value:
                values.add(value)
    yield from values
