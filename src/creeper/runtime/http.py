"""HTTP client environment policy shared by asynchronous runtimes."""

from __future__ import annotations

import os
from urllib.parse import urlsplit


def configured_http_proxy() -> str | None:
    """Return the first HTTP(S) proxy from the conventional environment.

    HTTPX's base install does not support SOCKS URLs. Selecting an explicit
    HTTP(S) proxy and disabling its implicit environment scan prevents an
    unrelated ``ALL_PROXY=socks://...`` value from breaking client creation.
    SOCKS remains an explicit deployment concern requiring ``httpx[socks]``.
    """
    for name in (
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        value = os.environ.get(name)
        if not value:
            continue
        if urlsplit(value).scheme.lower() in {"http", "https"}:
            return value
    return None
