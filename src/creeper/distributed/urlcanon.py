"""Deterministic URL identity for Creeper Fabric source discovery."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from creeper.authority.normalizer import normalize_official


def canonical_http_url(value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError("URL is required")
    parsed = urlsplit(text)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError("only http/https source URLs are supported")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credential-bearing source URLs are not supported")
    hostname = normalize_official(parsed.hostname or "")
    if hostname is None:
        raise ValueError("source URL hostname is invalid")

    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("source URL port is invalid") from exc
    default_port = (scheme == "http" and port == 80) or (
        scheme == "https" and port == 443
    )
    netloc = hostname if port is None or default_port else f"{hostname}:{port}"
    path = parsed.path or "/"
    return urlunsplit((scheme, netloc, path, parsed.query, ""))
