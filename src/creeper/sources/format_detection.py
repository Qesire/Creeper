"""Bounded deterministic detection for already-supported source parsers."""

from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import PurePosixPath

from creeper.authority.normalizer import normalize_official
from creeper.sources.archive.cdx import parse_cdx_line
from creeper.sources.archive.cdxj import parse_cdxj_line
from creeper.sources.format_binding import SourceFormatObservation
from creeper.sources.ftp_sitelist import is_ftp_sitelist_locator
from creeper.sources.locator import format_path_from_locator
from creeper.sources.non_snapshot import (
    is_dmoz_content_locator,
    is_mailbox_url_locator,
    is_squid_access_locator,
    parse_squid_access_line,
)
from creeper.sources.sbi_bbs import is_sbi_bbs_locator


def _suffix(locator: str) -> tuple[str, bool]:
    name = PurePosixPath(format_path_from_locator(locator)).name.lower()
    compressed = name.endswith(".gz")
    if compressed:
        name = name[:-3]
    return PurePosixPath(name).suffix.lower(), compressed


def _known_locator_parser(locator: str) -> str | None:
    if is_mailbox_url_locator(locator):
        return "mbox_urls"
    if is_squid_access_locator(locator):
        return "squid_access"
    if is_dmoz_content_locator(locator):
        return "dmoz_rdf_urls"
    if is_ftp_sitelist_locator(locator):
        return "ftp_sitelist_zip"
    if is_sbi_bbs_locator(locator):
        return "sbi_bbs_zip"
    path = format_path_from_locator(locator)
    if path.endswith((".warc.gz", ".arc.gz", ".warc", ".arc")):
        return "warc_arc"
    if path.endswith((".cdxj", ".cdxj.gz")):
        return "cdxj"
    if path.endswith((".cdx", ".cdx.gz")):
        return "cdx"
    if path.endswith((".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz")):
        return "jsonl"
    if path.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
        return "delimited"
    if path.endswith((".txt", ".txt.gz", ".list", ".list.gz", ".urls", ".urls.gz")):
        return "lines"
    return None


def _inflate_probe(payload: bytes, *, limit: int = 512 * 1024) -> bytes | None:
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as stream:
            return stream.read(limit)
    except (OSError, EOFError, gzip.BadGzipFile):
        return None


def _text_lines(payload: bytes, *, limit: int = 64) -> list[str]:
    text = payload.decode("utf-8", errors="replace")
    return [line for line in text.splitlines() if line.strip()][:limit]


def _looks_like_host(value: str) -> bool:
    text = value.strip().strip('"').strip("'")
    if "://" in text:
        from urllib.parse import urlsplit

        try:
            host = urlsplit(text).hostname
        except ValueError:
            return False
        return normalize_official(host or "") is not None
    if "/" in text:
        text = text.split("/", 1)[0]
    return normalize_official(text) is not None


def _signature_parser(payload: bytes) -> tuple[str, float] | None:
    if payload.startswith((b"WARC/", b"filedesc://")):
        return "warc_arc", 0.99

    lines = _text_lines(payload)
    if not lines:
        return None

    squid_hits = sum(parse_squid_access_line(line) is not None for line in lines[:24])
    if squid_hits >= 3 and squid_hits / min(24, len(lines)) >= 0.6:
        return "squid_access", 0.98

    cdxj_hits = sum(
        parse_cdxj_line(line, source_id="format-detect", locator=str(index)) is not None
        for index, line in enumerate(lines[:24])
    )
    if cdxj_hits >= 3 and cdxj_hits / min(24, len(lines)) >= 0.6:
        return "cdxj", 0.98

    cdx_hits = sum(
        parse_cdx_line(line, source_id="format-detect", locator=str(index)) is not None
        for index, line in enumerate(lines[:24])
    )
    if cdx_hits >= 3 and cdx_hits / min(24, len(lines)) >= 0.6:
        return "cdx", 0.98

    json_hits = 0
    json_host_hits = 0
    for line in lines[:24]:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        json_hits += 1
        if isinstance(value, dict):
            lowered = {str(key).lower(): item for key, item in value.items()}
            for key in ("hostname", "host", "domain", "url", "original", "original_url", "uri"):
                item = lowered.get(key)
                if isinstance(item, str) and _looks_like_host(item):
                    json_host_hits += 1
                    break
    if json_hits >= 3 and json_host_hits >= 2 and json_hits / min(24, len(lines)) >= 0.6:
        return "jsonl", 0.97

    text = payload.decode("utf-8", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(text[:16 * 1024], delimiters=",	;|")
    except csv.Error:
        dialect = None
    if dialect is not None:
        rows = list(csv.reader(io.StringIO(text), dialect=dialect))[:32]
        if rows and max((len(row) for row in rows), default=0) >= 2:
            host_rows = 0
            for row in rows:
                if any(_looks_like_host(cell) for cell in row):
                    host_rows += 1
            if host_rows >= 2:
                return "delimited", 0.92

    host_lines = sum(_looks_like_host(line) for line in lines[:32])
    if host_lines >= 3 and host_lines / min(32, len(lines)) >= 0.5:
        return "lines", 0.88
    return None


def detect_source_format(
    *,
    locator: str,
    payload: bytes,
    content_type: str = "",
) -> SourceFormatObservation | None:
    """Detect only parser families Creeper can already execute deterministically."""

    suffix, locator_gzip = _suffix(locator)
    gzip_magic = payload.startswith(b"\x1f\x8b")
    compression = "gzip" if locator_gzip or gzip_magic else "none"
    parser = _known_locator_parser(locator)
    if parser is not None:
        return SourceFormatObservation(
            parser_kind=parser,
            compression=compression,
            detection_method="locator",
            confidence=1.0,
            content_type=content_type,
        )

    media = content_type.split(";", 1)[0].strip().lower()
    content_map = {
        "application/warc": "warc_arc",
        "application/x-warc": "warc_arc",
        "application/arc": "warc_arc",
        "application/x-arc": "warc_arc",
        "application/x-ndjson": "jsonl",
        "application/ndjson": "jsonl",
        "text/csv": "delimited",
        "text/tab-separated-values": "delimited",
    }
    if media in content_map:
        return SourceFormatObservation(
            parser_kind=content_map[media],
            compression=compression,
            detection_method="content_type",
            confidence=0.96,
            content_type=content_type,
        )

    sniff_payload = payload
    if compression == "gzip":
        inflated = _inflate_probe(payload)
        if inflated is None:
            return None
        sniff_payload = inflated

    signature = _signature_parser(sniff_payload)
    if signature is None:
        return None
    parser_kind, confidence = signature
    return SourceFormatObservation(
        parser_kind=parser_kind,
        compression=compression,
        detection_method="content_signature",
        confidence=confidence,
        content_type=content_type,
    )
