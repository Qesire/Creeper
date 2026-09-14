"""Deterministic parsers for non-snapshot historical URL sources.

These helpers deliberately extract only HTTP(S) URLs/hostnames.  Mailbox
authors, addresses, message text, client IPs, and other personal fields never
cross into Creeper SourceRecord payloads.
"""

from __future__ import annotations

from datetime import datetime, timezone
import html
import io
import re
from urllib.parse import urlsplit
import zipfile


_TARGET_YEAR_RE = re.compile(r"^(199[6-9]|200[01])-(0[1-9]|1[0-2])$")
_HTTP_URL_RE = re.compile(
    r"""(?ix)
    \bhttps?://
    [^\s<>\[\]{}"'\\]+
    """
)
_TRAILING_URL_PUNCTUATION = ".,;:!?)]}>"
_SQUID_PATH_MARKERS = (
    "/cache/squid/rawlogs/",
    "/squid/rawlogs/",
    "/squid/access",
)
_IRCACHE_SANITIZED_ACCESS_RE = re.compile(
    r"^(?:[a-z0-9._-]+\.)?sanitized-access\."
    r"(199[6-9]|200[01])\d{4}(?:\.gz)?$",
    re.IGNORECASE,
)
_DMOZ_CONTENT_NAMES = frozenset(
    {
        "content.rdf.u8",
        "content.rdf.u8.gz",
        "kt-content.rdf.u8",
        "kt-content.rdf.u8.gz",
    }
)
_DMOZ_EXTERNAL_PAGE_RE = re.compile(
    r"""(?ix)
    <ExternalPage\b
    [^>]*\babout\s*=\s*
    ["']([^"']+)["']
    """
)
_FTP_SITELIST_ZIP_NAMES = frozenset({"ftp-list.zip"})
_FTP_SITELIST_SITE_RE = re.compile(r"^\s*Site\s*:\s*(\S+)\s*$", re.IGNORECASE)
_FTP_SITELIST_DATE_RE = re.compile(
    r"^\s*Date\s*:\s*(\d{1,2}-[A-Za-z]{3}-\d{2,4})\b",
    re.IGNORECASE,
)


def mailbox_year_from_locator(locator: str) -> int | None:
    """Return target year for an exact monthly mailbox shard, if encoded."""
    path = urlsplit(locator).path.rstrip("/")
    name = path.rsplit("/", 1)[-1].lower()
    for suffix in (".mbox.gz", ".mbox"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    match = _TARGET_YEAR_RE.fullmatch(name)
    if match is None:
        return None
    return int(match.group(1))


def is_mailbox_url_locator(locator: str) -> bool:
    """Recognize mailbox files and GNU-style extensionless monthly shards."""
    path = urlsplit(locator).path.lower().rstrip("/")
    if path.endswith((".mbox", ".mbox.gz")):
        return True
    if "/archive/mbox/" not in path:
        return False
    return mailbox_year_from_locator(locator) is not None


def is_target_mailbox_shard(locator: str) -> bool:
    return (
        is_mailbox_url_locator(locator)
        and mailbox_year_from_locator(locator) is not None
    )


def extract_http_urls(
    text: str,
    *,
    max_urls: int = 64,
) -> tuple[str, ...]:
    """Extract bounded HTTP(S) URLs without retaining surrounding text."""
    if max_urls < 1:
        raise ValueError("max_urls must be positive")
    result: list[str] = []
    seen: set[str] = set()
    for match in _HTTP_URL_RE.finditer(text):
        value = match.group(0).rstrip(_TRAILING_URL_PUNCTUATION)
        try:
            parsed = urlsplit(value)
        except ValueError:
            continue
        if parsed.scheme.lower() not in {"http", "https"} or parsed.hostname is None:
            continue
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
        if len(result) >= max_urls:
            break
    return tuple(result)


def is_ftp_sitelist_zip_locator(locator: str) -> bool:
    """Recognize the Perry Rovers Anonymous FTP sitelist package only."""
    path = urlsplit(locator).path.lower().rstrip("/")
    if not path:
        return False
    return path.rsplit("/", 1)[-1] in _FTP_SITELIST_ZIP_NAMES


def parse_ftp_sitelist_records(
    text: str,
) -> tuple[tuple[str, str, int, int], ...]:
    """Parse Site + record Date pairs from one sitelist text member.

    Returns (site, date_text, year, site_line). The date is the sitelist
    record last-modification date, not the package publication date.
    """
    records: list[tuple[str, str, int, int]] = []
    site: str | None = None
    site_line = 0
    date_text: str | None = None
    year: int | None = None

    def flush() -> None:
        nonlocal site, site_line, date_text, year
        if site is not None and date_text is not None and year is not None:
            try:
                parsed = urlsplit("ftp://" + site)
            except ValueError:
                parsed = None
            if parsed is not None and parsed.hostname:
                records.append((parsed.hostname, date_text, year, site_line))
        site = None
        site_line = 0
        date_text = None
        year = None

    for line_number, line in enumerate(text.splitlines(), 1):
        site_match = _FTP_SITELIST_SITE_RE.match(line)
        if site_match is not None:
            flush()
            site = site_match.group(1).strip().rstrip(".").lower()
            site_line = line_number
            continue
        if site is None:
            continue
        date_match = _FTP_SITELIST_DATE_RE.match(line)
        if date_match is None:
            continue
        raw_date = date_match.group(1)
        parsed_date = None
        for fmt in ("%d-%b-%y", "%d-%b-%Y"):
            try:
                parsed_date = datetime.strptime(raw_date, fmt)
                break
            except ValueError:
                continue
        if parsed_date is not None:
            date_text = raw_date
            year = parsed_date.year
    flush()
    return tuple(records)


def parse_ftp_sitelist_zip(
    payload: bytes,
    *,
    max_entries: int = 128,
    max_uncompressed_bytes: int = 16 * 1024 * 1024,
) -> tuple[tuple[str, int, str, int, str], ...]:
    """Safely parse an in-memory ftp-list.zip package.

    Returns (member, site_line, hostname, year, date_text) tuples without
    extracting archive paths to disk.
    """
    if max_entries < 1 or max_uncompressed_bytes < 1:
        raise ValueError("zip parser limits must be positive")
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise ValueError("invalid ftp sitelist zip") from exc
    with archive:
        infos = [item for item in archive.infolist() if not item.is_dir()]
        if len(infos) > max_entries:
            raise ValueError("ftp sitelist zip has too many members")
        total = sum(item.file_size for item in infos)
        if total > max_uncompressed_bytes:
            raise ValueError("ftp sitelist zip exceeds decompressed budget")
        result: list[tuple[str, int, str, int, str]] = []
        for info in infos:
            if info.flag_bits & 0x1:
                raise ValueError("encrypted ftp sitelist zip member")
            if info.file_size > max_uncompressed_bytes:
                raise ValueError("ftp sitelist member exceeds decompressed budget")
            raw = archive.read(info)
            text = raw.decode("utf-8", errors="replace")
            for hostname, date_text, record_year, site_line in parse_ftp_sitelist_records(text):
                result.append(
                    (info.filename, site_line, hostname, record_year, date_text)
                )
        return tuple(result)

def is_dmoz_content_locator(locator: str) -> bool:
    """Recognize DMOZ/ODP content dumps without claiming generic RDF files."""
    path = urlsplit(locator).path.lower().rstrip("/")
    if not path:
        return False
    return path.rsplit("/", 1)[-1] in _DMOZ_CONTENT_NAMES


def parse_dmoz_external_page_line(line: str) -> str | None:
    """Return one external HTTP(S) URL from a DMOZ ExternalPage row.

    DMOZ content dumps also contain category links and RDF namespace URLs.
    Restricting extraction to ExternalPage about=... avoids treating RDF
    metadata as candidates and avoids the common duplicate representation of
    one site as both a category link and an ExternalPage description.
    """
    match = _DMOZ_EXTERNAL_PAGE_RE.search(line)
    if match is None:
        return None
    value = html.unescape(match.group(1).strip())
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    if parsed.scheme.lower() not in {"http", "https"} or parsed.hostname is None:
        return None
    return value


def is_squid_access_locator(locator: str) -> bool:
    """Recognize explicit Squid access-log resources without matching generic logs."""
    path = urlsplit(locator).path.lower()
    name = path.rsplit("/", 1)[-1]
    if any(marker in path for marker in _SQUID_PATH_MARKERS):
        return not path.endswith("/")
    if _IRCACHE_SANITIZED_ACCESS_RE.fullmatch(name):
        return True
    return name.endswith(
        (
            ".squid",
            ".squid.gz",
            ".squid.log",
            ".squid.log.gz",
            ".access.log",
            ".access.log.gz",
        )
    )


def parse_squid_access_line(line: str) -> tuple[str, int | None] | None:
    """Return (URL, access-year) from one native Squid access line.

    Standard Squid native logs begin with an epoch timestamp and contain the
    requested URL later in the row.  We intentionally ignore client identity,
    usernames, peer fields and all remaining metadata.
    """
    fields = line.split()
    if len(fields) < 2:
        return None

    try:
        stamp = float(fields[0])
        if stamp < 0:
            return None
        year = datetime.fromtimestamp(
            stamp,
            tz=timezone.utc,
        ).year
    except (ValueError, OverflowError, OSError):
        return None
    if not 1996 <= year <= 2001:
        # A parseable off-window access is not an undated target-period URL.
        return None

    url: str | None = None
    for field in fields[1:]:
        if field.startswith(("http://", "https://")):
            urls = extract_http_urls(field)
            if urls:
                url = urls[0]
                break
    if url is None:
        return None
    return url, year
