"""Deterministic parsers for non-snapshot historical URL sources.

These helpers deliberately extract only HTTP(S) URLs/hostnames.  Mailbox
authors, addresses, message text, client IPs, and other personal fields never
cross into Creeper SourceRecord payloads.
"""

from __future__ import annotations

from datetime import datetime, timezone
from email import policy as email_policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
import html
import re
from urllib.parse import urlsplit


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


def iter_mbox_messages(
    payload: bytes,
    *,
    include_truncated_tail: bool = True,
):
    """Yield complete Unix-mbox messages from one bounded byte buffer.

    A mbox message begins at a line whose first five bytes are b"From ".
    Quoted body lines use >From and therefore do not create boundaries.
    When a network sample is known to end mid-file, callers can set
    include_truncated_tail=False so the final partial message is ignored.
    """
    if not payload:
        return
    starts = [
        match.start()
        for match in re.finditer(br"(?m)^From ", payload)
    ]
    if not starts:
        return
    for index, start in enumerate(starts):
        if index + 1 < len(starts):
            end = starts[index + 1]
        else:
            if not include_truncated_tail:
                break
            end = len(payload)
        if end > start:
            yield start, payload[start:end]


def parse_mbox_message(
    message: bytes,
    *,
    max_urls: int = 64,
) -> tuple[tuple[str, ...], int | None, str | None]:
    """Extract body URLs and the message-level observation timestamp.

    Only text message bodies are inspected; envelope/header addresses and
    attachment payloads are excluded. The returned timestamp is normalized
    to UTC ISO-8601. A missing or malformed Date header leaves the URLs
    usable for discovery but grants no direct annual evidence.
    """
    if max_urls < 1:
        raise ValueError("max_urls must be positive")
    try:
        parsed = BytesParser(policy=email_policy.default).parsebytes(message)
    except (TypeError, ValueError):
        return (), None, None

    raw_date = parsed.get("Date")
    year: int | None = None
    source_time: str | None = None
    if raw_date is not None:
        try:
            observed_at = parsedate_to_datetime(str(raw_date))
        except (TypeError, ValueError, OverflowError):
            observed_at = None
        if observed_at is not None:
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
            observed_at = observed_at.astimezone(timezone.utc)
            if 1996 <= observed_at.year <= 2001:
                year = observed_at.year
                source_time = observed_at.isoformat()

    bodies: list[str] = []
    parts = parsed.walk() if parsed.is_multipart() else (parsed,)
    for part in parts:
        if part.is_multipart():
            continue
        if part.get_content_maintype() != "text":
            continue
        disposition = (part.get_content_disposition() or "").lower()
        if disposition == "attachment":
            continue
        raw = part.get_payload(decode=True)
        if raw is None:
            value = part.get_payload()
            if isinstance(value, str):
                bodies.append(value)
            continue
        charset = part.get_content_charset() or "utf-8"
        try:
            bodies.append(raw.decode(charset, errors="replace"))
        except LookupError:
            bodies.append(raw.decode("utf-8", errors="replace"))

    urls = extract_http_urls(
        html.unescape("\n".join(bodies)),
        max_urls=max_urls,
    )
    return urls, year, source_time

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
