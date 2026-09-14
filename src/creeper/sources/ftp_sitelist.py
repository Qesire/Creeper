"""Parser for the maintained 1990s Anonymous FTP Sitelist.

The Perry Rovers FTP Sitelist binds each ``Site`` record to a ``Date`` field
defined by its FAQ as the date of last modification for that site. Creeper
only accepts those record-level fields; ZIP filename dates, HTTP metadata,
and mirror posting dates never create annual evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import io
import ipaddress
from pathlib import PurePosixPath
import re
from urllib.parse import urlsplit
import zipfile

from creeper.authority.normalizer import normalize_official


AUDITED_FTP_SITELIST_LOCATORS = frozenset(
    {
        (
            "https://ftpmirror1.infania.net/pub/simtelnet/msdos/info/"
            "ftp-list.zip"
        ),
    }
)

_SITE_RE = re.compile(r"^Site\s*:\s*(.*?)\s*$", re.IGNORECASE)
_DATE_RE = re.compile(r"^Date\s*:\s*(.*?)\s*$", re.IGNORECASE)

@dataclass(frozen=True)
class FtpSitelistRecord:
    hostname: str
    source_time: str
    year: int
    member_name: str
    record_index: int


def is_ftp_sitelist_locator(locator: str) -> bool:
    path = urlsplit(locator).path
    return PurePosixPath(path).name.lower() == "ftp-list.zip"


def is_audited_ftp_sitelist_locator(locator: str) -> bool:
    return locator in AUDITED_FTP_SITELIST_LOCATORS


def _parse_site_date(
    raw_site: str,
    raw_date: str,
) -> tuple[str, str, int] | None:
    site = raw_site.strip()
    if not site:
        return None
    if "://" in site:
        parsed = urlsplit(site)
        site = parsed.hostname or ""
    try:
        ipaddress.ip_address(site)
    except ValueError:
        pass
    else:
        return None
    hostname = normalize_official(site)
    if hostname is None:
        return None

    parsed_date = None
    for fmt in ("%d-%b-%y", "%d-%b-%Y"):
        try:
            parsed_date = datetime.strptime(raw_date.strip(), fmt)
            break
        except ValueError:
            continue
    if parsed_date is None:
        return None
    return hostname, parsed_date.strftime("%Y-%m-%d"), parsed_date.year


def parse_ftp_sitelist_text(
    text: str,
    *,
    member_name: str = "",
) -> tuple[FtpSitelistRecord, ...]:
    """Parse exact ``Site`` + ``Date`` pairs from one sitelist text member."""
    rows: list[FtpSitelistRecord] = []
    current_site: str | None = None
    current_date: str | None = None

    def flush() -> None:
        nonlocal current_site, current_date
        if current_site is None or current_date is None:
            current_site = None
            current_date = None
            return
        parsed = _parse_site_date(current_site, current_date)
        if parsed is not None:
            hostname, source_time, year = parsed
            rows.append(
                FtpSitelistRecord(
                    hostname=hostname,
                    source_time=source_time,
                    year=year,
                    member_name=member_name,
                    record_index=len(rows),
                )
            )
        current_site = None
        current_date = None

    for raw_line in text.splitlines():
        site_match = _SITE_RE.match(raw_line)
        if site_match is not None:
            flush()
            current_site = site_match.group(1)
            continue
        if current_site is None:
            continue
        date_match = _DATE_RE.match(raw_line)
        if date_match is not None:
            current_date = date_match.group(1)
    flush()
    return tuple(rows)


def parse_ftp_sitelist_zip(
    payload: bytes,
    *,
    max_decompressed_bytes: int = 32 * 1024 * 1024,
) -> tuple[FtpSitelistRecord, ...]:
    """Extract dated site records from one complete, bounded ZIP artifact."""
    if max_decompressed_bytes < 1:
        raise ValueError("max_decompressed_bytes must be positive")

    total_uncompressed = 0
    result: list[FtpSitelistRecord] = []
    seen: set[tuple[str, str]] = set()
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload), mode="r")
    except (zipfile.BadZipFile, OSError) as exc:
        raise ValueError("invalid FTP sitelist ZIP artifact") from exc

    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            if info.flag_bits & 0x1:
                raise ValueError("encrypted FTP sitelist ZIP member is unsupported")
            total_uncompressed += int(info.file_size)
            if total_uncompressed > max_decompressed_bytes:
                raise ValueError("FTP sitelist ZIP exceeds decompressed byte budget")
            raw = archive.read(info)
            text = raw.decode("utf-8", errors="replace")
            for record in parse_ftp_sitelist_text(
                text,
                member_name=info.filename,
            ):
                identity = (record.hostname, record.source_time)
                if identity in seen:
                    continue
                seen.add(identity)
                result.append(
                    FtpSitelistRecord(
                        hostname=record.hostname,
                        source_time=record.source_time,
                        year=record.year,
                        member_name=record.member_name,
                        record_index=len(result),
                    )
                )
    return tuple(result)
