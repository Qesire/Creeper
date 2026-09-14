"""Parser for the monthly Select BBSes on the Internet (SBI) guide.

The SBI quick list is a contemporaneous monthly directory of Internet-reachable
BBS systems. Direct annual authority is based only on the edition date embedded
inside the quick-list member (for example SBIQ0197.LST rev date 01/01/97) and
the hostname/address listed in that same edition. ZIP mtimes and mirror metadata
never create evidence.
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


AUDITED_SBI_BBS_LOCATORS = frozenset(
    {
        "https://files.mpoli.fi/software/TEXTS/MISC/SBI0197.ZIP",
        (
            "https://ftp.zx.net.nz/pub/mirror/files.mpoli.fi/pub/software/"
            "TEXTS/MISC/SBI0197.ZIP"
        ),
    }
)

_SBI_ZIP_RE = re.compile(r"^SBI(?P<month>\d{2})(?P<year>\d{2})\.ZIP$", re.IGNORECASE)
_SBI_QUICK_RE = re.compile(
    r"^SBIQ(?P<month>\d{2})(?P<year>\d{2})\.LST$",
    re.IGNORECASE,
)
_REV_DATE_RE = re.compile(
    r"SBIQ(?P<month>\d{2})(?P<year>\d{2})\.LST"
    r"\s*\(\s*rev\s+date\s*:\s*"
    r"(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s*\)",
    re.IGNORECASE,
)
_LIST_HEADER_RE = re.compile(
    r"^\s*System\s+Name\s+Telnet/Client\s+Address\s*$",
    re.IGNORECASE,
)
_LIST_END_RE = re.compile(r"^\s*TOTAL\s+SYSTEMS\s+LISTED\s*:", re.IGNORECASE)
_HTTP_URL_RE = re.compile(r"https?://[^\s<>\[\]{}\"']+", re.IGNORECASE)
_HOST_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", re.IGNORECASE)


@dataclass(frozen=True)
class SbiBbsRecord:
    hostname: str
    source_time: str
    year: int
    member_name: str
    record_index: int

    def __post_init__(self) -> None:
        if not isinstance(self.hostname, str) or not self.hostname.strip():
            raise ValueError("hostname is required")
        if not isinstance(self.source_time, str) or not self.source_time.strip():
            raise ValueError("source_time is required")
        if isinstance(self.year, bool) or not isinstance(self.year, int):
            raise ValueError("year must be an integer")
        if not isinstance(self.member_name, str) or not self.member_name.strip():
            raise ValueError("member_name is required")
        if (
            isinstance(self.record_index, bool)
            or not isinstance(self.record_index, int)
            or self.record_index < 0
        ):
            raise ValueError("record_index must be a non-negative integer")


def is_sbi_bbs_locator(locator: str) -> bool:
    name = PurePosixPath(urlsplit(locator).path).name
    return _SBI_ZIP_RE.fullmatch(name) is not None


def is_audited_sbi_bbs_locator(locator: str) -> bool:
    return locator in AUDITED_SBI_BBS_LOCATORS


def _edition_date(text: str, *, member_name: str) -> datetime | None:
    member = PurePosixPath(member_name).name
    member_match = _SBI_QUICK_RE.fullmatch(member)
    if member_match is None:
        return None

    match = _REV_DATE_RE.search(text)
    if match is None:
        return None
    if (
        match.group("month") != member_match.group("month")
        or match.group("year") != member_match.group("year")
    ):
        return None

    raw_date = match.group("date")
    parsed = None
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(raw_date, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None

    expected_month = int(member_match.group("month"))
    expected_year = 1900 + int(member_match.group("year"))
    if parsed.month != expected_month or parsed.year != expected_year:
        return None
    if not 1996 <= parsed.year <= 2001:
        return None
    return parsed


def _strict_hostname(value: str) -> str | None:
    raw = value.strip().rstrip(".")
    if not raw or "@" in raw:
        return None
    try:
        ipaddress.ip_address(raw)
    except ValueError:
        pass
    else:
        return None
    labels = raw.split(".")
    if len(labels) < 2 or len(labels[-1]) < 2:
        return None
    if any(_HOST_LABEL_RE.fullmatch(label) is None for label in labels):
        return None
    return normalize_official(raw)


def _hostname_from_address_line(line: str) -> str | None:
    url_match = _HTTP_URL_RE.search(line)
    if url_match is not None:
        try:
            parsed = urlsplit(url_match.group(0).rstrip(".,;:!?)]}>"))
        except ValueError:
            return None
        raw = parsed.hostname or ""
    else:
        raw = ""
        for token in reversed(line.split()):
            candidate = token.strip("()[]{}<>,;")
            if not candidate or candidate.lower() == "n/a":
                continue
            if candidate.isdigit():
                continue
            if "." not in candidate:
                continue
            if ":" in candidate:
                try:
                    parsed = urlsplit("//" + candidate)
                except ValueError:
                    continue
                candidate = parsed.hostname or ""
            candidate = candidate.rstrip(".")
            if not candidate:
                continue
            raw = candidate
            break

    if not raw:
        return None
    return _strict_hostname(raw)


def parse_sbi_quick_list_text(
    text: str,
    *,
    member_name: str,
) -> tuple[SbiBbsRecord, ...]:
    """Extract hostnames from one internally dated SBI quick-list member."""

    edition = _edition_date(text, member_name=member_name)
    if edition is None:
        return ()

    in_list = False
    saw_total = False
    saw_end = False
    rows: list[SbiBbsRecord] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        if not in_list:
            if _LIST_HEADER_RE.match(raw_line):
                in_list = True
            continue
        if _LIST_END_RE.match(raw_line):
            saw_total = True
            continue
        if raw_line.strip() == "[END OF LIST]":
            saw_end = True
            break
        stripped = raw_line.strip()
        if not stripped or set(stripped) <= {"-", "="}:
            continue
        hostname = _hostname_from_address_line(stripped)
        if hostname is None or hostname in seen:
            continue
        seen.add(hostname)
        rows.append(
            SbiBbsRecord(
                hostname=hostname,
                source_time=edition.strftime("%Y-%m-%d"),
                year=edition.year,
                member_name=member_name,
                record_index=len(rows),
            )
        )
    if not (in_list and saw_total and saw_end):
        return ()
    return tuple(rows)


def parse_sbi_bbs_zip(
    payload: bytes,
    *,
    max_decompressed_bytes: int = 32 * 1024 * 1024,
) -> tuple[SbiBbsRecord, ...]:
    """Extract dated hostname records from a complete bounded SBI ZIP."""

    if (
        isinstance(max_decompressed_bytes, bool)
        or not isinstance(max_decompressed_bytes, int)
        or max_decompressed_bytes < 1
    ):
        raise ValueError("max_decompressed_bytes must be a positive integer")
    if not isinstance(payload, bytes):
        raise ValueError("payload must be bytes")

    try:
        archive = zipfile.ZipFile(io.BytesIO(payload), mode="r")
    except (zipfile.BadZipFile, OSError) as exc:
        raise ValueError("invalid SBI BBS ZIP artifact") from exc

    total_uncompressed = 0
    result: list[SbiBbsRecord] = []
    seen: set[tuple[str, str]] = set()
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            if info.flag_bits & 0x1:
                raise ValueError("encrypted SBI ZIP member is unsupported")
            total_uncompressed += int(info.file_size)
            if total_uncompressed > max_decompressed_bytes:
                raise ValueError("SBI BBS ZIP exceeds decompressed byte budget")

            member_name = PurePosixPath(info.filename).name
            if _SBI_QUICK_RE.fullmatch(member_name) is None:
                continue
            raw = archive.read(info)
            text = raw.decode("utf-8", errors="replace")
            for record in parse_sbi_quick_list_text(
                text,
                member_name=member_name,
            ):
                identity = (record.hostname, record.source_time)
                if identity in seen:
                    continue
                seen.add(identity)
                result.append(
                    SbiBbsRecord(
                        hostname=record.hostname,
                        source_time=record.source_time,
                        year=record.year,
                        member_name=record.member_name,
                        record_index=len(result),
                    )
                )
    if not result:
        raise ValueError("SBI ZIP contains no internally dated quick-list records")
    return tuple(result)
