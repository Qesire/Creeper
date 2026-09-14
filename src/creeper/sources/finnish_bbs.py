"""Parser for maintained Finnish 24h BBS directory editions.

The LAHO-maintained list embeds an edition date ("Tilanne") inside each text
artifact and lists Internet-reachable BBS hostnames in the same edition.
Creeper accepts only conservative address shapes from the main BBS table:
explicit telnet/http/https/ftp/www addresses and bare hostnames on a primary
BBS row that also contains a telephone field. Mirror/ZIP mtimes never create
annual evidence.
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


AUDITED_FINNISH_BBS_LOCATORS = frozenset(
    {
        "https://files.mpoli.fi/software/TEXTS/MISC/FI980225.ZIP",
        "https://files.mpoli.fi/software/TEXTS/MISC/030698.ZIP",
    }
)

_FINNISH_ZIP_NAMES = frozenset({"fi980225.zip", "030698.zip"})
_BANNER = "Elektroniset 24h postilaatikot Suomessa"
_EDITION_RE = re.compile(
    r"^\s*Tilanne\s*:\s*(?P<day>\d{1,2})\.(?P<month>\d{1,2})\."
    r"(?P<year>\d{4})\s*$",
    re.IGNORECASE,
)
_TABLE_HEADER_RE = re.compile(
    r"^\s*nimi/softa\s+numero\s+modeemi\(t\)\s+net/node\s*/\s*sysop\s*$",
    re.IGNORECASE,
)
_APPENDIX_RE = re.compile(r"^\s*Net-osoitteet\s*:\s*$", re.IGNORECASE)
_DIVIDER_RE = re.compile(r"^\s*-{20,}\s*$")
_PHONE_RE = re.compile(
    r"(?<!\d)(?:0\d{1,3}|9[67]00|1063|0600)[- ]\d[\d -]{2,}"
)
_PROTOCOL_RE = re.compile(
    r"(?ix)\b(?:telnet|https?|ftp|www)\s*"
    r"(?:(?:://)|:)\s*"
    r"(?P<host>[a-z0-9][a-z0-9.-]*[a-z0-9])"
    r"(?::\d+)?"
)
_URL_RE = re.compile(r"(?ix)\b(?:https?|ftp|telnet)://[^\s<>\[\]{}\"']+")
_HOST_TOKEN_RE = re.compile(
    r"(?i)(?<![@a-z0-9-])"
    r"(?P<host>[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)"
    r"(?=$|[\s,;)])"
)
_HOST_LABEL_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class FinnishBbsRecord:
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


def is_finnish_bbs_locator(locator: str) -> bool:
    name = PurePosixPath(urlsplit(locator).path).name.lower()
    return name in _FINNISH_ZIP_NAMES


def is_audited_finnish_bbs_locator(locator: str) -> bool:
    return locator in AUDITED_FINNISH_BBS_LOCATORS


def _strict_hostname(value: str) -> str | None:
    raw = value.strip().strip("()[]{}<>,;").rstrip(".")
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


def _edition_date(text: str) -> datetime | None:
    if _BANNER.lower() not in text.lower():
        return None
    for line in text.splitlines()[:80]:
        match = _EDITION_RE.match(line)
        if match is None:
            continue
        try:
            parsed = datetime(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            )
        except ValueError:
            return None
        if not 1996 <= parsed.year <= 2001:
            return None
        return parsed
    return None


def _explicit_hosts(line: str) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()

    for match in _URL_RE.finditer(line):
        try:
            parsed = urlsplit(match.group(0).rstrip(".,;:!?)]}>"))
        except ValueError:
            continue
        hostname = _strict_hostname(parsed.hostname or "")
        if hostname is not None and hostname not in seen:
            seen.add(hostname)
            result.append(hostname)

    for match in _PROTOCOL_RE.finditer(line):
        hostname = _strict_hostname(match.group("host"))
        if hostname is not None and hostname not in seen:
            seen.add(hostname)
            result.append(hostname)

    return tuple(result)


def _primary_row_hosts(line: str) -> tuple[str, ...]:
    if _PHONE_RE.search(line) is None or "@" in line:
        return ()
    result: list[str] = []
    seen: set[str] = set()
    for match in _HOST_TOKEN_RE.finditer(line):
        hostname = _strict_hostname(match.group("host"))
        if hostname is not None and hostname not in seen:
            seen.add(hostname)
            result.append(hostname)
    return tuple(result)


def parse_finnish_bbs_text(
    text: str,
    *,
    member_name: str,
) -> tuple[FinnishBbsRecord, ...]:
    """Extract conservative Internet host observations from one complete edition."""

    edition = _edition_date(text)
    if edition is None:
        return ()

    lines = text.splitlines()
    table_start = None
    appendix = None
    for index, line in enumerate(lines):
        if table_start is None and _TABLE_HEADER_RE.match(line):
            table_start = index + 1
            continue
        if table_start is not None and _APPENDIX_RE.match(line):
            appendix = index
            break
    if table_start is None or appendix is None or appendix <= table_start:
        return ()

    rows: list[FinnishBbsRecord] = []
    seen: set[str] = set()
    for line in lines[table_start:appendix]:
        if not line.strip() or _DIVIDER_RE.match(line):
            continue
        candidates = list(_explicit_hosts(line))
        candidates.extend(_primary_row_hosts(line))
        for hostname in candidates:
            if hostname in seen:
                continue
            seen.add(hostname)
            rows.append(
                FinnishBbsRecord(
                    hostname=hostname,
                    source_time=edition.strftime("%Y-%m-%d"),
                    year=edition.year,
                    member_name=member_name,
                    record_index=len(rows),
                )
            )
    return tuple(rows)


def parse_finnish_bbs_zip(
    payload: bytes,
    *,
    max_decompressed_bytes: int = 32 * 1024 * 1024,
) -> tuple[FinnishBbsRecord, ...]:
    """Extract dated hostname records from one complete bounded Finnish BBS ZIP."""

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
        raise ValueError("invalid Finnish BBS ZIP artifact") from exc

    total_uncompressed = 0
    result: list[FinnishBbsRecord] = []
    seen: set[tuple[str, str]] = set()
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            if info.flag_bits & 0x1:
                raise ValueError("encrypted Finnish BBS ZIP member is unsupported")
            total_uncompressed += int(info.file_size)
            if total_uncompressed > max_decompressed_bytes:
                raise ValueError("Finnish BBS ZIP exceeds decompressed byte budget")

            member_name = PurePosixPath(info.filename).name
            if not member_name.lower().endswith((".txt", ".lst")):
                continue
            raw = archive.read(info)
            # Original artifacts are CP437-era DOS text; latin-1 fallback would
            # preserve bytes but corrupt headings needed for semantic checks.
            text = raw.decode("cp437", errors="replace")
            for record in parse_finnish_bbs_text(text, member_name=member_name):
                identity = (record.hostname, record.source_time)
                if identity in seen:
                    continue
                seen.add(identity)
                result.append(
                    FinnishBbsRecord(
                        hostname=record.hostname,
                        source_time=record.source_time,
                        year=record.year,
                        member_name=record.member_name,
                        record_index=len(result),
                    )
                )

    if not result:
        raise ValueError(
            "Finnish BBS ZIP contains no internally dated Internet host records"
        )
    return tuple(result)
