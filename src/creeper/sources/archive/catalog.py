"""Bounded parsing and selection of Arquivo.pt's public CDXJ directory."""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urljoin, urlsplit


@dataclass(frozen=True)
class CdxjCatalogEntry:
    name: str
    url: str
    size_bytes: int
    size_text: str


_LINK_RE = re.compile(
    r'<a\b[^>]*\bhref=["\']([^"\']+\.cdxj)["\'][^>]*>(.*?)</a>(.*?)(?=<a\b|</tr>|$)',
    flags=re.IGNORECASE | re.DOTALL,
)
_SIZE_RE = re.compile(r"(?<![\w.])(\d+(?:\.\d+)?)\s*([KMGT]?)\s*(?:B)?(?![\w])", re.IGNORECASE)
_MULTIPLIERS = {"": 1, "K": 1_000, "M": 1_000_000, "G": 1_000_000_000, "T": 1_000_000_000_000}


def _parse_size(text: str) -> tuple[int, str] | None:
    matches = _SIZE_RE.findall(text)
    if not matches:
        return None
    number, unit = matches[-1]
    unit = unit.upper()
    return round(float(number) * _MULTIPLIERS[unit]), f"{number}{unit}"


def parse_cdxj_catalog(html: str, *, base_url: str) -> list[CdxjCatalogEntry]:
    """Parse only same-directory ``.cdxj`` links with a usable size."""
    base = urlsplit(base_url)
    entries: list[CdxjCatalogEntry] = []
    for match in _LINK_RE.finditer(html):
        href, raw_name, tail = match.groups()
        url = urljoin(base_url, href)
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or parsed.netloc != base.netloc:
            continue
        if parsed.path.rsplit("/", 1)[0] != base.path.rstrip("/"):
            continue
        parsed_size = _parse_size(tail)
        if parsed_size is None:
            continue
        size_bytes, size_text = parsed_size
        name = re.sub(r"<[^>]+>", "", raw_name).strip()
        entries.append(CdxjCatalogEntry(name, url, size_bytes, size_text))
    return entries


def select_bounded_entries(
    entries: list[CdxjCatalogEntry], *, max_file_bytes: int, max_files: int
) -> list[CdxjCatalogEntry]:
    if max_file_bytes < 1 or max_files < 1:
        raise ValueError("max_file_bytes and max_files must be positive")
    return sorted(
        (entry for entry in entries if 0 < entry.size_bytes <= max_file_bytes),
        key=lambda entry: (entry.size_bytes, entry.name),
    )[:max_files]
