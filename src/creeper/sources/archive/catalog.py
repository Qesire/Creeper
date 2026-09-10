"""Bounded parsing and selection of Arquivo.pt's public CDXJ directory."""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urljoin, urlsplit

from selectolax.lexbor import LexborHTMLParser, LexborNode


@dataclass(frozen=True)
class CdxjCatalogEntry:
    name: str
    url: str
    size_bytes: int
    size_text: str


_SIZE_RE = re.compile(
    r"(?<![\w.])(\d+(?:\.\d+)?)\s*([KMGT]?)\s*(?:B)?(?![\w])",
    re.IGNORECASE,
)
_MULTIPLIERS = {
    "": 1,
    "K": 1_000,
    "M": 1_000_000,
    "G": 1_000_000_000,
    "T": 1_000_000_000_000,
}


def _parse_size(text: str) -> tuple[int, str] | None:
    matches = _SIZE_RE.findall(text)
    if not matches:
        return None
    number, unit = matches[-1]
    unit = unit.upper()
    return round(float(number) * _MULTIPLIERS[unit]), f"{number}{unit}"


def _nearest_row(node: LexborNode) -> LexborNode | None:
    """Return the nearest table row without walking outside the link container."""
    current = node.parent
    for _ in range(8):
        if current is None:
            return None
        if current.tag == "tr":
            return current
        if current.tag in {"table", "body", "html"}:
            return None
        current = current.parent
    return None


def _following_listing_text(anchor: LexborNode) -> str:
    """Collect the local text that follows one link in Apache-style listings.

    Lexbor exposes text nodes in the sibling chain. Stopping at the next anchor
    or a row boundary prevents one file from accidentally inheriting another
    file's size when the listing is wrapped in a single ``<pre>`` element.
    """
    parts: list[str] = []
    current = anchor.next
    for _ in range(16):
        if current is None:
            break
        tag = current.tag
        if tag == "a":
            break
        if tag in {"tr", "table"}:
            break
        if tag == "-text":
            value = current.text_content
            if value:
                parts.append(value)
        elif tag == "br":
            break
        else:
            text = current.text(deep=True, separator=" ", strip=True)
            if text:
                parts.append(text)
        current = current.next
    return " ".join(parts)


def _size_context(anchor: LexborNode) -> str:
    row = _nearest_row(anchor)
    if row is not None:
        return row.text(deep=True, separator=" ", strip=True)
    return _following_listing_text(anchor)


def parse_cdxj_catalog(html: str, *, base_url: str) -> list[CdxjCatalogEntry]:
    """Parse same-directory ``.cdxj`` links with a usable adjacent size.

    HTML tree construction and malformed-markup recovery are delegated to the
    Lexbor HTML5 parser. Creeper retains only catalog-specific URL and size
    policy so directory markup changes do not become handwritten HTML parsing.
    """
    base = urlsplit(base_url)
    base_directory = base.path.rstrip("/")
    tree = LexborHTMLParser(html)
    entries: list[CdxjCatalogEntry] = []

    for anchor in tree.css("a[href]"):
        href = anchor.attributes.get("href")
        if not href:
            continue
        url = urljoin(base_url, href)
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or parsed.netloc != base.netloc:
            continue
        if not parsed.path.lower().endswith(".cdxj"):
            continue
        if parsed.path.rsplit("/", 1)[0] != base_directory:
            continue

        parsed_size = _parse_size(_size_context(anchor))
        if parsed_size is None:
            continue
        size_bytes, size_text = parsed_size
        name = anchor.text(deep=True, separator=" ", strip=True) or parsed.path.rsplit("/", 1)[-1]
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
