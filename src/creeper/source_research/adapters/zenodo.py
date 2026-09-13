"""Zenodo REST root with bounded deterministic pagination and direct file leads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlsplit

from .base import (
    ArtifactLead,
    JsonTransport,
    RootCapabilityReport,
    RootQuery,
    SearchCheckpoint,
    SearchHit,
    SearchPage,
    checkpoint_with_page_budget,
    ensure_success,
    is_retryable,
    merge_native_filters,
    normalize_doi,
    response_bytes_len,
    response_json,
    retry_delay_seconds,
    safe_next_url,
    status_code,
)

API = "https://zenodo.org/api/records/"
_RESERVED_FILTERS = frozenset({"q", "page", "size", "all_versions"})
_AU_SUFFIXES = ("-auk.tar.gz", "-parquet.tar.gz")


def recognize_archives_unleashed(
    title: str,
    filenames: Iterable[str],
    schema_text: str = "",
) -> bool:
    """Return scheduling knowledge only; this is never evidence authority."""

    title_text = str(title).casefold()
    names = tuple(str(name).casefold() for name in filenames)
    schema = str(schema_text).casefold()
    derivative_file = any(name.endswith(_AU_SUFFIXES) for name in names)
    schema_signature = all(token in schema for token in ("crawl_date", "src", "dest", "anchor"))
    explicit_title = "web archive collection derivatives" in title_text
    return derivative_file and (explicit_title or schema_signature)


class ZenodoAdapter:
    root_id = "zenodo"

    def __init__(
        self,
        *,
        transport: JsonTransport,
        endpoint: str = API,
        token: str | None = None,
    ) -> None:
        self.transport = transport
        self.endpoint = endpoint if endpoint.endswith("/") else endpoint + "/"
        self.token = token
        parsed = urlsplit(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Zenodo endpoint must be an absolute http(s) URL")

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def probe_capabilities(self) -> RootCapabilityReport:
        try:
            response = await self.transport(
                self.endpoint,
                {"page": 1, "size": 1, "all_versions": "true"},
                self._headers(),
            )
        except Exception as exc:
            return RootCapabilityReport(self.root_id, False, reason=str(exc))
        code = status_code(response)
        if is_retryable(response):
            return RootCapabilityReport(
                self.root_id,
                False,
                reason=f"retryable_status:{code}",
                status_code=code,
            )
        try:
            ensure_success(response, url=self.endpoint)
            response_json(response)
        except Exception as exc:
            return RootCapabilityReport(self.root_id, False, reason=str(exc), status_code=code)
        return RootCapabilityReport(
            self.root_id,
            True,
            ("records", "files", "versions", "concept_identity"),
            status_code=code,
        )

    async def search(
        self,
        query: RootQuery,
        checkpoint: SearchCheckpoint | None,
    ) -> SearchPage:
        cp = checkpoint or SearchCheckpoint(page=1)
        if cp.page > query.max_pages:
            return SearchPage(requests=0, terminal=True)

        url = safe_next_url(self.endpoint, cp.next_url) if cp.next_url else self.endpoint
        if cp.next_url:
            params: dict[str, Any] = {}
        else:
            page_size = min(query.page_size, 100 if self.token else 25)
            params = merge_native_filters(
                {
                    "q": query.query_text,
                    "page": cp.page,
                    "size": page_size,
                    "all_versions": "true",
                },
                query.native_filters,
                reserved=_RESERVED_FILTERS,
            )

        response = await self.transport(url, params, self._headers())
        if is_retryable(response):
            return SearchPage(
                next_checkpoint=cp,
                terminal=False,
                retry_after=retry_delay_seconds(response),
            )
        ensure_success(response, url=url)
        payload = response_json(response)
        hit_container = payload.get("hits") or {}
        if not isinstance(hit_container, Mapping):
            raise ValueError("Zenodo response hits must be an object")
        rows = hit_container.get("hits") or []
        if not isinstance(rows, list):
            raise ValueError("Zenodo response hits.hits must be a list")

        hits: list[SearchHit] = []
        leads: list[ArtifactLead] = []
        seen_records: set[str] = set()
        seen_leads: set[tuple[str, str]] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            record_id = str(row.get("id") or "").strip()
            if not record_id or record_id in seen_records:
                continue
            seen_records.add(record_id)
            metadata = _record_metadata(row)
            hit = SearchHit(
                root_id=self.root_id,
                query_id=query.query_id,
                provider_native_id=record_id,
                provider_url=_record_url(row, record_id),
                provider_type="RECORD",
                title=str(metadata.get("title") or ""),
                description=str(metadata.get("description") or ""),
                metadata=metadata,
            )
            hits.append(hit)
            for lead in _artifact_leads(self.root_id, hit):
                key = (lead.provider_native_id, lead.locator)
                if key not in seen_leads:
                    seen_leads.add(key)
                    leads.append(lead)

        next_url = _next_link(payload, self.endpoint)
        next_cp: SearchCheckpoint | None = None
        if next_url:
            next_cp = SearchCheckpoint(next_url=next_url, page=cp.page + 1)
        else:
            total = _total_value(hit_container.get("total"))
            page_size = min(query.page_size, 100 if self.token else 25)
            if rows and total is not None and cp.page * page_size < total:
                next_cp = SearchCheckpoint(page=cp.page + 1)
        next_cp = checkpoint_with_page_budget(query, current_page=cp.page, checkpoint=next_cp)

        return SearchPage(
            hits=tuple(hits),
            artifact_leads=tuple(leads),
            next_checkpoint=next_cp,
            terminal=next_cp is None,
            bytes_read=response_bytes_len(response, payload),
        )

    async def resolve(self, node: SearchHit) -> tuple[ArtifactLead, ...]:
        if node.root_id != self.root_id:
            raise ValueError("cannot resolve a node from another root")
        return _artifact_leads(self.root_id, node)


def _next_link(payload: Mapping[str, Any], endpoint: str) -> str | None:
    links = payload.get("links") or {}
    if not isinstance(links, Mapping):
        return None
    raw = links.get("next")
    if isinstance(raw, Mapping):
        raw = raw.get("href")
    return safe_next_url(endpoint, str(raw)) if raw else None


def _total_value(value: Any) -> int | None:
    if isinstance(value, Mapping):
        value = value.get("value")
    try:
        total = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, total)


def _record_url(row: Mapping[str, Any], record_id: str) -> str:
    links = row.get("links") or {}
    if isinstance(links, Mapping):
        for key in ("html", "self_html"):
            value = str(links.get(key) or "").strip()
            if value:
                return value
    return f"https://zenodo.org/records/{record_id}"


def _record_doi(row: Mapping[str, Any], metadata: Mapping[str, A[WJHOˆİˆ›Û™N‚ˆØ[™Y]\Îˆ\İĞ[WHHÛY]Y]K™Ù]
™ÚHŠK›İË™Ù]
™ÚHŠWBˆYÈH›İË™Ù]
œYÈŠHÜˆßBˆYˆ\Ú[œİ[˜ÙJYËX\[™ÊN‚ˆÚHHYË™Ù]
™ÚHŠHÜˆßBˆYˆ\Ú[œİ[˜ÙJÚKX\[™ÊN‚ˆØ[™Y]\Ë˜\[™
ÚK™Ù]
šY[YšY\ˆŠJBˆ›Üˆ˜[YH[ˆØ[™Y]\Î‚ˆ›Ü›X[^™YH›Ü›X[^™WÙÚJ˜[YJBˆYˆ›Ü›X[^™Y‚ˆ™]\›ˆ›Ü›X[^™Yˆ™]\›ˆ›Û™B‚‚™YˆØÛÛ˜Ù\ÙÚJ›İÎˆX\[™ÖÜİ‹[WKY]Y]NˆX\[™ÖÜİ‹[WJHOˆİˆ›Û™N‚ˆ›Üˆ˜[YH[ˆ
›İË™Ù]
˜ÛÛ˜Ù\ÚHŠKY]Y]K™Ù]
˜ÛÛ˜Ù\ÚHŠJN‚ˆ›Ü›X[^™YH›Ü›X[^™WÙÚJ˜[YJBˆYˆ›Ü›X[^™Y‚ˆ™]\›ˆ›Ü›X[^™Yˆ™]\›ˆ›Û™B‚‚™YˆÚ]\—Ùš[\Ê›İÎˆX\[™ÖÜİ‹[WJHOˆ\VÙXİÜİ‹[WK‹‹—N‚ˆ˜]ÈH›İË™Ù]
™š[\ÈŠHÜˆ×Bˆ˜[Y\Îˆ\İÓX\[™ÖÜİ‹[WWHH×BˆYˆ\Ú[œİ[˜ÙJ˜]Ë\İ
N‚ˆ˜[Y\ÈHİ˜[YH›Üˆ˜[YH[ˆ˜]ÈYˆ\Ú[œİ[˜ÙJ˜[YKX\[™ÊWBˆ[Yˆ\Ú[œİ[˜ÙJ˜]ËX\[™ÊN‚ˆ[šY\ÈH˜]Ë™Ù]
™[šY\ÈŠBˆYˆ\Ú[œİ[˜ÙJ[šY\ËX\[™ÊN‚ˆ›ÜˆÙ^K˜[YH[ˆ[šY\Ëš][\Ê
N‚ˆYˆ›İ\Ú[œİ[˜ÙJ˜[YKX\[™ÊN‚ˆÛÛ[YBˆ][HHXİ
˜[YJBˆ][KœÙ]Y˜][
šÙ^H‹Ù^JBˆ˜[Y\Ë˜\[™
][JBˆ[Yˆ\Ú[œİ[˜ÙJ[šY\Ë\İ
N‚ˆ˜[Y\ÈHİ˜[YH›Üˆ˜[YH[ˆ[šY\ÈYˆ\Ú[œİ[˜ÙJ˜[YKX\[™ÊWBˆ›Ü›X[^™Yˆ\İÙXİÜİ‹[WWHH×Bˆ›Üˆ˜[YH[ˆ˜[Y\Î‚ˆ[šÜÈH˜[YK™Ù]
›[šÜÈŠHÜˆßBˆØØ]ÜˆHˆ‚ˆYˆ\Ú[œİ[˜ÙJ[šÜËX\[™ÊN‚ˆ›ÜˆÙ^H[ˆ
˜ÛÛ[‹™İÛ›ØY‹œÙ[ˆŠN‚ˆØØ]ÜˆHİŠ[šÜË™Ù]
Ù^JHÜˆˆŠKœİš\

BˆYˆØØ]Ü‚ˆœ™XZÂˆYˆ›İØØ]Ü‚ˆØØ]ÜˆHİŠ˜[YK™Ù]
›[šÈŠHÜˆˆŠKœİš\

BˆÙ^HHİŠ˜[YK™Ù]
šÙ^HŠHÜˆ˜[YK™Ù]
™š[[˜[YHŠHÜˆ˜[YK™Ù]
šYŠHÜˆˆŠKœİš\

Bˆ›Ü›X[^™Y˜\[™
ˆÂˆšÙ^HˆÙ^Kˆ›ØØ]ÜˆˆØØ]Ü‹ˆœÚ^™HˆÚ[
˜[YK™Ù]
œÚ^™HŠHYˆ˜[YK™Ù]
œÚ^™HŠH\È›İ›Û™H[ÙH˜[YK™Ù]
™š[\Ú^™HŠJKˆ˜ÚXÚÜİ[HˆØÚXÚÜİ[J˜[YK™Ù]
˜ÚXÚÜİ[HŠJKˆ˜ÛÛ[İ\HˆİŠ˜[YK™Ù]
›Z[Y]\HŠHÜˆ˜[YK™Ù]
\HŠHÜˆˆŠKˆBˆ
Bˆ™]\›ˆ\J›Ü›X[^™Y
B‚‚™YˆØÚXÚÜİ[J˜[YNˆ[JHOˆİˆ›Û™N‚ˆYˆ\Ú[œİ[˜ÙJ˜[YKX\[™ÊN‚ˆ[ÛÜš]HHİŠ˜[YK™Ù]
˜[ÛÜš]HŠHÜˆˆŠKœİš\

BˆYÙ\İHİŠ˜[YK™Ù]
˜[YHŠHÜˆ˜[YK™Ù]
™YÙ\İŠHÜˆˆŠKœİš\

BˆYˆYÙ\İ‚ˆ™]\›ˆˆØ[ÛÜš]_NÙYÙ\İHˆYˆ[ÛÜš]H[ÙHYÙ\İˆ™]\›ˆ›Û™Bˆ^HİŠ˜[YHÜˆˆŠKœİš\

Bˆ™]\›ˆ^Üˆ›Û™B‚‚™YˆÜ™XÛÜ™ÛY]Y]J›İÎˆX\[™ÖÜİ‹[WJHOˆXİÜİ‹[WN‚ˆY]Y]HH›İË™Ù]
›Y]Y]HŠHÜˆßBˆYˆ›İ\Ú[œİ[˜ÙJY]Y]KX\[™ÊN‚ˆY]Y]HHßBˆš[\ÈHÚ]\—Ùš[\Ê›İÊBˆÚHHÜ™XÛÜ™ÙÚJ›İËY]Y]JBˆÛÛ˜Ù\ÙÚHHØÛÛ˜Ù\ÙÚJ›İËY]Y]JBˆÛÛ˜Ù\Ü™XÛÜ™ÚYHİŠ›İË™Ù]
˜ÛÛ˜Ù\™XÚYŠHÜˆ›İË™Ù]
˜ÛÛ˜Ù\ÚYŠHÜˆˆŠKœİš\

HÜˆ›Û™Bˆ]HHİŠY]Y]K™Ù]
]HŠHÜˆ›İË™Ù]
]HŠHÜˆˆŠBˆ\ØÜš\[ÛˆHİŠY]Y]K™Ù]
™\ØÜš\[ÛˆŠHÜˆ›İË™Ù]
™\ØÜš\[ÛˆŠHÜˆˆŠBˆš[ÜˆH™XÛÙÛš^™WØ\˜Ú]™\×İ[›X\ÚY
ˆ]KˆÜİŠš[K™Ù]
šÙ^HŠHÜˆˆŠH›Üˆš[H[ˆš[\×Kˆ\ØÜš\[Û‹ˆ
Bˆ™]\›ˆÂˆœ™XÛÜ™ÚYˆİŠ›İË™Ù]
šYŠHÜˆˆŠKˆ˜ÛÛ˜Ù\Ü™XÛÜ™ÚYˆÛÛ˜Ù\Ü™XÛÜ™ÚYˆ™ÚHˆÚKˆ˜ÛÛ˜Ù\ÙÚHˆÛÛ˜Ù\ÙÚKˆ™\œÚ[ÛˆˆY]Y]K™Ù]
™\œÚ[ÛˆŠHÜˆ›İË™Ù]
™\œÚ[ÛˆŠKˆ]Hˆ]Kˆ™\ØÜš\[Ûˆˆ\ØÜš\[Û‹ˆ˜Ü™X]ÜœÈˆY]Y]K™Ù]
˜Ü™X]ÜœÈŠHÜˆ×Kˆ™š[\Èˆš[\ËˆÈØÚY[[™ÈÛ›İÛYÙHÛ›NÈ™]™\ˆ]šY[˜ÙH]]Üš]K‚ˆ™˜[Z[WÜš[Üˆˆš[Ü‹ˆœØÚY[[™×Üš[ÜˆˆÈ˜\˜Ú]™\×İ[›X\ÚYˆš[ÜŸKˆB‚‚™YˆØ\Y˜XİÛXYÊ›ÛİÚYˆİ‹›ÙNˆÙX\˜Ú]
HOˆ\VĞ\Y˜XİXY‹‹—N‚ˆš[\ÈH›ÙK›Y]Y]K™Ù]
™š[\ÈŠHÜˆ

Bˆ™XÛÜ™ÙÚHH›Ü›X[^™WÙÚJ›ÙK›Y]Y]K™Ù]
™ÚHŠJHÜˆ›Û™BˆÛÛ˜Ù\ÙÚHH›Ü›X[^™WÙÚJ›ÙK›Y]Y]K™Ù]
˜ÛÛ˜Ù\ÙÚHŠJHÜˆ›Û™Bˆİ]]ˆ\İĞ\Y˜XİXYHH×BˆÙY[ˆÙ]İ\VÜİ‹İ—WHHÙ]

Bˆ›Üˆ[™^š[H[ˆ[[Y\˜]Jš[\ÊN‚ˆYˆ›İ\Ú[œİ[˜ÙJš[KX\[™ÊN‚ˆÛÛ[YBˆØØ]ÜˆHİŠš[K™Ù]
›ØØ]ÜˆŠHÜˆˆŠKœİš\

Bˆ\œÙYH\›Ü]
ØØ]ÜŠBˆYˆ\œÙYœØÚ[YH›İ[ˆÈš‹šÈŸHÜˆ›İ\œÙY›™]ØÎ‚ˆÛÛ[YBˆÙ^HHİŠš[K™Ù]
šÙ^HŠHÜˆ[™^
BˆY[]HH
Ù^KØØ]ÜŠBˆYˆY[]H[ˆÙY[‚ˆÛÛ[YBˆÙY[‹˜Y
Y[]JBˆİ]]˜\[™
ˆ\Y˜XİXY
ˆ›ÛİÚY\›ÛİÚYˆ›İšY\—Û˜]]™WÚYYˆÛ›ÙKœ›İšY\—Û˜]]™WÚYNÚÙ^_H‹ˆØØ]Ü[ØØ]Ü‹ˆÛÛ[İ\O\İŠš[K™Ù]
˜ÛÛ[İ\HŠHÜˆˆŠKˆÚ^™OWÚ[
š[K™Ù]
œÚ^™HŠJKˆÚXÚÜİ[OWØÚXÚÜİ[Jš[K™Ù]
˜ÚXÚÜİ[HŠJKˆ\œÚ\İ[ÚY\™XÛÜ™ÙÚKˆ\™[Ü\œÚ\İ[ÚYXÛÛ˜Ù\ÙÚKˆ
Bˆ
Bˆ™]\›ˆ\Jİ]]
B‚‚™YˆÚ[
˜[YNˆ[JHOˆ[›Û™N‚ˆN‚ˆ™]\›ˆ[
˜[YJHYˆ˜[YH\È›İ›Û™H[ÙH›Û™Bˆ^Ù\
\Q\œ›Ü‹˜[YQ\œ›ÜŠN‚ˆ™]\›ˆ›Û™B