"""OAI-PMH research-root adapter with deterministic stage and token resume."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from .base import RootCapabilityReport, SearchCheckpoint, SearchHit, SearchPage, is_retryable, retry_delay_seconds

_OAI_NS = "{http://www.openarchives.org/OAI/2.0/}"

_BASEURL_TAG_RE = re.compile(r"<baseURL(?:\s[^>]*)?>(.*?)</baseURL>", re.I | re.S)
_URL_TOKEN_RE = re.compile(r"https?://[^\s<>\"']+", re.I)


def parse_listfriends_baseurls(text: str) -> tuple[str, ...]:
    """Extract deterministic OAI baseURL seeds from a historical ListFriends dump."""
    raw = str(text or "")
    tagged = [" ".join(match.split()) for match in _BASEURL_TAG_RE.findall(raw)]
    candidates = tagged if tagged else _URL_TOKEN_RE.findall(raw)
    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        value = candidate.strip().rstrip(".,;)")
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue
        normalized = value.rstrip("/")
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return tuple(out)


class OAIAdapter:
    """Run a bounded OAI-PMH repository probe/search pipeline.

    Metadata returned here is research material only.  No datestamp, repository
    registration date, or record date is given annual Web-evidence authority.
    """

    def __init__(self, base_url: str, *, transport: Any) -> None:
        self.base_url = base_url
        self.transport = transport
        host = urlparse(base_url).netloc or base_url
        self.root_id = f"oai:{host.lower()}"
        self.dead_reason: str | None = None

    async def probe_capabilities(self) -> RootCapabilityReport:
        try:
            response = await self.transport(self.base_url, {"verb": "Identify"}, {"Accept": "text/xml, application/xml"})
        except Exception as exc:
            return RootCapabilityReport(self.root_id, False, reason=f"TRANSPORT:{exc}")
        status = int(getattr(response, "status_code", 200))
        if is_retryable(response):
            return RootCapabilityReport(self.root_id, False, reason="RETRYABLE", status_code=status)
        if not 200 <= status < 300:
            return RootCapabilityReport(self.root_id, False, reason=f"HTTP_{status}", status_code=status)
        try:
            root = ET.fromstring(_response_text(response))
        except ET.ParseError as exc:
            return RootCapabilityReport(self.root_id, False, reason=f"BAD_XML:{exc}", status_code=status)
        if root.find(f".//{_OAI_NS}Identify") is None:
            return RootCapabilityReport(self.root_id, False, reason="NO_IDENTIFY", status_code=status)
        return RootCapabilityReport(
            self.root_id,
            True,
            ("identify", "metadata_formats", "sets", "records", "resumption_token"),
            status_code=status,
        )

    async def search(self, query: Any, checkpoint: SearchCheckpoint | None) -> SearchPage:
        cp = checkpoint or SearchCheckpoint(query_variant="identify")
        stage = cp.query_variant or "identify"
        if stage not in {"identify", "formats", "sets", "records"}:
            stage = "identify"
            cp = SearchCheckpoint(query_variant=stage)

        params = self._params(query, cp, stage)
        response = await self.transport(self.base_url, params, {"Accept": "text/xml, application/xml"})
        status = int(getattr(response, "status_code", 200))
        if is_retryable(response):
            return SearchPage(next_checkpoint=cp, terminal=False, retry_after=retry_delay_seconds(response))
        if not 200 <= status < 300:
            self.dead_reason = f"HTTP_{status}"
            return SearchPage(terminal=True)

        text = _response_text(response)
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            self.dead_reason = f"BAD_XML:{exc}"
            return SearchPage(terminal=True, bytes_read=len(text.encode("utf-8", errors="ignore")))

        error = root.find(f".//{_OAI_NS}error")
        if error is not None:
            code = str(error.attrib.get("code") or "OAI_ERROR")
            if stage == "sets" and code == "noSetHierarchy":
                return SearchPage(
                    next_checkpoint=SearchCheckpoint(query_variant="records"),
                    terminal=False,
                    bytes_read=len(text.encode("utf-8", errors="ignore")),
                )
            self.dead_reason = f"OAI_{code}"
            return SearchPage(terminal=True, bytes_read=len(text.encode("utf-8", errors="ignore")))

        hits = self._hits(query, stage, root)
        token = _resumption_token(root)
        if token:
            next_cp = SearchCheckpoint(cursor=token, query_variant=stage)
            terminal = False
        else:
            next_stage = _next_stage(stage)
            next_cp = SearchCheckpoint(query_variant=next_stage) if next_stage is not None else None
            terminal = next_stage is None
        return SearchPage(
            hits=hits,
            next_checkpoint=next_cp,
            terminal=terminal,
            bytes_read=len(text.encode("utf-8", errors="ignore")),
        )

    def _params(self, query: Any, cp: SearchCheckpoint, stage: str) -> dict[str, str]:
        if cp.cursor:
            return {"verb": _verb(stage), "resumptionToken": cp.cursor}
        filters = dict(getattr(query, "native_filters", {}) or {})
        params: dict[str, str] = {"verb": _verb(stage)}
        if stage == "records":
            params["metadataPrefix"] = str(filters.get("metadata_prefix") or "oai_dc")
            set_spec = filters.get("set")
            if set_spec:
                params["set"] = str(set_spec)
            if filters.get("from"):
                params["from"] = str(filters["from"])
            if filters.get("until"):
                params["until"] = str(filters["until"])
        return params

    def _hits(self, query: Any, stage: str, root: ET.Element) -> tuple[SearchHit, ...]:
        query_id = getattr(query, "query_id", "")
        if stage == "identify":
            identify = root.find(f".//{_OAI_NS}Identify")
            if identify is None:
                return ()
            name = _text(identify.find(f"{_OAI_NS}repositoryName"))
            base = _text(identify.find(f"{_OAI_NS}baseURL")) or self.base_url
            earliest = _text(identify.find(f"{_OAI_NS}earliestDatestamp"))
            return (
                SearchHit(
                    self.root_id,
                    query_id,
                    base,
                    base,
                    "REPOSITORY",
                    name,
                    metadata={
                        "earliest_datestamp": earliest,
                        "annual_evidence_authority": False,
                    },
                ),
            )
        if stage == "formats":
            hits: list[SearchHit] = []
            for fmt in root.findall(f".//{_OAI_NS}metadataFormat"):
                prefix = _text(fmt.find(f"{_OAI_NS}metadataPrefix"))
                if not prefix:
                    continue
                hits.append(
                    SearchHit(
                        self.root_id,
                        query_id,
                        f"{self.base_url}#metadata:{prefix}",
                        self.base_url,
                        "SCHEMA",
                        prefix,
                        metadata={
                            "schema": _text(fmt.find(f"{_OAI_NS}schema")),
                            "metadata_namespace": _text(fmt.find(f"{_OAI_NS}metadataNamespace")),
                            "annual_evidence_authority": False,
                        },
                    )
                )
            return tuple(hits)
        if stage == "sets":
            hits = []
            for item in root.findall(f".//{_OAI_NS}set"):
                spec = _text(item.find(f"{_OAI_NS}setSpec"))
                if not spec:
                    continue
                hits.append(
                    SearchHit(
                        self.root_id,
                        query_id,
                        f"{self.base_url}#set:{spec}",
                        self.base_url,
                        "SET",
                        _text(item.find(f"{_OAI_NS}setName")) or spec,
                        metadata={"set_spec": spec, "annual_evidence_authority": False},
                    )
                )
            return tuple(hits)

        hits = []
        for record in root.findall(f".//{_OAI_NS}record"):
            header = record.find(f"{_OAI_NS}header")
            if header is None:
                continue
            identifier = _text(header.find(f"{_OAI_NS}identifier"))
            if not identifier:
                continue
            datestamp = _text(header.find(f"{_OAI_NS}datestamp"))
            sets = tuple(
                text for text in (_text(node) for node in header.findall(f"{_OAI_NS}setSpec")) if text
            )
            metadata = record.find(f"{_OAI_NS}metadata")
            flattened = _flatten_metadata(metadata) if metadata is not None else {}
            hits.append(
                SearchHit(
                    self.root_id,
                    query_id,
                    identifier,
                    self.base_url,
                    "RECORD",
                    str(flattened.get("title") or identifier),
                    str(flattened.get("description") or ""),
                    {
                        "datestamp": datestamp,
                        "sets": sets,
                        "metadata": flattened,
                        "pivot_urls": tuple(_extract_urls(flattened)),
                        "annual_evidence_authority": False,
                    },
                )
            )
        return tuple(hits)

    async def resolve(self, node: Any) -> tuple[Any, ...]:
        # OAI records/metadata remain pivots until the resolver establishes a
        # concrete artifact.  The adapter itself never creates evidence.
        return (node,)


def _verb(stage: str) -> str:
    return {
        "identify": "Identify",
        "formats": "ListMetadataFormats",
        "sets": "ListSets",
        "records": "ListRecords",
    }[stage]


def _next_stage(stage: str) -> str | None:
    return {
        "identify": "formats",
        "formats": "sets",
        "sets": "records",
        "records": None,
    }[stage]


def _resumption_token(root: ET.Element) -> str | None:
    node = root.find(f".//{_OAI_NS}resumptionToken")
    value = _text(node)
    return value or None


def _flatten_metadata(metadata: ET.Element) -> dict[str, Any]:
    values: dict[str, list[str]] = {}
    for node in metadata.iter():
        if node is metadata:
            continue
        key = node.tag.rsplit("}", 1)[-1].lower()
        value = _text(node)
        if not value:
            continue
        values.setdefault(key, []).append(value)
    result: dict[str, Any] = {}
    for key, entries in values.items():
        result[key] = entries[0] if len(entries) == 1 else tuple(entries)
    return result


def _extract_urls(metadata: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for value in metadata.values():
        entries = value if isinstance(value, tuple) else (value,)
        for entry in entries:
            text = str(entry)
            if text.startswith(("http://", "https://")):
                values.append(text)
    return list(dict.fromkeys(values))


def _response_text(response: Any) -> str:
    value = getattr(response, "text", "")
    if callable(value):
        value = value()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value or "")


def _text(node: ET.Element | None) -> str:
    if node is None or node.text is None:
        return ""
    return " ".join(node.text.split())
