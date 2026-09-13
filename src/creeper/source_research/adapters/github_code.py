"""GitHub API code-fossil research root.

Only API/token based code search is supported.  Search hits are classified before
candidate pressure; schema/software/noise hits remain research knowledge, while
concrete download fossils and data artifacts may create ArtifactLead objects.
"""
from __future__ import annotations

import base64
import re
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .base import ArtifactLead, RootCapabilityReport, SearchCheckpoint, SearchHit, SearchPage, is_retryable, response_json, retry_after_seconds

API = "https://api.github.com/search/code"
DEFAULT_SEED_QUERIES = (
    '"crawl_date" "src" "dest" "anchor"',
    '"urlkey" "timestamp" "original"',
    "webbase-2001",
)
_DATA_EXTENSIONS = (
    ".warc", ".warc.gz", ".arc", ".arc.gz", ".cdx", ".cdxj", ".parquet",
    ".csv", ".csv.gz", ".tsv", ".tsv.gz", ".jsonl", ".jsonl.gz", ".tar.gz",
)
_CODE_EXTENSIONS = (".py", ".js", ".ts", ".java", ".go", ".rs", ".c", ".cc", ".cpp", ".h", ".sh")
_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.I)


class GitHubHitClass(StrEnum):
    SOFTWARE_ONLY = "SOFTWARE_ONLY"
    SCHEMA_DOC = "SCHEMA_DOC"
    DOWNLOAD_FOSSIL = "DOWNLOAD_FOSSIL"
    MANIFEST = "MANIFEST"
    DATASET_README = "DATASET_README"
    DATA_ARTIFACT = "DATA_ARTIFACT"
    NOISE = "NOISE"


_USEFUL_CLASSES = {
    GitHubHitClass.DOWNLOAD_FOSSIL,
    GitHubHitClass.MANIFEST,
    GitHubHitClass.DATASET_README,
    GitHubHitClass.DATA_ARTIFACT,
}


class GitHubCodeAdapter:
    root_id = "github-code"

    def __init__(self, *, transport: Any, token: str | None, endpoint: str = API) -> None:
        self.transport = transport
        self.token = token.strip() if token else None
        self.endpoint = endpoint
        self.state = "ENABLED" if self.token else "DISABLED_AUTH"

    def _headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.text-match+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def probe_capabilities(self) -> RootCapabilityReport:
        if not self.token:
            return RootCapabilityReport(self.root_id, False, reason="DISABLED_AUTH")
        try:
            response = await self.transport(self.endpoint, {"q": "webbase-2001", "per_page": 1, "page": 1}, self._headers())
        except Exception as exc:
            return RootCapabilityReport(self.root_id, False, reason=f"TRANSPORT:{exc}")
        status = int(getattr(response, "status_code", 200))
        if 200 <= status < 300:
            return RootCapabilityReport(self.root_id, True, ("code_search", "text_matches", "bounded_pagination"), status_code=status)
        reason = "AUTH_FAILED" if status in (401, 403) else ("RETRYABLE" if is_retryable(response) else f"HTTP_{status}")
        return RootCapabilityReport(self.root_id, False, reason=reason, status_code=status)

    async def search(self, query: Any, checkpoint: SearchCheckpoint | None) -> SearchPage:
        if not self.token:
            self.state = "DISABLED_AUTH"
            return SearchPage(terminal=True, requests=0)

        cp = checkpoint or SearchCheckpoint(page=1)
        page = max(1, cp.page)
        per_page = min(100, max(1, int(getattr(query, "page_size", 100))))
        params = {
            "q": getattr(query, "query_text", ""),
            "per_page": per_page,
            "page": page,
        }
        response = await self.transport(self.endpoint, params, self._headers())
        status = int(getattr(response, "status_code", 200))
        if is_retryable(response) or (status == 403 and retry_after_seconds(response) is not None):
            return SearchPage(next_checkpoint=cp, terminal=False, retry_after=retry_after_seconds(response))
        if status in (401, 403):
            self.state = "DISABLED_AUTH"
            return SearchPage(terminal=True)
        if not 200 <= status < 300:
            return SearchPage(terminal=True)

        payload = response_json(response)
        rows = payload.get("items") or []
        hits: list[SearchHit] = []
        lead_by_locator: dict[str, ArtifactLead] = {}
        seen_hits: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            repo = (row.get("repository") or {}).get("full_name") or ""
            path = str(row.get("path") or row.get("name") or "")
            revision = str(row.get("sha") or "")
            if not repo or not path:
                continue
            native_id = f"{repo}:{path}@{revision or 'HEAD'}"
            if native_id in seen_hits:
                continue
            seen_hits.add(native_id)
            text = _match_text(row)
            classification = classify_code_hit(path, text)
            provider_url = str(row.get("html_url") or row.get("url") or "")
            raw_url = _raw_url(repo, path, revision) if revision else ""
            extracted_urls = tuple(_extract_urls(text))
            hits.append(
                SearchHit(
                    self.root_id,
                    getattr(query, "query_id", ""),
                    native_id,
                    provider_url,
                    "CODE_HIT",
                    path,
                    metadata={
                        "repo": repo,
                        "path": path,
                        "revision": revision,
                        "classification": classification.value,
                        "raw_url": raw_url,
                        "extracted_urls": extracted_urls,
                        "annual_evidence_authority": False,
                    },
                )
            )
            if classification not in _USEFUL_CLASSES:
                continue
            candidates: list[str] = []
            if classification == GitHubHitClass.DATA_ARTIFACT and raw_url:
                candidates.append(raw_url)
            if classification == GitHubHitClass.DOWNLOAD_FOSSIL:
                candidates.extend(extracted_urls)
            elif classification == GitHubHitClass.MANIFEST:
                # A version-pinned manifest/config file is itself a finite
                # artifact lead; any concrete data URLs inside it are additional
                # leads. README files remain pivots unless they name data.
                if raw_url:
                    candidates.append(raw_url)
                candidates.extend(url for url in extracted_urls if _looks_like_data_url(url))
            elif classification == GitHubHitClass.DATASET_README:
                candidates.extend(url for url in extracted_urls if _looks_like_data_url(url))
            for locator in candidates:
                key = _canonical_locator(locator)
                if not key or key in lead_by_locator:
                    continue
                lead_by_locator[key] = ArtifactLead(
                    self.root_id,
                    native_id,
                    locator,
                    persistent_id=f"github:{native_id}",
                )

        total = _safe_int(payload.get("total_count")) or 0
        max_pages = max(1, int(getattr(query, "max_pages", 1)))
        # GitHub Code Search exposes at most 1000 results.  Treat incomplete
        # results as remaining work while within the bounded query program.
        next_page = page + 1
        more_by_total = page * per_page < min(total, 1000)
        incomplete = bool(payload.get("incomplete_results"))
        more = page < max_pages and bool(rows) and (more_by_total or incomplete)
        next_cp = SearchCheckpoint(page=next_page) if more else None
        return SearchPage(
            hits=tuple(hits),
            artifact_leads=tuple(lead_by_locator.values()),
            next_checkpoint=next_cp,
            terminal=not more,
            bytes_read=len(str(payload)),
        )

    async def resolve(self, node: Any) -> tuple[Any, ...]:
        return (node,)


def classify_code_hit(path: str, text: str) -> GitHubHitClass:
    p = path.lower()
    t = text.lower()
    if any(part in p for part in ("node_modules/", "vendor/", "dist/", "build/")) or p.endswith((".min.js", ".map")):
        return GitHubHitClass.NOISE
    if p.endswith(_DATA_EXTENSIONS):
        return GitHubHitClass.DATA_ARTIFACT

    urls = _extract_urls(text)
    data_urls = [url for url in urls if _looks_like_data_url(url)]
    fossil_signal = bool(data_urls) or any(token in t for token in ("wget ", "curl ", "webbase-2001", "download_url"))
    if fossil_signal and (data_urls or "webbase-2001" in t):
        return GitHubHitClass.DOWNLOAD_FOSSIL

    schema_primary = all(token in t for token in ("crawl_date", "src", "dest", "anchor"))
    schema_cdx = all(token in t for token in ("urlkey", "timestamp", "original"))
    if schema_primary or schema_cdx:
        return GitHubHitClass.SCHEMA_DOC

    basename = p.rsplit("/", 1)[-1]
    manifest_name = any(token in basename for token in ("manifest", "index", "files")) and basename.endswith((".json", ".jsonl", ".yaml", ".yml", ".toml", ".txt", ".csv"))
    if manifest_name and any(token in t for token in ("http://", "https://", "dataset", "archive", "crawl")):
        return GitHubHitClass.MANIFEST

    if basename.startswith("readme") and any(token in t for token in ("dataset", "web crawl", "web archive", "webbase", "download")):
        return GitHubHitClass.DATASET_README

    if p.endswith(_CODE_EXTENSIONS):
        return GitHubHitClass.SOFTWARE_ONLY
    return GitHubHitClass.NOISE


def _match_text(row: dict[str, Any]) -> str:
    parts: list[str] = []
    raw_content = row.get("content")
    if isinstance(raw_content, str):
        if row.get("encoding") == "base64":
            try:
                raw_content = base64.b64decode(raw_content).decode("utf-8", errors="replace")
            except Exception:
                pass
        parts.append(str(raw_content))
    for match in row.get("text_matches") or []:
        if not isinstance(match, dict):
            continue
        fragment = match.get("fragment")
        if fragment:
            parts.append(str(fragment))
        for item in match.get("matches") or []:
            if isinstance(item, dict) and item.get("text"):
                parts.append(str(item["text"]))
    return "\n".join(parts)


def _extract_urls(text: str) -> list[str]:
    urls: list[str] = []
    for match in _URL_RE.finditer(text):
        url = match.group(0).rstrip(".,);]}")
        if url not in urls:
            urls.append(url)
    return urls


def _looks_like_data_url(url: str) -> bool:
    path = urlsplit(url).path.lower()
    return path.endswith(_DATA_EXTENSIONS) or any(token in path for token in ("/download/", "/api/access/datafile/", "webbase-2001"))


def _canonical_locator(url: str) -> str:
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return ""
    path = re.sub(r"/+", "/", parts.path)
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def _raw_url(repo: str, path: str, revision: str) -> str:
    return f"https://raw.githubusercontent.com/{repo}/{revision}/{path}"


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
