"""Resolve research metadata into ArtifactLead/NewRootLead only."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .models import ArtifactLead, NewRootLead, ResearchNode, RootKind
from .pivot import pivots_for_node


@dataclass(frozen=True)
class ResolutionResult:
    artifact_leads: tuple[ArtifactLead, ...] = ()
    new_root_leads: tuple[NewRootLead, ...] = ()


def _rows(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, Mapping):
                yield item
            elif isinstance(item, str):
                yield {"locator": item}


def _locator(row: Mapping[str, Any]) -> str:
    for key in ("locator", "url", "self", "download", "link"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    links = row.get("links")
    if isinstance(links, Mapping):
        for key in ("self", "download", "content"):
            value = links.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _str(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def resolve_node(
    node: ResearchNode, *, query_id: str = "", program_id: str = ""
) -> ResolutionResult:
    """Metadata resolution never returns evidence and never asserts a year."""
    artifacts: list[ArtifactLead] = []
    roots: list[NewRootLead] = []
    md = node.metadata

    rows: list[Mapping[str, Any]] = []
    rows.extend(_rows(md.get("files")))
    rows.extend(_rows(md.get("artifacts")))
    for key in ("content_urls", "contentUrl"):
        rows.extend(_rows(md.get(key)))

    for row in rows:
        locator = _locator(row)
        if not locator:
            continue
        artifacts.append(
            ArtifactLead(
                root_id=node.root_id,
                provider_native_id=str(
                    row.get("id") or row.get("key") or row.get("filename")
                    or node.provider_native_id
                ),
                locator=locator,
                content_type=str(
                    row.get("content_type") or row.get("type")
                    or row.get("contentType") or ""
                ),
                size=_int(row.get("size") or row.get("filesize")),
                checksum=_str(
                    row.get("checksum") or row.get("md5") or row.get("sha256")
                ),
                persistent_id=_str(
                    row.get("persistent_id") or row.get("persistentId") or md.get("doi")
                ),
                parent_persistent_id=_str(
                    row.get("parent_persistent_id")
                    or row.get("datasetPersistentId")
                    or md.get("conceptrecid")
                ),
                immutable_identity=_str(row.get("immutable_identity")),
                source_node_id=node.node_id,
                query_id=query_id,
                program_id=program_id,
            )
        )

    for action in pivots_for_node(node):
        if action.pivot_kind not in {"ROOT_SURFACE", "REPOSITORY_SURFACE"}:
            continue
        entrypoint = action.payload.get("entrypoint")
        if isinstance(entrypoint, str) and entrypoint.strip():
            kind = RootKind.OAI if "oai" in entrypoint.casefold() else RootKind.GENERIC
            roots.append(
                NewRootLead(
                    entrypoint=entrypoint,
                    kind=kind,
                    discovered_from_node_id=node.node_id,
                    rationale=action.pivot_kind,
                )
            )

    artifact_by_id = {lead.artifact_identity: lead for lead in artifacts}
    root_by_url = {lead.entrypoint: lead for lead in roots}
    return ResolutionResult(
        artifact_leads=tuple(artifact_by_id[key] for key in sorted(artifact_by_id)),
        new_root_leads=tuple(root_by_url[key] for key in sorted(root_by_url)),
    )


__all__ = ["ResolutionResult", "resolve_node"]
