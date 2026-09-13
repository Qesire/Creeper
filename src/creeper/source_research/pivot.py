"""Deterministic reusable pivots over research nodes."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .models import PivotAction, ResearchNode, ResearchNodeKind


def _strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        if value.strip():
            yield value.strip()
    elif isinstance(value, Mapping):
        for key in ("url", "identifier", "relatedIdentifier", "value", "id"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                yield raw.strip()
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)


def pivots_for_node(node: ResearchNode) -> tuple[PivotAction, ...]:
    actions: list[PivotAction] = []
    md = node.metadata

    def add(kind: str, payload: Mapping[str, Any]) -> None:
        actions.append(PivotAction(node.node_id, kind, dict(payload), root_id=node.root_id))

    if node.kind in {ResearchNodeKind.DATASET, ResearchNodeKind.RECORD, ResearchNodeKind.DOI}:
        for value in _strings(md.get("related_identifiers") or md.get("relatedIdentifiers")):
            add("RELATED_IDENTIFIER", {"value": value})
        for value in _strings(md.get("content_urls") or md.get("files")):
            add("ARTIFACT_LOCATOR", {"locator": value})
        repository = md.get("repository_url") or md.get("landing_url")
        if isinstance(repository, str) and repository.strip():
            add("REPOSITORY_SURFACE", {"entrypoint": repository.strip()})
    elif node.kind is ResearchNodeKind.REPOSITORY:
        for value in _strings(md.get("collections")):
            add("COLLECTION", {"value": value})
        base = md.get("oai_base_url") or md.get("api_base_url")
        if isinstance(base, str) and base.strip():
            add("ROOT_SURFACE", {"entrypoint": base.strip()})
    elif node.kind is ResearchNodeKind.CODE:
        for value in _strings(md.get("releases") or md.get("artifacts")):
            add("CODE_ARTIFACT", {"locator": value})
        if md.get("revision") or md.get("path"):
            add("CODE_REVISION", {
                "revision": md.get("revision") or md.get("sha") or "",
                "path": md.get("path") or "",
            })
    elif node.kind is ResearchNodeKind.COLLECTION:
        for value in _strings(md.get("artifacts") or md.get("files")):
            add("ARTIFACT_LOCATOR", {"locator": value})
    elif node.kind in {ResearchNodeKind.ARTIFACT, ResearchNodeKind.FILE}:
        for value in _strings(md.get("mirrors")):
            add("ARTIFACT_MIRROR", {"locator": value})

    unique = {action.pivot_id: action for action in actions}
    return tuple(unique[key] for key in sorted(unique))


__all__ = ["pivots_for_node"]
