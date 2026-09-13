"""Validated negative knowledge; incomplete pagination is never negative evidence."""
from __future__ import annotations

from .models import NegativeKnowledge, stable_hash


def make_negative_knowledge(
    *,
    root_id: str,
    scope_kind: str,
    scope_key: str,
    reason: str,
    exhaustive: bool,
    pagination_complete: bool,
    context_hash: str = "",
) -> NegativeKnowledge:
    if not exhaustive or not pagination_complete:
        raise ValueError("incomplete work cannot become negative knowledge")
    negative_id = stable_hash(
        "negative", root_id, scope_kind, scope_key, reason, context_hash
    )
    return NegativeKnowledge(
        negative_id=negative_id,
        root_id=root_id,
        scope_kind=scope_kind,
        scope_key=scope_key,
        reason=reason,
        exhaustive=True,
        context_hash=context_hash,
    )


__all__ = ["make_negative_knowledge"]
