"""Scoped negative knowledge for deterministic source exploration."""

from __future__ import annotations

from dataclasses import dataclass
from time import time


@dataclass(frozen=True)
class NegativeKnowledge:
    scope_kind: str
    scope_key: str
    reason_code: str
    evidence_ref: str = ""
    policy_version: str = "v7-integrated-l1"
    created_at: float = 0.0
    expires_at: float | None = None

    def __post_init__(self) -> None:
        for name in ("scope_kind", "scope_key", "reason_code", "policy_version"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} is required")
        if self.created_at < 0:
            raise ValueError("created_at must be non-negative")
        if self.expires_at is not None and self.expires_at < self.created_at:
            raise ValueError("expires_at must not precede created_at")

    @property
    def permanent(self) -> bool:
        return self.expires_at is None

    def is_active(self, now: float | None = None) -> bool:
        current = time() if now is None else float(now)
        return self.expires_at is None or self.expires_at > current
