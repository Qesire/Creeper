"""Stable, bounded context supplied to the optional research LLM."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class ResearchCompilerContext:
    root_id: str
    root_capabilities: tuple[str, ...]
    seed_current_program_exhausted: bool
    equivalent_unexecuted_program: bool
    cooldown_satisfied: bool
    recent_query_hashes: tuple[str, ...] = ()
    unclassified_clusters: tuple[str, ...] = ()
    productive_source_families: tuple[str, ...] = ()
    saturated_source_families: tuple[str, ...] = ()
    negative_knowledge: tuple[str, ...] = ()
    baseline_scale: tuple[tuple[str, str], ...] = (
        ("annual_rows", "87475505"),
        ("annual_hosts", "59922609"),
        ("candidates", "77715415"),
    )

    def __post_init__(self) -> None:
        if not self.root_id.strip():
            raise ValueError("root_id is required")
        if any(not item.strip() for item in self.root_capabilities):
            raise ValueError("root capabilities must be non-empty strings")

    def as_prompt_payload(self) -> dict[str, object]:
        """Return only bounded metadata; no hits, cursors, or artifact bodies."""
        return {
            "root_id": self.root_id,
            "root_capabilities": list(self.root_capabilities),
            "seed_current_program_exhausted": self.seed_current_program_exhausted,
            "equivalent_unexecuted_program": self.equivalent_unexecuted_program,
            "cooldown_satisfied": self.cooldown_satisfied,
            "recent_query_hashes": list(self.recent_query_hashes),
            "unclassified_clusters": list(self.unclassified_clusters),
            "productive_source_families": list(self.productive_source_families),
            "saturated_source_families": list(self.saturated_source_families),
            "negative_knowledge": list(self.negative_knowledge),
            "baseline_scale": dict(self.baseline_scale),
        }

    @property
    def context_hash(self) -> str:
        encoded = json.dumps(
            self.as_prompt_payload(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

