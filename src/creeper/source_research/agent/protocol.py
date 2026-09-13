"""Typed boundary between an LLM proposal and deterministic adapters."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class RootQuery:
    query: str
    filters: Mapping[str, Any] = field(default_factory=dict)
    expected_signal: str = ""
    expected_family: str = ""
    max_pages: int = 1

    @property
    def query_hash(self) -> str:
        payload = {
            "query": self.query.strip(),
            "filters": dict(self.filters),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True)
class RootQueryProgram:
    root_id: str
    strategy: str
    queries: tuple[RootQuery, ...]
    hard_max_requests: int
    stop_conditions: tuple[str, ...]
    compiler_version: str = "v7.1-k1"
    context_hash: str = ""
    program_id: str = ""

    def __post_init__(self) -> None:
        if not self.program_id:
            object.__setattr__(
                self,
                "program_id",
                "program:" + hashlib.sha256(
                    json.dumps(
                        {
                            "root_id": self.root_id,
                            "strategy": self.strategy,
                            "queries": [
                                {"query": q.query, "filters": dict(q.filters)}
                                for q in self.queries
                            ],
                            "context_hash": self.context_hash,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode()
                ).hexdigest(),
            )

