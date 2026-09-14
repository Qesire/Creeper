"""Bounded context assembly for Codex source-intelligence subagents."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from creeper.source_discovery.manager import SearchDirective
from creeper.source_discovery.models import SourceState
from creeper.source_discovery.registry import SourceDiscoveryRegistry


@dataclass(frozen=True)
class SourceIntelligenceContextPolicy:
    max_top_sources: int = 8
    max_failures: int = 8
    max_strategy_rewards: int = 8
    max_recent_searches: int = 8
    max_known_origins: int = 16

    def __post_init__(self) -> None:
        if min(
            self.max_top_sources,
            self.max_failures,
            self.max_strategy_rewards,
            self.max_recent_searches,
            self.max_known_origins,
        ) < 1:
            raise ValueError("context limits must be positive")


class SourceIntelligenceContextBuilder:
    """Produce a small high-density state snapshot for one child-agent call."""

    def __init__(
        self,
        registry: SourceDiscoveryRegistry,
        *,
        policy: SourceIntelligenceContextPolicy | None = None,
    ) -> None:
        self.registry = registry
        self.policy = policy or SourceIntelligenceContextPolicy()

    def _top_sources(self) -> list[dict[str, Any]]:
        candidates = self.registry.list_candidates_in_states(
            (SourceState.WARM, SourceState.ACTIVE, SourceState.HOLD)
        )
        scored: list[tuple[float, dict[str, Any]]] = []
        for candidate in candidates:
            measurement = self.registry.get_scout_measurement(candidate.source_key)
            if measurement is None:
                continue
            value = measurement.novel_eed_per_second
            scored.append(
                (
                    value,
                    {
                        "source_key": candidate.source_key,
                        "entrypoint": candidate.canonical_entrypoint,
                        "family": candidate.source_family,
                        "state": candidate.state.value,
                        "measurement_mode": measurement.measurement_mode.value,
                        "novel_eed": measurement.novel_eed_for_ranking,
                        "novel_eed_per_second": value,
                        "observed": measurement.observed_count_for_threshold,
                        "novel": measurement.novel_count_for_threshold,
                    },
                )
            )
        scored.sort(key=lambda item: (-item[0], item[1]["source_key"]))
        return [
            payload
            for _score, payload in scored[: self.policy.max_top_sources]
        ]

    def _recent_failures(self) -> list[dict[str, Any]]:
        rows = self.registry.connection.execute(
            """
            SELECT
                source_key,
                canonical_entrypoint,
                source_family,
                state,
                state_reason
            FROM source_candidates
            WHERE state IN ('HOLD', 'REJECTED', 'EXHAUSTED')
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (self.policy.max_failures,),
        ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            source_key = str(row["source_key"])
            candidate = self.registry.get_candidate(source_key)
            payload: dict[str, Any] = {
                "source_key": source_key,
                "entrypoint": str(row["canonical_entrypoint"]),
                "family": str(row["source_family"]),
                "state": str(row["state"]),
                "state_reason": str(row["state_reason"] or ""),
            }
            if candidate is not None:
                payload["origin"] = candidate.origin
                suppression = self.registry.suppression_reason(candidate)
                if suppression is not None:
                    payload["suppression_reason"] = suppression
            measurement = self.registry.get_scout_measurement(source_key)
            if measurement is not None:
                payload["measurement"] = {
                    "measurement_mode": measurement.measurement_mode.value,
                    "novel_eed": measurement.novel_eed_for_ranking,
                    "novel_eed_per_second": measurement.novel_eed_per_second,
                    "observed": measurement.observed_count_for_threshold,
                    "novel": measurement.novel_count_for_threshold,
                    "measured_baseline_overlap": (
                        measurement.measured_baseline_overlap
                    ),
                    "estimated_unseen_fraction": (
                        measurement.estimated_unseen_fraction
                    ),
                }
            result.append(payload)
        return result

    def _recent_searches(self) -> list[dict[str, Any]]:
        rows = self.registry.connection.execute(
            """
            SELECT episode_id, strategy, query, accepted_proposals, new_sources
            FROM source_search_episodes
            WHERE finished_at IS NOT NULL
            ORDER BY finished_at DESC, episode_id DESC
            LIMIT ?
            """,
            (self.policy.max_recent_searches,),
        ).fetchall()
        return [
            {
                "strategy": str(row["strategy"]),
                "query": str(row["query"]),
                "accepted_proposals": int(row["accepted_proposals"] or 0),
                "new_sources": int(row["new_sources"] or 0),
            }
            for row in rows
        ]

    def _known_origins(self) -> list[str]:
        rows = self.registry.connection.execute(
            """
            SELECT canonical_entrypoint
            FROM source_candidates
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (self.policy.max_known_origins * 8,),
        ).fetchall()
        origins: list[str] = []
        seen: set[str] = set()
        for row in rows:
            parsed = urlsplit(str(row["canonical_entrypoint"]))
            if not parsed.scheme or not parsed.netloc:
                continue
            origin = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"
            if origin in seen:
                continue
            seen.add(origin)
            origins.append(origin)
            if len(origins) >= self.policy.max_known_origins:
                break
        return origins

    def _subject_sources(
        self,
        directive: SearchDirective,
    ) -> list[dict[str, Any]]:
        if not directive.subject:
            return []
        result: list[dict[str, Any]] = []
        candidates = self.registry.list_candidates_in_states(
            (
                SourceState.DISCOVERED,
                SourceState.TRIAGED,
                SourceState.SCOUT_READY,
                SourceState.WARM,
                SourceState.ACTIVE,
                SourceState.HOLD,
            )
        )
        for candidate in candidates:
            if (
                candidate.source_family == directive.subject
                or candidate.origin == directive.subject
                or candidate.canonical_entrypoint == directive.subject
            ):
                measurement = self.registry.get_scout_measurement(
                    candidate.source_key
                )
                payload: dict[str, Any] = {
                    "source_key": candidate.source_key,
                    "entrypoint": candidate.canonical_entrypoint,
                    "origin": candidate.origin,
                    "family": candidate.source_family,
                    "level": candidate.level.value,
                    "state": candidate.state.value,
                }
                triage = self.registry.get_triage_observation(
                    candidate.source_key
                )
                if triage is not None:
                    payload["http_triage"] = triage
                if measurement is not None:
                    payload["measurement"] = {
                        "novel_eed": measurement.novel_eed_for_ranking,
                        "novel_eed_per_second": (
                            measurement.novel_eed_per_second
                        ),
                        "observed": (
                            measurement.observed_count_for_threshold
                        ),
                        "novel": measurement.novel_count_for_threshold,
                        "estimated_unseen_fraction": (
                            measurement.estimated_unseen_fraction
                        ),
                    }
                child_keys = self.registry.children(candidate.source_key)
                children: list[dict[str, Any]] = []
                for child_key in child_keys[:32]:
                    child = self.registry.get_candidate(child_key)
                    if child is None:
                        continue
                    children.append(
                        {
                            "entrypoint": child.canonical_entrypoint,
                            "family": child.source_family,
                            "level": child.level.value,
                            "state": child.state.value,
                            "strategy": child.discovery_strategy,
                        }
                    )
                if children:
                    payload["deterministic_children"] = children
                result.append(payload)
        result.sort(
            key=lambda item: (
                -float(
                    item.get("measurement", {}).get(
                        "novel_eed_per_second",
                        0.0,
                    )
                ),
                item["source_key"],
            )
        )
        return result[: self.policy.max_top_sources]

    def build(self, directive: SearchDirective) -> dict[str, Any]:
        inventory = {
            state.value: count
            for state, count in self.registry.inventory().items()
        }
        rewards = sorted(
            self.registry.strategy_rewards(),
            key=lambda item: (-item.reward_per_cost, item.strategy),
        )[: self.policy.max_strategy_rewards]
        return {
            "objective": (
                "maximize marginal FINAL Accepted Novel EED "
                "per total resource cost"
            ),
            "inventory": inventory,
            "llm_task_history": self.registry.llm_task_rewards(),
            "strategy_history": [
                {
                    "strategy": item.strategy,
                    "episodes": item.episodes,
                    "credited_eed": item.accepted_novel_eed,
                    "search_cost_seconds": item.search_cost_seconds,
                    "reward_per_cost": item.reward_per_cost,
                }
                for item in rewards
            ],
            "top_measured_sources": self._top_sources(),
            "recent_terminal_sources": self._recent_failures(),
            "recent_searches": self._recent_searches(),
            "known_origins": self._known_origins(),
            "subject_sources": self._subject_sources(directive),
            "constraints": {
                "target_year_from": 1996,
                "target_year_to": 2001,
                "common_crawl_corpus_excluded": True,
                "baseline_authority_hidden_from_agent": True,
                "agent_authority": "proposal_only",
                "measured_zero_yield_is_negative_search_feedback": True,
                "do_not_infer_hostname_value_from_page_count_alone": True,
            },
        }

    @staticmethod
    def digest(context: dict[str, Any]) -> str:
        encoded = json.dumps(
            context,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()
