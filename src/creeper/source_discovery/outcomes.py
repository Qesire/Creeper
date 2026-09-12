"""Replay-safe source outcome rows for offline policy evaluation."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from creeper.source_discovery.registry import SourceDiscoveryRegistry


def iter_source_outcomes(
    registry: SourceDiscoveryRegistry,
) -> Iterator[dict[str, Any]]:
    """Yield one auditable outcome row per durable source.

    decision_features contains immutable proposal priors. Triage, scout, and
    final fields are separated as progressively revealed observations/labels.
    """
    rows = registry.connection.execute(
        """
        WITH first_proposal AS (
            SELECT source_key, episode_id, created_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY source_key
                       ORDER BY created_at, proposal_id
                   ) AS rn
            FROM source_proposals
        ),
        llm_link AS (
            SELECT a.source_key,
                   h.hypothesis_id,
                   h.action AS hypothesis_action,
                   h.confidence AS hypothesis_confidence,
                   le.task_type AS llm_task_type,
                   le.cost_seconds AS llm_cost_seconds,
                   ROW_NUMBER() OVER (
                       PARTITION BY a.source_key
                       ORDER BY h.created_at, h.hypothesis_id
                   ) AS rn
            FROM source_llm_source_attribution a
            JOIN source_llm_hypotheses h
              ON h.hypothesis_id = a.hypothesis_id
            JOIN source_llm_episodes le
              ON le.episode_id = h.episode_id
        )
        SELECT
            c.*,
            fp.episode_id AS search_episode_id,
            se.strategy AS search_strategy,
            se.search_cost_seconds,
            t.status_code,
            t.method AS triage_method,
            t.content_type,
            t.content_length,
            t.range_supported,
            m.sampled_records,
            m.unique_hosts,
            m.novel_hosts,
            m.direct_host_years,
            m.requests AS scout_requests,
            m.bytes_read AS scout_bytes_read,
            m.elapsed_seconds AS scout_elapsed_seconds,
            m.novel_eed AS scout_novel_eed,
            m.measurement_mode,
            m.observed_host_year_pairs,
            m.novel_host_year_pairs,
            m.novel_pair_eed,
            m.singleton_observations,
            m.doubleton_observations,
            m.estimated_unseen_fraction,
            f.final_accepted_eed,
            f.cost_seconds AS final_cost_seconds,
            llm.hypothesis_id,
            llm.hypothesis_action,
            llm.hypothesis_confidence,
            llm.llm_task_type,
            llm.llm_cost_seconds
        FROM source_candidates c
        LEFT JOIN first_proposal fp
          ON fp.source_key = c.source_key AND fp.rn = 1
        LEFT JOIN source_search_episodes se
          ON se.episode_id = fp.episode_id
        LEFT JOIN source_triage_metrics t
          ON t.source_key = c.source_key
        LEFT JOIN source_scout_metrics m
          ON m.source_key = c.source_key
        LEFT JOIN source_final_rewards f
          ON f.source_key = c.source_key
        LEFT JOIN llm_link llm
          ON llm.source_key = c.source_key AND llm.rn = 1
        ORDER BY c.created_at, c.source_key
        """
    )
    for row in rows:
        decision_features = {
            "entrypoint": str(row["canonical_entrypoint"]),
            "source_family": str(row["source_family"]),
            "source_level": str(row["source_level"]),
            "discovered_by": str(row["discovered_by"]),
            "discovery_strategy": str(row["discovery_strategy"]),
            "expected_year_from": row["expected_year_from"],
            "expected_year_to": row["expected_year_to"],
            "expected_volume": row["expected_volume"],
            "temporal_semantics_prior": float(row["temporal_semantics_prior"]),
            "enumerability_prior": float(row["enumerability_prior"]),
            "direct_evidence_prior": float(row["direct_evidence_prior"]),
            "baseline_overlap_prior": float(row["baseline_overlap_prior"]),
            "access_cost_prior": float(row["access_cost_prior"]),
            "adapter_cost_prior": float(row["adapter_cost_prior"]),
            "confidence": float(row["confidence"]),
        }
        triage = (
            None
            if row["status_code"] is None
            else {
                "status_code": int(row["status_code"]),
                "method": row["triage_method"],
                "content_type": row["content_type"],
                "content_length": row["content_length"],
                "range_supported": (
                    None
                    if row["range_supported"] is None
                    else bool(row["range_supported"])
                ),
            }
        )
        scout = (
            None
            if row["sampled_records"] is None
            else {
                "sampled_records": int(row["sampled_records"]),
                "unique_hosts": int(row["unique_hosts"]),
                "novel_hosts": int(row["novel_hosts"]),
                "direct_host_years": int(row["direct_host_years"]),
                "requests": int(row["scout_requests"]),
                "bytes_read": int(row["scout_bytes_read"]),
                "elapsed_seconds": float(row["scout_elapsed_seconds"]),
                "novel_eed": float(row["scout_novel_eed"]),
                "measurement_mode": str(row["measurement_mode"]),
                "observed_host_year_pairs": int(
                    row["observed_host_year_pairs"]
                ),
                "novel_host_year_pairs": int(row["novel_host_year_pairs"]),
                "novel_pair_eed": float(row["novel_pair_eed"]),
                "singleton_observations": int(
                    row["singleton_observations"]
                ),
                "doubleton_observations": int(
                    row["doubleton_observations"]
                ),
                "estimated_unseen_fraction": float(
                    row["estimated_unseen_fraction"]
                ),
            }
        )
        yield {
            "contract": "creeper.source-outcome.v1",
            "source_key": str(row["source_key"]),
            "state": str(row["state"]),
            "decision_features": decision_features,
            "search": {
                "episode_id": row["search_episode_id"],
                "strategy": row["search_strategy"],
                "cost_seconds": (
                    None
                    if row["search_cost_seconds"] is None
                    else float(row["search_cost_seconds"])
                ),
            },
            "llm": {
                "task_type": row["llm_task_type"],
                "hypothesis_id": row["hypothesis_id"],
                "hypothesis_action": row["hypothesis_action"],
                "hypothesis_confidence": (
                    None
                    if row["hypothesis_confidence"] is None
                    else float(row["hypothesis_confidence"])
                ),
                "cost_seconds": (
                    None
                    if row["llm_cost_seconds"] is None
                    else float(row["llm_cost_seconds"])
                ),
            },
            "triage": triage,
            "scout": scout,
            "label": {
                "final_accepted_eed": (
                    None
                    if row["final_accepted_eed"] is None
                    else float(row["final_accepted_eed"])
                ),
                "final_cost_seconds": (
                    None
                    if row["final_cost_seconds"] is None
                    else float(row["final_cost_seconds"])
                ),
            },
        }


def encode_jsonl(rows: Iterator[dict[str, Any]]) -> str:
    return "".join(
        json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for row in rows
    )
