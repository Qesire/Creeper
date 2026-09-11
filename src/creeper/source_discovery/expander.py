"""Apply bounded Scrapy link expansion to the durable source-level DAG."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from creeper.source_discovery.models import SourceState, is_common_crawl_provenance
from creeper.source_discovery.promotion import (
    LinkPromotionAccumulator,
    PromotionPolicy,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.scrapy_sidecar import iter_scrapy_link_discoveries


@dataclass(frozen=True)
class ExpansionResult:
    input_links: int
    promoted: int
    inserted_candidates: int
    added_edges: int
    suppressed: int
    rejected_lineage: int


def expand_scrapy_spool(
    registry: SourceDiscoveryRegistry,
    *,
    parent_source_key: str,
    spool_path: Path,
    policy: PromotionPolicy | None = None,
    relation: str = "links_to_resource",
) -> ExpansionResult:
    """Promote a bounded JSONL prefix into source candidates and DAG edges.

    Parsing/aggregation completes before durable mutations begin, so a corrupt
    committed spool row cannot leave a half-applied prefix. Re-running the same
    append spool is idempotent for candidates and edges: existing children are
    linked but do not receive duplicate proposal events.
    """
    parent = registry.get_candidate(parent_source_key)
    if parent is None:
        raise KeyError(f"unknown parent source: {parent_source_key}")
    if not relation.strip():
        raise ValueError("source graph relation is required")

    actual_policy = policy or PromotionPolicy()
    accumulator = LinkPromotionAccumulator(policy=actual_policy)
    for link in iter_scrapy_link_discoveries(
        Path(spool_path),
        expected_source_key=parent_source_key,
    ):
        if accumulator.input_links >= actual_policy.max_input_links:
            break
        accumulator.add(link)

    promotions = accumulator.promoted()
    inserted_candidates = 0
    added_edges = 0
    suppressed = 0
    rejected_lineage = 0

    for promotion in promotions:
        proposed = promotion.candidate
        if proposed.source_key == parent_source_key:
            rejected_lineage += 1
            continue

        existing = registry.get_candidate(proposed.source_key)
        effective = existing or proposed
        if registry.suppression_reason(effective) is not None:
            suppressed += 1
            continue

        inserted = False
        if existing is None:
            if is_common_crawl_provenance(
                proposed.source_family,
                proposed.canonical_entrypoint,
                proposed.discovered_by,
            ):
                suppressed += 1
                continue
            effective, inserted = registry.register_proposal(proposed)
            inserted_candidates += int(inserted)

        try:
            added_edges += int(
                registry.add_edge(
                    parent_source_key,
                    effective.source_key,
                    relation=relation,
                )
            )
        except ValueError:
            rejected_lineage += 1
            # A newly-created child must not enter the usable cold pool if its
            # only known lineage violates the source graph depth/cycle budget.
            if inserted:
                registry.transition(effective.source_key, SourceState.REJECTED)

    return ExpansionResult(
        input_links=accumulator.input_links,
        promoted=len(promotions),
        inserted_candidates=inserted_candidates,
        added_edges=added_edges,
        suppressed=suppressed,
        rejected_lineage=rejected_lineage,
    )
