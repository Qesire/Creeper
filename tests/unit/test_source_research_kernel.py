from __future__ import annotations

import pytest

from creeper.source_research.models import (
    ArtifactLead, DecisionRecord, FrontierState, FrontierTask, QueryProgram,
    RewardKind, RewardRecord, RewardScope, RootKind, RootQuery, RootSurface,
    SearchCheckpoint, SearchHit,
)
from creeper.source_research.negative_knowledge import make_negative_knowledge
from creeper.source_research.ranking import ResearchYield
from creeper.source_research.registry import ResearchRegistry
from creeper.source_research.resolver import resolve_node
from creeper.source_research.resume import ResearchResumeManager
from creeper.storage.control_store import ControlStore


def setup_registry(tmp_path):
    store = ControlStore(tmp_path / "control.sqlite")
    reg = ResearchRegistry(store, clock=lambda: 100.0)
    root = RootSurface(
        root_id="datacite",
        kind=RootKind.STRUCTURED_REPOSITORY,
        canonical_locator="https://api.datacite.org/dois",
    )
    reg.upsert_root(root)
    query = RootQuery(
        root_id="datacite",
        query_text="web archive collection",
        native_filters={"type": "dataset"},
    )
    program = QueryProgram(
        root_id="datacite",
        strategy="seed",
        queries=(query,),
        hard_max_requests=10,
        stop_conditions=("exhausted",),
    )
    reg.register_program(program)
    return store, reg, query, program


def test_cross_root_doi_dedup(tmp_path):
    _, reg, query, _ = setup_registry(tmp_path)
    a = reg.upsert_hit(
        SearchHit(
            "datacite", query.query_id, "10.1234/ABC",
            "https://doi.org/10.1234/ABC", "DOI",
        )
    )
    b = reg.upsert_hit(
        SearchHit(
            "zenodo", query.query_id, "998",
            "https://zenodo.org/records/998", "RECORD",
            metadata={"doi": "https://doi.org/10.1234/abc"},
        )
    )
    assert a.node_id == b.node_id
    assert a.canonical_key == "doi:10.1234/abc"


def test_artifact_mirror_dedup(tmp_path):
    _, reg, query, program = setup_registry(tmp_path)
    a = ArtifactLead(
        "datacite", "a", "https://a.example/file.gz",
        checksum="sha256:DEAD", query_id=query.query_id,
        program_id=program.program_id,
    )
    b = ArtifactLead(
        "zenodo", "b", "https://b.example/mirror.gz",
        checksum="sha256:dead", query_id=query.query_id,
        program_id=program.program_id,
    )
    aid, _, _ = reg.register_artifact_lead(a)
    bid, _, _ = reg.register_artifact_lead(b)
    assert aid == bid


def test_seed_replay_suppression(tmp_path):
    _, reg, query, program = setup_registry(tmp_path)
    replay = RootQuery(
        root_id="datacite",
        query_text=" WEB   archive COLLECTION ",
        native_filters={"type": "dataset"},
    )
    query_id, created = reg.register_query(program.program_id, replay)
    assert not created
    assert query_id == query.query_id


def test_restart_cursor_resume(tmp_path):
    store, reg, query, _ = setup_registry(tmp_path)
    reg.update_query_checkpoint(
        query.query_id,
        checkpoint=SearchCheckpoint(cursor="abc", page=4),
        state="RETRYABLE",
        pages_delta=3,
    )
    store.connection.close()

    store2 = ControlStore(tmp_path / "control.sqlite")
    reg2 = ResearchRegistry(store2, clock=lambda: 200.0)
    checkpoint = reg2.query_checkpoint(query.query_id)
    assert checkpoint is not None
    assert checkpoint.cursor == "abc"
    assert checkpoint.page == 4


def test_stale_lease_reclaim(tmp_path):
    _, reg, _, _ = setup_registry(tmp_path)
    task_id = reg.enqueue_frontier(
        FrontierTask("t1", "QUERY", "q1", priority=1.0)
    )
    claimed = reg.claim_frontier(owner="worker", lease_seconds=10, now=100)
    assert claimed is not None
    assert claimed.task_id == task_id
    assert claimed.state is FrontierState.CLAIMED
    assert reg.reclaim_stale_leases(now=111) == 1
    assert reg.get_frontier(task_id).state is FrontierState.READY


def test_final_reward_idempotent_and_lineage_closure(tmp_path):
    _, reg, query, program = setup_registry(tmp_path)
    node = reg.upsert_hit(
        SearchHit(
            "datacite", query.query_id, "10.555/x",
            "https://doi.org/10.555/x", "DOI",
        )
    )
    lead = ArtifactLead(
        "datacite", "f1", "https://repo.example/f1.cdx.gz",
        checksum="sha256:123", query_id=query.query_id,
        program_id=program.program_id, source_node_id=node.node_id,
        pivot_id="pivot:1",
    )
    artifact_id, _, _ = reg.register_artifact_lead(lead)
    assert reg.bind_artifact_source(
        artifact_id, source_key="src:1", source_exposure_id="exp:1"
    ) >= 1

    first = reg.close_final_reward(
        source_key="src:1", exposure_id="exp:1", final_eed=7.5,
        validation_closed=True, idempotency_token="close:1",
    )
    second = reg.close_final_reward(
        source_key="src:1", exposure_id="exp:1", final_eed=7.5,
        validation_closed=True, idempotency_token="close:1",
    )
    assert first >= 5
    assert second == 0
    scopes = {reward.scope for reward in reg.rewards(kind=RewardKind.FINAL)}
    assert {
        RewardScope.SOURCE, RewardScope.ARTIFACT, RewardScope.QUERY,
        RewardScope.PROGRAM, RewardScope.ROOT, RewardScope.PIVOT,
    } <= scopes


def test_final_reward_requires_validation_closed(tmp_path):
    _, reg, _, _ = setup_registry(tmp_path)
    with pytest.raises(ValueError):
        reg.close_final_reward(
            source_key="src:1", exposure_id="e", final_eed=1.0,
            validation_closed=False,
        )


def test_policy_schema_rebuild_preserves_immutable_rewards(tmp_path):
    _, reg, _, _ = setup_registry(tmp_path)
    reg.enqueue_frontier(
        FrontierTask("t1", "QUERY", "q1", policy_version="p1")
    )
    reg.record_decision(
        DecisionRecord("d1", "t1", "arm:a", "p1", "snap:1", 0.5, "ctx", 100.0)
    )
    reg.record_reward(
        RewardRecord(
            "r1", "PROXY", "QUERY", "q1", 2.0,
            decision_id="d1", policy_version="p1", idempotency_key="r1",
        )
    )
    reg.record_reward(
        RewardRecord(
            "r2", "FINAL", "QUERY", "q1", 3.0,
            decision_id="d1", policy_version="p1",
            validation_closed=True, idempotency_key="r2",
        )
    )
    stats = reg.rebuild_arm_stats(policy_version="p1", schema_version=2)
    assert stats[0].pulls == 1
    assert stats[0].proxy_reward == 2.0
    assert stats[0].final_reward == 3.0
    assert stats[0].schema_version == 2
    assert len(reg.rewards(policy_version="p1")) == 2


def test_resume_rebuilds_missing_derived_stats(tmp_path):
    _, reg, _, _ = setup_registry(tmp_path)
    reg.enqueue_frontier(
        FrontierTask("t1", "QUERY", "q1", policy_version="p1")
    )
    reg.record_decision(
        DecisionRecord("d1", "t1", "arm:a", "p1", "snap:1", 1.0, "ctx", 100.0)
    )
    report = ResearchResumeManager(reg).recover(
        policy_version="p1", expected_schema_version=3, now=100.0
    )
    assert report.rebuilt_policy_version == "p1"
    assert reg.arm_stats(policy_version="p1")[0].schema_version == 3


def test_hit_heavy_no_artifact_loses_ranking():
    noisy = ResearchYield(hits=5000, resolved_artifacts=0, requests=10)
    useful = ResearchYield(
        hits=20, resolved_artifacts=4, productive_sources=1, requests=10
    )
    assert useful.score() > noisy.score()


def test_incomplete_pagination_is_not_negative_knowledge():
    with pytest.raises(ValueError):
        make_negative_knowledge(
            root_id="datacite", scope_kind="QUERY", scope_key="q1",
            reason="empty page", exhaustive=False, pagination_complete=False,
        )


def test_resolution_produces_lead_not_evidence():
    node = reg_node = None
    hit = SearchHit(
        "zenodo", "q", "123", "https://zenodo.org/records/123", "RECORD",
        metadata={"files": [{"key": "x.cdx.gz", "url": "https://x.test/x.cdx.gz"}]},
    )
    from creeper.source_research.models import ResearchNode
    node = ResearchNode.from_hit(hit)
    result = resolve_node(node, query_id="q", program_id="p")
    assert len(result.artifact_leads) == 1
    assert result.artifact_leads[0].evidence_year is None
