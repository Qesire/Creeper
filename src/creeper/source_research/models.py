"""Stable models and canonical identities for the V7 research state plane.

This module is evidence-authority free: root/search metadata and LLM proposals
are observations only and cannot become annual Web evidence here.
"""
from __future__ import annotations

import hashlib
import json
import posixpath
import re
import unicodedata
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping
from urllib.parse import SplitResult, urlsplit, urlunsplit

RESEARCH_SCHEMA_VERSION = 2


class RootKind(StrEnum):
    STRUCTURED_REPOSITORY = "STRUCTURED_REPOSITORY"
    ARCHIVE = "ARCHIVE"
    OAI = "OAI"
    CODE = "CODE"
    GENERIC = "GENERIC"


class ResearchNodeKind(StrEnum):
    ROOT = "ROOT"
    DATASET = "DATASET"
    REPOSITORY = "REPOSITORY"
    CODE = "CODE"
    COLLECTION = "COLLECTION"
    ARTIFACT = "ARTIFACT"
    RECORD = "RECORD"
    FILE = "FILE"
    DOI = "DOI"
    OTHER = "OTHER"


class QueryState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    RETRYABLE = "RETRYABLE"
    BLOCKED = "BLOCKED"
    COMPLETE = "COMPLETE"
    EXHAUSTED = "EXHAUSTED"
    FAILED = "FAILED"


class FrontierState(StrEnum):
    READY = "READY"
    CLAIMED = "CLAIMED"
    RETRYABLE = "RETRYABLE"
    BLOCKED = "BLOCKED"
    EXHAUSTED = "EXHAUSTED"
    DONE = "DONE"


class RewardKind(StrEnum):
    PROXY = "PROXY"
    FINAL = "FINAL"


class RewardScope(StrEnum):
    SOURCE = "SOURCE"
    ARTIFACT = "ARTIFACT"
    QUERY = "QUERY"
    PROGRAM = "PROGRAM"
    ROOT = "ROOT"
    PIVOT = "PIVOT"
    DECISION = "DECISION"


class RuleState(StrEnum):
    CANDIDATE = "CANDIDATE"
    VALIDATED = "VALIDATED"
    DISABLED = "DISABLED"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(prefix: str, *parts: Any) -> str:
    return prefix + ":" + hashlib.sha256(_canonical_json(parts).encode("utf-8")).hexdigest()


def normalize_query_text(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("query text must be non-empty")
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def normalize_doi(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("DOI must be non-empty")
    value = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", value.strip(), flags=re.I)
    value = re.sub(r"^doi:\s*", "", value, flags=re.I).strip().casefold()
    if not value or "/" not in value:
        raise ValueError("invalid DOI")
    return value


def canonicalize_locator(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("locator must be non-empty")
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"}:
        return value.strip()
    if parsed.hostname is None:
        raise ValueError("HTTP locator requires a hostname")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("locator must not contain userinfo")
    try:
        host = parsed.hostname.encode("idna").decode("ascii").casefold()
        port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ValueError("invalid locator") from exc
    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        host = host + ":" + str(port)
    raw_path = parsed.path or "/"
    path = posixpath.normpath(raw_path)
    if not path.startswith("/"):
        path = "/" + path
    if raw_path.endswith("/") and path != "/" and not path.endswith("/"):
        path += "/"
    return urlunsplit(SplitResult(scheme, host, path, parsed.query, ""))


def canonical_seed_identity(
    root_id: str,
    seed_library_version: str,
    query_text: str,
    native_filters: Mapping[str, Any] | None = None,
) -> str:
    return stable_hash(
        "seed",
        root_id.strip(),
        seed_library_version.strip(),
        normalize_query_text(query_text),
        dict(native_filters or {}),
    )


def _metadata_doi(metadata: Mapping[str, Any]) -> str | None:
    for key in ("doi", "DOI", "persistent_id", "global_id"):
        raw = metadata.get(key)
        if isinstance(raw, str) and "10." in raw and "/" in raw:
            try:
                return normalize_doi(raw)
            except ValueError:
                pass
    return None


def canonical_node_key(
    *,
    root_id: str,
    provider_native_id: str,
    provider_type: str,
    provider_url: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    metadata = metadata or {}
    ptype = str(provider_type or "").strip().upper()
    native = str(provider_native_id or "").strip()
    doi = _metadata_doi(metadata)
    if ptype == "DOI" or native.casefold().startswith("10."):
        try:
            doi = normalize_doi(native)
        except ValueError:
            pass
    if doi:
        return "doi:" + doi

    root_cf = root_id.casefold()
    if root_cf.startswith("zenodo"):
        concept = metadata.get("conceptrecid") or metadata.get("concept_id")
        version = metadata.get("version")
        record = metadata.get("record_id") or native
        if concept:
            return "zenodo:concept:" + str(concept).strip() + "|version:" + str(version or record).strip()
        return "zenodo:record:" + str(record).strip()
    if root_cf.startswith("dataverse"):
        pid = metadata.get("persistent_id") or metadata.get("global_id") or native
        return "dataverse:" + str(pid).strip().casefold()
    if root_cf.startswith("archive-it") or root_cf.startswith("archiveit"):
        return "archive-it:" + str(metadata.get("collection_id") or native).strip()
    if root_cf.startswith("github"):
        owner = str(metadata.get("owner") or "").strip().casefold()
        repo = str(metadata.get("repo") or "").strip().casefold()
        path = str(metadata.get("path") or "").strip()
        revision = str(metadata.get("revision") or metadata.get("sha") or "").strip()
        if owner and repo:
            return "github:" + owner + "/" + repo + ":" + path + "@" + revision
    if root_cf.startswith("oai"):
        base = metadata.get("base_url") or provider_url or native
        if base:
            return "oai:" + canonicalize_locator(str(base))
    if native:
        return stable_hash("node", root_id.strip(), ptype, native)
    if provider_url:
        return "url:" + canonicalize_locator(provider_url)
    raise ValueError("node identity requires a native id or provider URL")


def canonical_artifact_identity(
    *,
    locator: str,
    checksum: str | None = None,
    persistent_id: str | None = None,
    immutable_identity: str | None = None,
    size: int | None = None,
) -> str:
    if immutable_identity and immutable_identity.strip():
        return "immutable:" + immutable_identity.strip().casefold()
    if checksum and checksum.strip():
        return "checksum:" + checksum.strip().casefold()
    canonical = canonicalize_locator(locator)
    if persistent_id and persistent_id.strip():
        basename = posixpath.basename(urlsplit(canonical).path).casefold()
        return stable_hash("artifact-pid", persistent_id.strip().casefold(), basename, size)
    return "locator:" + canonical


@dataclass(frozen=True)
class RootSurface:
    root_id: str
    kind: RootKind | str
    canonical_locator: str
    capabilities: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.root_id.strip():
            raise ValueError("root_id is required")
        object.__setattr__(self, "kind", RootKind(self.kind))
        object.__setattr__(self, "canonical_locator", canonicalize_locator(self.canonical_locator))
        object.__setattr__(self, "capabilities", tuple(str(v) for v in self.capabilities))
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True, init=False)
class RootQuery:
    root_id: str
    query_text: str
    max_pages: int
    max_wall_seconds: float
    page_size: int
    native_filters: Mapping[str, Any]
    expected_signal: str
    expected_artifact_family: str
    seed_library_version: str
    query_id: str

    def __init__(
        self,
        root_id: str = "",
        query_text: str = "",
        max_pages: int = 1,
        max_wall_seconds: float = 60.0,
        page_size: int = 100,
        native_filters: Mapping[str, Any] | None = None,
        expected_signal: str = "",
        expected_artifact_family: str = "",
        seed_library_version: str = "seed-v1",
        query_id: str = "",
        *,
        query: str | None = None,
        filters: Mapping[str, Any] | None = None,
        expected_family: str | None = None,
    ) -> None:
        if query is not None and not query_text:
            query_text = query
        if filters is not None and native_filters is None:
            native_filters = filters
        if expected_family is not None and not expected_artifact_family:
            expected_artifact_family = expected_family
        if not root_id.strip():
            raise ValueError("root_id is required")
        if max_pages < 1 or max_wall_seconds <= 0 or page_size < 1:
            raise ValueError("query bounds must be positive")
        normalized = normalize_query_text(query_text)
        native = dict(native_filters or {})
        qid = query_id or stable_hash("query", root_id.strip(), normalized, native)
        object.__setattr__(self, "root_id", root_id.strip())
        object.__setattr__(self, "query_text", query_text.strip())
        object.__setattr__(self, "max_pages", int(max_pages))
        object.__setattr__(self, "max_wall_seconds", float(max_wall_seconds))
        object.__setattr__(self, "page_size", int(page_size))
        object.__setattr__(self, "native_filters", native)
        object.__setattr__(self, "expected_signal", str(expected_signal))
        object.__setattr__(self, "expected_artifact_family", str(expected_artifact_family))
        object.__setattr__(self, "seed_library_version", str(seed_library_version))
        object.__setattr__(self, "query_id", qid)

    @property
    def query(self) -> str:
        return self.query_text

    @property
    def filters(self) -> Mapping[str, Any]:
        return self.native_filters

    @property
    def expected_family(self) -> str:
        return self.expected_artifact_family

    @property
    def query_hash(self) -> str:
        return stable_hash("query-hash", normalize_query_text(self.query_text), dict(self.native_filters))

    @property
    def seed_identity(self) -> str:
        return canonical_seed_identity(
            self.root_id,
            self.seed_library_version,
            self.query_text,
            self.native_filters,
        )


@dataclass(frozen=True)
class QueryProgram:
    root_id: str
    strategy: str
    queries: tuple[RootQuery, ...]
    hard_max_requests: int
    stop_conditions: tuple[str, ...]
    seed_library_version: str = "seed-v1"
    compiler_version: str = "deterministic-seed-v1"
    context_hash: str = ""
    program_id: str = ""
    source: str = "SEED"

    def __post_init__(self) -> None:
        if not self.root_id.strip() or not self.strategy.strip():
            raise ValueError("root_id and strategy are required")
        if self.hard_max_requests < 1:
            raise ValueError("hard_max_requests must be positive")
        queries = tuple(self.queries)
        for query in queries:
            if query.root_id != self.root_id:
                raise ValueError("program/query root mismatch")
        object.__setattr__(self, "queries", queries)
        object.__setattr__(self, "stop_conditions", tuple(self.stop_conditions))
        if not self.program_id:
            object.__setattr__(
                self,
                "program_id",
                stable_hash(
                    "program",
                    self.root_id,
                    self.strategy,
                    [q.seed_identity for q in queries],
                    self.context_hash,
                    self.compiler_version,
                ),
            )


RootQueryProgram = QueryProgram


@dataclass(frozen=True)
class SearchCheckpoint:
    cursor: str | None = None
    next_url: str | None = None
    page: int = 1
    start: int = 0
    query_variant: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "cursor": self.cursor,
            "next_url": self.next_url,
            "page": self.page,
            "start": self.start,
            "query_variant": self.query_variant,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any] | None) -> "SearchCheckpoint | None":
        if not value:
            return None
        return cls(
            cursor=value.get("cursor"),
            next_url=value.get("next_url"),
            page=int(value.get("page", 1)),
            start=int(value.get("start", 0)),
            query_variant=value.get("query_variant"),
        )


@dataclass(frozen=True)
class SearchHit:
    root_id: str
    query_id: str
    provider_native_id: str
    provider_url: str
    provider_type: str
    title: str = ""
    description: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)
    observed_at: float = 0.0
    research_node_id: str = ""

    @property
    def canonical_key(self) -> str:
        return canonical_node_key(
            root_id=self.root_id,
            provider_native_id=self.provider_native_id,
            provider_type=self.provider_type,
            provider_url=self.provider_url,
            metadata=self.metadata,
        )


@dataclass(frozen=True)
class ResearchNode:
    node_id: str
    canonical_key: str
    kind: ResearchNodeKind | str
    root_id: str
    provider_native_id: str
    provider_url: str = ""
    title: str = ""
    description: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ResearchNodeKind(self.kind))
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_hit(cls, hit: SearchHit) -> "ResearchNode":
        kind_map = {
            "DATASET": ResearchNodeKind.DATASET,
            "REPOSITORY": ResearchNodeKind.REPOSITORY,
            "CODE": ResearchNodeKind.CODE,
            "COLLECTION": ResearchNodeKind.COLLECTION,
            "ARTIFACT": ResearchNodeKind.ARTIFACT,
            "FILE": ResearchNodeKind.FILE,
            "DOI": ResearchNodeKind.DOI,
            "RECORD": ResearchNodeKind.RECORD,
        }
        key = hit.canonical_key
        return cls(
            node_id=hit.research_node_id or stable_hash("research-node", key),
            canonical_key=key,
            kind=kind_map.get(hit.provider_type.upper(), ResearchNodeKind.OTHER),
            root_id=hit.root_id,
            provider_native_id=hit.provider_native_id,
            provider_url=hit.provider_url,
            title=hit.title,
            description=hit.description,
            metadata=dict(hit.metadata),
        )


@dataclass(frozen=True)
class ResearchEdge:
    from_node_id: str
    to_node_id: str
    relation: str
    root_id: str = ""
    query_id: str = ""
    pivot_id: str = ""
    observed_at: float = 0.0
    edge_id: str = ""

    def __post_init__(self) -> None:
        if not self.edge_id:
            object.__setattr__(
                self,
                "edge_id",
                stable_hash(
                    "research-edge",
                    self.from_node_id,
                    self.to_node_id,
                    self.relation,
                    self.query_id,
                    self.pivot_id,
                ),
            )


@dataclass(frozen=True)
class PivotAction:
    node_id: str
    pivot_kind: str
    payload: Mapping[str, Any]
    root_id: str = ""
    pivot_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", dict(self.payload))
        if not self.pivot_id:
            object.__setattr__(
                self,
                "pivot_id",
                stable_hash("pivot", self.node_id, self.pivot_kind, dict(self.payload)),
            )


@dataclass(frozen=True)
class ArtifactLead:
    root_id: str
    provider_native_id: str
    locator: str
    content_type: str = ""
    size: int | None = None
    checksum: str | None = None
    persistent_id: str | None = None
    parent_persistent_id: str | None = None
    kind: str = "ARTIFACT_LEAD"
    evidence_year: None = None
    immutable_identity: str | None = None
    source_node_id: str = ""
    query_id: str = ""
    program_id: str = ""
    pivot_id: str = ""
    decision_id: str = ""

    def __post_init__(self) -> None:
        if self.evidence_year is not None:
            raise ValueError("research metadata cannot assert annual evidence")
        object.__setattr__(self, "locator", canonicalize_locator(self.locator))
        if self.size is not None and self.size < 0:
            raise ValueError("artifact size must be non-negative")

    @property
    def artifact_identity(self) -> str:
        return canonical_artifact_identity(
            locator=self.locator,
            checksum=self.checksum,
            persistent_id=self.persistent_id,
            immutable_identity=self.immutable_identity,
            size=self.size,
        )

    @property
    def artifact_id(self) -> str:
        return stable_hash("artifact", self.artifact_identity)


@dataclass(frozen=True)
class NewRootLead:
    entrypoint: str
    kind: RootKind | str
    discovered_from_node_id: str
    capabilities: tuple[str, ...] = ()
    rationale: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", RootKind(self.kind))
        object.__setattr__(self, "entrypoint", canonicalize_locator(self.entrypoint))


@dataclass(frozen=True)
class FrontierTask:
    task_id: str
    task_kind: str
    entity_id: str
    state: FrontierState | str = FrontierState.READY
    priority: float = 0.0
    checkpoint: Mapping[str, Any] = field(default_factory=dict)
    attempt: int = 0
    retry_at: float | None = None
    lease_owner: str | None = None
    lease_until: float | None = None
    policy_version: str = ""
    schema_version: int = RESEARCH_SCHEMA_VERSION
    last_error: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", FrontierState(self.state))
        object.__setattr__(self, "checkpoint", dict(self.checkpoint))
        if self.attempt < 0:
            raise ValueError("attempt must be non-negative")


@dataclass(frozen=True)
class DecisionRecord:
    decision_id: str
    task_id: str
    arm_id: str
    policy_version: str
    policy_snapshot_id: str
    propensity: float
    context_hash: str
    chosen_at: float
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 < float(self.propensity) <= 1.0:
            raise ValueError("decision propensity must be in (0, 1]")
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def parent_decision_ids(self) -> tuple[str, ...]:
        """Return ordered hierarchical parents recorded by L7.

        Current L7 persists the ancestry inside
        metadata["context_features"]["parent_decision_ids"].  Accept a direct
        top-level key as well so the L3 retrieval contract remains stable if the
        writer is later simplified.
        """
        raw = self.metadata.get("parent_decision_ids")
        if raw is None:
            context = self.metadata.get("context_features", {})
            raw = context.get("parent_decision_ids", ()) if isinstance(context, Mapping) else ()
        if not isinstance(raw, (list, tuple)):
            raise ValueError("parent_decision_ids must be an ordered array")
        result: list[str] = []
        seen: set[str] = set()
        for value in raw:
            decision_id = str(value).strip()
            if not decision_id or decision_id == self.decision_id or decision_id in seen:
                continue
            seen.add(decision_id)
            result.append(decision_id)
        return tuple(result)


@dataclass(frozen=True)
class PolicySnapshot:
    snapshot_id: str
    policy_version: str
    schema_version: int
    parameters: Mapping[str, Any]
    created_at: float
    active: bool = False


@dataclass(frozen=True)
class ArmStats:
    arm_id: str
    policy_version: str
    pulls: int
    proxy_reward: float
    final_reward: float
    decayed_reward: float
    schema_version: int
    updated_at: float
    final_observation_count: int = 0

    @property
    def has_final(self) -> bool:
        return self.final_observation_count > 0

    @property
    def authoritative_reward(self) -> float:
        return self.final_reward if self.has_final else self.proxy_reward


@dataclass(frozen=True)
class RuleRecord:
    rule_id: str
    rule_kind: str
    version: str
    context_hash: str
    deterministic_payload: Mapping[str, Any]
    state: RuleState | str = RuleState.CANDIDATE
    validation_digest: str = ""
    reuse_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", RuleState(self.state))
        object.__setattr__(self, "deterministic_payload", dict(self.deterministic_payload))


@dataclass(frozen=True)
class LearningEpoch:
    epoch_id: str
    policy_version: str
    schema_version: int
    state: str = "OPEN"
    checkpoint: Mapping[str, Any] = field(default_factory=dict)
    notes: str = ""


@dataclass(frozen=True)
class RewardRecord:
    reward_id: str
    kind: RewardKind | str
    scope: RewardScope | str
    entity_id: str
    amount: float
    source_key: str = ""
    exposure_id: str = ""
    decision_id: str = ""
    validation_closed: bool = False
    policy_version: str = ""
    idempotency_key: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", RewardKind(self.kind))
        object.__setattr__(self, "scope", RewardScope(self.scope))
        if self.kind is RewardKind.FINAL and not self.validation_closed:
            raise ValueError("FINAL reward requires closed validation")
        if not self.idempotency_key:
            object.__setattr__(
                self,
                "idempotency_key",
                stable_hash(
                    "reward-key",
                    self.kind.value,
                    self.scope.value,
                    self.entity_id,
                    self.source_key,
                    self.exposure_id,
                    self.decision_id,
                ),
            )


@dataclass(frozen=True)
class NegativeKnowledge:
    negative_id: str
    root_id: str
    scope_kind: str
    scope_key: str
    reason: str
    exhaustive: bool
    context_hash: str = ""


__all__ = [
    "RESEARCH_SCHEMA_VERSION", "ArmStats", "ArtifactLead", "DecisionRecord",
    "FrontierState", "FrontierTask", "LearningEpoch", "NegativeKnowledge",
    "NewRootLead", "PivotAction", "PolicySnapshot", "QueryProgram", "QueryState",
    "ResearchEdge", "ResearchNode", "ResearchNodeKind", "RewardKind",
    "RewardRecord", "RewardScope", "RootKind", "RootQuery", "RootQueryProgram",
    "RootSurface", "RuleRecord", "RuleState", "SearchCheckpoint", "SearchHit",
    "canonical_artifact_identity", "canonical_node_key",
    "canonical_seed_identity", "canonicalize_locator", "normalize_doi",
    "normalize_query_text", "stable_hash",
]
