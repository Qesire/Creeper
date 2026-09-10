"""Durable source-discovery control plane."""

from creeper.source_discovery.admission import SearchAdmissionPolicy
from creeper.source_discovery.agent_search import (
    CommandAgentSearchExecutor,
    CommandAgentSearchPolicy,
    SearchAgentProtocolError,
)
from creeper.source_discovery.coordinator import (
    CoordinatorBusyError,
    CoordinatorCycleReport,
    ScoutDisposition,
    ScoutResult,
    SearchBatch,
    SourceDiscoveryCoordinator,
    TriageDisposition,
    TriageResult,
)
from creeper.source_discovery.expander import ExpansionResult, expand_scrapy_spool
from creeper.source_discovery.manager import (
    ReservoirPlan,
    SearchDirective,
    SearchDirectiveKind,
    SourcePoolTargets,
    SourceReservoirManager,
)
from creeper.source_discovery.measured_scout import (
    MeasuredYieldScoutExecutor,
    MeasuredYieldScoutPolicy,
)
from creeper.source_discovery.models import (
    MeasurementMode,
    ScoutMeasurement,
    SearchEpisode,
    SourceCandidate,
    SourceLevel,
    SourceState,
    StrategyReward,
    SuppressionScope,
    canonicalize_source_entrypoint,
    source_key,
    source_origin,
)
from creeper.source_discovery.promotion import (
    LinkPromotionAccumulator,
    PromotedSource,
    PromotionEvidence,
    PromotionPolicy,
)
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.scout_router import SourceScoutRouter, SourceScoutRouterPolicy
from creeper.source_discovery.scrapy_scout import (
    ScrapyStructuralScoutExecutor,
    ScrapyStructuralScoutPolicy,
)
from creeper.source_discovery.scrapy_sidecar import (
    ScrapyLinkDiscovery,
    ScrapyScoutLauncher,
    ScrapyScoutRun,
    ScrapyScoutSpec,
    iter_scrapy_link_discoveries,
    prepare_append_spool,
    prepare_jobdir_binding,
)
from creeper.source_discovery.triage import (
    HttpSourceTriageExecutor,
    HttpTriagePolicy,
    TriageTransientError,
)

__all__ = [
    "CommandAgentSearchExecutor",
    "CommandAgentSearchPolicy",
    "CoordinatorBusyError",
    "CoordinatorCycleReport",
    "ExpansionResult",
    "HttpSourceTriageExecutor",
    "HttpTriagePolicy",
    "LinkPromotionAccumulator",
    "MeasuredYieldScoutExecutor",
    "MeasuredYieldScoutPolicy",
    "MeasurementMode",
    "PromotedSource",
    "PromotionEvidence",
    "PromotionPolicy",
    "ReservoirPlan",
    "ScoutDisposition",
    "ScoutMeasurement",
    "ScoutResult",
    "ScrapyLinkDiscovery",
    "ScrapyScoutLauncher",
    "ScrapyScoutRun",
    "ScrapyScoutSpec",
    "ScrapyStructuralScoutExecutor",
    "ScrapyStructuralScoutPolicy",
    "SearchAdmissionPolicy",
    "SearchAgentProtocolError",
    "SearchBatch",
    "SearchDirective",
    "SearchDirectiveKind",
    "SearchEpisode",
    "SourceCandidate",
    "SourceDiscoveryCoordinator",
    "SourceDiscoveryRegistry",
    "SourceLevel",
    "SourcePoolTargets",
    "SourceReservoirManager",
    "SourceScoutRouter",
    "SourceScoutRouterPolicy",
    "SourceState",
    "StrategyReward",
    "SuppressionScope",
    "TriageDisposition",
    "TriageResult",
    "TriageTransientError",
    "canonicalize_source_entrypoint",
    "expand_scrapy_spool",
    "iter_scrapy_link_discoveries",
    "prepare_append_spool",
    "prepare_jobdir_binding",
    "source_key",
    "source_origin",
]
