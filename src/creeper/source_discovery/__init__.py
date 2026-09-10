"""Durable source-discovery control plane."""

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
from creeper.source_discovery.models import (
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
from creeper.source_discovery.scrapy_sidecar import (
    ScrapyLinkDiscovery,
    ScrapyScoutLauncher,
    ScrapyScoutRun,
    ScrapyScoutSpec,
    iter_scrapy_link_discoveries,
    prepare_append_spool,
    prepare_jobdir_binding,
)

__all__ = [
    "CoordinatorBusyError",
    "CoordinatorCycleReport",
    "ExpansionResult",
    "LinkPromotionAccumulator",
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
    "SourceState",
    "StrategyReward",
    "SuppressionScope",
    "TriageDisposition",
    "TriageResult",
    "canonicalize_source_entrypoint",
    "expand_scrapy_spool",
    "iter_scrapy_link_discoveries",
    "prepare_append_spool",
    "prepare_jobdir_binding",
    "source_key",
    "source_origin",
]
