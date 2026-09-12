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
from creeper.source_discovery.harvest import (
    RegionHarvestError,
    RegionHarvestExecutor,
    RegionHarvestPolicy,
    RegionHarvestReport,
)
from creeper.source_discovery.harvest_service import (
    RegionHarvestService,
    RegionHarvestServiceReport,
)
from creeper.source_discovery.index_registry import IndexSpaceRegistry
from creeper.source_discovery.index_space import (
    CompiledIndexSpace,
    HarvestRegion,
    QueryCapabilityHints,
    RegionKind,
    RegionState,
    RegionSynopsis,
    SourceAccessMode,
    SourceCapabilities,
    SourceFactorySpec,
    SourceIndexSpec,
    child_region,
    compile_candidate_index_space,
)
from creeper.source_discovery.portfolio import (
    RegionPortfolioEstimate,
    RegionPortfolioPlan,
    RegionPortfolioPlanner,
    RegionPortfolioPolicy,
)
from creeper.source_discovery.region_probe import (
    RegionProbeError,
    RegionProbeExecutor,
    RegionProbePolicy,
    RegionProbeResult,
    SampledByteRange,
)
from creeper.source_discovery.tomography_service import (
    RegionTomographyReport,
    RegionTomographyService,
)
from creeper.source_discovery.tomography import (
    RegionTomographyPlanner,
    RegionTomographyPolicy,
    TomographyAction,
    TomographyActionKind,
    region_size_bytes,
    split_byte_region,
)
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
    "RegionHarvestError",
    "RegionHarvestExecutor",
    "RegionHarvestPolicy",
    "RegionHarvestReport",
    "RegionHarvestService",
    "RegionHarvestServiceReport",
    "LinkPromotionAccumulator",
    "CompiledIndexSpace",
    "HarvestRegion",
    "IndexSpaceRegistry",
    "QueryCapabilityHints",
    "RegionPortfolioEstimate",
    "RegionPortfolioPlan",
    "RegionPortfolioPlanner",
    "RegionPortfolioPolicy",
    "RegionKind",
    "RegionState",
    "RegionProbeError",
    "RegionProbeExecutor",
    "RegionProbePolicy",
    "RegionProbeResult",
    "RegionTomographyPlanner",
    "RegionTomographyPolicy",
    "RegionTomographyReport",
    "RegionTomographyService",
    "RegionSynopsis",
    "SampledByteRange",
    "SourceAccessMode",
    "TomographyAction",
    "TomographyActionKind",
    "SourceCapabilities",
    "SourceFactorySpec",
    "SourceIndexSpec",
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
    "child_region",
    "compile_candidate_index_space",
    "region_size_bytes",
    "split_byte_region",
    "expand_scrapy_spool",
    "iter_scrapy_link_discoveries",
    "prepare_append_spool",
    "prepare_jobdir_binding",
    "source_key",
    "source_origin",
]
