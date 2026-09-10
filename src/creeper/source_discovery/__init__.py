"""Durable source-discovery control plane."""

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
from creeper.source_discovery.registry import SourceDiscoveryRegistry
from creeper.source_discovery.scrapy_sidecar import (
    ScrapyLinkDiscovery,
    ScrapyScoutLauncher,
    ScrapyScoutRun,
    ScrapyScoutSpec,
    iter_scrapy_link_discoveries,
    prepare_jobdir_binding,
)

__all__ = [
    "ReservoirPlan",
    "ScoutMeasurement",
    "ScrapyLinkDiscovery",
    "ScrapyScoutLauncher",
    "ScrapyScoutRun",
    "ScrapyScoutSpec",
    "SearchDirective",
    "SearchDirectiveKind",
    "SearchEpisode",
    "SourceCandidate",
    "SourceDiscoveryRegistry",
    "SourceLevel",
    "SourcePoolTargets",
    "SourceReservoirManager",
    "SourceState",
    "StrategyReward",
    "SuppressionScope",
    "canonicalize_source_entrypoint",
    "iter_scrapy_link_discoveries",
    "prepare_jobdir_binding",
    "source_key",
    "source_origin",
]
