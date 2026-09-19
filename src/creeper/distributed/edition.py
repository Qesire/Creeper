"""Independent edition identity and compatibility contract for Creeper Fabric."""

from __future__ import annotations

FABRIC_EDITION_NAME = "Creeper Fabric"
FABRIC_EDITION_VERSION = "0.1.0-dev"
FABRIC_PROTOCOL_VERSION = "creeper-fabric-v1"
FABRIC_AUTHORITY_SCHEMA_VERSION = 1

# Edge/thin runtimes are opportunistic positive-evidence producers only.
FABRIC_THIN_MAX_PROVIDER_REQUESTS = 1
FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES = 256 * 1024

# Production exploration keeps raw URLs/hostnames/frontier state task-local.
# The Local Authority receives only HY admission traffic plus empty checkpoints.
FABRIC_EVIDENCE_ONLY_PRODUCERS = frozenset(
    {
        "HistoricalCrawlerProducer",
        "SeededExplorationProducer",
    }
)

# The derivative may reuse these stable, mostly dependency-free core primitives.
# Any new dependency on the monolithic runtime must be reviewed explicitly.
ALLOWED_CORE_IMPORTS = frozenset(
    {
        "creeper.authority.baseline_index",
        "creeper.authority.identity",
        "creeper.authority.normalizer",
        "creeper.evidence.contracts",
        "creeper.evidence.policies",
        "creeper.evidence.providers.async_cdx",
        "creeper.evidence.providers.multi_cdx",
        "creeper.records.models",
        "creeper.runtime.http",
        "creeper.scheduler.leases",
        "creeper.sources.production",
        "creeper.sources.reservoirs",
    }
)


def edition_metadata() -> dict[str, str | int]:
    return {
        "edition": FABRIC_EDITION_NAME,
        "edition_version": FABRIC_EDITION_VERSION,
        "protocol_version": FABRIC_PROTOCOL_VERSION,
        "authority_schema_version": FABRIC_AUTHORITY_SCHEMA_VERSION,
    }
