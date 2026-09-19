"""Protocol identity for Creeper Fabric v2."""

from __future__ import annotations

FABRIC_EDITION_NAME = "Creeper Fabric"
FABRIC_EDITION_VERSION = "2.0.0-dev"
FABRIC_PROTOCOL_VERSION = "creeper-fabric-v2"
FABRIC_AUTHORITY_SCHEMA_VERSION = 2


def edition_metadata() -> dict[str, str | int]:
    return {
        "edition": FABRIC_EDITION_NAME,
        "edition_version": FABRIC_EDITION_VERSION,
        "protocol_version": FABRIC_PROTOCOL_VERSION,
        "authority_schema_version": FABRIC_AUTHORITY_SCHEMA_VERSION,
    }
