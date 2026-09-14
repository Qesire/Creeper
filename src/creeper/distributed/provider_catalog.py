"""Canonical physical provider identities used by Fabric deployment profiles."""

from __future__ import annotations

from creeper.evidence.providers.multi_cdx import CDXProviderConfig


_CANONICAL_CDX_PROVIDERS: dict[str, CDXProviderConfig] = {
    "internet_archive": CDXProviderConfig(
        name="internet_archive",
        endpoint="https://web.archive.org/cdx/search/cdx",
        dialect="wayback",
        requests_per_second=1.0,
        max_inflight=4,
        max_connections=8,
        max_keepalive_connections=4,
        keepalive_expiry_seconds=20.0,
        row_limit=150_000,
        weight=1.0,
    ),
    "arquivo_pt": CDXProviderConfig(
        name="arquivo_pt",
        endpoint="https://arquivo.pt/wayback/cdx",
        dialect="arquivo",
        requests_per_second=1.0,
        max_inflight=4,
        max_connections=8,
        max_keepalive_connections=4,
        keepalive_expiry_seconds=20.0,
        row_limit=100_000,
        weight=1.0,
    ),
}


def canonical_cdx_provider_configs(
    names: tuple[str, ...],
) -> tuple[CDXProviderConfig, ...]:
    if not names:
        raise ValueError("at least one canonical CDX provider is required")
    selected = {
        raw_name.strip()
        for raw_name in names
        if raw_name.strip()
    }
    if not selected:
        raise ValueError("no canonical CDX providers selected")
    unknown = selected - set(_CANONICAL_CDX_PROVIDERS)
    if unknown:
        raise ValueError(
            "unknown canonical CDX provider: "
            + ",".join(sorted(unknown))
        )
    return tuple(
        config
        for name, config in _CANONICAL_CDX_PROVIDERS.items()
        if name in selected
    )
