"""Competition-aware domain amplification planning.

This module deliberately stays conservative: it only derives a parent domain
from simple non-ccTLD hostnames, requires the root hostname itself to be
observed in the same source batch, and emits at most one bounded Wayback domain
probe per qualified parent. The resulting provider query may return evidence
for many concrete hostnames, but never grants negative coverage.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceQueryKey, TemporalScope

DOMAIN_AMPLIFICATION_POLICY_VERSION = "domain-amplification-v1"


def amplification_parent(hostname: str) -> str | None:
    """Return a conservative parent suitable for matchType=domain.

    Country-code public suffixes are skipped entirely rather than guessing PSL
    semantics. This keeps the first production implementation fail-closed.
    """
    normalized = normalize_official(hostname)
    if normalized is None:
        return None
    labels = normalized.split(".")
    if len(labels) < 3:
        return None
    tld = labels[-1]
    if len(tld) == 2:
        return None
    return ".".join(labels[-2:])


def plan_domain_amplification(
    hostnames: Iterable[str],
    *,
    provider: str,
    min_distinct_hosts: int = 4,
) -> tuple[EvidenceQueryKey, ...]:
    """Create one bounded 1996-2001 domain task for each high-fanout parent."""
    if min_distinct_hosts < 2:
        raise ValueError("min_distinct_hosts must be at least two")

    groups: dict[str, set[str]] = defaultdict(set)
    observed: set[str] = set()
    for raw in hostnames:
        hostname = normalize_official(raw)
        if hostname is None:
            continue
        observed.add(hostname)
        parent = amplification_parent(hostname)
        if parent is not None:
            groups[parent].add(hostname)

    keys: list[EvidenceQueryKey] = []
    for parent in sorted(groups):
        members = groups[parent]
        # Root observation is a useful safety signal that the suffix is likely
        # an actual site/domain rather than an accidentally broad shared suffix.
        if parent not in observed:
            continue
        if len(members) < min_distinct_hosts:
            continue
        keys.append(
            EvidenceQueryKey(
                hostname=parent,
                temporal_scope=TemporalScope(1996, 2001),
                provider=provider,
                policy_version=DOMAIN_AMPLIFICATION_POLICY_VERSION,
            )
        )
    return tuple(keys)


def is_domain_amplification_key(key: EvidenceQueryKey) -> bool:
    return key.policy_version == DOMAIN_AMPLIFICATION_POLICY_VERSION
