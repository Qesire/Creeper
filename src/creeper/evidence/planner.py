"""Pure planning of direct and externally verified evidence work."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from creeper.authority.baseline_index import YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey, TemporalScope
from creeper.records.models import HostObservation


@dataclass(frozen=True)
class EvidencePlan:
    """The disjoint evidence work implied by one host observation."""

    direct_capsules: tuple[EvidenceCapsule, ...]
    external_keys: tuple[EvidenceQueryKey, ...]


class EvidencePlanner:
    """Convert host-year masks into deterministic evidence work.

    A record-level ``direct_year_mask`` is only a temporal claim. It becomes
    accepted direct evidence when the control plane explicitly authorizes the
    owning Reservoir for direct-year evidence. Otherwise the claimed years are
    conservatively demoted to external evidence hints.
    """

    def plan(
        self,
        observation: HostObservation,
        *,
        official_mask: int,
        local_mask: int,
        provider: str,
        policy_version: str,
        allow_direct: bool = False,
    ) -> EvidencePlan:
        hostname = normalize_official(observation.hostname)
        if hostname is None:
            raise ValueError("invalid hostname")

        suppressed_mask = official_mask | local_mask
        claimed_direct_mask = observation.direct_year_mask & ~suppressed_mask
        direct_mask = claimed_direct_mask if allow_direct else 0
        hint_mask = observation.year_hint_mask
        if observation.source_year in YEAR_BITS:
            hint_mask |= YEAR_BITS[observation.source_year]
        if not allow_direct:
            hint_mask |= claimed_direct_mask
        hint_mask &= ~suppressed_mask
        hint_mask &= ~direct_mask

        direct_capsules = tuple(
            self._direct_capsule(observation, hostname, year, policy_version)
            for year, bit in YEAR_BITS.items()
            if direct_mask & bit
        )
        years = [year for year, bit in YEAR_BITS.items() if hint_mask & bit]
        ranges: list[tuple[int, int]] = []
        for year in years:
            if not ranges or year != ranges[-1][1] + 1:
                ranges.append((year, year))
            else:
                ranges[-1] = (ranges[-1][0], year)
        external_keys = tuple(
            EvidenceQueryKey(
                hostname=hostname,
                temporal_scope=TemporalScope(year_from, year_to),
                provider=provider,
                policy_version=policy_version,
            )
            for year_from, year_to in ranges
        )
        return EvidencePlan(direct_capsules, external_keys)

    @staticmethod
    def _direct_capsule(
        observation: HostObservation,
        hostname: str,
        year: int,
        policy_version: str,
    ) -> EvidenceCapsule:
        identity = "\x00".join(
            (
                hostname,
                str(year),
                observation.source_id,
                observation.locator,
                observation.artifact_ref,
                observation.record_type,
            )
        ).encode("utf-8")
        return EvidenceCapsule(
            hostname=hostname,
            year=year,
            provider=f"direct:{observation.source_id}",
            temporal_semantics="source_direct_year",
            evidence_timestamp=f"{year}0101000000",
            source_locator=observation.locator,
            payload_hash=hashlib.sha256(identity).hexdigest(),
            policy_version=policy_version,
        )
