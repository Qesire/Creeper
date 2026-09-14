"""Pure planning of direct and externally verified evidence work."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from creeper.authority.baseline_index import ALL_YEAR_MASK, YEAR_BITS
from creeper.authority.normalizer import normalize_official
from creeper.evidence.policies import EvidenceCapsule, EvidenceQueryKey, TemporalScope
from creeper.records.candidates import CandidateSourceScope
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
    owning Reservoir for direct-year evidence.

    Remote archive verification is deliberately narrower: only a hostname with
    *no temporal information at all* may create external archive work. Dated
    metadata, year hints, or unauthorized direct-year claims remain discovery
    signals and never spend Wayback capacity. This keeps bulk CDX/CDXJ evidence
    authoritative and reserves rate-limited fallback for undated candidate
    pools and bare hostname lists.

    Over the six competition years, an undated hostname's unresolved-year mask
    can contain at most three disjoint contiguous runs (for example
    1996/1998/2000). This is the hard external-task expansion bound for one
    HostObservation and is used by source admission to reserve queue capacity.
    """

    MAX_EXTERNAL_TASKS_PER_OBSERVATION = 3
    # Admission must reserve not only the initial disjoint range/exact tasks,
    # but also the worst-case exact fanout of a bounded multi-year probe plus
    # one domain-amplification task. Across six competition years, the peak
    # nonterminal backlog contribution of one observation is at most seven.
    MAX_BACKLOG_CAPACITY_PER_OBSERVATION = 7

    def plan(
        self,
        observation: HostObservation,
        *,
        official_mask: int,
        local_mask: int,
        provider: str,
        policy_version: str,
        allow_direct: bool = False,
        external_covered_mask: int = 0,
        range_first_fraction: float = 0.0,
    ) -> EvidencePlan:
        hostname = normalize_official(observation.hostname)
        if hostname is None:
            raise ValueError("invalid hostname")
        if not 0.0 <= float(range_first_fraction) <= 1.0:
            raise ValueError("range_first_fraction must be between 0 and 1")

        suppressed_mask = official_mask | local_mask
        claimed_direct_mask = observation.direct_year_mask & ~suppressed_mask
        restricted_source = observation.scope in {
            CandidateSourceScope.ISC_REFERENCE,
            CandidateSourceScope.COMMON_CRAWL_CORPUS_EXCLUDED,
        }
        direct_mask = claimed_direct_mask if allow_direct and not restricted_source else 0
        has_temporal_claim = bool(
            observation.year_hint_mask
            or observation.direct_year_mask
            or observation.source_year in YEAR_BITS
        )

        # Wayback / external archive work is fallback for undated hostname
        # records only. Temporal hints are useful for discovery/ranking, but
        # using them to trigger remote verification would spend rate-limited
        # requests on objects that already carry a date-bearing provenance path.
        # range_first_fraction is retained as an ABI/config compatibility input;
        # it cannot widen a dated observation into remote archive work.
        external_mask = 0 if has_temporal_claim else ALL_YEAR_MASK
        external_mask &= ~suppressed_mask
        external_mask &= ~direct_mask
        external_mask &= ~external_covered_mask

        direct_capsules = tuple(
            self._direct_capsule(observation, hostname, year, policy_version)
            for year, bit in YEAR_BITS.items()
            if direct_mask & bit
        )
        years = [year for year, bit in YEAR_BITS.items() if external_mask & bit]
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
    def _range_first_selected(hostname: str, fraction: float) -> bool:
        value = float(fraction)
        if value <= 0:
            return False
        if value >= 1:
            return True
        bucket = int.from_bytes(
            hashlib.sha256(hostname.encode("utf-8")).digest()[:8],
            "big",
        )
        return bucket < int(value * (1 << 64))

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
        extraction_method = observation.record_type or "source_record"
        if observation.evidence_contract_id:
            contract_version = (
                observation.evidence_contract_version or "unknown"
            )
            extraction_method = (
                f"{extraction_method};contract="
                f"{observation.evidence_contract_id}@{contract_version}"
            )
        return EvidenceCapsule(
            hostname=hostname,
            year=year,
            provider=f"direct:{observation.source_id}",
            temporal_semantics=(
                observation.temporal_semantics or "source_direct_year"
            ),
            evidence_timestamp=observation.source_time or f"{year}0101000000",
            source_locator=observation.locator,
            payload_hash=hashlib.sha256(identity).hexdigest(),
            policy_version=policy_version,
            evidence_type=(
                observation.evidence_type or "dated_archive_index"
            ),
            source_id=observation.source_id,
            original_url=observation.original_url or observation.locator,
            record_locator=observation.locator,
            extraction_method=extraction_method,
        )
