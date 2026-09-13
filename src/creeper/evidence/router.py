"""Direct-first routing for unresolved external evidence work.

The planner decides *what* evidence is still missing.  This router decides
*where* that unresolved work should run and durably spills work that cannot
enter a provider-specific backlog yet.  Direct proof never depends on this
queue: SourceProducer commits direct capsules before/independently from remote
provider admission.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from creeper.evidence.policies import EvidenceQueryKey, TemporalScope
from creeper.scheduler.admission import EvidenceBacklogAdmission
from creeper.storage.control_store import ControlStore


@dataclass(frozen=True)
class EvidenceNeed:
    key: EvidenceQueryKey
    source_key: str
    reservoir_id: str
    lease_id: str


@dataclass(frozen=True)
class EvidenceRouteResult:
    enqueued: int = 0
    staged: int = 0


class EvidenceRouter:
    """Route unresolved work into independently bounded provider lanes.

    Provider backlog capacity is an admission constraint, not evidence
    authority.  When a lane is full or unavailable, needs are persisted in a
    small spillover table and may be promoted later without replaying a direct
    source lease.
    """

    def __init__(
        self,
        control_store: ControlStore,
        admission: EvidenceBacklogAdmission,
        *,
        backlog_capacities: Mapping[str, int],
        generic_archive_provider: str = "wayback",
    ) -> None:
        if not generic_archive_provider.strip():
            raise ValueError("generic_archive_provider is required")
        capacities = dict(backlog_capacities)
        if any(
            not provider
            or not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or capacity < 0
            for provider, capacity in capacities.items()
        ):
            raise ValueError("backlog capacities must be non-negative integers")
        self.control_store = control_store
        self.connection = control_store.connection
        self.admission = admission
        self.backlog_capacities = capacities
        self.generic_archive_provider = generic_archive_provider
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS evidence_route_backlog_v1 (
                    hostname TEXT NOT NULL,
                    year_from INTEGER NOT NULL,
                    year_to INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    reservoir_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    PRIMARY KEY(
                        hostname, year_from, year_to, provider, policy_version,
                        source_key, reservoir_id, lease_id
                    )
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_evidence_route_backlog_provider_v1
                    ON evidence_route_backlog_v1(provider, created_at);
                """
            )

    def route_external(
        self,
        keys: Iterable[EvidenceQueryKey],
        *,
        preferred_provider: str | None = None,
    ) -> tuple[EvidenceQueryKey, ...]:
        provider = (
            self.generic_archive_provider
            if preferred_provider is None
            else preferred_provider
        )
        if not provider.strip():
            raise ValueError("preferred_provider must be non-empty")
        routed: list[EvidenceQueryKey] = []
        for key in dict.fromkeys(keys):
            final_provider = key.provider
            if final_provider != "rdap":
                final_provider = provider
            routed.append(
                EvidenceQueryKey(
                    hostname=key.hostname,
                    temporal_scope=TemporalScope(
                        key.temporal_scope.year_from,
                        key.temporal_scope.year_to,
                    ),
                    provider=final_provider,
                    policy_version=key.policy_version,
                )
            )
        return tuple(routed)

    @staticmethod
    def _reservation_amount(key: EvidenceQueryKey) -> int:
        if key.provider == "rdap" or key.policy_version.startswith("cdx-domain-"):
            return 1
        return max(
            1,
            key.temporal_scope.year_to - key.temporal_scope.year_from + 1,
        )

    def stage(self, needs: Iterable[EvidenceNeed]) -> int:
        rows = list(dict.fromkeys(needs))
        if not rows:
            return 0
        now = float(self.control_store.clock())
        before = self.connection.total_changes
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO evidence_route_backlog_v1(
                    hostname, year_from, year_to, provider, policy_version,
                    source_key, reservoir_id, lease_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        need.key.hostname,
                        need.key.temporal_scope.year_from,
                        need.key.temporal_scope.year_to,
                        need.key.provider,
                        need.key.policy_version,
                        need.source_key,
                        need.reservoir_id,
                        need.lease_id,
                        now,
                    )
                    for need in rows
                ],
            )
        return self.connection.total_changes - before

    def pending_count(self, *, provider: str | None = None) -> int:
        if provider is None:
            row = self.connection.execute(
                "SELECT COUNT(*) FROM evidence_route_backlog_v1"
            ).fetchone()
        else:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM evidence_route_backlog_v1
                WHERE provider = ?
                """,
                (provider,),
            ).fetchone()
        return int(row[0])

    def _delete_staged(self, needs: Iterable[EvidenceNeed]) -> None:
        rows = list(dict.fromkeys(needs))
        if not rows:
            return
        with self.connection:
            self.connection.executemany(
                """
                DELETE FROM evidence_route_backlog_v1
                WHERE hostname = ?
                  AND year_from = ?
                  AND year_to = ?
                  AND provider = ?
                  AND policy_version = ?
                  AND source_key = ?
                  AND reservoir_id = ?
                  AND lease_id = ?
                """,
                [
                    (
                        need.key.hostname,
                        need.key.temporal_scope.year_from,
                        need.key.temporal_scope.year_to,
                        need.key.provider,
                        need.key.policy_version,
                        need.source_key,
                        need.reservoir_id,
                        need.lease_id,
                    )
                    for need in rows
                ],
            )

    def _try_enqueue_group(
        self,
        needs: list[EvidenceNeed],
        *,
        ttl_seconds: float,
    ) -> int:
        if not needs:
            return 0
        provider = needs[0].key.provider
        lineage = (
            needs[0].source_key,
            needs[0].reservoir_id,
            needs[0].lease_id,
        )
        if any(
            need.key.provider != provider
            or (
                need.source_key,
                need.reservoir_id,
                need.lease_id,
            ) != lineage
            for need in needs
        ):
            raise ValueError("router enqueue group must share provider and lineage")
        capacity = self.backlog_capacities.get(provider, 0)
        if capacity <= 0:
            return 0
        amount = sum(self._reservation_amount(need.key) for need in needs)
        reservation = self.admission.try_reserve(
            provider=provider,
            amount=amount,
            capacity=capacity,
            ttl_seconds=ttl_seconds,
        )
        if reservation is None:
            return 0
        try:
            self.admission.bind_lease(reservation, lineage[2])
            inserted = self.admission.enqueue_reserved(
                reservation,
                [need.key for need in needs],
                source_key=lineage[0],
                reservoir_id=lineage[1],
                lease_id=lineage[2],
            )
            # enqueue_reserved is idempotent: an already-existing task still
            # receives origin attribution.  Therefore every successfully
            # processed need can now leave the spillover table.
            self._delete_staged(needs)
            return inserted
        finally:
            self.admission.release(reservation)

    def enqueue_or_stage(
        self,
        keys: Iterable[EvidenceQueryKey],
        *,
        source_key: str,
        reservoir_id: str,
        lease_id: str,
        ttl_seconds: float,
        preferred_provider: str | None = None,
    ) -> EvidenceRouteResult:
        routed = self.route_external(
            keys,
            preferred_provider=preferred_provider,
        )
        needs = [
            EvidenceNeed(
                key=key,
                source_key=source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
            )
            for key in routed
        ]
        staged = self.stage(needs)
        enqueued = 0
        grouped: dict[str, list[EvidenceNeed]] = {}
        for need in needs:
            grouped.setdefault(need.key.provider, []).append(need)
        for group in grouped.values():
            enqueued += self._try_enqueue_group(
                group,
                ttl_seconds=ttl_seconds,
            )
        return EvidenceRouteResult(
            enqueued=enqueued,
            staged=max(0, self.pending_count_for_lineage(
                source_key=source_key,
                reservoir_id=reservoir_id,
                lease_id=lease_id,
            )),
        )

    def pending_count_for_lineage(
        self,
        *,
        source_key: str,
        reservoir_id: str,
        lease_id: str,
    ) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM evidence_route_backlog_v1
            WHERE source_key = ? AND reservoir_id = ? AND lease_id = ?
            """,
            (source_key, reservoir_id, lease_id),
        ).fetchone()
        return int(row[0])

    def flush_pending(
        self,
        *,
        ttl_seconds: float,
        max_needs: int = 256,
    ) -> int:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        if max_needs < 1:
            return 0
        rows = self.connection.execute(
            """
            SELECT hostname, year_from, year_to, provider, policy_version,
                   source_key, reservoir_id, lease_id
            FROM evidence_route_backlog_v1
            ORDER BY created_at, provider, hostname, year_from, year_to
            LIMIT ?
            """,
            (int(max_needs),),
        ).fetchall()
        needs = [
            EvidenceNeed(
                key=EvidenceQueryKey(
                    hostname=str(row["hostname"]),
                    temporal_scope=TemporalScope(
                        int(row["year_from"]),
                        int(row["year_to"]),
                    ),
                    provider=str(row["provider"]),
                    policy_version=str(row["policy_version"]),
                ),
                source_key=str(row["source_key"]),
                reservoir_id=str(row["reservoir_id"]),
                lease_id=str(row["lease_id"]),
            )
            for row in rows
        ]
        grouped: dict[tuple[str, str, str, str], list[EvidenceNeed]] = {}
        for need in needs:
            group_key = (
                need.key.provider,
                need.source_key,
                need.reservoir_id,
                need.lease_id,
            )
            grouped.setdefault(group_key, []).append(need)

        inserted = 0
        for group in grouped.values():
            capacity = self.backlog_capacities.get(group[0].key.provider, 0)
            if capacity <= 0:
                continue
            available = self.admission.available_capacity(
                provider=group[0].key.provider,
                capacity=capacity,
            )
            if available <= 0:
                continue
            selected: list[EvidenceNeed] = []
            required = 0
            for need in group:
                amount = self._reservation_amount(need.key)
                if required + amount > available:
                    break
                selected.append(need)
                required += amount
            if selected:
                inserted += self._try_enqueue_group(
                    selected,
                    ttl_seconds=ttl_seconds,
                )
        return inserted
