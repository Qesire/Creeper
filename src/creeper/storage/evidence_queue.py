"""Persistent ACK/visibility-timeout queue for EvidenceTask rows.

This isolates generic durable-queue mechanics from Creeper's evidence policy.
The schema and terminal states remain owned by ControlStore, while claims and
visibility renewal follow the mature persistent-queue pattern.
"""

from __future__ import annotations

from collections.abc import Iterable

from creeper.evidence.actions import (
    ACTION_PRIOR_STRENGTH,
    ACTION_RETRY_PENALTY,
    EvidenceActionKind,
    action_prior_yield,
)

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceQueryKey,
    TemporalScope,
)
from creeper.storage.control_store import ControlStore, EvidenceTask


class DurableEvidenceQueue:
    """Atomic provider-filtered claims over ControlStore evidence tasks."""

    def __init__(self, control_store: ControlStore):
        self.control_store = control_store
        self.connection = control_store.connection
        # Provider workers normally claim a tiny batch out of a potentially
        # multi-million-row backlog. Keep the hot index narrow: duplicating the
        # hostname/policy primary key here would materially increase SSD cost.
        with self.connection:
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_evidence_tasks_provider_claim
                ON evidence_tasks(provider, state, retry_at, lease_until)
                """
            )

    @staticmethod
    def _key(row) -> EvidenceQueryKey:
        return EvidenceQueryKey(
            str(row["hostname"]),
            TemporalScope(int(row["year_from"]), int(row["year_to"])),
            str(row["provider"]),
            str(row["policy_version"]),
        )

    @classmethod
    def _task(
        cls,
        row,
        *,
        owner: str,
        lease_until: float,
    ) -> EvidenceTask:
        return EvidenceTask(
            key=cls._key(row),
            state=str(row["state"]),
            attempt=int(row["attempt"]) + 1,
            retry_at=row["retry_at"],
            lease_owner=owner,
            lease_until=lease_until,
        )

    @staticmethod
    def _values(key: EvidenceQueryKey) -> tuple[object, ...]:
        scope = key.temporal_scope
        return (
            key.hostname,
            scope.year_from,
            scope.year_to,
            key.provider,
            key.policy_version,
        )

    def claim(
        self,
        *,
        owner: str,
        limit: int,
        providers: Iterable[str],
        lease_seconds: float,
    ) -> list[EvidenceTask]:
        if not owner:
            raise ValueError("owner is required")
        if limit < 1:
            return []
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        provider_list = tuple(dict.fromkeys(str(item) for item in providers if str(item)))
        if not provider_list:
            return []

        now = float(self.control_store.clock())
        lease_until = now + float(lease_seconds)
        placeholders = ",".join("?" for _ in provider_list)
        params: list[object] = [
            CDXQueryState.PENDING.value,
            CDXQueryState.INCOMPLETE.value,
            CDXQueryState.TRANSIENT_ERROR.value,
            now,
            now,
            *provider_list,
            limit,
        ]
        query = f"""
            WITH eligible AS (
                SELECT e.*,
                       CASE
                           WHEN e.policy_version LIKE 'cdx-domain-%'
                               THEN 'domain'
                           WHEN e.provider = 'rdap' THEN 'rdap'
                           WHEN e.year_to > e.year_from THEN 'range'
                           ELSE 'exact'
                       END AS action_kind
                FROM evidence_tasks e
                WHERE e.state IN (?, ?, ?)
                  AND (e.lease_until IS NULL OR e.lease_until <= ?)
                  AND (e.retry_at IS NULL OR e.retry_at <= ?)
                  AND e.provider IN ({placeholders})
            )
            SELECT eligible.*
            FROM eligible
            LEFT JOIN evidence_action_cost_stats cost
              ON cost.task_kind = eligible.action_kind
            LEFT JOIN evidence_action_final_rewards reward
              ON reward.task_kind = eligible.action_kind
            LEFT JOIN evidence_action_reward_authority authority
              ON authority.singleton = 1
            -- Empirical-Bayes competition-value order:
            -- posterior final novel host-years/request, scaled by this task's
            -- official TLD EED weight and discounted after repeated attempts.
            ORDER BY
                (
                    eligible.eed_weight
                    * (
                        CASE
                            WHEN authority.singleton IS NULL THEN 0
                            ELSE COALESCE(reward.final_novel_host_years, 0)
                        END
                        + {ACTION_PRIOR_STRENGTH}
                          * CASE eligible.action_kind
                                WHEN 'domain' THEN {
                                    action_prior_yield(EvidenceActionKind.DOMAIN)
                                }
                                WHEN 'rdap' THEN {
                                    action_prior_yield(EvidenceActionKind.RDAP)
                                }
                                WHEN 'range' THEN {
                                    action_prior_yield(EvidenceActionKind.RANGE)
                                }
                                ELSE {
                                    action_prior_yield(EvidenceActionKind.EXACT)
                                }
                            END
                    )
                    / (
                        MAX(
                            CASE
                                WHEN authority.singleton IS NULL THEN 0
                                ELSE COALESCE(cost.provider_requests, 0)
                            END,
                            CASE
                                WHEN authority.singleton IS NULL THEN 0
                                ELSE COALESCE(cost.attempts, 0)
                            END
                        )
                        + {ACTION_PRIOR_STRENGTH}
                    )
                    / (
                        1.0
                        + {ACTION_RETRY_PENALTY} * eligible.attempt
                    )
                ) DESC,
                CASE eligible.action_kind
                    WHEN 'domain' THEN 3
                    WHEN 'rdap' THEN 2
                    WHEN 'range' THEN 1
                    ELSE 0
                END DESC,
                eligible.eed_weight DESC,
                (eligible.year_to - eligible.year_from) DESC,
                eligible.year_from,
                eligible.hostname,
                eligible.year_to,
                eligible.provider,
                eligible.policy_version
            LIMIT ?
        """

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            rows = self.connection.execute(query, params).fetchall()
            self.connection.executemany(
                """
                UPDATE evidence_tasks
                SET lease_owner = ?, lease_until = ?, attempt = attempt + 1
                WHERE hostname = ? AND year_from = ? AND year_to = ?
                  AND provider = ? AND policy_version = ?
                  AND (lease_until IS NULL OR lease_until <= ?)
                """,
                [
                    (
                        owner,
                        lease_until,
                        *self._values(self._key(row)),
                        now,
                    )
                    for row in rows
                ],
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return [
            self._task(row, owner=owner, lease_until=lease_until)
            for row in rows
        ]

    def renew(
        self,
        keys: Iterable[EvidenceQueryKey],
        *,
        owner: str,
        lease_seconds: float,
    ) -> int:
        """Extend visibility for a bounded set of tasks still owned by caller."""
        if not owner:
            raise ValueError("owner is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        key_list = list(dict.fromkeys(keys))
        if not key_list:
            return 0
        lease_until = float(self.control_store.clock()) + float(lease_seconds)
        changed = 0
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            for key in key_list:
                changed += self.connection.execute(
                    """
                    UPDATE evidence_tasks SET lease_until = ?
                    WHERE hostname = ? AND year_from = ? AND year_to = ?
                      AND provider = ? AND policy_version = ?
                      AND lease_owner = ?
                    """,
                    (lease_until, *self._values(key), owner),
                ).rowcount
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        return changed
