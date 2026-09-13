"""Separate proxy feedback from validation-closed FINAL reward."""
from __future__ import annotations

from .models import RewardKind, RewardRecord, RewardScope, stable_hash
from .registry import ResearchRegistry


class ResearchFeedback:
    def __init__(self, registry: ResearchRegistry) -> None:
        self.registry = registry

    def record_proxy(
        self,
        *,
        scope: RewardScope | str,
        entity_id: str,
        amount: float,
        source_key: str = "",
        exposure_id: str = "",
        decision_id: str = "",
        policy_version: str = "",
        idempotency_key: str = "",
    ) -> bool:
        scope = RewardScope(scope)
        key = idempotency_key or stable_hash(
            "proxy", scope.value, entity_id, source_key, exposure_id, decision_id
        )
        return self.registry.record_reward(
            RewardRecord(
                reward_id=stable_hash("reward", "PROXY", scope.value, entity_id, key),
                kind=RewardKind.PROXY,
                scope=scope,
                entity_id=entity_id,
                amount=amount,
                source_key=source_key,
                exposure_id=exposure_id,
                decision_id=decision_id,
                policy_version=policy_version,
                idempotency_key=key,
            )
        )

    def close_final(
        self,
        *,
        source_key: str,
        exposure_id: str,
        final_eed: float,
        validation_closed: bool,
        policy_version: str = "",
        idempotency_token: str = "",
    ) -> int:
        return self.registry.close_final_reward(
            source_key=source_key,
            exposure_id=exposure_id,
            final_eed=final_eed,
            validation_closed=validation_closed,
            policy_version=policy_version,
            idempotency_token=idempotency_token,
        )


__all__ = ["ResearchFeedback"]
