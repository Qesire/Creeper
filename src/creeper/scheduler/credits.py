"""Finite accounting for evidence-provider capacity."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ResourceCredits:
    """Work credits granted to each runtime stage."""

    source_fetch: int
    parse: int
    evidence: Mapping[str, int]
    commit: int
    reserved_evidence_tasks: int = 0

    def __post_init__(self) -> None:
        for name, value in (
            ("source_fetch", self.source_fetch),
            ("parse", self.parse),
            ("commit", self.commit),
            ("reserved_evidence_tasks", self.reserved_evidence_tasks),
        ):
            if not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        evidence = dict(self.evidence)
        if any(not isinstance(value, int) or value < 0 for value in evidence.values()):
            raise ValueError("evidence credits must be non-negative integers")
        object.__setattr__(self, "evidence", evidence)


@dataclass(frozen=True)
class CreditBalance:
    """Current accounting state for one evidence provider."""

    provider: str
    capacity: int
    queued: int
    claimed: int
    reserved: int

    @property
    def available(self) -> int:
        # Durable backlog can legitimately exceed a newly lowered capacity.
        # In that state the scheduler must grant zero new work, not expose a
        # negative credit count.
        return max(0, self.capacity - self.queued - self.claimed - self.reserved)


class CreditLedger:
    """Track bounded queued, claimed, and reserved work per provider.

    The ledger is an in-memory scheduling cache. Durable queue state belongs to
    ControlStore and should be restored into this object after process restart.
    """

    def __init__(self, capacities: Mapping[str, int]):
        self._capacity = self._validate_capacities(capacities)
        self._queued = {provider: 0 for provider in self._capacity}
        self._claimed = {provider: 0 for provider in self._capacity}
        self._reserved = {provider: 0 for provider in self._capacity}

    @staticmethod
    def _validate_capacities(capacities: Mapping[str, int]) -> dict[str, int]:
        result = dict(capacities)
        if any(
            not provider or not isinstance(capacity, int) or capacity < 0
            for provider, capacity in result.items()
        ):
            raise ValueError("provider capacities must be non-negative integers")
        return result

    def providers(self) -> tuple[str, ...]:
        return tuple(self._capacity)

    def _check_provider(self, provider: str) -> None:
        if provider not in self._capacity:
            raise KeyError(f"unknown evidence provider: {provider}")

    @staticmethod
    def _check_amount(amount: int) -> None:
        if not isinstance(amount, int) or amount < 0:
            raise ValueError("amount must be a non-negative integer")

    def balance(self, provider: str) -> CreditBalance:
        self._check_provider(provider)
        return CreditBalance(
            provider=provider,
            capacity=self._capacity[provider],
            queued=self._queued[provider],
            claimed=self._claimed[provider],
            reserved=self._reserved[provider],
        )

    def restore_backlog(
        self,
        provider: str,
        *,
        queued: int,
        claimed: int,
        reserved: int = 0,
    ) -> None:
        """Replace volatile counters from the durable queue snapshot.

        Restored backlog may exceed configured capacity when limits are lowered
        between runs. This is safe: ``available`` becomes zero until workers
        drain below the new high-water mark.
        """
        self._check_provider(provider)
        for amount in (queued, claimed, reserved):
            self._check_amount(amount)
        self._queued[provider] = queued
        self._claimed[provider] = claimed
        self._reserved[provider] = reserved

    def note_queued(self, provider: str, amount: int = 1) -> None:
        self._check_provider(provider)
        self._check_amount(amount)
        reserved = min(amount, self._reserved[provider])
        new_queued = self._queued[provider] + amount
        new_reserved = self._reserved[provider] - reserved
        if new_queued + self._claimed[provider] + new_reserved > self._capacity[provider]:
            raise ValueError("queued evidence exceeds provider capacity")
        self._reserved[provider] = new_reserved
        self._queued[provider] = new_queued

    def claim_evidence(self, provider: str, amount: int = 1) -> None:
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self._queued[provider]:
            raise ValueError("cannot claim more evidence than queued")
        self._queued[provider] -= amount
        self._claimed[provider] += amount

    def note_claimed(self, provider: str, amount: int = 1) -> None:
        """Compatibility spelling for callers that record a claim explicitly."""

        self.claim_evidence(provider, amount)

    def complete_evidence(self, provider: str, amount: int = 1) -> None:
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self._claimed[provider]:
            raise ValueError("cannot complete more evidence than claimed")
        self._claimed[provider] -= amount

    def release_queued(self, provider: str, amount: int = 1) -> None:
        """Discard queued work after its owning lease is aborted."""
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self._queued[provider]:
            raise ValueError("cannot release more evidence than queued")
        self._queued[provider] -= amount

    def release_claimed(self, provider: str, amount: int = 1) -> None:
        """Release claimed work after a worker fails before completion."""
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self._claimed[provider]:
            raise ValueError("cannot release more evidence than claimed")
        self._claimed[provider] -= amount

    def reserve_evidence(self, provider: str, amount: int = 1) -> bool:
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self.balance(provider).available:
            return False
        self._reserved[provider] += amount
        return True

    def release_evidence(self, provider: str, amount: int = 1) -> None:
        self._check_provider(provider)
        self._check_amount(amount)
        if amount > self._reserved[provider]:
            raise ValueError("cannot release more evidence than reserved")
        self._reserved[provider] -= amount
