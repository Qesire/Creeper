"""Compact source-overlap sketches for marginal-value scheduling."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MinHashSketch:
    values: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError("MinHash sketch must contain at least one value")

    def similarity(self, other: "MinHashSketch") -> float:
        if len(self.values) != len(other.values):
            raise ValueError("MinHash sketches must use the same width")
        matches = sum(
            a == b for a, b in zip(self.values, other.values, strict=True)
        )
        return matches / len(self.values)


def _hash64(seed: int, value: str) -> int:
    digest = hashlib.blake2b(
        value.encode("utf-8"),
        digest_size=8,
        person=seed.to_bytes(8, "little", signed=False),
    ).digest()
    return int.from_bytes(digest, "big", signed=False)


class MinHashAccumulator:
    """Streaming MinHash builder with O(width) memory.

    Region synopsis construction can consume millions of host-year keys.  The
    original build_minhash helper was already iterator-friendly, but this
    stateful form lets a caller update the sketch while simultaneously doing
    baseline reconciliation and histograms without retaining observation keys.
    """

    def __init__(self, *, width: int = 64) -> None:
        if width < 1:
            raise ValueError("MinHash width must be positive")
        self.width = int(width)
        self._minima = [(1 << 64) - 1] * self.width
        self._seen = False

    def update(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            return
        self._seen = True
        for seed in range(self.width):
            hashed = _hash64(seed, value)
            if hashed < self._minima[seed]:
                self._minima[seed] = hashed

    def extend(self, values: Iterable[str]) -> None:
        for value in values:
            self.update(value)

    def sketch(self) -> MinHashSketch:
        if not self._seen:
            return MinHashSketch((0,) * self.width)
        return MinHashSketch(tuple(self._minima))


def build_minhash(
    values: Iterable[str],
    *,
    width: int = 64,
) -> MinHashSketch:
    accumulator = MinHashAccumulator(width=width)
    accumulator.extend(values)
    return accumulator.sketch()
