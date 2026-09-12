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


def build_minhash(
    values: Iterable[str],
    *,
    width: int = 64,
) -> MinHashSketch:
    if width < 1:
        raise ValueError("MinHash width must be positive")
    minima = [(1 << 64) - 1] * width
    seen = False
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        seen = True
        for seed in range(width):
            hashed = _hash64(seed, value)
            if hashed < minima[seed]:
                minima[seed] = hashed
    if not seen:
        minima = [0] * width
    return MinHashSketch(tuple(minima))
