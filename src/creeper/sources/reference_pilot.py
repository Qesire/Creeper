"""Streaming deterministic samples for dated reference hostname files."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import heapq
from pathlib import Path

from creeper.authority.normalizer import normalize_official


@dataclass(frozen=True)
class ReferenceCandidate:
    hostname: str
    source_id: str
    locator: str
    source_year: int


def _hash_sample_host_file(
    path: Path,
    *,
    source_id: str,
    source_year: int,
    sample_size: int,
    seed: int,
) -> tuple[tuple[ReferenceCandidate, ...], int, int]:
    if sample_size < 1:
        raise ValueError("sample_size must be positive")
    heap: list[tuple[int, str, str]] = []
    selected: dict[str, ReferenceCandidate] = {}
    raw_lines = valid_lines = 0
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line_number, line in enumerate(source, 1):
            raw_lines += 1
            hostname = normalize_official(line)
            if hostname is None:
                continue
            valid_lines += 1
            if hostname in selected:
                continue
            score = int.from_bytes(
                hashlib.sha256(f"{seed}\0{hostname}".encode()).digest(), "big"
            )
            locator = f"{path}:{line_number}"
            key = (-score, hostname, locator)
            candidate = ReferenceCandidate(hostname, source_id, locator, source_year)
            if len(heap) < sample_size:
                heapq.heappush(heap, key)
                selected[hostname] = candidate
                continue
            if key <= heap[0]:
                continue
            _, evicted_hostname, _ = heapq.heapreplace(heap, key)
            selected.pop(evicted_hostname, None)
            selected[hostname] = candidate
    return tuple(selected[name] for name in sorted(selected)), raw_lines, valid_lines


def hash_sample_host_file(
    path: Path,
    *,
    source_id: str,
    source_year: int,
    sample_size: int,
    seed: int = 20260909,
) -> tuple[ReferenceCandidate, ...]:
    return _hash_sample_host_file(
        path,
        source_id=source_id,
        source_year=source_year,
        sample_size=sample_size,
        seed=seed,
    )[0]


def hash_sample_host_file_with_stats(
    path: Path,
    *,
    source_id: str,
    source_year: int,
    sample_size: int,
    seed: int = 20260909,
) -> tuple[tuple[ReferenceCandidate, ...], int, int]:
    return _hash_sample_host_file(
        path,
        source_id=source_id,
        source_year=source_year,
        sample_size=sample_size,
        seed=seed,
    )
