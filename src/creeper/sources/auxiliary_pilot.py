"""Deterministic sampling and year-window handling for auxiliary discoveries."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import heapq
import json
from pathlib import Path
import random
import re

from creeper.authority.normalizer import normalize_official

_SOURCE_RANGE = re.compile(r"deduplicated_urls_(\d{4})-(\d{4})$")
_REFERENCE_YEAR = re.compile(r"isc_reference:(\d{4})$")
_CDXJ_YEAR = re.compile(r"arquivo_pt_cdxj:.*:(\d{4})$")
COMPETITION_YEARS = tuple(range(1996, 2002))


@dataclass(frozen=True)
class AuxiliaryCandidate:
    hostname: str
    source_id: str
    locator: str


def _hash_sample_auxiliary_file(
    path: Path,
    *,
    sample_size: int,
    seed: int = 20260909,
) -> tuple[tuple[AuxiliaryCandidate, ...], int, int]:
    """Select globally stable, unique hostname samples from a complete file.

    The scan is streaming and retains only ``sample_size`` candidates. A
    hostname's SHA-256 rank is independent of line order, so the sample does
    not overfit the beginning of a large source file.
    """
    if sample_size < 1:
        raise ValueError("sample_size must be positive")
    source_id = f"v3_auxiliary:{path.stem}"
    heap: list[tuple[int, str, str]] = []
    selected: dict[str, AuxiliaryCandidate] = {}
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
            digest = hashlib.sha256(f"{seed}\0{hostname}".encode()).digest()
            score = int.from_bytes(digest, "big")
            key = (-score, hostname, f"{path}:{line_number}")
            if len(heap) < sample_size:
                heapq.heappush(heap, key)
                selected[hostname] = AuxiliaryCandidate(
                    hostname, source_id, f"{path}:{line_number}"
                )
                continue
            if key <= heap[0]:
                continue
            _, evicted_hostname, _ = heapq.heapreplace(heap, key)
            selected.pop(evicted_hostname, None)
            selected[hostname] = AuxiliaryCandidate(
                hostname, source_id, f"{path}:{line_number}"
            )
    return (
        tuple(selected[hostname] for hostname in sorted(selected)),
        raw_lines,
        valid_lines,
    )


def hash_sample_auxiliary_file(
    path: Path,
    *,
    sample_size: int,
    seed: int = 20260909,
) -> tuple[AuxiliaryCandidate, ...]:
    """Return a deterministic unique sample from a complete auxiliary file."""
    return _hash_sample_auxiliary_file(path, sample_size=sample_size, seed=seed)[0]


def hash_sample_auxiliary_file_with_stats(
    path: Path,
    *,
    sample_size: int,
    seed: int = 20260909,
) -> tuple[tuple[AuxiliaryCandidate, ...], int, int]:
    """Return the sample together with raw and valid line counts."""
    return _hash_sample_auxiliary_file(path, sample_size=sample_size, seed=seed)


def source_years(source_id: str) -> tuple[int, ...]:
    """Return source years inside the six-year competition window.

    Auxiliary filenames describe a two-year source window, not a precise
    hostname observation year. The returned values are therefore query targets
    for a pilot, never proof that the source itself establishes that year.
    """
    match = _SOURCE_RANGE.search(source_id)
    if match is not None:
        start, end = (int(value) for value in match.groups())
        return tuple(year for year in COMPETITION_YEARS if start <= year <= end)
    reference = _REFERENCE_YEAR.search(source_id)
    if reference is not None:
        year = int(reference.group(1))
        return (year,) if year in COMPETITION_YEARS else ()
    cdxj = _CDXJ_YEAR.search(source_id)
    if cdxj is not None:
        year = int(cdxj.group(1))
        return (year,) if year in COMPETITION_YEARS else ()
    return ()


def sample_auxiliary_candidates(
    path: Path,
    *,
    per_source: int,
    seed: int = 20260909,
) -> tuple[AuxiliaryCandidate, ...]:
    """Reservoir-sample active auxiliary records, deterministically by source."""
    if per_source < 1:
        raise ValueError("per_source must be positive")
    selected: dict[str, list[AuxiliaryCandidate]] = {}
    seen: dict[str, int] = {}
    rngs: dict[str, random.Random] = {}
    with path.open("r", encoding="utf-8") as source:
        for raw in source:
            try:
                row = json.loads(raw)
                hostname = str(row["hostname"])
                source_id = str(row["source_id"])
                locator = str(row["locator"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            if not hostname or not source_years(source_id):
                continue
            count = seen.get(source_id, 0) + 1
            seen[source_id] = count
            values = selected.setdefault(source_id, [])
            rng = rngs.setdefault(source_id, random.Random(f"{seed}:{source_id}"))
            candidate = AuxiliaryCandidate(hostname, source_id, locator)
            if len(values) < per_source:
                values.append(candidate)
                continue
            replacement = rng.randrange(count)
            if replacement < per_source:
                values[replacement] = candidate
    return tuple(
        candidate
        for source_id in sorted(selected)
        for candidate in sorted(selected[source_id], key=lambda item: item.hostname)
    )
