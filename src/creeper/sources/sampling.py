"""Deterministic, bounded candidate sampling for engineering pilots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from creeper.authority.normalizer import normalize_official


DEFAULT_BUCKET_CAP = 32


@dataclass(frozen=True)
class SampledHostname:
    hostname: str
    tld: str
    depth: int
    length_bucket: str
    www_prefix: bool
    bucket: str


def _describe(hostname: str) -> SampledHostname:
    labels = hostname.split(".")
    tld = labels[-1]
    depth = len(labels) - 2
    length_bucket = "short" if len(hostname) <= 15 else "medium" if len(hostname) <= 30 else "long"
    www_prefix = hostname.startswith("www.")
    bucket = f"{tld}|{depth}|{length_bucket}|{'www' if www_prefix else 'nonwww'}"
    return SampledHostname(hostname, tld, depth, length_bucket, www_prefix, bucket)


def _select(
    buckets: dict[str, list[tuple[bytes, SampledHostname]]], limit: int, cap: int
) -> list[SampledHostname]:
    ordered_buckets = sorted(
        buckets,
        key=lambda bucket: hashlib.sha256(bucket.encode("utf-8")).digest(),
    )
    for bucket in ordered_buckets:
        buckets[bucket].sort(key=lambda pair: pair[0])
    selected: list[SampledHostname] = []
    position = 0
    while len(selected) < limit and position < cap:
        progressed = False
        for bucket in ordered_buckets:
            values = buckets[bucket]
            if position < len(values):
                selected.append(values[position][1])
                progressed = True
                if len(selected) >= limit:
                    break
        if not progressed:
            break
        position += 1
    return selected


def stratified_hostnames(
    path: Path,
    limit: int,
    *,
    bucket_cap: int | None = None,
    cache_path: Path | None = None,
) -> list[SampledHostname]:
    """Scan a candidate file and return a deterministic multi-bucket sample."""
    if limit < 1:
        raise ValueError("limit must be positive")
    # Keep one stable default so 1k and 10k pilots reuse the same cache. A
    # larger explicit bucket_cap can be used for unusually large studies; a
    # small change in limit must never invalidate the default cache.
    cap = bucket_cap or DEFAULT_BUCKET_CAP
    stat = path.stat()
    if cache_path is not None and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if (
                cached.get("source_size") == stat.st_size
                and cached.get("source_mtime_ns") == stat.st_mtime_ns
                and cached.get("bucket_cap", 0) >= cap
            ):
                buckets: dict[str, list[tuple[bytes, SampledHostname]]] = {}
                for raw in cached.get("items", []):
                    item = SampledHostname(**raw)
                    score = hashlib.sha256(item.hostname.encode("utf-8")).digest()
                    buckets.setdefault(item.bucket, []).append((score, item))
                return _select(buckets, limit, int(cached["bucket_cap"]))
        except (OSError, ValueError, TypeError, KeyError):
            pass
    buckets: dict[str, list[tuple[bytes, SampledHostname]]] = {}
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            hostname = normalize_official(line)
            if hostname is None:
                continue
            item = _describe(hostname)
            score = hashlib.sha256(hostname.encode("utf-8")).digest()
            values = buckets.setdefault(item.bucket, [])
            if any(existing[1].hostname == hostname for existing in values):
                continue
            if len(values) < cap:
                values.append((score, item))
            else:
                worst = max(range(len(values)), key=lambda i: values[i][0])
                if score < values[worst][0]:
                    values[worst] = (score, item)

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps(
                {
                    "source_size": stat.st_size,
                    "source_mtime_ns": stat.st_mtime_ns,
                    "bucket_cap": cap,
                    "items": [
                        asdict(item)
                        for values in buckets.values()
                        for _, item in values
                    ],
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
    return _select(buckets, limit, cap)
