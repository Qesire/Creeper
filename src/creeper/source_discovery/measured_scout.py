"""Deterministic measured-yield scouting for concrete bulk sources.

The scout deliberately supports only formats with a conservative parser contract.
It reads a bounded prefix, extracts exact hostnames, performs one batch baseline
reconciliation, and computes Equivalent-English Domain yield from the official
weight model. Unsupported archive formats remain HOLD rather than being guessed.
"""

from __future__ import annotations

import csv
import io
import json
import math
import time
import zlib
from dataclasses import dataclass
from decimal import Decimal
from pathlib import PurePosixPath
from urllib.parse import urlsplit

import httpx

from creeper.authority.baseline_index import BaselineIndex
from creeper.authority.normalizer import normalize_official
from creeper.source_discovery.coordinator import ScoutDisposition, ScoutResult
from creeper.source_discovery.models import ScoutMeasurement, SourceCandidate
from creeper.sources.archive.cdxj import parse_cdxj_line


@dataclass(frozen=True)
class MeasuredYieldScoutPolicy:
    max_download_bytes: int = 8 * 1024 * 1024
    max_decompressed_bytes: int = 32 * 1024 * 1024
    max_records: int = 5_000
    max_line_bytes: int = 64 * 1024
    min_unique_hosts: int = 100
    min_novel_hosts: int = 10
    min_novel_fraction: float = 0.01
    min_novel_eed: float = 1.0
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "max_download_bytes",
            "max_decompressed_bytes",
            "max_records",
            "max_line_bytes",
            "min_unique_hosts",
            "min_novel_hosts",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if not math.isfinite(self.min_novel_fraction) or not 0 <= self.min_novel_fraction <= 1:
            raise ValueError("min_novel_fraction must be within [0, 1]")
        if not math.isfinite(self.min_novel_eed) or self.min_novel_eed < 0:
            raise ValueError("min_novel_eed must be finite and non-negative")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


def _hostname_from_scalar(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if "://" in text or text.startswith("//"):
        parsed = urlsplit(text if not text.startswith("//") else "http:" + text)
        return normalize_official(parsed.hostname or "")
    if "/" in text:
        parsed = urlsplit("http://" + text)
        return normalize_official(parsed.hostname or "")
    return normalize_official(text)


def _mapping_hostname(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    lowered = {str(key).strip().lower(): value for key, value in payload.items()}
    for key in ("hostname", "host", "domain"):
        hostname = _hostname_from_scalar(lowered.get(key))
        if hostname is not None:
            return hostname
    for key in ("url", "original", "original_url", "uri"):
        hostname = _hostname_from_scalar(lowered.get(key))
        if hostname is not None:
            return hostname
    return None


def _suffix(path: str) -> tuple[str, bool]:
    name = PurePosixPath(path).name.lower()
    compressed = name.endswith(".gz")
    if compressed:
        name = name[:-3]
    return PurePosixPath(name).suffix.lower(), compressed


def _inflate_gzip_prefix(payload: bytes, limit: int) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        decoded = decoder.decompress(payload, limit + 1)
    except zlib.error as exc:
        raise ValueError(f"invalid gzip source prefix: {exc}") from exc
    if len(decoded) > limit:
        decoded = decoded[:limit]
    return decoded


def _iter_text_lines(payload: bytes, *, max_line_bytes: int):
    text = payload.decode("utf-8", errors="replace")
    for line in text.splitlines():
        encoded = line.encode("utf-8", errors="replace")
        if len(encoded) <= max_line_bytes:
            yield line


def _extract_hosts(
    payload: bytes,
    *,
    url: str,
    content_type: str,
    policy: MeasuredYieldScoutPolicy,
) -> tuple[int, set[str]] | None:
    suffix, compressed = _suffix(urlsplit(url).path)
    lower_type = content_type.lower()
    if urlsplit(url).path.lower().endswith((".warc", ".warc.gz", ".arc", ".arc.gz")):
        return None
    if compressed:
        payload = _inflate_gzip_prefix(payload, policy.max_decompressed_bytes)

    lines = _iter_text_lines(payload, max_line_bytes=policy.max_line_bytes)
    hosts: set[str] = set()
    sampled = 0

    if suffix == ".cdxj":
        for line in lines:
            if sampled >= policy.max_records:
                break
            sampled += 1
            record = parse_cdxj_line(line, source_id="measured-scout", locator=str(sampled))
            if record is None or record.source_year is None or not 1996 <= record.source_year <= 2001:
                continue
            hostname = _hostname_from_scalar(record.payload)
            if hostname is not None:
                hosts.add(hostname)
        return sampled, hosts

    if suffix in {".jsonl", ".ndjson"} or "ndjson" in lower_type:
        for line in lines:
            if sampled >= policy.max_records:
                break
            if not line.strip():
                continue
            sampled += 1
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            hostname = _mapping_hostname(value)
            if hostname is not None:
                hosts.add(hostname)
        return sampled, hosts

    if suffix in {".csv", ".tsv"} or "text/csv" in lower_type or "tab-separated-values" in lower_type:
        text = payload.decode("utf-8", errors="replace")
        dialect = "excel-tab" if suffix == ".tsv" or "tab-separated-values" in lower_type else "excel"
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        for row in reader:
            if sampled >= policy.max_records:
                break
            sampled += 1
            hostname = _mapping_hostname(row)
            if hostname is not None:
                hosts.add(hostname)
        return sampled, hosts

    if suffix in {"", ".txt", ".list"} or lower_type.startswith("text/plain"):
        for line in lines:
            if sampled >= policy.max_records:
                break
            if not line.strip():
                continue
            sampled += 1
            hostname = _hostname_from_scalar(line)
            if hostname is not None:
                hosts.add(hostname)
        return sampled, hosts

    return None


class MeasuredYieldScoutExecutor:
    """Measure concrete source yield without granting evidence authority."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        baseline: BaselineIndex,
        english_weights: dict[str, Decimal],
        *,
        policy: MeasuredYieldScoutPolicy | None = None,
        clock=time.perf_counter,
    ) -> None:
        self.client = client
        self.baseline = baseline
        self.english_weights = dict(english_weights)
        self.policy = policy or MeasuredYieldScoutPolicy()
        self.clock = clock

    async def _download_prefix(self, url: str) -> tuple[bytes, str]:
        payload = bytearray()
        headers = {"Range": f"bytes=0-{self.policy.max_download_bytes - 1}"}
        timeout = httpx.Timeout(self.policy.timeout_seconds)
        async with self.client.stream("GET", url, headers=headers, timeout=timeout) as response:
            if response.status_code in {404, 410}:
                return b"", "__permanent_missing__"
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                remaining = self.policy.max_download_bytes - len(payload)
                if remaining <= 0:
                    break
                payload.extend(chunk[:remaining])
                if len(payload) >= self.policy.max_download_bytes:
                    break
            return bytes(payload), response.headers.get("content-type", "")

    def _measurement(self, *, sampled: int, hosts: set[str], bytes_read: int, elapsed: float) -> ScoutMeasurement:
        resolved = self.baseline.resolve_batch(hosts)
        novel = [hostname for hostname in hosts if resolved.get(hostname, (0, False))[0] == 0]
        novel_eed = Decimal("0")
        for hostname in novel:
            tld = hostname.rsplit(".", 1)[-1]
            novel_eed += self.english_weights.get(tld, Decimal("0"))
        return ScoutMeasurement(
            sampled_records=sampled,
            unique_hosts=len(hosts),
            novel_hosts=len(novel),
            direct_host_years=0,
            requests=1,
            bytes_read=bytes_read,
            elapsed_seconds=elapsed,
            novel_eed=float(novel_eed),
        )

    async def __call__(self, candidate: SourceCandidate) -> ScoutResult:
        started = float(self.clock())
        payload, content_type = await self._download_prefix(candidate.canonical_entrypoint)
        if content_type == "__permanent_missing__":
            return ScoutResult(ScoutDisposition.HOLD, reason="bulk source returned HTTP 404/410")
        parsed = _extract_hosts(
            payload,
            url=candidate.canonical_entrypoint,
            content_type=content_type,
            policy=self.policy,
        )
        if parsed is None:
            return ScoutResult(
                ScoutDisposition.HOLD,
                reason="unsupported measured source format; requires a format-specific mature parser",
            )
        sampled, hosts = parsed
        elapsed = max(0.0, float(self.clock()) - started)
        measurement = self._measurement(
            sampled=sampled,
            hosts=hosts,
            bytes_read=len(payload),
            elapsed=elapsed,
        )
        if measurement.unique_hosts < self.policy.min_unique_hosts:
            return ScoutResult(
                ScoutDisposition.HOLD,
                measurement=measurement,
                reason="measured sample has too few unique hostnames",
            )
        novel_fraction = measurement.novel_hosts / measurement.unique_hosts
        if (
            measurement.novel_hosts < self.policy.min_novel_hosts
            or novel_fraction < self.policy.min_novel_fraction
            or measurement.novel_eed < self.policy.min_novel_eed
        ):
            return ScoutResult(
                ScoutDisposition.HOLD,
                measurement=measurement,
                reason="measured baseline-external/EED yield below warm threshold",
            )
        return ScoutResult(
            ScoutDisposition.WARM,
            measurement=measurement,
            reason="bounded deterministic sample met warm-yield thresholds",
        )
