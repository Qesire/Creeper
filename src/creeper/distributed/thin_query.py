"""Positive-only single-request historical query reference for thin runtimes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from urllib.parse import urlsplit

import httpx

from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.edition import (
    FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES,
    FABRIC_THIN_MAX_PROVIDER_REQUESTS,
)
from creeper.distributed.http_transport import build_authority_transport
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.evidence.providers.multi_cdx import CDXProviderConfig


class ThinQueryTransientError(RuntimeError):
    """One opportunistic thin query should be retried later."""


def _parse_wayback_rows(payload: bytes) -> list[dict[str, object]]:
    try:
        value = json.loads(payload.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return []
    if isinstance(value, dict):
        return [dict(value)]
    if not isinstance(value, list) or not value:
        return []
    if all(isinstance(item, dict) for item in value):
        return [dict(item) for item in value]
    header = value[0]
    if not isinstance(header, list) or not all(
        isinstance(field, str) for field in header
    ):
        return []
    rows: list[dict[str, object]] = []
    for raw in value[1:]:
        if not isinstance(raw, list):
            continue
        rows.append(
            {
                str(field): raw[index]
                for index, field in enumerate(header)
                if index < len(raw)
            }
        )
    return rows


def _parse_arquivo_rows(payload: bytes) -> list[dict[str, object]]:
    text = payload.decode("utf-8", errors="replace").strip()
    if not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        values: list[object] = []
        for line in text.splitlines():
            try:
                values.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    else:
        values = value if isinstance(value, list) else [value]

    rows: list[dict[str, object]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if "original" not in row and "url" in row:
            row["original"] = row["url"]
        if "statuscode" not in row and "status" in row:
            row["statuscode"] = row["status"]
        rows.append(row)
    return rows


def _accepted_row(
    rows: list[dict[str, object]],
    *,
    hostname: str,
    year: int,
) -> dict[str, object] | None:
    for row in rows:
        timestamp = str(row.get("timestamp", ""))
        original = str(row.get("original", row.get("url", "")))
        status = str(row.get("statuscode", row.get("status", "")))
        parsed = urlsplit(original)
        original_host = normalize_official(parsed.hostname or "")
        if (
            len(timestamp) >= 4
            and timestamp[:4].isdigit()
            and int(timestamp[:4]) == year
            and original_host == hostname
            and status[:1] in {"2", "3"}
        ):
            return row
    return None


class ThinHistoricalQueryProducer:
    """Reference implementation of the Fabric one-request thin contract."""

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        *,
        transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        if not configs:
            raise ValueError("thin query requires at least one provider config")
        self.configs = {config.name: config for config in configs}
        if len(self.configs) != len(configs):
            raise ValueError("thin query provider names must be unique")
        self.transports = dict(transports or {})

    @staticmethod
    def _params(
        config: CDXProviderConfig,
        *,
        hostname: str,
        year: int,
    ) -> dict[str, str]:
        if config.dialect == "arquivo":
            return {
                "url": f"http://{hostname}/",
                "matchType": "host",
                "from": str(year),
                "to": str(year),
                "output": "json",
                "fields": "url,timestamp,status",
                "limit": "1",
            }
        return {
            "url": f"http://{hostname}/",
            "matchType": "host",
            "from": f"{year}0101000000",
            "to": f"{year}1231235959",
            "output": "json",
            "fl": "timestamp,original,statuscode",
            "filter": "statuscode:[23][0-9][0-9]",
            "limit": "1",
        }

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.HOST_BATCH:
            raise ValueError("ThinHistoricalQueryProducer requires HOST_BATCH")
        coverage = dict(lease.work.coverage)
        if coverage.get("thin_eligible") is not True:
            raise ValueError("thin query work is not thin_eligible")
        if int(coverage.get("max_provider_requests", 0)) != (
            FABRIC_THIN_MAX_PROVIDER_REQUESTS
        ):
            raise ValueError("thin query must allow exactly one provider request")
        max_bytes = int(coverage.get("estimated_response_bytes", 0))
        if not 1 <= max_bytes <= FABRIC_THIN_MAX_ESTIMATED_RESPONSE_BYTES:
            raise ValueError("thin query response bound is invalid")

        provider = str(coverage.get("provider", "")).strip()
        config = self.configs.get(provider)
        if config is None:
            raise ValueError(f"thin query provider not configured: {provider}")
        hostname = normalize_official(lease.work.input_identity)
        year_from = int(coverage.get("year_from", 0))
        year_to = int(coverage.get("year_to", 0))
        if hostname is None or not 1996 <= year_from == year_to <= 2001:
            raise ValueError("thin query requires one normalized exact year")
        year = year_from

        gate = DistributedProviderGate(
            coordinator,
            keeper,
            provider,
            throttle_floor_seconds=config.throttle_floor_seconds,
        )
        transport = build_authority_transport(
            acquire=gate.acquire,
            report=gate.report,
            max_connections=1,
            max_keepalive_connections=1,
            keepalive_expiry_seconds=config.keepalive_expiry_seconds,
            inner=self.transports.get(provider),
        )

        payload = b""
        async with httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(config.timeout),
            follow_redirects=False,
            trust_env=False,
            headers={
                "User-Agent": (
                    "Creeper-Fabric/0.1 thin-positive "
                    "(research; https://github.com/Qesire/Creeper)"
                ),
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
        ) as client:
            keeper.assert_owned()
            async with client.stream(
                "GET",
                config.endpoint,
                params=self._params(
                    config,
                    hostname=hostname,
                    year=year,
                ),
            ) as response:
                status = int(response.status_code)
                if status in {429, 503} or status >= 500:
                    raise ThinQueryTransientError(
                        f"thin provider returned HTTP {status}"
                    )
                if status >= 300:
                    return

                raw_length = response.headers.get("Content-Length")
                if raw_length is not None:
                    try:
                        declared = int(raw_length)
                    except ValueError:
                        declared = -1
                    if declared > max_bytes:
                        return

                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    keeper.assert_owned()
                    if total + len(chunk) > max_bytes:
                        return
                    chunks.append(chunk)
                    total += len(chunk)
                payload = b"".join(chunks)

        rows = (
            _parse_arquivo_rows(payload)
            if config.dialect == "arquivo"
            else _parse_wayback_rows(payload)
        )
        row = _accepted_row(rows, hostname=hostname, year=year)
        if row is None:
            # Positive-only lane: absence is deliberately non-authoritative.
            return

        timestamp = str(row.get("timestamp", ""))
        original = str(row.get("original", row.get("url", "")))
        locator = f"{provider}:{hostname}:{year}:thin"
        decisions = await coordinator.hy_probe(
            keeper.lease,
            [
                {
                    "hostname": hostname,
                    "year": year,
                    "locator": locator,
                }
            ],
        )
        if not decisions or decisions[0].status != "NEED_FULL_EVIDENCE":
            return

        canonical_row = json.dumps(
            row,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        await coordinator.hy_full(
            keeper.lease,
            [
                {
                    "hostname": hostname,
                    "year": year,
                    "evidence_class": "exact_host_cdx_capture",
                    "source": provider,
                    "timestamp": timestamp,
                    "locator": locator,
                    "original_url": original,
                    "provider": "wayback",
                    "policy_version": "fabric-thin-positive-v1",
                    "payload_hash": hashlib.sha256(canonical_row).hexdigest(),
                    "extraction_method": "fabric_thin_exact_year",
                }
            ],
        )
