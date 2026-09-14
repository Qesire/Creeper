"""Low-rate provider × region qualification producer."""

from __future__ import annotations

import time
from collections.abc import Mapping

import httpx

from creeper.authority.normalizer import normalize_official
from creeper.distributed.coordinator_client import CoordinatorClient
from creeper.distributed.lease_keeper import LeaseKeeper
from creeper.distributed.models import TaskClass, TaskLease
from creeper.distributed.provider_gate import DistributedProviderGate
from creeper.evidence.providers.multi_cdx import CDXProviderConfig
from creeper.runtime.http import configured_http_proxy


class RegionProbeProducer:
    """Collect a bounded number of small provider observations.

    Qualification work is allowed from UNKNOWN regions, but every actual
    provider request still consumes the provider's global Authority budget.
    """

    def __init__(
        self,
        configs: tuple[CDXProviderConfig, ...],
        *,
        transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
        user_agent: str = (
            "Creeper/2.2 distributed qualification "
            "(research; https://github.com/Qesire/Creeper)"
        ),
    ) -> None:
        if not configs:
            raise ValueError("at least one provider config is required")
        self.configs = {config.name: config for config in configs}
        if len(self.configs) != len(configs):
            raise ValueError("provider names must be unique")
        self.transports = dict(transports or {})
        self.user_agent = user_agent

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
            "limit": "1",
        }

    async def __call__(
        self,
        lease: TaskLease,
        coordinator: CoordinatorClient,
        keeper: LeaseKeeper,
    ) -> None:
        if lease.work.task_class is not TaskClass.PROBE:
            raise ValueError("RegionProbeProducer requires PROBE")
        coverage = dict(lease.work.coverage)
        provider = str(
            coverage.get("provider", lease.work.input_identity)
        ).strip()
        config = self.configs.get(provider)
        if config is None:
            raise ValueError(f"unknown probe provider: {provider}")

        raw_hostname = coverage.get("probe_hostname")
        if not isinstance(raw_hostname, str):
            raise ValueError("region probe requires coverage.probe_hostname")
        hostname = normalize_official(raw_hostname)
        year = int(coverage.get("year", 0))
        samples = int(coverage.get("samples", 3))
        if hostname is None or not 1996 <= year <= 2001:
            raise ValueError("invalid region probe hostname/year")
        if not 1 <= samples <= 10:
            raise ValueError("region probe samples must be between 1 and 10")

        gate = DistributedProviderGate(
            coordinator,
            keeper,
            provider,
            throttle_floor_seconds=config.throttle_floor_seconds,
        )
        transport = self.transports.get(provider)
        client_options = dict(
            timeout=httpx.Timeout(config.timeout),
            limits=httpx.Limits(
                max_connections=1,
                max_keepalive_connections=1,
                keepalive_expiry=config.keepalive_expiry_seconds,
            ),
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
            },
            follow_redirects=True,
            trust_env=False,
            transport=transport,
        )
        if transport is None:
            client_options["proxy"] = configured_http_proxy()

        async with httpx.AsyncClient(**client_options) as client:
            for _ in range(samples):
                keeper.assert_owned()
                permit = await gate.acquire()
                started = time.monotonic()
                response: httpx.Response | None = None
                timeout = False
                try:
                    response = await client.get(
                        config.endpoint,
                        params=self._params(
                            config,
                            hostname=hostname,
                            year=year,
                        ),
                    )
                except httpx.TimeoutException:
                    timeout = True
                except httpx.TransportError:
                    pass
                finally:
                    latency_ms = max(
                        0.0,
                        (time.monotonic() - started) * 1000.0,
                    )
                    await gate.report(
                        permit,
                        (
                            None
                            if response is None
                            else int(response.status_code)
                        ),
                        (
                            None
                            if response is None
                            else response.headers
                        ),
                    )

                keeper.assert_owned()
                await coordinator.provider_observation(
                    keeper.lease,
                    provider=provider,
                    connect_success=response is not None,
                    status_code=(
                        None
                        if response is None
                        else int(response.status_code)
                    ),
                    latency_ms=latency_ms,
                    response_bytes=(
                        0 if response is None else len(response.content)
                    ),
                    timeout=timeout,
                    policy_block=(
                        response is not None
                        and int(response.status_code) == 403
                    ),
                )
