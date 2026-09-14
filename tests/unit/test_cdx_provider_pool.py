from __future__ import annotations

import asyncio
import unittest
from collections import Counter

from creeper.evidence.policies import (
    CDXQueryState,
    EvidenceCapsule,
    EvidenceQueryKey,
    EvidenceQueryResult,
    RangeEvidenceQueryResult,
    TemporalScope,
)
from creeper.evidence.providers.cdx_pool import (
    AsyncCDXProviderPool,
    CDXServiceConfig,
)


class FakeCDX:
    def __init__(
        self,
        provider: str,
        *,
        exact_state: CDXQueryState = CDXQueryState.EMPTY_EXHAUSTIVE,
        hit_hosts: set[str] | None = None,
        delay: float = 0.0,
    ) -> None:
        self.provider = provider
        self.exact_state = exact_state
        self.hit_hosts = set(hit_hosts or ())
        self.delay = delay
        self.keys: list[EvidenceQueryKey] = []
        self.active = 0
        self.max_active = 0
        self.http_requests = 0
        self.throttle_responses = 0
        self.transport_errors = 0
        self.circuit_open_events = 0
        self.circuit_fast_failures = 0
        self.http_elapsed_milliseconds = 0
        self.cooldown_wait_milliseconds = 0
        self.rate_limit_wait_milliseconds = 0
        self.retry_backoff_wait_milliseconds = 0
        self.request_start_segments = 0
        self.request_start_gaps = 0
        self.request_start_gap_milliseconds = 0
        self.request_start_excess_gap_milliseconds = 0
        self.transport_error_counts = Counter()
        self.http_latency_buckets = Counter()
        self.http_status_counts = Counter()
        self.request_start_gap_buckets = Counter()

    async def query_key(self, key: EvidenceQueryKey) -> EvidenceQueryResult:
        self.keys.append(key)
        self.http_requests += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if key.hostname in self.hit_hosts:
                capsule = EvidenceCapsule(
                    hostname=key.hostname,
                    year=key.temporal_scope.year_from,
                    provider=self.provider,
                    temporal_semantics="capture_timestamp_year",
                    evidence_timestamp=(
                        f"{key.temporal_scope.year_from}0101000000"
                    ),
                    source_locator=f"https://{key.hostname}/",
                    payload_hash=("a" * 64),
                    policy_version=key.policy_version,
                    source_id=self.provider,
                    record_locator=f"{self.provider}:fixture",
                    extraction_method="fixture",
                )
                return EvidenceQueryResult(
                    key.hostname,
                    key.temporal_scope.year_from,
                    CDXQueryState.PASS,
                    capsule=capsule,
                    provider_requests=1,
                    key=key,
                )
            return EvidenceQueryResult(
                key.hostname,
                key.temporal_scope.year_from,
                self.exact_state,
                provider_requests=1,
                key=key,
            )
        finally:
            self.active -= 1

    async def query_range(self, key: EvidenceQueryKey) -> RangeEvidenceQueryResult:
        self.keys.append(key)
        self.http_requests += 1
        return RangeEvidenceQueryResult(
            hostname=key.hostname,
            key=key,
            state=CDXQueryState.EMPTY_EXHAUSTIVE,
            provider_requests=1,
        )


class CDXProviderPoolTests(unittest.IsolatedAsyncioTestCase):
    def pool(
        self,
        *,
        first_hits: set[str] | None = None,
        second_hits: set[str] | None = None,
        delay: float = 0.0,
    ):
        configs = (
            CDXServiceConfig("a", "https://a.example/cdx", 10.0, 1, 1.0),
            CDXServiceConfig("b", "https://b.example/cdx", 10.0, 1, 1.0),
        )
        clients = {
            "a": FakeCDX("a", hit_hosts=first_hits, delay=delay),
            "b": FakeCDX("b", hit_hosts=second_hits, delay=delay),
        }
        return (
            AsyncCDXProviderPool(clients, configs=configs),
            clients,
        )

    @staticmethod
    def key(host: str, year_from: int = 1999, year_to: int = 1999):
        return EvidenceQueryKey(
            host,
            TemporalScope(year_from, year_to),
            "wayback",
            "cdx-v1",
        )

    async def test_exact_query_never_duplicates_one_logical_task_concurrently(self):
        pool, clients = self.pool(
            first_hits={"hit.example"},
            second_hits={"hit.example"},
            delay=0.02,
        )
        result = await pool.query_key(self.key("hit.example"))

        self.assertEqual(result.state, CDXQueryState.PASS)
        self.assertEqual(
            sum(len(client.keys) for client in clients.values()),
            1,
        )
        self.assertLessEqual(
            max(client.max_active for client in clients.values()),
            1,
        )
        self.assertEqual(result.capsule.provider, "wayback")
        self.assertIn(result.capsule.source_id, {"a", "b"})

    async def test_exact_negative_requires_every_service_to_be_exhaustive(self):
        pool, clients = self.pool()
        result = await pool.query_key(self.key("empty.example"))

        self.assertEqual(result.state, CDXQueryState.EMPTY_EXHAUSTIVE)
        self.assertEqual(
            {name: len(client.keys) for name, client in clients.items()},
            {"a": 1, "b": 1},
        )
        self.assertEqual(result.provider_requests, 2)

    async def test_transient_service_cannot_create_global_negative(self):
        pool, clients = self.pool()
        clients["a"].exact_state = CDXQueryState.TRANSIENT_ERROR
        result = await pool.query_key(self.key("uncertain.example"))

        self.assertEqual(result.state, CDXQueryState.TRANSIENT_ERROR)
        self.assertEqual(result.provider_requests, 2)

    async def test_range_uses_one_service_then_exact_children_cover_missing_years(self):
        pool, clients = self.pool()
        result = await pool.query_range(self.key("range.example", 1998, 2000))

        self.assertEqual(result.state, CDXQueryState.DECOMPOSED)
        self.assertEqual(result.followup_years, (1998, 1999, 2000))
        self.assertEqual(
            sum(len(client.keys) for client in clients.values()),
            1,
        )

    async def test_rendezvous_spreads_distinct_tasks_across_services(self):
        pool, clients = self.pool(
            first_hits={f"h{i}.example" for i in range(64)},
            second_hits={f"h{i}.example" for i in range(64)},
        )
        for index in range(64):
            result = await pool.query_key(self.key(f"h{index}.example"))
            self.assertEqual(result.state, CDXQueryState.PASS)

        used = {
            name
            for name, client in clients.items()
            if client.keys
        }
        self.assertEqual(used, {"a", "b"})
        self.assertEqual(sum(len(client.keys) for client in clients.values()), 64)


if __name__ == "__main__":
    unittest.main()
