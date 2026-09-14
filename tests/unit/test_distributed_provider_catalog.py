from __future__ import annotations

import unittest

from creeper.distributed.host_query import (
    DistributedHostQueryProducer,
    distributed_cdx_resolver_identity,
)
from creeper.distributed.provider_catalog import canonical_cdx_provider_configs


class DistributedProviderCatalogTests(unittest.TestCase):
    def test_provider_order_is_canonical(self) -> None:
        first = canonical_cdx_provider_configs(
            ("internet_archive", "arquivo_pt")
        )
        reversed_order = canonical_cdx_provider_configs(
            ("arquivo_pt", "internet_archive")
        )
        self.assertEqual(
            tuple(config.name for config in first),
            ("internet_archive", "arquivo_pt"),
        )
        self.assertEqual(first, reversed_order)

    def test_catalog_identity_matches_host_query_producer(self) -> None:
        configs = canonical_cdx_provider_configs(
            ("internet_archive", "arquivo_pt")
        )
        digest, coverage_provider, resolver_version = (
            distributed_cdx_resolver_identity(configs)
        )
        producer = DistributedHostQueryProducer(configs)

        self.assertEqual(producer.provider_set_digest, digest)
        self.assertEqual(producer.coverage_provider, coverage_provider)
        self.assertEqual(producer.resolver_version, resolver_version)

    def test_unknown_provider_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown canonical CDX provider"):
            canonical_cdx_provider_configs(("unknown",))


if __name__ == "__main__":
    unittest.main()
