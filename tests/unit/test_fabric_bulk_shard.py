from __future__ import annotations

import unittest

import httpx

from creeper.distributed.bulk_shard import (
    BulkShardProducer,
    bulk_shard_work_definition,
    deserialize_witness_group,
)
from creeper.distributed.models import TaskLease
from creeper.distributed.worker import ProducerContext
from creeper.source_discovery.index_identity import HistoricalIndexObjectIdentity


class _Keeper:
    def assert_owned(self) -> None:
        return None


class BulkShardTests(unittest.IsolatedAsyncioTestCase):
    async def test_streaming_shard_reduces_real_witnesses_with_safe_cursor(self) -> None:
        body=(
            b"com,example)/ 19970102123456 http://example.com/ text/html 200 D 1 1 f.arc\n"
            b"com,example)/a 19980102123456 http://example.com/a text/html 200 D 2 2 f.arc\n"
            b"org,other)/ 19990102123456 http://other.org/ text/html 200 D 3 3 f.arc\n"
        )
        etag='"bulk-v1"'

        class AsyncBytes(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield body

        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.headers["range"],f"bytes=0-{len(body)-1}")
            return httpx.Response(
                206,
                headers={
                    "Content-Range":f"bytes 0-{len(body)-1}/{len(body)}",
                    "ETag":etag,
                },
                stream=AsyncBytes(),
            )

        identity=HistoricalIndexObjectIdentity(
            kind="remote",
            content_length=len(body),
            etag=etag,
        )
        work=bulk_shard_work_definition(
            region_key="region-a",
            index_key="index-a",
            source_key="source-a",
            locator="https://example.test/index.cdx",
            index_format="CDX",
            byte_start=0,
            byte_end_exclusive=len(body),
            expected_identity=identity,
            boundary_record_max_bytes=4096,
            timeout_seconds=10,
            group_batch_size=1,
        )
        lease=TaskLease(
            task_id="task-a",
            work_key=work.work_key,
            worker_id="worker-a",
            worker_instance_id="instance-a",
            generation=1,
            lease_deadline=9999999999.0,
            attempt=1,
            work=work,
        )
        producer=BulkShardProducer(transport=httpx.MockTransport(handler))
        context=ProducerContext(
            client=None,  # type: ignore[arg-type]
            keeper=_Keeper(),  # type: ignore[arg-type]
            descriptor=None,  # type: ignore[arg-type]
        )

        batches=[batch async for batch in producer.run(lease,context)]
        self.assertEqual(len(batches),2)
        self.assertFalse(batches[0].final)
        self.assertTrue(batches[1].final)
        self.assertTrue(str(batches[0].cursor_after).startswith("byte:"))

        first=deserialize_witness_group(batches[0].results[0])
        self.assertEqual(first.hostname,"example.com")
        self.assertEqual([item.year for item in first.witnesses],[1997,1998])
        self.assertEqual(first.witnesses[0].source_time,"19970102123456")

        final_groups=[
            deserialize_witness_group(item)
            for item in batches[1].results
            if item.get("kind")=="BULK_WITNESS_GROUP"
        ]
        self.assertEqual([group.hostname for group in final_groups],["other.org"])
        summaries=[
            item for item in batches[1].results
            if item.get("kind")=="BULK_SUMMARY"
        ]
        self.assertEqual(len(summaries),1)
        self.assertEqual(summaries[0]["records"],3)

    def test_work_identity_binds_region_range_and_object(self) -> None:
        identity=HistoricalIndexObjectIdentity(
            kind="remote",
            content_length=1000,
            etag='"v1"',
        )
        first=bulk_shard_work_definition(
            region_key="r",
            index_key="i",
            source_key="s",
            locator="https://example.test/index.cdxj",
            index_format="CDXJ",
            byte_start=0,
            byte_end_exclusive=100,
            expected_identity=identity,
            boundary_record_max_bytes=1024,
            timeout_seconds=10,
        )
        second=bulk_shard_work_definition(
            region_key="r",
            index_key="i",
            source_key="s",
            locator="https://example.test/index.cdxj",
            index_format="CDXJ",
            byte_start=100,
            byte_end_exclusive=200,
            expected_identity=identity,
            boundary_record_max_bytes=1024,
            timeout_seconds=10,
        )
        self.assertNotEqual(first.work_key,second.work_key)
        self.assertEqual(
            first.required_capabilities,
            ("STREAMING_BULK","ARTIFACT_FETCH"),
        )
        self.assertEqual(first.required_providers,())


if __name__=="__main__":
    unittest.main()
