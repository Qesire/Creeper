# Scrapy acquisition sidecar

This directory is an intentionally isolated uv project. Scrapy owns HTTP request scheduling, retry, politeness, duplicate filtering, depth limits, page-count limits, and JOBDIR persistence. Creeper core does not import Scrapy or Twisted.

The `bounded_links` spider is a finite scout for one already-selected source root. It emits HTTP(S) link discoveries as JSONL feed items and follows only conservative same-site HTML-like links. External and dataset-looking links are emitted but are not recursively fetched.

Use a distinct `JOBDIR` per source scout. Reusing a JOBDIR for a different source is invalid. The spider uses Scrapy SpiderState to persist seed emission so a graceful resume drains the durable scheduler frontier without re-fetching the seed URL.

Example:

```bash
cd tools/scrapy_acquisition
uv sync --locked
uv run scrapy crawl bounded_links \
  -a start_url=https://example.org/archive/ \
  -a source_key=src:<sha256> \
  -O /path/to/spool.jsonl \
  -s JOBDIR=/path/to/jobs/<source-key> \
  -s DEPTH_LIMIT=2 \
  -s CLOSESPIDER_PAGECOUNT=100
```

Do not place baseline, evidence, EED, novelty, or submission logic in this sidecar. Those remain Creeper authority semantics.
