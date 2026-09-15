# Residual Population Discovery Architecture

## Frozen objective

Creeper exists to maximize **Accepted Novel EED / wall-clock** for exact
`(hostname, year)` evidence in 1996-2001.

The current baseline appears highly saturated with respect to common Web archive
and public crawl-derived datasets.  Therefore source discovery must target
*residual populations* produced by mechanisms that are not merely another
presentation of the same archive/crawl distribution.

## Architecture decision

Creeper is **single-node, offline-first and restart-safe**.  There is no
always-on coordinator requirement.

The normal discovery hot path is deterministic:

1. select an uncovered `SearchCell`;
2. execute bounded metadata searches through configured providers;
3. canonicalize URLs/artifacts/datasets/source families;
4. record duplicate/new-family supply in the durable coverage ledger;
5. probe only unseen, relevant roots;
6. sample before large downloads;
7. baseline-reconcile the sample;
8. harvest only sources with positive residual economics.

LLMs are removed from routine source enumeration.  They remain permitted only
for:

- `UNKNOWN_FORMAT`: synthesize an adapter plus fixtures/tests;
- `AMBIGUOUS_STRUCTURE`: interpret a bounded README/schema/catalog;
- `SEARCH_STAGNATION`: propose a new *data-generating mechanism*, not URLs.

LLM output never grants evidence authority.

## Search cell

A search is not identified by its query string.  The durable unit is:

`mechanism x institution x period x artifact`

The initial mechanism families intentionally emphasize distributions plausibly
orthogonal to generic Web crawls: proxy/access traces, client traces, DNS
surveys, FTP/BBS/Gopher inventories, mail/Usenet URL reservoirs, historical
search-engine/frontier artifacts, human directories, link graphs, NIC/ISP
inventories and software mirrors.

Only relevance-preserving query variants are allowed.  Every query retains:

- an explicit 1996-2001 time anchor;
- a mechanism anchor;
- an institution anchor;
- an artifact anchor;
- a URL/hostname content anchor.

This prevents diversity pressure from drifting into irrelevant data.

## Anti-repetition authority

The ledger records per-cell:

- attempts;
- returned results;
- duplicate results;
- unique roots;
- new source families;
- qualified roots;
- Accepted Novel EED;
- search cost;
- repeated family fingerprints.

A cell becomes `SATURATED` only when it has enough observations and results are
both highly duplicate and supply almost no new families.  Query paraphrasing
then stops automatically.

Frequently repeated source families become **cell-local exclusions**.  They are
not global blacklists because the same family may later be intentionally
queried for a mirror, README or schema.

## Network policy

Optimize bytes and online minutes before bandwidth:

`metadata -> HEAD/range probe -> stratified sample -> baseline sample -> harvest`

Large artifacts must not be downloaded merely because they are large or famous.
Uncompressed line-oriented data should be range sampled; non-seekable compressed
formats should use bounded prefix probes or an index/sharded representation.

Online work and local work are separate durable queues.  A machine may be
offline or powered down indefinitely without invalidating state.

## Current implementation step

`residual_search.py` introduces the durable `ResidualSearchLedger`,
`SearchCell`, saturation logic, cell-local family exclusions and deterministic
`SearchCellScheduler`.  Search provider adapters and result canonicalization
are wired in subsequent steps; broad LLM source-search calls are removed only
when the deterministic executor is present, avoiding a production discovery
blackout during migration.
