# Residual Population Discovery Architecture

## Frozen objective

Creeper exists to maximize **Accepted Novel EED / wall-clock** for exact
`(hostname, year)` evidence in 1996-2001.

The current baseline appears highly saturated with respect to common Web archive
and public crawl-derived datasets. Source discovery therefore targets
*residual populations* produced by acquisition mechanisms that are not merely
another presentation of the same archive/crawl distribution.

## Architecture decision

Creeper is **single-node, offline-first and restart-safe**. There is no
always-on coordinator requirement.

The normal discovery hot path is deterministic:

1. select an uncovered `SearchCell`;
2. execute one finite query-program step through every configured metadata provider;
3. fail the step closed unless every provider completes with a recognized response schema;
4. canonicalize URL, artifact, dataset and source-family identities;
5. preserve at least one canonicalization slot per configured provider;
6. commit episode, identities, proposals, SearchCell economics and cursor atomically;
7. probe only unseen, relevant roots;
8. sample before large downloads and reconcile the sample against the baseline;
9. harvest only sources with positive residual economics.

The current configured provider family is DataCite, Zenodo, Harvard Dataverse
and Internet Archive. Provider membership is part of the durable search profile:
changing the provider/query-policy profile reopens coverage rather than
inheriting stale saturation.

LLMs are removed from automatic source enumeration. The production planner
permits exactly one bounded automatic exception:

- **`COMPILE_ADAPTER` / unknown format**: inspect one bounded textual sample and
  propose a declarative binding for an already-mature `jsonl` or `delimited`
  reader.

A structural catalog/metasource left in `HOLD` remains deterministic/manual
engineering work; it does not trigger an LLM interpretation call. Exhausting the
finite residual query program is an observable saturation condition; it does not
trigger an LLM request for a new mechanism, root, query family, or URL.

LLM-generated executable parser code is not admitted to the runtime.
LLM-derived record schemas are forced to `direct_year_eligible=False`.
Consequently an LLM can help Creeper understand *how to read* an already-found
source but can never discover replacement sources automatically or grant
annual-evidence authority.

## Search cell and finite query program

A search is not identified by its query string. The durable unit remains:

`mechanism x institution x period x artifact`

The initial mechanism families intentionally emphasize distributions plausibly
orthogonal to generic Web crawls: proxy/access traces, client traces, DNS
surveys, FTP/BBS/Gopher inventories, mail/Usenet URL reservoirs, historical
search-engine/frontier artifacts, human directories, link graphs, NIC/ISP
inventories and software mirrors.

Each mechanism exposes a finite set of mechanism phrases. For every phrase the
current `residual-query-program-v3` executes exactly two relevance-preserving
shapes:

1. **`STRICT_4D`**: period + mechanism + institution + artifact;
2. **`RELAX_INSTITUTION`**: period + mechanism + artifact.

The relaxed shape exists because repository metadata frequently omits the
institution even when the artifact is relevant. The older pseudo-anchor
`(URL OR hostname OR host)` is deliberately absent: metadata providers do not
interpret it as a reliable content constraint and it reduced recall without
creating an independent search dimension.

The program is finite. Rephrasing does not continue indefinitely once all
mechanism-phrase/shape pairs have been consumed.

## Completion and restart authority

A QueryPlan advances its durable cursor only when all of the following are true:

\[
\mathrm{complete} =
\mathrm{all\ providers\ succeeded}
\land \mathrm{schemas\ recognized}
\land \mathrm{bounded\ subrequests\ complete}
\land \mathrm{provider\ coverage\ preserved}
\land \mathrm{atomic\ commit}
\]

HTTP 2xx alone is not success. A WAF document, missing provider envelope or
partially completed Internet Archive metadata expansion fails the whole finite
variant rather than masquerading as an empty result.

The deterministic commit boundary covers the search episode, four-level search
identity, accepted proposals, SearchCell economics/family memory, cursor
advance and episode closure. A crash therefore leaves either the complete
variant or the exact pre-variant state.

A versioned residual protocol recovery runs before planning. Legacy state
created under weaker provider-completion/schema/transaction semantics is
conservatively reopened. After the revision is current, recovery is cell-local
for unfinished legacy residual episodes. Baseline, evidence and production
authority are never rewritten by this recovery.

Read-only calibration is also protocol-guarded: it refuses to report stale
protocol revisions or unfinished residual episodes rather than turning
unrecovered state into tuning evidence.

## Anti-repetition authority

The ledger records per cell:

- attempts and finite-program cursor;
- returned and duplicate results;
- unique roots and new source families;
- qualified roots;
- Accepted Novel EED and search cost;
- repeated family fingerprints.

A cell becomes `SATURATED` only when it has enough observations and results are
both highly duplicate and supply almost no new families. Frequently repeated
source families become **cell-local exclusions**, not global blacklists.

Canonical identity is retained at URL, artifact, dataset and family levels, so
switching providers or repositories does not reset novelty memory.

## Deterministic research-lead ledger

Prior source research that is too specific for the generic SearchCell grid is
persisted in the same local ControlStore database rather than injected into an
LLM prompt. The curated ledger currently distinguishes three authority classes:

- **HARD_NEGATIVE**: a known public distribution irreversibly loses hostname or
  URL identity. Matching search results are written to the normal four-level
  search-identity ledger, but never become SourceCandidates. This prevents the
  same artifact from consuming downstream triage/scout work while retaining
  anti-repetition memory.
- **EXACT_RECOVERY**: prior research established a concrete historical identity
  such as an exact filename or trace label. Each lead maps to one bounded
  high-priority SearchCell and therefore reuses the normal DataCite, Zenodo,
  Harvard Dataverse and Internet Archive provider set, provider-completeness
  contract, canonical identity and atomic residual commit. There is no second
  search backend and no unbounded synonym generation.
- **PROVENANCE_HOLD**: a live-looking artifact is known but its target-period
  provenance is unresolved. A deterministic match may be retained only in
  `HOLD`; target-year bounds, temporal prior and direct-evidence prior are
  removed until provenance is established independently.

The initial exact-recovery program contains only five previously researched
leads: NLANR UC 2000-07-14, CA*netII 1999-09-19/20, BU98 filtered trace,
January 2001 DMOZ `content.rdf.u8.gz`, and historical RIPE/ISC hostcount raw
output. Their finite query variants are scheduled ahead of generic unseen
SearchCells, but ordinary saturation/exhaustion semantics still terminate them.

Research-lead matches are committed inside the same SQLite transaction as the
search episode, search identity, SourceCandidate proposal and SearchCell cursor.
The calibration report separates `research_recovery` from
`residual_population` so targeted recovery economics cannot silently distort
generic residual-search tuning.

## Unknown-format recovery

A concrete SOURCE that cannot be parsed deterministically remains in `HOLD`.
For recognized JSONL/delimited containers, the scout first attempts deterministic
record-layout inference across arbitrary field names/columns. A layout is
accepted only when one unambiguous hostname field/column has at least three
matches and covers at least 90% of the bounded sample; ambiguous equal-strength
columns fail closed.

Only after deterministic layout/schema inference fails does a textual object
enter the LLM path. The measured scout persists a bounded unknown-format case
containing a sample SHA-256, content type/compression metadata and at most
4 KiB of preview text. The complete downloaded object is not copied into the
control database.

The manager may then schedule exactly one per-source `COMPILE_ADAPTER` task.
The model receives the bounded case and may return exactly one declarative
proposal containing only:

- parser kind: `jsonl` or `delimited`;
- compression;
- hostname field/column;
- optional timestamp field/column;
- delimiter when applicable.

Hostname extraction and temporal semantics are separate durable layers.
`SourceRecordLayout` freezes only parser + hostname extraction and is valid
without any timestamp. `SourceRecordSchema` remains the timestamp-bearing
temporal layer and is created only when an independently validated timestamp is
present.

The parent accepts a layout only when at least three sampled records expose a
valid hostname and at least 90% of the evaluated sample agrees. If a timestamp
field is proposed, it must independently satisfy the same bounded validation.
Generated code, replacement URLs, unknown parser kinds and evidence-authority
claims are rejected.

Validated format/layout/optional-schema bindings and `HOLD -> SCOUT_READY` are
committed in one SQLite transaction. A hostname-only source is therefore
eligible for measured discovery and later provider completion instead of being
stranded in HOLD. Only the normal evidence-contract machinery can grant direct
annual authority.

Binary/opaque unknown formats, or textual formats that require a genuinely new
parser implementation, remain `HOLD` for explicit engineering rather than
being guessed by the model.

## Network policy

Optimize bytes and online minutes before bandwidth:

`metadata -> HEAD/range probe -> stratified sample -> baseline sample -> harvest`

Large artifacts are not downloaded merely because they are large or famous.
Uncompressed line-oriented data is range sampled when possible; compressed or
non-seekable formats use bounded probes or an index/sharded representation.

Online work and local work remain separate durable queues. A machine may be
offline or powered down indefinitely without invalidating committed state.

## Current implementation status

The production path now includes:

- durable SearchCells and finite `STRICT_4D/RELAX_INSTITUTION` programs;
- DataCite, Zenodo, Harvard Dataverse and Internet Archive provider adapters;
- provider-completeness and provider-schema fail-closed semantics;
- URL/artifact/dataset/family canonical identity and cell-local exclusions;
- provider-diverse result-cap enforcement;
- atomic deterministic residual commits and versioned restart recovery;
- protocol-guarded residual calibration reports;
- durable hard-negative / exact-recovery / provenance-hold research leads,
  with targeted recovery reported separately from residual-population search;
- deterministic measured-yield scouting with format/layout/schema bindings;
- deterministic arbitrary-field/column hostname-layout inference before any
  LLM call; and
- bounded `UNKNOWN_FORMAT -> COMPILE_ADAPTER -> deterministic validation ->
  re-scout` recovery for layouts executable by existing mature readers,
  including hostname-only records whose year must be completed later.

Routine LLM URL/source enumeration is not part of the production refill path.
