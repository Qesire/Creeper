# Competition Readiness EED Test Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Determine whether Creeper can enter the competition by proving exact annual EED correctness, evidence completeness, sustainable novel-EED production, and readiness to form a valid 5% submission batch.

**Architecture:** The test program treats `(canonical_hostname, year)` as the unit of truth. It computes annual novel EED from committed, exact-year evidence after baseline exclusion, then independently recomputes the same result with the official EED script. Performance is measured downstream from evidence commits; raw hosts, candidate counts, and estimated weights are diagnostic only.

**Tech Stack:** Python 3.12, `unittest`, `uv`, Creeper baseline/EED modules, SQLite evidence stores, WARC/ARC fixtures, bounded runtime leases, and the official EED model.

## Global Constraints

- Annual primary years are exactly `1996` through `2001`.
- Annual novelty is evaluated on exact `(hostname, year)` pairs after official normalization.
- EED uses the configured `eed-v1` model and right-most-TLD English share.
- A hostname-year without valid evidence is not an accepted annual result.
- An incomplete evidence query cannot be treated as negative evidence.
- Annual baseline overlap is excluded and must not enter the submission annual files.
- Candidate and annual tracks remain separate until the organizer confirms score combination.
- Common Crawl may provide the EED weight model but its discovered records cannot enter active candidates.
- Synthetic evidence is valid for correctness tests only and cannot be reported as competition production.
- Every production result must retain source, locator, provider, policy, and code revision provenance.
- Every performance run must record wall time, requests, bytes, queue depth, and resource usage.

---

## 1. Primary decision metrics

For a run window (W), define the committed annual result set:

```text
A_W = unique {(canonical_hostname, year) | valid exact-year evidence committed in W}
      minus the corresponding official baseline pairs
```

The primary result is:

```text
annual_novel_eed(W) = sum(EnglishWeight(rightmost_tld(hostname)) for (hostname, year) in A_W)
annual_eed_per_day = annual_novel_eed(W) / wall_seconds(W) * 86400
```

The official baseline EED must be loaded from the current authority manifest/EED report. Do not hardcode the planning estimate. The submission gate is:

```text
five_percent_delta = baseline_eed * 0.05
eta_to_five_percent_days = five_percent_delta / annual_eed_per_day
```

For orientation only, if the currently recorded baseline estimate is approximately `34.887M`, the required novel EED is approximately `1.744M`; the final test uses the actual current manifest value.

### Performance interpretation

| Annual novel EED/day | Interpretation |
|---:|---|
| `<100k` | Engineering works, but competition entry is not yet competitive under the current plan |
| `100k–250k` | Minimum competitive band; requires strong evidence-quality audit |
| `>=250k` | Clearly competitive planning target |
| `>=500k` | Strong target |
| `>=1M` | Planning target for a decisive advantage; requires a high-yield source portfolio |

The formal “ready to submit” decision also requires an actually materialized valid 5% package or a demonstrated production run whose conservative ETA is within the competition deadline. A projection alone is never an official score.

---

## 2. Test matrix and gates

### Gate E0 — Authority and EED oracle

Purpose: prove that the baseline snapshot, normalizer, and EED implementation are using the same authority.

| ID | Test | Required assertion |
|---|---|---|
| E0.1 | Baseline identity | `baseline_id`, six annual files, SHA-256 hashes, line counts, and model path match the current authority manifest |
| E0.2 | Official calculator parity | `creeper.authority.eed.calculate_eed()` and `scripts/official_eed.py` produce identical total EED and TLD rows for the same file/model |
| E0.3 | Decimal stability | Re-running the calculation produces byte-identical JSON numeric strings; no binary-float rounding changes the total |
| E0.4 | Duplicate invariance | Adding repeated identical hostnames does not change unique valid count or EED |
| E0.5 | Order invariance | Permuting input lines does not change the EED report |
| E0.6 | Host-year distinction | The same hostname in two years counts as two annual records; base hostname and subdomain remain distinct |
| E0.7 | Invalid/unmatched behavior | Invalid hostnames and valid hostnames with no EED model weight contribute zero and are counted in the correct report fields |
| E0.8 | Model audit | Every nonzero EED contribution can be traced to one normalized hostname, right-most TLD, and exact model weight |

Pass condition: all assertions pass and the independent oracle difference is exactly zero for the bundled model. If a separately implemented oracle is used, absolute difference must be at most `1e-12` EED and every discrepancy must be explained.

### Gate E1 — Evidence-to-EED correctness

Purpose: prove that only accepted annual evidence can create annual novel EED.

| ID | Test | Required assertion |
|---|---|---|
| E1.1 | Exact target year | A 1998 target needs accepted 1998 evidence; 1997/1999 captures do not satisfy it |
| E1.2 | Status policy | Only status codes allowed by `evidence-v1` can produce PASS |
| E1.3 | Exact hostname | A capture for `www.example.com` cannot satisfy `example.com`, and vice versa |
| E1.4 | Baseline exclusion | A PASS evidence pair already present in the annual baseline contributes zero novel EED |
| E1.5 | Other-year acceptance | A baseline hit in 1998 does not exclude the same hostname in 1999 when 1999 is absent from baseline |
| E1.6 | Incomplete negative | `INCOMPLETE` and transient failures do not create accepted negative evidence or a submission-ready result |
| E1.7 | Direct-year authority | A source record with trusted direct year metadata bypasses external evidence only through the authorized direct-evidence path and still produces a complete capsule |
| E1.8 | Discovery-only boundary | A source record with only a year hint creates an evidence task, never an annual accepted result by itself |
| E1.9 | Idempotency | Replaying the same capsule/task does not increase annual records or EED |
| E1.10 | Provider/policy identity | The task key distinguishes hostname, temporal scope, provider, and policy version; changing provider or policy cannot be incorrectly skipped |

Pass condition: zero false-positive annual pairs, zero unexplained missing evidence pairs, zero duplicate EED contribution, and all accepted records have complete `EvidenceCapsule` provenance.

### Gate E2 — End-to-end EED golden fixture

Purpose: prove the complete path from source records to an independently checkable EED result.

Use a small fixture containing at least:

```text
known.com       1996  baseline overlap
novel.com       1997  accepted exact-year evidence
novel.com       1998  accepted distinct year
www.novel.com   1997  accepted distinct hostname
late.org        2004  out of annual scope
missing.net     1999  no accepted evidence
duplicate.com   2000  same pair repeated twice
```

The test must:

1. run the source adapter through one bounded lease;
2. normalize and baseline-resolve host observations;
3. create direct evidence or durable evidence tasks as appropriate;
4. commit evidence through the normal writer path;
5. build the runtime snapshot;
6. export annual files and `evidence.jsonl`;
7. run the independent submission verifier;
8. run the official EED calculator over exported annual files; and
9. compare the calculator total with the snapshot `novel_eed` field.

Pass condition: the exported package is verifier-ready, annual files contain only the expected novel pairs, every annual pair has evidence, and snapshot/report/calculator totals are identical.

### Gate E3 — Durable and lossless production semantics

Purpose: ensure that throughput measurements are not inflated by dropped or replayed work.

| ID | Test | Required assertion |
|---|---|---|
| E3.1 | Multi-lease progression | Two or more leases consume disjoint source ranges and produce no repeated source records |
| E3.2 | Restart/resume | A process restart resumes from the durable cursor and eventually covers every fixture record exactly once logically |
| E3.3 | Queue saturation | A full downstream queue pauses or limits upstream work; it never silently drops observations or evidence tasks |
| E3.4 | Crash recovery | Killing the runtime between evidence commit and task completion leaves an idempotently recoverable state without losing accepted EED |
| E3.5 | Exhaustion | An exhausted reservoir is not reset or reissued after restart |
| E3.6 | Bounded memory | RSS remains within the configured budget while processing a fixture larger than every queue capacity |
| E3.7 | Baseline refresh | Refreshing the official baseline changes the novelty view and EED report without re-querying already PASS evidence |

Pass condition: final committed annual pair set equals the no-failure reference run; set equality is the primary assertion, not only row counts.

### Gate E4 — Source yield and evidence conversion pilot

Purpose: measure actual annual EED production by source and identify the bottleneck.

Each source must be evaluated on disjoint reservoir partitions. Re-running the same partition is a reliability check, not an independent production replicate.

#### Pilot sizes

| Stage | Work unit | Purpose |
|---|---:|---|
| E4.0 smoke | 10,000 source records or the first bounded lease | detect parser, cursor, normalization, and evidence-policy errors |
| E4.1 block | 100,000 source records or 1 hour, whichever comes first | estimate accepted annual EED/hour and conversion rates |
| E4.2 soak | 24 continuous hours per selected source portfolio | measure sustainable EED/day, queue stability, and provider behavior |
| E4.3 submission batch | until `annual_novel_eed >= baseline_eed * 0.05` | demonstrate actual 5% package formation |

For every stage record:

```text
source_domain
reservoir_id
lease_ids
partition/cursor range
source records
raw hostname observations
unique canonical hosts
baseline-external hosts
direct evidence pairs
external evidence tasks
PASS tasks
EMPTY_EXHAUSTIVE tasks
INCOMPLETE tasks
transient/invalid tasks
accepted annual host-years
annual novel EED
requests and bytes by provider
wall time and CPU time
RSS and spool bytes
queue p50/p95/max depth
```

Derived metrics:

```text
host_yield = unique_canonical_hosts / source_records
novel_host_fraction = baseline_external_hosts / unique_canonical_hosts
evidence_pass_rate = PASS / evidence_tasks
annual_accept_rate = accepted_annual_pairs / evidence_tasks
raw_to_eed = annual_novel_eed / baseline_external_hosts
annual_eed_per_hour = annual_novel_eed / wall_hours
eed_per_evidence_request = annual_novel_eed / evidence_requests
bytes_per_eed = total_bytes / annual_novel_eed
```

The production result is `annual_novel_eed`; `raw_to_eed` is a diagnostic ratio and must not replace the official EED calculation.

#### Blocking and run order

- Block by source domain, reservoir partition, calendar day, and evidence provider.
- Perform a fixed warm-up and exclude it from the production numerator/denominator.
- Randomize the order of source partitions within each measurement block using a recorded seed.
- Do not run all annual-only sources on one day and all candidate-only sources on another; candidate and annual remain separate analysis tracks.
- Keep provider rate limits and concurrency configuration fixed within a block; change one resource factor at a time in a follow-up benchmark.

Pass condition for the entry-readiness pilot: no E0–E3 failure, sustained annual EED/day is measured from committed evidence, and the report includes a reproducible source/lease ledger.

### Gate E5 — Throughput and 5% submission readiness

Purpose: determine whether the system is operationally competitive, not merely correct on a small fixture.

Required outputs:

```text
actual_annual_novel_eed
actual_annual_eed_per_day
five_percent_delta
confirmed_fraction_of_five_percent
eta_to_five_percent_days
number_of_validated_submissions
```

Decision thresholds:

| Gate | Requirement |
|---|---|
| E5.1 correctness | E0–E3 all pass |
| E5.2 minimum viable production | At least `100k annual novel EED/day` over E4.2, with no unresolved audit defect |
| E5.3 clearly competitive | At least `250k annual novel EED/day` over E4.2 |
| E5.4 strong | At least `500k annual novel EED/day` over E4.2 |
| E5.5 5% batch | Actual valid annual novel EED reaches `baseline_eed * 0.05`; package verifier and official EED oracle both pass |
| E5.6 continuous operation | No queue growth trend, no repeated lease ranges, and no resource limit breach for the full soak |

If E5.2 passes but E5.5 is not yet materialized, the correct status is “technically eligible for extended production, not yet submission-ready.” If E5.3 and E5.5 pass, the system meets the current competition-entry target. Candidate metrics may be appended as a separate scenario report only.

---

## 3. Required test artifacts

Every test run produces a directory with:

```text
run.json
authority-manifest.json
eed-report.json
source-ledger.jsonl
lease-ledger.jsonl
evidence-audit.jsonl
queue-metrics.jsonl
resource-metrics.jsonl
submission-snapshot.json
annual/{1996..2001}.txt
evidence.jsonl
```

`run.json` must include:

```json
{
  "run_id": "...",
  "code_revision": "...",
  "baseline_id": "merged260909-3",
  "eed_policy_version": "eed-v1",
  "normalizer_version": "official-calculator-regex-v1",
  "evidence_policy_version": "evidence-v1",
  "source_partition_seed": 0,
  "started_at": "...",
  "ended_at": "...",
  "track": "annual"
}
```

The annual report must contain both the total EED and the per-TLD contribution table. A single aggregate count without the TLD breakdown is insufficient for audit.

---

## 4. Implementation tasks for the test harness

### Task 1: Add EED metamorphic and oracle tests

**Files:**
- Modify: `tests/golden/test_official_eed.py`
- Create: `tests/golden/test_eed_oracle_parity.py`
- Use: `src/creeper/authority/eed.py`, `scripts/official_eed.py`, `conf/policies/eed-v1.toml`

**Interfaces:**
- Consumes: a temporary annual hostname file and the configured EED model JSON.
- Produces: exact total/report equality assertions and permutation/duplicate invariants.

- [ ] Add fixtures covering duplicate lines, case/whitespace, base/subdomain distinction, invalid values, unmatched TLDs, and six target years.
- [ ] Compare `calculate_eed()` to a subprocess invocation of `scripts/official_eed.py`.
- [ ] Assert exact equality of `equivalent_english_domains`, matched/unmatched counts, and sorted TLD rows.
- [ ] Run `uv run python -m unittest tests.golden.test_official_eed tests.golden.test_eed_oracle_parity -v`.

### Task 2: Add evidence-to-EED acceptance tests

**Files:**
- Modify: `tests/unit/test_evidence_policy.py`, `tests/unit/test_evidence_planner.py`
- Create: `tests/integration/test_eed_evidence_pipeline.py`
- Use: `src/creeper/evidence/policies.py`, `src/creeper/evidence/planner.py`, `src/creeper/runtime/submission.py`

**Interfaces:**
- Consumes: `EvidenceQueryResult`, `EvidenceCapsule`, `BaselineIndex`, and a temporary evidence store.
- Produces: accepted annual pair sets and exact EED totals.

- [ ] Add exact-year, exact-hostname, status, baseline-overlap, incomplete-query, direct-year, and year-hint cases.
- [ ] Assert replaying evidence does not change pair count or EED.
- [ ] Assert provider and policy changes create distinct durable task identities.
- [ ] Run `uv run python -m unittest tests.unit.test_evidence_policy tests.unit.test_evidence_planner tests.integration.test_eed_evidence_pipeline -v`.

### Task 3: Add independent golden submission test

**Files:**
- Create: `tests/integration/test_eed_submission_golden.py`
- Use: `src/creeper/submission/builder.py`, `src/creeper/submission/export.py`, `src/creeper/submission/verify.py`, `scripts/official_eed.py`

**Interfaces:**
- Consumes: the E2 fixture and a temporary baseline/model.
- Produces: a verifier-ready package plus independently recalculated annual EED.

- [ ] Execute the normal source-to-evidence-to-snapshot path.
- [ ] Export annual files and evidence.
- [ ] Verify the package with the independent verifier.
- [ ] Recalculate every annual file with the official EED script.
- [ ] Assert package report, snapshot report, and calculator report are identical.

### Task 4: Add production measurement harness

**Files:**
- Create: `scripts/run_eed_readiness.py`
- Create: `tests/integration/test_eed_readiness_metrics.py`
- Modify: `src/creeper/metrics/source_stats.py`
- Modify: `src/creeper/runtime/submission.py`

**Interfaces:**
- Consumes: source/lease results, evidence commits, authority manifest, and EED model.
- Produces: `run.json`, source/lease ledgers, exact EED report, rate/ETA fields, and resource metrics.

- [ ] Use disjoint cursor partitions and record the partition seed.
- [ ] Compute EED only from the committed annual pair set after baseline exclusion.
- [ ] Emit annual and Candidate metrics in separate namespaces.
- [ ] Add explicit `five_percent_delta` and `eta_to_five_percent_days` fields.
- [ ] Add tests for zero-EED, partial-EED, exact-threshold, and over-threshold ETA calculations.

### Task 5: Execute the readiness campaign

**Files:**
- Create: `reports/competition-readiness/<run-id>/` at execution time
- Use: `scripts/run_eed_readiness.py`, `scripts/official_eed.py`, `scripts/verify_submission.py`, `scripts/evaluate_performance.py`

**Interfaces:**
- Consumes: current authority snapshot, EED model, selected source reservoirs, and live evidence providers under configured rate limits.
- Produces: a signed-off readiness report with E0–E5 gate status.

- [ ] Run E0–E2 offline with the full current test suite.
- [ ] Run E4.0 and E4.1 on at least two disjoint source partitions.
- [ ] Run E4.2 for a continuous 24-hour annual-track soak.
- [ ] Run E4.3 until the exact 5% delta is reached or the available reservoir is exhausted.
- [ ] Publish the final report only after official EED and independent submission verification pass.

---

## 5. Current baseline and expected status

The repository already covers many component-level prerequisites: the golden EED calculator test, normalizer tests, evidence policy tests, scheduler EED tests, runtime submission tests, and WARC/ARC parser tests. The missing evidence for competition entry is not another unit-test count; it is:

```text
independent calculator parity on exported annual results
→ exact evidence-to-EED end-to-end closure
→ lossless multi-lease production measurement
→ sustained annual novel EED/day
→ actual 5% package formation
```

Until E5.2–E5.5 are measured on the current authority and real evidence path, the honest status is:

```text
correctness foundation: eligible for readiness testing
competition throughput: not demonstrated
submission readiness: not demonstrated
Candidate advantage: separate scenario only
```

---

## 6. Self-review checklist

- [ ] EED is always computed from accepted annual host-year pairs, never raw host counts.
- [ ] The baseline is explicit and refreshed from its manifest.
- [ ] The official calculator is used as an independent oracle.
- [ ] Candidate data never enters the annual numerator.
- [ ] Incomplete evidence is not interpreted as absence.
- [ ] Duplicate and replay behavior is tested as set equality.
- [ ] Source partitions are disjoint; repeated runs are not falsely treated as independent yield.
- [ ] The 5% gate uses the current baseline EED, not a hardcoded estimate.
- [ ] Performance reports include exact EED, wall time, and resource metrics.
- [ ] A final “enter” decision requires both correctness and demonstrated production output.
