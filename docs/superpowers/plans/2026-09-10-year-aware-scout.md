# Year-Aware Measured Scout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make measured source admission rank dated resources by baseline-missing hostname-year pairs while keeping undated resources conservative.

**Architecture:** Extend the immutable scout measurement with an explicit `HOST_ONLY` or `HOST_YEAR` mode. Dated parsers emit unique host-year pairs and compare each pair against the corresponding annual baseline bit; undated parsers retain the existing whole-host test. Admission and source portfolio ranking consume the mode-appropriate EED value.

**Tech Stack:** Python 3.12, `unittest`, SQLite `BaselineIndex`, `Decimal` EED weights, existing WARC/ARC/CDXJ/JSONL/CSV/TXT parsers.

## Global Constraints

- Annual years are exactly `1996` through `2001`.
- A year is novel only when the exact hostname's corresponding baseline bit is absent.
- Undated records must never receive an inferred year.
- Existing `ScoutMeasurement` callers remain valid through defaults.
- Existing source parser behavior remains unchanged for undated inputs.
- Synthetic fixtures validate correctness only and are not competition throughput.

---

### Task 1: Extend measurement semantics

**Files:**
- Modify: `src/creeper/source_discovery/models.py`
- Test: `tests/unit/test_source_discovery_registry.py`

**Interfaces:**
- Consumes: existing `ScoutMeasurement` construction and source manager ranking.
- Produces: `MeasurementMode`, `observed_host_year_pairs`, `novel_host_year_pairs`, `novel_pair_eed`, and a `novel_eed_for_ranking` property.

- [ ] **Step 1: Write failing tests** for explicit mode, pair counts, pair EED, and compatibility with old constructors.
- [ ] **Step 2: Run the focused tests and confirm the new fields/properties are missing.**
- [ ] **Step 3: Add the enum and fields with validation; default old callers to `HOST_ONLY`.**
- [ ] **Step 4: Make ranking use `novel_pair_eed` only for `HOST_YEAR`, otherwise `novel_eed`.**
- [ ] **Step 5: Run the focused model and source-reservoir tests.**

### Task 2: Preserve dated pairs through measured parsing

**Files:**
- Modify: `src/creeper/source_discovery/measured_scout.py`
- Test: `tests/unit/test_measured_yield_scout.py`
- Test: `tests/unit/test_measured_yield_warc.py`
- Test: `tests/unit/test_measured_yield_tabular.py`

**Interfaces:**
- Consumes: parser output and `BaselineIndex.resolve_batch()`.
- Produces: a measurement with `HOST_YEAR` for WARC/ARC, CDXJ, and structured records with a recognized year; `HOST_ONLY` for undated text/structured records.

- [ ] **Step 1: Add a fixture where `foo.edu` exists in baseline 1997 but not 1998, and a dated source record reports 1998.**
- [ ] **Step 2: Run the focused test and confirm current hostname-only logic reports zero novel yield.**
- [ ] **Step 3: Add internal parsed-observation representation carrying optional year and derive unique hosts/pairs.**
- [ ] **Step 4: Implement per-pair baseline masking and mode-appropriate EED calculation.**
- [ ] **Step 5: Preserve HOST_ONLY behavior for TXT and undated JSONL/CSV.**
- [ ] **Step 6: Run all measured-scout format tests.**

### Task 3: Verify admission and ranking behavior

**Files:**
- Modify: `src/creeper/source_discovery/measured_scout.py`
- Modify: `src/creeper/source_discovery/manager.py`
- Test: `tests/unit/test_source_reservoir_manager.py`
- Test: `tests/unit/test_measured_yield_scout.py`

**Interfaces:**
- Consumes: `ScoutMeasurement.novel_eed_for_ranking` and mode-appropriate novel count/fraction.
- Produces: WARM/HOLD decisions that do not discard dated missing-year inventory and portfolio ranking based on pair EED rate when available.

- [ ] **Step 1: Add a failing admission test where host novelty is zero but pair novelty clears the warm thresholds.**
- [ ] **Step 2: Run it and confirm current admission holds the source.**
- [ ] **Step 3: Select pair metrics for HOST_YEAR and host metrics for HOST_ONLY.**
- [ ] **Step 4: Run focused admission, manager, and scout tests.**

### Task 4: Full verification and handoff

**Files:**
- Modify: `docs/superpowers/specs/2026-09-10-year-aware-scout-design.md` only if implementation semantics require clarification.

- [ ] **Step 1: Run all tests, lock check, build, and diff check.**
- [ ] **Step 2: Inspect the final diff for accidental changes to production evidence authority.**
- [ ] **Step 3: Commit and push the implementation.**
- [ ] **Step 4: Report that real EED/day competitiveness remains unmeasured until a productive dated reservoir and evidence provider are available.**
