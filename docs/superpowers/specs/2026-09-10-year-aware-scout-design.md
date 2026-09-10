# Year-Aware Measured Scout Design

**Goal:** Make source admission estimate missing `(hostname, year)` inventory when a
source provides capture-year semantics, while preserving conservative hostname-only
measurement for undated sources.

## Semantics

Measured sources use one of two explicit modes:

- `HOST_YEAR`: the parser emits unique `(canonical_hostname, year)` pairs. A pair is
  novel when its year's baseline bit is absent.
- `HOST_ONLY`: the parser emits hostnames without reliable years. A hostname is
  novel only when its complete annual baseline mask is zero.

No source may infer a year from an undated hostname-only record.

## Measurement model

`ScoutMeasurement` retains the existing `unique_hosts`, `novel_hosts`, and `novel_eed`
fields for compatibility and adds:

- `measurement_mode`;
- `observed_host_year_pairs`;
- `novel_host_year_pairs`;
- `novel_pair_eed`.

For `HOST_YEAR`, `novel_pair_eed` is the primary downstream reward. For `HOST_ONLY`,
the existing `novel_eed` remains the primary conservative reward and pair metrics are
zero.

## Parser coverage

- WARC/ARC metadata and CDXJ timestamps use `HOST_YEAR`.
- JSONL/CSV/TSV use `HOST_YEAR` only when a recognized year/timestamp field parses to
  1996–2001; otherwise they use `HOST_ONLY`.
- TXT/list resources use `HOST_ONLY`.

## Admission and ranking

Warm thresholds use the mode-appropriate novel count and EED value. Source ranking
uses the same mode-appropriate EED rate, so a dated source with already-seen hosts but
new years is not discarded.

## Compatibility and safety

Existing callers that construct `ScoutMeasurement` without the new fields continue to
work through defaults. Existing undated-source behavior remains unchanged. Tests must
prove that a hostname present in one baseline year but absent in another contributes
the missing year pair, while undated input never fabricates a pair.
