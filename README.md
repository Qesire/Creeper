# Creeper V2.2 Runtime Foundation

This tree is a local, reproducible implementation scaffold for the V3
historical Web hostname competition package.

The authority snapshot is external to the source tree. Configure its path
with `CREEPER_HOME` or pass paths explicitly. The current implementation
supports:

- exact reproduction of the bundled hostname normalizer and EED calculator;
- a year-aware SQLite baseline index with resumable imports;
- V3 candidate-source provenance and Common Crawl corpus exclusion;
- exact-host, exact-year CDX evidence acceptance states; and
- durable `SourceDomain → Reservoir → WorkLease` progress with byte cursors,
  bounded evidence draining, and restart recovery;
- immutable submission zip export with evidence provenance.

The fixed Common Crawl TLD model remains available to EED calculation only.
Common Crawl corpus discoveries cannot enter the active candidate set.

The project-level competition rules and current compliance audit are maintained
in [`docs/competition-rules-v3.md`](docs/competition-rules-v3.md) and
[`docs/competition-rules-v3-audit.md`](docs/competition-rules-v3-audit.md).
In particular, a dated JISC/UKWA or Arquivo CDX/CDXJ record is treated as
direct year-specific evidence when its timestamp and record provenance are
preserved; undated discovery sources and metadata-only hints remain outside
the annual master files.

## Local commands

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v
PYTHONPATH=src python3 scripts/authority_manifest.py <task-root> <manifest.json>
PYTHONPATH=src python3 scripts/official_eed.py <annual.txt> <q2_tld_top_langs.json> <out-dir>
PYTHONPATH=src python3 scripts/build_baseline.py <task-root> <index.sqlite3>
PYTHONPATH=src python3 -m creeper.cli doctor <task-root> <data-root>
PYTHONPATH=src python3 -m creeper.cli run --once conf/creeper.example.toml
PYTHONPATH=src python3 scripts/run_evidence_pilot.py <task-root> <index.sqlite3> <report-dir> --limit 3 --year 1997 --requests-per-second 1
PYTHONPATH=src python3 scripts/run_offline_dry_run.py <task-root> <index.sqlite3> <report-dir> --documentation <methods.docx>
PYTHONPATH=src python3 scripts/run_lookup_bench.py <task-root> <index.sqlite3> <report.json> --limit 100000
PYTHONPATH=src python3 scripts/evaluate_performance.py <performance-gate.json> --lookup-report <lookup.json> --efficiency-report <efficiency_v1.json>
PYTHONPATH=src python3 scripts/run_eed_readiness.py <report-dir> --accepted-dir <annual-results> --baseline-dir <annual-baseline> --model <q2_tld_top_langs.json> --baseline-eed <value> --elapsed-seconds <seconds> --run-id <id>
```

The full V3 index trial on the reference workspace produced 41,007,905
annual hostnames and 61,507,012 candidate hostnames in about 2m49s, with a
peak resident set of about 24 MB. Re-run the benchmark on the target machine;
the result is machine- and filesystem-dependent.

The real-network pilot is intentionally bounded and single-worker. It records
`PASS`, `EMPTY_EXHAUSTIVE`, `INCOMPLETE`, and transient failures separately;
synthetic dry-run evidence is never treated as an official competition result.
The pilot writes a JSONL checkpoint and can resume terminal tasks without
repeating HTTP requests; transient and incomplete tasks remain retryable.

`run --once` is currently an offline, synchronous production-contract test.
Each invocation claims one fresh bounded lease, advances the durable Reservoir
cursor, and never resets an exhausted source. An optional `[submission]` table
can load local baseline-manifest and EED-report JSON files so the command also
reports `snapshot_ready` and the current novel-record count; it does not create
or upload a competition package.

`run_eed_readiness.py` is the measurement entry point for E0/E5 readiness
reports. It calculates annual novel EED only after subtracting the matching
baseline year files, then writes `eed-report.json` and `run.json` with the
five-percent delta and ETA. Its input annual files must be committed evidence
outputs; synthetic fixture results are valid for correctness tests only and
must not be reported as competition throughput.

## Competition throughput gates

The annual formal track is evaluated separately from the Candidate track. The
current planning gates are 250k annual EED/day for a clearly competitive run
and 500k annual EED/day for a strong run. The local batch resolver benchmark
uses 100k hosts/s as an engineering floor; the current 100k-present plus
100k-absent sample reached about 518k hosts/s. These are engineering gates,
not organizer scores. The user-supplied peer result used for projections is
recorded as unverified in `performance_gate_v1.json`.
