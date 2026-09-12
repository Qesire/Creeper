# Creeper Source Intelligence vNext

## Purpose

Creeper vNext separates source acquisition into three layers:

1. a deterministic data plane for HTTP triage, parsing, baseline reconciliation, evidence, and submission authority;
2. a Codex intelligence plane for bounded source hypotheses and structure reasoning;
3. a statistical control plane for value estimation, exploration, and resource allocation.

The parent process always owns authority. Codex is a child process with proposal-only authority.

## Parent process -> Codex child process

SourceDiscoveryService creates SourceReservoirManager and CommandAgentSearchExecutor. The manager decides whether an intelligence call is useful. The executor then launches a fresh child command:

~~~text
<agent.command...> --request REQUEST.json --response RESPONSE.json
~~~

The default example uses scripts/creeper_codex_subagent.py. That wrapper launches a fresh non-interactive Codex CLI process in read-only sandbox mode with approval disabled and live web search enabled.

The child cannot access the registry connection, baseline index, EvidenceStore, or submission authority through the Creeper protocol.

### Task classes

The parent emits only four intelligence task types.

| Task | Parent trigger | Child objective |
| --- | --- | --- |
| DISCOVER_NEW_SOURCE | cold reservoir needs refill or explicit exploration | find new large enumerable resources or metasources |
| EXPLOIT_SUCCESS_PATTERN | a source family or archive origin has measured yield | infer sibling resources, manifests, catalogs, or compact URL templates |
| INTERPRET_STRUCTURE | deterministic structural scout leaves a catalog/metasource in HOLD | infer a small bounded set of resource/template hypotheses from the observed structure |
| RECOVER_STAGNATION | the recent completed search tail has no credited reward | search for a materially different source family or acquisition route |

Search and task allocation use UCB rather than pure greedy historical reward. Direct timestamp-bearing evidence and generic reservoir refill remain operationally reserved arms.

## Request input

Every invocation uses contract creeper.llm-source-intelligence.v2.

The request is a bounded state snapshot, not a database dump. It contains:

- task identity: task_type, strategy, subject, reason, desired candidate count;
- objective: maximize marginal FINAL Accepted Novel EED per total resource cost;
- inventory counts by source state;
- recent search-strategy reward/cost aggregates;
- Codex task reward/cost aggregates;
- top measured sources;
- recent terminal sources;
- subject-specific local source context;
- HTTP triage metadata when known;
- measured scout yield, residual-unseen estimate, and deterministic children when known;
- target years and hard competition constraints;
- admission thresholds;
- a context hash for reproducibility.

The builder intentionally omits SQLite paths, raw baseline data, raw hostname corpora, EvidenceStore authority, and submission state.

A conceptual request looks like:

~~~json
{
  "contract": "creeper.llm-source-intelligence.v2",
  "execution": {
    "parent_process": "creeper-source-discovery",
    "mode": "SUBAGENT",
    "authority": "proposal_only"
  },
  "task": {
    "task_type": "EXPLOIT_SUCCESS_PATTERN",
    "strategy": "EXPLOIT_DIRECT_ORIGIN",
    "subject": "https://archive.example",
    "desired_candidates": 20,
    "objective": "maximize marginal FINAL Accepted Novel EED per total resource cost"
  },
  "context_hash": "...",
  "context": {
    "inventory": {},
    "strategy_history": [],
    "llm_task_history": [],
    "top_measured_sources": [],
    "recent_terminal_sources": [],
    "subject_sources": [],
    "constraints": {}
  }
}
~~~

## Child output

Codex must return structured hypotheses matching conf/codex-source-intelligence.schema.json.

Allowed actions are:

- PROBE_URL
- SEARCH_WEB_RESULT
- EXPAND_CATALOG
- ENUMERATE_TEMPLATE

For templates, the child supplies a compact template plus explicit finite variable lists. Creeper performs the enumeration itself under max_motif_expansions.

Example:

~~~json
{
  "query": "annual archive siblings",
  "hypotheses": [
    {
      "hypothesis_id": "annual-cdx",
      "action": "ENUMERATE_TEMPLATE",
      "template": "https://archive.example/{YEAR}/index.cdxj.gz",
      "variables": {
        "YEAR": [1996, 1997, 1998, 1999, 2000, 2001]
      },
      "candidate_defaults": {
        "source_family": "BULK_ARTIFACT",
        "level": "SOURCE",
        "expected_year_from": 1996,
        "expected_year_to": 2001,
        "expected_volume": 100000,
        "temporal_semantics_prior": 1.0,
        "enumerability_prior": 0.95,
        "direct_evidence_prior": 1.0,
        "baseline_overlap_prior": 0.5,
        "access_cost_prior": 0.5,
        "adapter_cost_prior": 0.5,
        "confidence": 0.9
      },
      "expected_mechanism": "year-partitioned timestamp index",
      "confidence": 0.9,
      "validation": {
        "method": "HEAD_OR_RANGE",
        "max_requests": 6,
        "max_bytes": 65536
      }
    }
  ]
}
~~~

Codex never returns an authoritative novelty, evidence, WARM/ACTIVE, or submission decision. Every candidate still passes deterministic admission, triage, scout, baseline reconciliation, and evidence rules.

## Deterministic exploitation before another LLM call

Successful annual resources are also passed through the year-sibling motif inference. For example, one proven 2001 resource can deterministically produce bounded 1996-2000 siblings without another Codex call. Generated siblings are tagged so they cannot recursively fan out.

This implements the policy:

~~~text
LLM proposes an abstraction
    -> deterministic code enumerates
    -> deterministic code validates
    -> measured scout estimates value
~~~

rather than asking the LLM to enumerate long URL lists.

## Multi-fidelity scout

Line-oriented measurable resources use staged sampling with a fixed total byte ceiling. Default fidelity targets are approximately:

~~~text
64 KiB -> 512 KiB -> 2 MiB -> configured full scout budget
~~~

After every stage Creeper can:

- early-accept a strongly positive source;
- early-stop a saturated low-yield source;
- continue to the next fidelity.

Scout metrics include baseline-external EED, host-year mode, singleton/doubleton observations, Good-Turing-style unseen fraction, and a MinHash sketch.

These are scheduling signals only. They are not competition evidence.

## Source value

SourceReservoirManager ranks discovered, scout-ready, and warm sources using InterpretableSourceValueModel.

The model combines:

- Bayesian hurdle estimate of P(final reward > 0);
- positive final/scout conversion learned per source family;
- direct measured opportunity;
- discounted descendant final reward for gateway/catalog nodes;
- uncertainty bonus;
- MinHash overlap penalty against active sources;
- residual opportunity from the measured unseen fraction.

This makes the scheduler rank marginal expected final reward per cost rather than raw link count or static source type.

The deterministic link-promotion score remains only a bounded bootstrap/noise filter before learned value ranking.

## Reward closure

The lineage is:

~~~text
Codex episode
  -> hypothesis
  -> SourceCandidate
  -> measured scout proxy reward
  -> production/evidence
  -> incremental readiness
  -> FINAL Accepted Novel EED by source
  -> originating search strategy and Codex hypothesis
~~~

Scout EED is only a temporary proxy. SourceDiscoveryRegistry.record_final_reward supersedes it when formal readiness attribution is available.

If baseline or EED authority identity changes, readiness resets formal source rewards before replaying evidence under the new authority.

## Replay data

scripts/export_source_outcomes.py exports replay-safe JSONL rows. Each row separates:

- decision-time source priors;
- later HTTP triage observations;
- later scout observations;
- search and LLM lineage;
- final accepted-EED label and cost.

This separation is required to avoid future-information leakage in offline policy evaluation.

## Configuration

See conf/source-discovery.example.toml.

Important controls include:

~~~toml
[coordinator]
search_ucb_exploration = 0.35
stagnation_window = 6

[measurement]
progressive_initial_bytes = 65536
early_accept_multiplier = 4.0
early_reject_unseen_fraction = 0.01

[agent]
command = ["python", "../scripts/creeper_codex_subagent.py"]
backend = "codex-cli-subagent"
actor = "codex:source-intelligence"
timeout_seconds = 180.0
max_returned_hypotheses = 128
max_motif_expansions = 256
~~~

Path-like command arguments are resolved relative to the TOML file, so daemon behavior does not depend on shell working directory.

The Codex wrapper also accepts CODEX_BIN and optional model/effort arguments when used directly.

## Validation

The minimum merge gate is the repository CI:

- Python 3.12 full offline unittest suite;
- package build;
- git diff whitespace check;
- isolated Scrapy sidecar suite;
- Scrapy settings validation.

Production performance must still be established with bounded validation windows. The key comparison is final Novel EED per provider request / wall-clock under identical resource budgets, not merely scout novelty.
