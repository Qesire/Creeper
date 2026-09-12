"""Safe deterministic expansion of LLM-proposed source motifs.

Codex may infer a compact URL template, but it never enumerates an unbounded
frontier inside Creeper. This module accepts only explicit finite variable
lists and expands them under a hard cardinality bound.
"""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass
from typing import Any

_PLACEHOLDER_RE = re.compile(r"\\{([A-Z][A-Z0-9_]*)\\}")


class MotifProtocolError(ValueError):
    """Raised when an LLM motif cannot be expanded safely."""


@dataclass(frozen=True)
class MotifExpansionPolicy:
    max_expansions: int = 256

    def __post_init__(self) -> None:
        if self.max_expansions < 1:
            raise ValueError("max_expansions must be positive")


def expand_template(
    template: str,
    variables: dict[str, list[str | int]],
    *,
    policy: MotifExpansionPolicy | None = None,
) -> tuple[str, ...]:
    policy = policy or MotifExpansionPolicy()
    if not isinstance(template, str) or not template.strip():
        raise MotifProtocolError("template must be a non-empty string")
    if not isinstance(variables, dict):
        raise MotifProtocolError("variables must be an object")

    placeholders = tuple(dict.fromkeys(_PLACEHOLDER_RE.findall(template)))
    if set(placeholders) != set(variables):
        raise MotifProtocolError(
            "template placeholders and variable keys must match exactly"
        )
    if not placeholders:
        raise MotifProtocolError("template must contain at least one placeholder")

    choices: list[tuple[str | int, ...]] = []
    cardinality = 1
    for name in placeholders:
        raw = variables[name]
        if (
            not isinstance(raw, list)
            or not raw
            or any(
                isinstance(value, bool) or not isinstance(value, (str, int))
                for value in raw
            )
        ):
            raise MotifProtocolError(
                f"variable {name} must be a non-empty scalar list"
            )
        values = tuple(dict.fromkeys(raw))
        cardinality *= len(values)
        if cardinality > policy.max_expansions:
            raise MotifProtocolError(
                f"motif expansion exceeds max_expansions={policy.max_expansions}"
            )
        choices.append(values)

    expanded: list[str] = []
    for combination in itertools.product(*choices):
        url = template
        for name, value in zip(placeholders, combination, strict=True):
            url = url.replace("{" + name + "}", str(value))
        expanded.append(url)
    return tuple(expanded)


def candidate_payloads_from_hypothesis(
    hypothesis: dict[str, Any],
    *,
    policy: MotifExpansionPolicy | None = None,
) -> tuple[tuple[str, dict[str, Any]], ...]:
    """Reduce one LLM hypothesis to finite candidate metadata."""
    if not isinstance(hypothesis, dict):
        raise MotifProtocolError("hypothesis must be an object")
    allowed = {
        "hypothesis_id",
        "action",
        "candidate",
        "template",
        "variables",
        "candidate_defaults",
        "expected_mechanism",
        "confidence",
        "validation",
    }
    unknown = set(hypothesis) - allowed
    if unknown:
        raise MotifProtocolError(
            f"unknown hypothesis fields: {sorted(unknown)}"
        )

    hypothesis_id = hypothesis.get("hypothesis_id")
    action = hypothesis.get("action")
    if not isinstance(hypothesis_id, str) or not hypothesis_id.strip():
        raise MotifProtocolError("hypothesis_id is required")
    if not isinstance(action, str) or not action.strip():
        raise MotifProtocolError("hypothesis action is required")

    if action in {"PROBE_URL", "SEARCH_WEB_RESULT", "EXPAND_CATALOG"}:
        candidate = hypothesis.get("candidate")
        if not isinstance(candidate, dict):
            raise MotifProtocolError(f"{action} requires candidate metadata")
        return ((hypothesis_id, dict(candidate)),)

    if action == "ENUMERATE_TEMPLATE":
        defaults = hypothesis.get("candidate_defaults")
        if not isinstance(defaults, dict):
            raise MotifProtocolError(
                "ENUMERATE_TEMPLATE requires candidate_defaults"
            )
        template = hypothesis.get("template")
        variables = hypothesis.get("variables")
        urls = expand_template(template, variables, policy=policy)
        return tuple(
            (
                hypothesis_id,
                {**defaults, "canonical_entrypoint": url},
            )
            for url in urls
        )

    raise MotifProtocolError(f"unsupported hypothesis action: {action}")
