#!/usr/bin/env python3
"""Run one bounded Codex source-intelligence child agent.

Creeper remains the parent authority. This adapter invokes a fresh non-interactive
Codex child, gives it only request.json, constrains its final response with a
JSON schema, normalizes nullable schema fields, and writes response.json.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any


_SYSTEM_POLICY = """You are a source-intelligence child agent of Creeper.

Your only authority is to propose bounded, testable source hypotheses.
Optimize expected marginal FINAL Accepted Novel EED per total resource cost.

You must not:
- declare a hostname or host-year novel,
- declare evidence valid,
- bypass baseline checks,
- authorize submission,
- invent measurements,
- modify the Creeper repository or runtime state.

Prefer:
1. large enumerable historical-web resources,
2. timestamp-bearing direct evidence,
3. compact URL templates over long URL lists,
4. metasources/catalogs that reveal many concrete resources,
5. hypotheses testable with very few requests.

Use live web research when the task requires discovery. Return only the
structured final response required by the supplied schema.
"""


def _drop_nulls(candidate: dict[str, Any] | None) -> dict[str, Any] | None:
    if candidate is None:
        return None
    return {key: value for key, value in candidate.items() if value is not None}


def _normalize_hypothesis(raw: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "hypothesis_id": raw["hypothesis_id"],
        "action": raw["action"],
        "expected_mechanism": raw.get("expected_mechanism", ""),
        "confidence": raw.get("confidence", 0.0),
        "validation": raw.get(
            "validation",
            {"method": "HEAD_OR_RANGE", "max_requests": 1, "max_bytes": 65536},
        ),
    }
    candidate = _drop_nulls(raw.get("candidate"))
    if candidate is not None:
        result["candidate"] = candidate

    template = raw.get("template")
    if template is not None:
        result["template"] = template

    variables = raw.get("variables")
    if isinstance(variables, dict):
        filtered = {
            key: value
            for key, value in variables.items()
            if value is not None
        }
        if filtered:
            result["variables"] = filtered

    defaults = _drop_nulls(raw.get("candidate_defaults"))
    if defaults is not None:
        result["candidate_defaults"] = defaults
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--response", required=True, type=Path)
    parser.add_argument("--codex-bin", default=os.environ.get("CODEX_BIN", "codex"))
    parser.add_argument("--schema", type=Path)
    parser.add_argument("--model")
    parser.add_argument("--effort")
    args = parser.parse_args(argv)

    request_path = args.request.resolve()
    response_path = args.response.resolve()
    request = json.loads(request_path.read_text(encoding="utf-8"))

    schema = args.schema
    if schema is None:
        schema = (
            Path(__file__).resolve().parents[1]
            / "conf"
            / "codex-source-intelligence.schema.json"
        )
    schema = schema.resolve()
    if not schema.is_file():
        raise SystemExit(f"Codex output schema not found: {schema}")

    prompt = (
        _SYSTEM_POLICY
        + "\n\nCREEPER REQUEST JSON:\n"
        + json.dumps(request, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    )

    response_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="creeper-codex-",
        dir=response_path.parent,
    ) as tmp_raw:
        tmp = Path(tmp_raw)
        output = tmp / "codex-final.json"
        command = [
            args.codex_bin,
            "exec",
            "--sandbox",
            "read-only",
            "--skip-git-repo-check",
            "--ephemeral",
            "--config",
            'approval_policy="never"',
            "--config",
            'web_search="live"',
            "--output-schema",
            str(schema),
            "--output-last-message",
            str(output),
        ]
        if args.model:
            command.extend(["--model", args.model])
        if args.effort:
            command.extend(
                ["--config", f'model_reasoning_effort="{args.effort}"']
            )
        command.append("-")

        stderr_path = response_path.with_suffix(
            response_path.suffix + ".codex-stderr.log"
        )
        with stderr_path.open("w", encoding="utf-8") as stderr_stream:
            completed = subprocess.run(
                command,
                input=prompt,
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=stderr_stream,
                check=False,
            )
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)
        if not output.is_file():
            raise SystemExit("Codex completed without structured final output")
        payload = json.loads(output.read_text(encoding="utf-8"))

    hypotheses = payload.get("hypotheses")
    if not isinstance(hypotheses, list):
        raise SystemExit("Codex response hypotheses must be an array")
    normalized = {
        "query": payload.get("query", "codex source-intelligence episode"),
        "hypotheses": [
            _normalize_hypothesis(item)
            for item in hypotheses
            if isinstance(item, dict)
        ],
    }

    temporary = response_path.with_suffix(response_path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, response_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
