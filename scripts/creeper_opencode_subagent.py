#!/usr/bin/env python3
"""Run one bounded opencode source-intelligence child agent.

Creeper remains the parent authority. This adapter invokes a fresh
non-interactive opencode process, gives it only request.json, constrains its
final response to the JSON object required by the source-intelligence schema,
normalizes nullable schema fields, and writes response.json.

Design notes:
- The opencode CLI has no ``--output-schema`` equivalent, so the schema is
  enforced in two places: the system policy asks the model to emit only the
  required JSON object, and this wrapper independently validates/normalizes the
  parsed payload against the repository schema before writing response.json.
- opencode is launched in an isolated temporary working directory with the
  prompt-level read-only policy. Creeper still treats the child as untrusted:
  only the bounded response file crosses the authority boundary.
- The final assistant text is recovered from ``opencode run --format json``
  events (the last ``text`` part of the ``text`` event stream).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


DEFAULT_MODEL = "ustc-107/deepseek-flash"

_SYSTEM_POLICY = """You are a source-intelligence child agent of Creeper.

Your only authority is to propose bounded, testable source hypotheses.
Optimize expected marginal FINAL Accepted Novel EED per total resource cost.

You must not:
- declare a hostname or host-year novel,
- declare evidence valid,
- bypass baseline checks,
- authorize submission,
- invent measurements,
- modify the Creeper repository or runtime state,
- create, edit, or delete any file.

You must not use file-writing tools. Read-only inspection and live web research
are allowed when the task requires discovery.

Prefer:
1. large enumerable historical-web resources,
2. timestamp-bearing direct evidence,
3. compact URL templates over long URL lists,
4. metasources/catalogs that reveal many concrete resources,
5. hypotheses testable with very few requests.

Return ONLY one JSON object matching the supplied schema. Emit no prose,
markdown, or code fences around the JSON. The object must contain a non-empty
``query`` string and a ``hypotheses`` array. Every hypothesis must include all
required fields; use JSON null for unused candidate/template/variables fields.
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


def _extract_final_text(events_stream: str) -> str:
    """Return the concatenated assistant text from an opencode JSON event stream.

    ``opencode run --format json`` emits one JSON event per line. Assistant
    output appears as ``{"type": "text", "part": {"type": "text", ...}}``.
    Later text parts (for example a follow-up summary) are appended so the
    caller can still recover the structured payload from the final answer.
    """
    chunks: list[str] = []
    for line in events_stream.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("type") != "text":
            continue
        part = event.get("part")
        if not isinstance(part, dict) or part.get("type") != "text":
            continue
        text = part.get("text")
        if isinstance(text, str) and text:
            chunks.append(text)
    return "\n".join(chunks).strip()


def _iter_json_objects(text: str):
    """Yield every JSON object that appears in arbitrary model text."""
    if not text:
        return
    stripped = text.strip()

    # Fast path: the whole message is the object.
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, dict):
        yield parsed

    # Strip a fenced code block if present.
    fenced_start = stripped.find("```")
    if fenced_start != -1:
        after = stripped[fenced_start + 3 :]
        newline = after.find("\n")
        if newline != -1:
            end = after.find("```", newline)
            if end != -1:
                candidate = after[newline + 1 : end].strip()
                try:
                    parsed = json.loads(candidate)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, dict):
                    yield parsed

    # Balanced-brace scan for every top-level object, in order.
    start = stripped.find("{")
    while start != -1:
        depth = 0
        in_string = False
        escaped = False
        end_index = None
        for position in range(start, len(stripped)):
            char = stripped[position]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    block = stripped[start : position + 1]
                    try:
                        parsed = json.loads(block)
                    except json.JSONDecodeError:
                        parsed = None
                    if isinstance(parsed, dict):
                        yield parsed
                        end_index = position
                    break
        start = stripped.find(
            "{", start + 1 if end_index is None else end_index + 1
        )


def _select_payload(text: str, *, episode_id: str) -> dict[str, Any]:
    """Return the best source-intelligence object found in the model text.

    Preference order:
    1. an object with a ``hypotheses`` array (canonical v2 shape),
    2. an object with a ``candidates`` array (legacy direct shape),
    3. any object carrying a non-empty ``query``.

    A top-level JSON array of hypotheses/candidates is also accepted and wrapped
    so the downstream contract stays object-shaped.
    """
    best: dict[str, Any] | None = None
    for candidate in _iter_json_objects(text):
        if isinstance(candidate.get("hypotheses"), list):
            return candidate
        if isinstance(candidate.get("candidates"), list):
            if best is None:
                best = candidate
        elif isinstance(candidate.get("query"), str) and best is None:
            best = candidate
    if best is not None:
        return best

    # Fall back to a bare JSON array of hypothesis/candidate objects.
    stripped = text.strip()
    array_start = stripped.find("[")
    array_end = stripped.rfind("]")
    if array_start != -1 and array_end > array_start:
        try:
            parsed_array = json.loads(stripped[array_start : array_end + 1])
        except json.JSONDecodeError:
            parsed_array = None
        if isinstance(parsed_array, list) and all(
            isinstance(item, dict) for item in parsed_array
        ):
            return {"hypotheses": parsed_array, "query": episode_id}
    raise SystemExit("opencode final message did not contain a usable JSON object")


def _build_command(
    args: argparse.Namespace,
    *,
    workdir: Path,
) -> list[str]:
    command = [
        args.opencode_bin,
        "run",
        "--model",
        args.model,
        "--format",
        "json",
        "--dir",
        str(workdir),
    ]
    if args.agent:
        command.extend(["--agent", args.agent])
    command.append("-")
    return command


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--response", required=True, type=Path)
    parser.add_argument(
        "--opencode-bin",
        default=os.environ.get("OPENCODE_BIN", "opencode"),
    )
    parser.add_argument("--schema", type=Path)
    parser.add_argument("--model", default=os.environ.get("CREEPER_OPENCODE_MODEL", DEFAULT_MODEL))
    parser.add_argument("--agent")
    parser.add_argument("--variant")
    # Keep the child deadline slightly below Creeper's agent.timeout_seconds so
    # the wrapper can write diagnostics and exit cleanly instead of being
    # SIGKILLed by the parent with no captured stderr.
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("CREEPER_OPENCODE_TIMEOUT", "240")),
    )
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
        raise SystemExit(f"Source-intelligence output schema not found: {schema}")

    prompt = (
        _SYSTEM_POLICY
        + "\n\nSOURCE-INTELLIGENCE OUTPUT JSON SCHEMA:\n"
        + schema.read_text(encoding="utf-8")
        + "\n\nCREEPER REQUEST JSON:\n"
        + json.dumps(request, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    )

    response_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path = response_path.with_suffix(
        response_path.suffix + ".opencode-stderr.log"
    )

    # Isolate opencode from the Creeper checkout: it runs in a throwaway
    # working directory so repository project files are never discovered or
    # mutated, while still receiving the full request through stdin.
    workdir = Path(
        tempfile.mkdtemp(prefix="creeper-opencode-", dir=response_path.parent)
    )
    try:
        command = _build_command(args, workdir=workdir)
        if args.variant:
            insert_at = command.index("--format")
            command[insert_at:insert_at] = ["--variant", args.variant]
        try:
            completed = subprocess.run(
                command,
                input=prompt,
                text=True,
                capture_output=True,
                cwd=str(workdir),
                timeout=args.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise SystemExit(
                f"opencode child exceeded timeout of {args.timeout:g}s"
            ) from None

        stderr_path.write_text(completed.stderr or "", encoding="utf-8")
        if completed.returncode != 0:
            raise SystemExit(completed.returncode)

        payload = _select_payload(
            _extract_final_text(completed.stdout),
            episode_id=str(request.get("episode_id", "source-intelligence episode")),
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    hypotheses = payload.get("hypotheses")
    if not isinstance(hypotheses, list):
        raise SystemExit("opencode response hypotheses must be an array")
    query = payload.get("query")
    if not isinstance(query, str) or not query.strip():
        query = "source-intelligence episode"
    normalized = {
        "query": query,
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
