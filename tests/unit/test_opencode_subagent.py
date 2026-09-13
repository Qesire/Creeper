from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "creeper_opencode_subagent.py"
)
spec = importlib.util.spec_from_file_location(
    "creeper_opencode_subagent_under_test", MODULE_PATH
)
assert spec is not None and spec.loader is not None
subagent = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = subagent
spec.loader.exec_module(subagent)


def _text_event(text: str) -> str:
    return json.dumps({"type": "text", "part": {"type": "text", "text": text}})


class ExtractFinalTextTests(unittest.TestCase):
    def test_concatenates_text_parts_joined_by_newline(self) -> None:
        stream = "\n".join(
            [
                json.dumps({"type": "step_start", "part": {"type": "step"}}),
                _text_event("first chunk"),
                json.dumps({"type": "step_finish", "part": {"type": "step"}}),
                _text_event("second chunk"),
            ]
        )

        self.assertEqual(
            subagent._extract_final_text(stream),
            "first chunk\nsecond chunk",
        )

    def test_ignores_non_json_lines_and_non_text_events(self) -> None:
        stream = "\n".join(
            [
                "not json at all",
                "random prose before json",
                json.dumps({"type": "step_start", "part": {"type": "step"}}),
                json.dumps({"type": "tool", "part": {"type": "text", "text": "tool"}}),
                json.dumps({"type": "text", "part": {"type": "tool", "text": "x"}}),
                _text_event("real text"),
                "{malformed json",
            ]
        )

        self.assertEqual(subagent._extract_final_text(stream), "real text")

    def test_empty_string_when_nothing_found(self) -> None:
        self.assertEqual(subagent._extract_final_text(""), "")
        self.assertEqual(
            subagent._extract_final_text("only\nprose\nlines\n"),
            "",
        )
        self.assertEqual(
            subagent._extract_final_text(
                json.dumps({"type": "step_start", "part": {"type": "step"}})
            ),
            "",
        )


class SelectPayloadTests(unittest.TestCase):
    def test_prefers_object_with_hypotheses_list(self) -> None:
        payload = {
            "query": "ep",
            "hypotheses": [{"hypothesis_id": "h1", "action": "a"}],
        }

        result = subagent._select_payload(
            json.dumps(payload), episode_id="ep"
        )

        self.assertEqual(result, payload)

    def test_falls_back_to_candidates_list(self) -> None:
        payload = {"candidates": [{"url": "https://example.test"}]}

        result = subagent._select_payload(
            json.dumps(payload), episode_id="ep"
        )

        self.assertIs(result, result)
        self.assertEqual(result["candidates"], payload["candidates"])

    def test_recovers_object_embedded_in_prose(self) -> None:
        payload = {
            "hypotheses": [{"hypothesis_id": "h1", "action": "a"}],
            "query": "prose episode",
        }
        text = "Here is my answer:\n" + json.dumps(payload) + "\nDone."

        result = subagent._select_payload(text, episode_id="ep")

        self.assertEqual(result, payload)

    def test_recovers_object_from_json_fenced_block(self) -> None:
        payload = {
            "hypotheses": [{"hypothesis_id": "h1", "action": "a"}],
        }
        text = "```json\n" + json.dumps(payload) + "\n```"

        result = subagent._select_payload(text, episode_id="ep")

        self.assertEqual(result, payload)

    def test_bare_array_wrapped_with_hypotheses_and_query(self) -> None:
        array = [
            {"hypothesis_id": "h1", "action": "a"},
            {"hypothesis_id": "h2", "action": "b"},
        ]

        result = subagent._select_payload(
            json.dumps(array), episode_id="episode-42"
        )

        self.assertEqual(
            result,
            {"hypotheses": array, "query": "episode-42"},
        )

    def test_raises_system_exit_when_no_usable_object(self) -> None:
        for text in ("", "no json here", "[1, 2, 3]", "{\"foo\": 1}"):
            with self.subTest(text=text):
                with self.assertRaises(SystemExit):
                    subagent._select_payload(text, episode_id="ep")


class NormalizeHypothesisTests(unittest.TestCase):
    def test_drops_null_candidate_template_variables_and_defaults(self) -> None:
        raw = {
            "hypothesis_id": "h1",
            "action": "probe",
            "confidence": 0.5,
            "expected_mechanism": "because",
            "candidate": None,
            "template": None,
            "variables": None,
            "candidate_defaults": None,
        }

        result = subagent._normalize_hypothesis(raw)

        self.assertNotIn("candidate", result)
        self.assertNotIn("template", result)
        self.assertNotIn("variables", result)
        self.assertNotIn("candidate_defaults", result)

    def test_preserves_core_fields(self) -> None:
        raw = {
            "hypothesis_id": "h1",
            "action": "HEAD",
            "confidence": 0.25,
            "expected_mechanism": "mechanism",
            "validation": {"method": "GET", "max_requests": 3, "max_bytes": 10},
        }

        result = subagent._normalize_hypothesis(raw)

        self.assertEqual(result["hypothesis_id"], "h1")
        self.assertEqual(result["action"], "HEAD")
        self.assertEqual(result["confidence"], 0.25)
        self.assertEqual(result["expected_mechanism"], "mechanism")
        self.assertEqual(
            result["validation"],
            {"method": "GET", "max_requests": 3, "max_bytes": 10},
        )

    def test_filters_null_values_from_variables(self) -> None:
        raw = {
            "hypothesis_id": "h1",
            "action": "a",
            "variables": {"YEAR": 1999, "SHARD": None, "MONTH": "01"},
        }

        result = subagent._normalize_hypothesis(raw)

        self.assertEqual(result["variables"], {"YEAR": 1999, "MONTH": "01"})

    def test_default_validation_when_absent(self) -> None:
        raw = {"hypothesis_id": "h1", "action": "a"}

        result = subagent._normalize_hypothesis(raw)

        self.assertEqual(
            result["validation"],
            {"method": "HEAD_OR_RANGE", "max_requests": 1, "max_bytes": 65536},
        )


class BuildCommandTests(unittest.TestCase):
    def _args(self, *, agent=None) -> argparse.Namespace:
        return argparse.Namespace(
            opencode_bin="opencode",
            model="ustc-107/deepseek-flash",
            agent=agent,
        )

    def test_command_prefix_and_trailing_stdin_marker(self) -> None:
        workdir = Path("/tmp/creeper-opencode-test")

        command = subagent._build_command(self._args(), workdir=workdir)

        self.assertEqual(
            command,
            [
                "opencode",
                "run",
                "--model",
                "ustc-107/deepseek-flash",
                "--format",
                "json",
                "--dir",
                str(workdir),
                "-",
            ],
        )
        self.assertEqual(
            command[:8],
            [
                "opencode",
                "run",
                "--model",
                "ustc-107/deepseek-flash",
                "--format",
                "json",
                "--dir",
                str(workdir),
            ],
        )
        self.assertEqual(command[-1], "-")

    def test_agent_appended_before_trailing_stdin_marker(self) -> None:
        workdir = Path("/tmp/creeper-opencode-test")

        command = subagent._build_command(
            self._args(agent="source-scout"), workdir=workdir
        )

        self.assertEqual(
            command,
            [
                "opencode",
                "run",
                "--model",
                "ustc-107/deepseek-flash",
                "--format",
                "json",
                "--dir",
                str(workdir),
                "--agent",
                "source-scout",
                "-",
            ],
        )
        self.assertEqual(command[-1], "-")


if __name__ == "__main__":
    unittest.main()
