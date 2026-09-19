from __future__ import annotations

import ast
import unittest
from pathlib import Path

from creeper.distributed.edition import (
    ALLOWED_CORE_IMPORTS,
    FABRIC_EDITION_NAME,
    FABRIC_EDITION_VERSION,
    FABRIC_PROTOCOL_VERSION,
)


class DistributedDerivativeBoundaryTests(unittest.TestCase):
    def test_derivative_has_independent_edition_identity(self) -> None:
        self.assertEqual(FABRIC_EDITION_NAME, "Creeper Fabric")
        self.assertTrue(FABRIC_EDITION_VERSION)
        self.assertTrue(FABRIC_PROTOCOL_VERSION.startswith("creeper-fabric-"))

    def test_distributed_namespace_imports_only_reviewed_core_primitives(self) -> None:
        root = Path(__file__).resolve().parents[2] / "src" / "creeper" / "distributed"
        violations: list[str] = []

        for path in sorted(root.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                modules: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    modules.append(node.module)
                elif isinstance(node, ast.Import):
                    modules.extend(alias.name for alias in node.names)

                for module in modules:
                    if not module.startswith("creeper."):
                        continue
                    if module.startswith("creeper.distributed"):
                        continue
                    if module in ALLOWED_CORE_IMPORTS:
                        continue
                    violations.append(f"{path.name}: {module}")

        self.assertEqual(
            violations,
            [],
            "Creeper Fabric imported unreviewed core runtime modules:\n"
            + "\n".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
