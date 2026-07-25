from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_scope", REPO_ROOT / "scripts" / "ci_scope.py")
assert SPEC and SPEC.loader
ci_scope = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_scope)


class CiScopeTests(unittest.TestCase):
    def test_docs_only_skips_language_boundaries(self) -> None:
        self.assertEqual(
            ci_scope.select_scopes(["docs/STATUS.md"]),
            {"rust": False, "go": False, "python": False},
        )

    def test_language_paths_select_only_their_boundary(self) -> None:
        self.assertEqual(
            ci_scope.select_scopes(["crates/aether_ast/src/lib.rs"]),
            {"rust": True, "go": False, "python": False},
        )
        self.assertEqual(
            ci_scope.select_scopes(["go/cmd/aether/main.go"]),
            {"rust": False, "go": True, "python": False},
        )
        self.assertEqual(
            ci_scope.select_scopes(["python/aether_sdk/client.py"]),
            {"rust": False, "go": False, "python": True},
        )

    def test_root_rust_and_release_control_changes_select_expected_scopes(self) -> None:
        self.assertEqual(
            ci_scope.select_scopes(["Cargo.lock"]),
            {"rust": True, "go": False, "python": False},
        )
        self.assertEqual(
            ci_scope.select_scopes([".github/workflows/release-readiness.yml"]),
            {"rust": True, "go": True, "python": True},
        )


if __name__ == "__main__":
    unittest.main()
