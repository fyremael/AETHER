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
            {
                "rust": False,
                "go": False,
                "python": False,
                "product_integration": False,
            },
        )

    def test_language_paths_select_only_their_boundary(self) -> None:
        self.assertEqual(
            ci_scope.select_scopes(["crates/aether_ast/src/lib.rs"]),
            {
                "rust": True,
                "go": False,
                "python": False,
                "product_integration": True,
            },
        )
        self.assertEqual(
            ci_scope.select_scopes(["go/cmd/aether/main.go"]),
            {
                "rust": False,
                "go": True,
                "python": False,
                "product_integration": True,
            },
        )
        self.assertEqual(
            ci_scope.select_scopes(["python/aether_sdk/client.py"]),
            {
                "rust": False,
                "go": False,
                "python": True,
                "product_integration": True,
            },
        )

    def test_root_rust_and_release_control_changes_select_expected_scopes(self) -> None:
        self.assertEqual(
            ci_scope.select_scopes(["Cargo.lock"]),
            {
                "rust": True,
                "go": False,
                "python": False,
                "product_integration": True,
            },
        )
        self.assertEqual(
            ci_scope.select_scopes([".github/workflows/release-readiness.yml"]),
            {
                "rust": True,
                "go": True,
                "python": True,
                "product_integration": False,
            },
        )

    def test_tooling_only_push_skips_product_integration(self) -> None:
        selected = ci_scope.select_scopes(
            [
                ".github/workflows/release-readiness.yml",
                "scripts/release_preflight.py",
                "schemas/release/evidence-bundle-v2.schema.json",
                "docs/STATUS.md",
            ]
        )

        self.assertFalse(selected["product_integration"])

    def test_product_and_unknown_paths_fail_closed_to_integration(self) -> None:
        for path in (
            "crates/aether_runtime/src/lib.rs",
            "scripts/new_release_helper.py",
            ".github/workflows/new-release-workflow.yml",
            "scripts/release_preflight.py.bak",
            "__full_product_integration__",
        ):
            with self.subTest(path=path):
                self.assertTrue(
                    ci_scope.select_scopes([path])["product_integration"]
                )

    def test_ci_architecture_changes_validate_with_full_integration(self) -> None:
        for path in (
            ".github/workflows/ci.yml",
            "scripts/ci_scope.py",
            "scripts/ci_gate.py",
            "python/tests/test_ci_scope.py",
        ):
            with self.subTest(path=path):
                self.assertTrue(
                    ci_scope.select_scopes([path])["product_integration"]
                )

    def test_empty_path_set_fails_closed_to_integration(self) -> None:
        self.assertTrue(ci_scope.select_scopes([])["product_integration"])


if __name__ == "__main__":
    unittest.main()
