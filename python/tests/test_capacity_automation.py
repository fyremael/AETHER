from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_inventory_module():
    path = REPO_ROOT / "scripts" / "capacity_artifact_inventory.py"
    spec = importlib.util.spec_from_file_location("capacity_artifact_inventory", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CapacityAutomationTests(unittest.TestCase):
    def test_inventory_records_hashes_and_fails_missing_layout(self) -> None:
        module = load_inventory_module()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            expected = root / "matrix" / "latest.json"
            expected.parent.mkdir(parents=True)
            expected.write_text('{"status":"ok"}\n', encoding="utf-8")

            valid = module.build_inventory(
                root,
                required_paths=["matrix/latest.json"],
                required_globs=["matrix/*.json"],
            )
            self.assertTrue(valid["valid"])
            self.assertEqual(valid["files"][0]["path"], "matrix/latest.json")
            self.assertRegex(valid["files"][0]["sha256"], r"^[0-9a-f]{64}$")

            invalid = module.build_inventory(
                root,
                required_paths=["perturbation/latest.json"],
                required_globs=["perturbation/runs/**/capacity-curves.json"],
            )
            self.assertFalse(invalid["valid"])
            self.assertEqual(invalid["missing_paths"], ["perturbation/latest.json"])

    def test_workflow_fixes_layout_and_always_uploads_inventory(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "capacity-planning.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("path: artifacts/performance\n", workflow)
        self.assertIn("capacity_artifact_inventory.py", workflow)
        self.assertIn("if: always()", workflow)
        self.assertIn("name: capacity-input-inventory", workflow)
        self.assertNotIn("path: artifacts/performance/matrix\n\n      - name: Build capacity report", workflow)

    def test_performance_verdict_is_predeclared_without_retry_until_green(self) -> None:
        policy = json.loads(
            (REPO_ROOT / "fixtures" / "performance" / "verdict-policy.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(policy["samples_per_workload"], 5)
        self.assertEqual(policy["latency_statistic"], "arithmetic_mean")
        self.assertEqual(policy["pass_severities"], ["ok", "warn"])

        launch = (REPO_ROOT / "scripts" / "run-pilot-launch-validation.ps1").read_text(
            encoding="utf-8"
        )
        drift = (REPO_ROOT / "scripts" / "run-performance-drift.ps1").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("Invoke-StepWithRetry", launch)
        self.assertNotIn("retrying once", launch.lower())
        self.assertIn("--verdict-policy", drift)


if __name__ == "__main__":
    unittest.main()
