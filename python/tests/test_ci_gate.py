from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("ci_gate", REPO_ROOT / "scripts" / "ci_gate.py")
assert SPEC and SPEC.loader
ci_gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci_gate)


class CiGateTests(unittest.TestCase):
    def pr_jobs(self) -> dict:
        jobs = {"scope": {"result": "success", "outputs": {"rust": "true", "go": "false", "python": "false"}}}
        jobs.update({job: {"result": "skipped"} for job in ci_gate.PR_JOBS})
        jobs.update({job: {"result": "skipped"} for job in ci_gate.INTEGRATION_JOBS})
        return jobs

    def test_selected_pr_scope_cannot_skip(self) -> None:
        failures = ci_gate.ci_failures("pull_request", self.pr_jobs())
        self.assertIn("rust-pr=skipped (expected success)", failures)

    def test_selected_pr_scope_passes_and_unselected_scopes_skip(self) -> None:
        jobs = self.pr_jobs()
        jobs["rust-pr"]["result"] = "success"
        self.assertEqual(ci_gate.ci_failures("pull_request", jobs), [])

    def test_supply_chain_tiers_are_event_specific(self) -> None:
        pr_jobs = {
            "pr-policy": {"result": "success"}, "package": {"result": "skipped"},
            "dependency-and-package": {"result": "skipped"}, "codeql": {"result": "skipped"},
        }
        self.assertEqual(ci_gate.supply_failures("pull_request", pr_jobs), [])
        self.assertTrue(ci_gate.supply_failures("push", pr_jobs))


if __name__ == "__main__":
    unittest.main()
