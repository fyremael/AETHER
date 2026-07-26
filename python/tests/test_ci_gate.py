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
        jobs = {
            "scope": {
                "result": "success",
                "outputs": {
                    "rust": "true",
                    "go": "false",
                    "python": "false",
                    "product_integration": "true",
                },
            }
        }
        jobs.update({job: {"result": "skipped"} for job in ci_gate.PR_JOBS})
        jobs.update({job: {"result": "skipped"} for job in ci_gate.INTEGRATION_JOBS})
        return jobs

    def main_jobs(self, product_integration: str | None) -> dict:
        outputs = {}
        if product_integration is not None:
            outputs["product_integration"] = product_integration
        jobs = {"scope": {"result": "success", "outputs": outputs}}
        jobs.update({job: {"result": "skipped"} for job in ci_gate.PR_JOBS})
        integration_result = (
            "success" if product_integration == "true" else "skipped"
        )
        jobs.update(
            {
                job: {"result": integration_result}
                for job in ci_gate.INTEGRATION_JOBS
            }
        )
        return jobs

    def test_selected_pr_scope_cannot_skip(self) -> None:
        failures = ci_gate.ci_failures("pull_request", self.pr_jobs())
        self.assertIn("rust-pr=skipped (expected success)", failures)

    def test_selected_pr_scope_passes_and_unselected_scopes_skip(self) -> None:
        jobs = self.pr_jobs()
        jobs["rust-pr"]["result"] = "success"
        self.assertEqual(ci_gate.ci_failures("pull_request", jobs), [])

    def test_main_product_integration_requires_successes(self) -> None:
        self.assertEqual(ci_gate.ci_failures("push", self.main_jobs("true")), [])

    def test_main_tooling_only_requires_integration_skips(self) -> None:
        self.assertEqual(ci_gate.ci_failures("push", self.main_jobs("false")), [])

    def test_malformed_product_scope_fails_closed(self) -> None:
        for value in (None, "False", "", "yes"):
            with self.subTest(value=value):
                failures = ci_gate.ci_failures("push", self.main_jobs(value))
                self.assertTrue(
                    any(
                        failure.startswith(
                            "scope.outputs.product_integration="
                        )
                        for failure in failures
                    ),
                    failures,
                )
                self.assertTrue(
                    any(
                        "rust-integration=skipped (expected success)"
                        == failure
                        for failure in failures
                    ),
                    failures,
                )

    def test_malformed_scope_job_fails_closed(self) -> None:
        jobs = self.main_jobs("false")
        jobs["scope"] = None

        failures = ci_gate.ci_failures("push", jobs)

        self.assertIn(
            "scope.outputs.product_integration=None (expected 'true' or 'false')",
            failures,
        )
        self.assertIn("scope=None (expected success)", failures)
        self.assertIn(
            "rust-integration=skipped (expected success)", failures
        )

    def test_malformed_pr_language_scope_fails_closed(self) -> None:
        jobs = self.pr_jobs()
        del jobs["scope"]["outputs"]["rust"]

        failures = ci_gate.ci_failures("pull_request", jobs)

        self.assertIn(
            "scope.outputs.rust=None (expected 'true' or 'false')", failures
        )
        self.assertIn("rust-pr=skipped (expected success)", failures)

    def test_supply_chain_tiers_are_event_specific(self) -> None:
        pr_jobs = {
            "pr-policy": {"result": "success"}, "package": {"result": "skipped"},
            "dependency-and-package": {"result": "skipped"}, "codeql": {"result": "skipped"},
        }
        self.assertEqual(ci_gate.supply_failures("pull_request", pr_jobs), [])
        self.assertTrue(ci_gate.supply_failures("push", pr_jobs))


if __name__ == "__main__":
    unittest.main()
