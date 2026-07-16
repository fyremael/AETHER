from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def job_block(workflow: str, job_id: str) -> str:
    content = (REPO_ROOT / ".github" / "workflows" / workflow).read_text(
        encoding="utf-8"
    )
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\s*$.*?(?=^  [a-z][a-z0-9-]*:\s*$|\Z)",
        content,
    )
    if match is None:
        raise AssertionError(f"missing job {job_id} in {workflow}")
    return match.group(0)


class RequiredHostedGateTests(unittest.TestCase):
    def test_ci_aggregate_is_stable_and_fail_closed(self) -> None:
        block = job_block("ci.yml", "required-ci-gate")

        self.assertIn("name: Required CI gate", block)
        self.assertIn("if: always()", block)
        for required in (
            "rust",
            "go-shell",
            "postgres-journal",
            "container-smoke",
            "python-sdk",
            "pilot-launch-gate",
            "pilot-package",
        ):
            self.assertIn(f"- {required}", block)
        self.assertIn('allowed = {"success", "skipped"} if job in conditional', block)
        self.assertIn('else {"success"}', block)

    def test_supply_chain_aggregate_requires_every_job(self) -> None:
        block = job_block("supply-chain.yml", "required-supply-chain-gate")

        self.assertIn("name: Required Supply Chain gate", block)
        self.assertIn("if: always()", block)
        for required in ("package", "dependency-and-package", "codeql"):
            self.assertIn(f"- {required}", block)
        self.assertIn('payload["result"] != "success"', block)

    def test_release_work_is_downstream_of_protected_approval(self) -> None:
        approval = job_block("release-readiness.yml", "protected-release-approval")
        exact = job_block("release-readiness.yml", "exact-candidate-evidence")
        readiness = job_block("release-readiness.yml", "release-readiness")

        self.assertIn("environment: release", approval)
        self.assertIn("name: Protected release approval", approval)
        self.assertIn("- protected-release-approval", exact)
        self.assertIn("- release-readiness", exact)
        self.assertIn("needs: protected-release-approval", readiness)


if __name__ == "__main__":
    unittest.main()
