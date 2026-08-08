from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify_repository_controls.py"


def load_module():
    spec = importlib.util.spec_from_file_location("verify_repository_controls", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RepositoryControlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_module()
        cls.policy = json.loads(
            (REPO_ROOT / ".github" / "repository-controls.json").read_text(
                encoding="utf-8"
            )
        )

    def passing_snapshots(self):
        branch = self.policy["protected_branch"]
        actions = self.policy["actions"]
        security = self.policy["security"]
        return {
            "branch_protection": {
                "required_status_checks": {
                    "strict": branch["strict"],
                    "checks": [
                        {"context": context}
                        for context in branch["required_status_checks"]
                    ],
                },
                "required_pull_request_reviews": {
                    "required_approving_review_count": branch["minimum_approvals"],
                    "dismiss_stale_reviews": branch["dismiss_stale_reviews"],
                    "require_code_owner_reviews": branch[
                        "require_code_owner_reviews"
                    ],
                    "require_last_push_approval": branch[
                        "require_last_push_approval"
                    ],
                },
                "required_conversation_resolution": {
                    "enabled": branch["required_conversation_resolution"]
                },
                "enforce_admins": {"enabled": branch["enforce_admins"]},
                "lock_branch": {"enabled": branch["lock_branch"]},
                "allow_force_pushes": {"enabled": branch["allow_force_pushes"]},
                "allow_deletions": {"enabled": branch["allow_deletions"]},
            },
            "actions": {
                "allowed_actions": actions["allowed_actions"],
                "sha_pinning_required": actions["sha_pinning_required"],
            },
            "selected_actions": {
                "github_owned_allowed": actions["github_owned_allowed"],
                "verified_allowed": actions["verified_allowed"],
                "patterns_allowed": actions["patterns_allowed"],
            },
            "repository": {
                **self.policy["repository_settings"],
                "security_and_analysis": {
                    key: {"status": status}
                    for key, status in security.items()
                    if key != "private_vulnerability_reporting"
                }
            },
            "custom_properties": [
                {"property_name": name, "value": value}
                for name, value in self.policy["custom_properties"].items()
            ],
            "private_vulnerability_reporting": {"enabled": True},
            "environments": {
                name: {
                    "environment": {
                        "can_admins_bypass": environment["can_admins_bypass"],
                        "protection_rules": (
                            [
                                {
                                    "type": "required_reviewers",
                                    "reviewers": [
                                        {"reviewer": {"login": "release-owner"}}
                                        for _ in range(environment["minimum_reviewers"])
                                    ],
                                }
                            ]
                            if environment["minimum_reviewers"]
                            else []
                        )
                    },
                    "branch_policies": {
                        "branch_policies": [
                            {"name": branch_name}
                            for branch_name in environment["allowed_branches"]
                        ]
                    },
                }
                for name, environment in self.policy["environments"].items()
            },
        }

    def test_tracked_policy_has_stable_aggregate_checks(self) -> None:
        self.assertEqual(
            self.policy["protected_branch"]["required_status_checks"],
            ["Required CI gate", "Required Supply Chain gate"],
        )

    def test_matching_snapshot_passes(self) -> None:
        self.assertEqual(self.module.audit(self.policy, self.passing_snapshots()), [])

    def test_missing_protection_and_release_approval_block(self) -> None:
        snapshots = self.passing_snapshots()
        snapshots["branch_protection"] = {"_error": "Branch not protected"}
        snapshots["environments"]["release"]["environment"]["protection_rules"] = []

        blockers = self.module.audit(self.policy, snapshots)

        self.assertTrue(any("branch protection unavailable" in item for item in blockers))
        self.assertTrue(any("reviewer count" in item for item in blockers))

    def test_private_vulnerability_reporting_is_required(self) -> None:
        snapshots = self.passing_snapshots()
        snapshots["private_vulnerability_reporting"] = {"enabled": False}

        blockers = self.module.audit(self.policy, snapshots)

        self.assertIn(
            "security setting differs: private_vulnerability_reporting", blockers
        )

    def test_custom_property_projection_is_exact(self) -> None:
        snapshots = self.passing_snapshots()
        snapshots["custom_properties"][0]["value"] = "cross-programme"

        blockers = self.module.audit(self.policy, snapshots)

        self.assertIn("custom property projection differs", blockers)


if __name__ == "__main__":
    unittest.main()
