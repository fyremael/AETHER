#!/usr/bin/env python3
"""Capture and verify AETHER's hosted repository control boundary."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / ".github" / "repository-controls.json"


def gh_json(endpoint: str) -> dict[str, Any]:
    process = subprocess.run(
        ["gh", "api", endpoint],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        return {
            "_error": process.stderr.strip() or process.stdout.strip(),
            "_returncode": process.returncode,
        }
    return json.loads(process.stdout)


def git_head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def capture_snapshots(policy: dict[str, Any]) -> dict[str, Any]:
    repository = policy["repository"]
    branch = policy["protected_branch"]["name"]
    snapshots: dict[str, Any] = {
        "branch_protection": gh_json(f"repos/{repository}/branches/{branch}/protection"),
        "actions": gh_json(f"repos/{repository}/actions/permissions"),
        "selected_actions": gh_json(
            f"repos/{repository}/actions/permissions/selected-actions"
        ),
        "repository": gh_json(f"repos/{repository}"),
        "environments": {},
    }
    for name in sorted(policy["environments"]):
        snapshots["environments"][name] = {
            "environment": gh_json(f"repos/{repository}/environments/{name}"),
            "branch_policies": gh_json(
                f"repos/{repository}/environments/{name}/deployment-branch-policies"
            ),
        }
    return snapshots


def audit(policy: dict[str, Any], snapshots: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    branch_policy = policy["protected_branch"]
    protection = snapshots["branch_protection"]
    if "_error" in protection:
        blockers.append(f"branch protection unavailable: {protection['_error']}")
    else:
        observed_checks = {
            item.get("context")
            for item in protection.get("required_status_checks", {}).get("checks", [])
        }
        missing_checks = set(branch_policy["required_status_checks"]) - observed_checks
        if missing_checks:
            blockers.append(f"required status checks missing: {sorted(missing_checks)}")
        if protection.get("required_status_checks", {}).get("strict") is not branch_policy["strict"]:
            blockers.append("required status checks strictness differs")
        reviews = protection.get("required_pull_request_reviews", {})
        if reviews.get("required_approving_review_count", 0) < branch_policy["minimum_approvals"]:
            blockers.append("pull-request approval count is below policy")
        if reviews.get("dismiss_stale_reviews") is not branch_policy["dismiss_stale_reviews"]:
            blockers.append("stale-review dismissal differs")
        if protection.get("enforce_admins", {}).get("enabled") is not branch_policy["enforce_admins"]:
            blockers.append("administrator enforcement differs")
        for key in ("allow_force_pushes", "allow_deletions"):
            if protection.get(key, {}).get("enabled") is not branch_policy[key]:
                blockers.append(f"{key} differs")

    actions_policy = policy["actions"]
    actions = snapshots["actions"]
    selected = snapshots["selected_actions"]
    for key in ("allowed_actions", "sha_pinning_required"):
        if actions.get(key) != actions_policy[key]:
            blockers.append(f"Actions setting differs: {key}")
    for key in ("github_owned_allowed", "verified_allowed"):
        if selected.get(key) != actions_policy[key]:
            blockers.append(f"selected Actions setting differs: {key}")
    if set(selected.get("patterns_allowed", [])) != set(actions_policy["patterns_allowed"]):
        blockers.append("selected Actions allowlist differs")

    observed_security = snapshots["repository"].get("security_and_analysis", {})
    for key, expected in policy["security"].items():
        if observed_security.get(key, {}).get("status") != expected:
            blockers.append(f"security setting differs: {key}")

    for name, expected in sorted(policy["environments"].items()):
        snapshot = snapshots["environments"].get(name, {})
        environment = snapshot.get("environment", {})
        if "_error" in environment:
            blockers.append(f"environment unavailable: {name}")
            continue
        reviewers = [
            item
            for item in environment.get("protection_rules", [])
            if item.get("type") == "required_reviewers"
        ]
        reviewer_count = len(reviewers[0].get("reviewers", [])) if reviewers else 0
        if reviewer_count < expected["minimum_reviewers"]:
            blockers.append(f"environment reviewer count is below policy: {name}")
        policies = snapshot.get("branch_policies", {}).get("branch_policies", [])
        observed_branches = {item.get("name") for item in policies}
        missing_branches = set(expected["allowed_branches"]) - observed_branches
        if missing_branches:
            blockers.append(
                f"environment branches missing for {name}: {sorted(missing_branches)}"
            )
    return sorted(blockers)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default=str(DEFAULT_POLICY))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    policy = json.loads(Path(args.policy).read_text(encoding="utf-8"))
    snapshots = capture_snapshots(policy)
    blockers = audit(policy, snapshots)
    evidence = {
        "schema_version": "aether.repository-controls-evidence.v1",
        "candidate_commit_sha": git_head(),
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "repository": policy["repository"],
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "snapshots": snapshots,
    }
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": evidence["status"], "blockers": blockers}, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    raise SystemExit(main())
