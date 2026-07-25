#!/usr/bin/env python3
"""Fail-closed aggregate job conclusion checks for tiered CI workflows."""

from __future__ import annotations

import argparse
import json
from typing import Any


PR_JOBS = {"rust-pr": "rust", "go-pr": "go", "python-pr": "python"}
INTEGRATION_JOBS = {
    "rust-integration", "go-integration", "python-integration",
    "postgres-journal", "container-smoke", "pilot-launch-gate", "pilot-package",
    "hardening-admin", "hardening-operator",
}


def ci_failures(event_name: str, jobs: dict[str, Any]) -> list[str]:
    expected = {"scope": "success"}
    if event_name == "pull_request":
        expected.update({job: "skipped" for job in INTEGRATION_JOBS})
        scopes = jobs["scope"]["outputs"]
        expected.update(
            {job: "success" if scopes[scope] == "true" else "skipped" for job, scope in PR_JOBS.items()}
        )
    else:
        expected.update({job: "skipped" for job in PR_JOBS})
        expected.update({job: "success" for job in INTEGRATION_JOBS})
    return [
        f"{job}={jobs.get(job, {}).get('result')} (expected {result})"
        for job, result in sorted(expected.items())
        if jobs.get(job, {}).get("result") != result
    ]


def supply_failures(event_name: str, jobs: dict[str, Any]) -> list[str]:
    if event_name == "pull_request":
        expected = {"pr-policy": "success", "package": "skipped", "dependency-and-package": "skipped", "codeql": "skipped"}
    else:
        expected = {"pr-policy": "skipped", "package": "success", "dependency-and-package": "success", "codeql": "success"}
    return [
        f"{job}={jobs.get(job, {}).get('result')} (expected {result})"
        for job, result in sorted(expected.items())
        if jobs.get(job, {}).get("result") != result
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=["ci", "supply-chain"])
    parser.add_argument("--event", required=True)
    parser.add_argument("--needs-json", required=True)
    args = parser.parse_args()
    jobs = json.loads(args.needs_json)
    failures = ci_failures(args.event, jobs) if args.kind == "ci" else supply_failures(args.event, jobs)
    if failures:
        raise SystemExit("required jobs did not pass: " + ", ".join(failures))
    print("tier-appropriate required jobs passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
