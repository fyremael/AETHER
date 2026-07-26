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


def scope_output(
    jobs: dict[str, Any], name: str, failures: list[str]
) -> str | None:
    scope_job = jobs.get("scope")
    outputs = scope_job.get("outputs") if isinstance(scope_job, dict) else None
    value = outputs.get(name) if isinstance(outputs, dict) else None
    if value not in {"true", "false"}:
        failures.append(
            f"scope.outputs.{name}={value!r} (expected 'true' or 'false')"
        )
        return None
    return value


def job_result(jobs: dict[str, Any], name: str) -> Any:
    job = jobs.get(name)
    return job.get("result") if isinstance(job, dict) else None


def ci_failures(event_name: str, jobs: dict[str, Any]) -> list[str]:
    expected = {"scope": "success"}
    failures: list[str] = []
    if event_name == "pull_request":
        expected.update({job: "skipped" for job in INTEGRATION_JOBS})
        for job, scope in PR_JOBS.items():
            value = scope_output(jobs, scope, failures)
            # Malformed scope output fails closed: require the job to succeed
            # as well as reporting the bad output.
            expected[job] = "skipped" if value == "false" else "success"
    else:
        expected.update({job: "skipped" for job in PR_JOBS})
        product_integration = scope_output(
            jobs, "product_integration", failures
        )
        integration_result = (
            "skipped" if product_integration == "false" else "success"
        )
        expected.update({job: integration_result for job in INTEGRATION_JOBS})
    failures.extend(
        f"{job}={job_result(jobs, job)} (expected {result})"
        for job, result in sorted(expected.items())
        if job_result(jobs, job) != result
    )
    return failures


def supply_failures(event_name: str, jobs: dict[str, Any]) -> list[str]:
    if event_name == "pull_request":
        expected = {"pr-policy": "success", "package": "skipped", "dependency-and-package": "skipped", "codeql": "skipped"}
    else:
        expected = {"pr-policy": "skipped", "package": "success", "dependency-and-package": "success", "codeql": "success"}
    return [
        f"{job}={job_result(jobs, job)} (expected {result})"
        for job, result in sorted(expected.items())
        if job_result(jobs, job) != result
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
