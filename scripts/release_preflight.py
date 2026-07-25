#!/usr/bin/env python3
"""Cheap fail-fast validation for AETHER release-control contracts and inputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

import release_evidence as evidence
import release_subjects


REQUIRED_DEPENDENCIES = {"jsonschema", "pytest"}
EXPECTED_SUBJECT_COUNT = 18


def require(condition: bool, message: str) -> None:
    if not condition:
        raise evidence.EvidenceError(message)


def dependency_names(path: Path) -> set[str]:
    names = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            names.add(value.split("==", 1)[0].lower())
    return names


def validate_static(root: Path) -> dict[str, Any]:
    policy = evidence.load_json(root / "fixtures" / "release" / "gate-policy.json")
    evidence.validate_policy(policy)
    subjects = policy.get("future_required_bundle_subjects", [])
    require(len(subjects) == EXPECTED_SUBJECT_COUNT, "release policy subject count changed")
    require(len(subjects) == len(set(subjects)), "release policy has duplicate subjects")
    require(policy.get("qualification_tooling_allowed_paths"), "tooling-only path policy is missing")
    dependencies = dependency_names(root / "requirements-release.txt")
    require(REQUIRED_DEPENDENCIES <= dependencies, "pinned release dependencies are incomplete")

    schema_paths = sorted((root / "schemas" / "release").glob("*.schema.json"))
    require(schema_paths, "release schemas are missing")
    for path in schema_paths:
        Draft202012Validator.check_schema(evidence.load_json(path))

    release_workflow = (root / ".github" / "workflows" / "release-readiness.yml").read_text(encoding="utf-8")
    reusable = (root / ".github" / "workflows" / "reusable-exact-candidate-evidence.yml").read_text(encoding="utf-8")
    require("Qualification preflight" in release_workflow, "release workflow has no early qualification preflight")
    require("candidate_sha:" in release_workflow, "release workflow does not select an explicit product candidate")
    require("qualification_tooling_sha:" in reusable, "reusable producer omits tooling identity")
    require(policy["official_workflow"] in {".github/workflows/reusable-exact-candidate-evidence.yml"}, "official workflow projection changed")
    return {
        "policy_id": policy["policy_id"],
        "gate_count": len(policy["gates"]),
        "subject_count": len(subjects),
        "schema_count": len(schema_paths),
    }


def find_one(root: Path, pattern: str) -> Path:
    matches = [path for path in root.rglob(pattern) if path.is_file()]
    require(len(matches) == 1, f"expected exactly one {pattern}, found {len(matches)}")
    return matches[0]


def validate_inputs(root: Path, input_dir: Path) -> dict[str, Any]:
    manifest = evidence.load_json(input_dir / "qualification-inputs.json")
    require(manifest.get("schema_version") == "aether.release-qualification-inputs.v2", "qualification inputs use the legacy one-identity contract")
    candidate_sha = manifest.get("candidate_commit_sha")
    tooling = manifest.get("qualification_tooling", {})
    require(isinstance(candidate_sha, str) and len(candidate_sha) == 40, "candidate SHA is invalid")
    require(isinstance(tooling, dict) and len(str(tooling.get("commit_sha", ""))) == 40, "tooling SHA is invalid")
    require(candidate_sha != tooling.get("commit_sha") or manifest.get("candidate_tree_sha") == tooling.get("tree_sha"), "same commit has inconsistent candidate/tooling tree identity")
    for key in ("ci", "supply_chain", "pages", "capacity"):
        run = manifest.get("runs", {}).get(key, {})
        require(run.get("head_sha") == candidate_sha, f"{key} prerequisite is cross-candidate")
        require(run.get("conclusion") == "success", f"{key} prerequisite did not pass")
    require(manifest.get("jobs", {}).get("ci_gate", {}).get("name") == "Required CI gate", "CI protected-check projection changed")
    require(manifest.get("jobs", {}).get("supply_gate", {}).get("name") == "Required Supply Chain gate", "Supply Chain protected-check projection changed")

    package = input_dir / evidence.safe_bundle_relative(manifest["package_path"])
    require(package.is_file(), "canonical package is missing")
    require(evidence.sha256_file(package) == manifest.get("package_sha256"), "canonical package digest changed")

    policy = evidence.load_json(root / "fixtures" / "release" / "gate-policy.json")
    capacity_receipt = manifest["artifacts"]["capacity"]
    capacity_root = input_dir / capacity_receipt["artifact_name"]
    capacity = evidence.load_json(find_one(capacity_root, f"{capacity_receipt['artifact_name']}.json"))
    capacity_policy = policy["capacity_acceptance"]
    node_class = capacity_policy["node_class"]
    envelopes = [item for item in capacity.get("single_node_envelopes", []) if item.get("node_class") == node_class]
    require(len(envelopes) == 1, "capacity report lacks one policy-selected envelope")
    acceptance = release_subjects.recompute_capacity_acceptance(
        capacity_policy,
        envelopes[0],
        capacity.get("recommended_hardware", {}),
        capacity.get("concurrency_pack"),
    )
    require(all(acceptance["checks"].values()), "capacity acceptance failed before qualification")
    return {
        "candidate_sha": candidate_sha,
        "qualification_tooling_sha": tooling["commit_sha"],
        "package_sha256": manifest["package_sha256"],
        "capacity_node_class": node_class,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir")
    parser.add_argument("--out")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    try:
        result = {"schema_version": "aether.release-preflight.v1", "static": validate_static(root)}
        if args.input_dir:
            result["inputs"] = validate_inputs(root, Path(args.input_dir).resolve())
        if args.out:
            evidence.write_canonical_json(Path(args.out), result)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (evidence.EvidenceError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"release preflight failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
