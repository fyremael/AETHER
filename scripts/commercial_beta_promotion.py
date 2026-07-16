#!/usr/bin/env python3
"""Generate and verify an immutable AETHER commercial-beta promotion record."""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any, Callable

import release_evidence as evidence
import verify_release_evidence as release_verify


PROMOTION_VERSION = "aether.commercial-beta-promotion.v1"
GA_BLOCKERS = {
    "support-incident-posture",
    "multi-platform-distribution",
    "signed-promotion",
    "distributed-truth-qualification",
}
SHA64 = re.compile(r"^[a-f0-9]{64}$")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise evidence.EvidenceError(message)


def verify_verdict_pair(official: dict[str, Any], independent: dict[str, Any]) -> None:
    for label, verdict in (("official", official), ("independent", independent)):
        require(verdict.get("schema_version") == "aether.release-evidence-verdict.v1", f"{label} verdict schema is invalid")
        require(verdict.get("official") is True, f"{label} diagnostic verdict cannot promote beta")
        require(verdict.get("computed_verdict") == "passed", f"{label} verdict did not pass")
        require(verdict.get("blockers") == [], f"{label} verdict retains blockers")
    for field in ("bundle_id", "candidate", "policy_id", "package_sha256", "verifier"):
        require(official.get(field) == independent.get(field), f"official and independent verdicts disagree on {field}")
    candidate = official["candidate"]
    require(candidate.get("ref") == "refs/heads/main", "only protected main candidates can promote beta")
    require(candidate.get("dirty") is False, "dirty candidate cannot promote beta")


def verify_dependent_verdict_artifact(
    verdict: dict[str, Any],
    expected_bytes: bytes,
    *,
    api: Callable[[str], Any] = release_verify.github_api,
    download_artifact: Callable[[str, int], bytes] = release_verify.github_artifact_archive,
) -> dict[str, Any]:
    repository = verdict["official_repository"]
    workflow = verdict["workflow"]
    candidate = verdict["candidate"]
    name = (
        f"aether-release-evidence-verdict-{candidate['commit_sha']}-"
        f"{workflow['run_id']}-{workflow['attempt']}"
    )
    payload = api(f"repos/{repository}/actions/runs/{workflow['run_id']}/artifacts?per_page=100")
    matches = [item for item in payload.get("artifacts", []) if item.get("name") == name]
    require(len(matches) == 1, "dependent verdict artifact is missing or ambiguous")
    artifact = matches[0]
    require(artifact.get("expired") is False, "dependent verdict artifact expired")
    digest = str(artifact.get("digest", ""))
    require(digest.startswith("sha256:") and bool(SHA64.fullmatch(digest.removeprefix("sha256:"))), "dependent verdict artifact digest is invalid")
    archive = download_artifact(repository, artifact["id"])
    require(len(archive) == artifact.get("size_in_bytes"), "dependent verdict artifact size mismatch")
    require(evidence.sha256_bytes(archive) == digest.removeprefix("sha256:"), "dependent verdict artifact digest mismatch")
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            files = [item for item in bundle.infolist() if not item.is_dir()]
            require(len(files) == 1, "dependent verdict artifact must contain exactly one file")
            evidence.safe_bundle_relative(files[0].filename)
            require(Path(files[0].filename).name == "verified-verdict.json", "dependent verdict artifact contains an unexpected file")
            archived_verdict = bundle.read(files[0])
    except zipfile.BadZipFile as exc:
        raise evidence.EvidenceError("dependent verdict artifact is not a valid ZIP") from exc
    require(archived_verdict == expected_bytes, "dependent verdict artifact bytes differ from live recomputation")
    return {
        "artifact_id": artifact["id"],
        "artifact_name": artifact["name"],
        "sha256": digest.removeprefix("sha256:"),
        "byte_size": artifact["size_in_bytes"],
        "run_id": str(workflow["run_id"]),
        "attempt": workflow["attempt"],
    }


def generate(
    args: argparse.Namespace,
    *,
    bundle_verifier: Callable[..., dict[str, Any]] = release_verify.verify_bundle,
    verdict_artifact_verifier: Callable[[dict[str, Any], bytes], dict[str, Any]] = verify_dependent_verdict_artifact,
) -> dict[str, Any]:
    official_path = Path(args.official_verdict)
    independent_path = Path(args.independent_verdict)
    official = evidence.load_json(official_path)
    independent = evidence.load_json(independent_path)
    verify_verdict_pair(official, independent)
    bundle_path = Path(args.bundle)
    require("latest" not in bundle_path.name.lower(), "latest bundle cannot promote beta")
    canonical = bundle_verifier(
        bundle_path,
        expected_commit_sha=args.expected_commit_sha,
        expected_tree_sha=args.expected_tree_sha,
        expected_ref=args.expected_ref,
        require_official=True,
    )
    require(canonical.get("computed_verdict") == "passed" and canonical.get("blockers") == [], "live recomputed verdict did not pass")
    canonical_bytes = evidence.canonical_bytes(canonical)
    require(official_path.read_bytes() == canonical_bytes, "dependent workflow verdict bytes differ from live recomputation")
    require(independent_path.read_bytes() == canonical_bytes, "independent verdict bytes differ from live recomputation")
    dependent_artifact = verdict_artifact_verifier(canonical, canonical_bytes)
    bundle_artifact = canonical.get("official_artifact")
    require(isinstance(bundle_artifact, dict), "live verifier did not return the official bundle artifact receipt")
    candidate = canonical["candidate"]
    record: dict[str, Any] = {
        "schema_version": PROMOTION_VERSION,
        "promotion_id": "",
        "stage": "commercial_beta",
        "candidate": candidate,
        "policy_id": canonical["policy_id"],
        "package_sha256": canonical["package_sha256"],
        "workflow": canonical["workflow"],
        "bundle": {"bundle_id": canonical["bundle_id"], **bundle_artifact},
        "dependent_verdict": dependent_artifact,
        "verifier": canonical["verifier"],
        "independent_verification": {
            "verdict_sha256": evidence.sha256_file(independent_path),
            "verdict_byte_size": independent_path.stat().st_size,
            "official_verdict_sha256": evidence.sha256_file(official_path),
            "live_recomputed_verdict_sha256": evidence.sha256_bytes(canonical_bytes),
            "byte_for_byte_equal": official_path.read_bytes() == independent_path.read_bytes() == canonical_bytes,
        },
        "beta_boundary": {
            "platform": "windows-x86_64",
            "topology": "single-node",
            "default_journal": "sqlite",
            "optional_journal": "postgres-verify_full-with-local-sqlite-sidecars",
            "http": ["loopback-http", "trusted-tls-ingress-with-direct-backend-blocked"],
            "excluded_claims": [
                "native-http-tls",
                "multi-host-failover",
                "consensus",
                "generalized-distributed-truth",
            ],
        },
        "ga": {
            "status": "blocked",
            "passed_gates": 0,
            "required_gates": 4,
            "blockers": [
                {"id": blocker, "status": "blocked"}
                for blocker in sorted(GA_BLOCKERS)
            ],
        },
    }
    require(record["independent_verification"]["byte_for_byte_equal"], "official and independent verdict bytes differ")
    record["promotion_id"] = evidence.identity_digest(record, "promotion_id")
    verify_record(record)
    return record


def verify_record(record: Any) -> dict[str, Any]:
    require(isinstance(record, dict), "promotion record must be an object")
    require(record.get("schema_version") == PROMOTION_VERSION, "promotion record schema is invalid")
    require(record.get("promotion_id") == evidence.identity_digest(record, "promotion_id"), "promotion record identity mismatch")
    require(record.get("stage") == "commercial_beta", "promotion record stage is invalid")
    candidate = record.get("candidate", {})
    require(candidate.get("ref") == "refs/heads/main" and candidate.get("dirty") is False, "promotion candidate is not protected main")
    require(bool(SHA64.fullmatch(str(record.get("package_sha256", "")))), "promotion package digest is invalid")
    require(record.get("independent_verification", {}).get("byte_for_byte_equal") is True, "independent verdict bytes disagree")
    ga = record.get("ga", {})
    require(ga.get("status") == "blocked", "GA must remain blocked after beta promotion")
    require(ga.get("passed_gates") == 0 and ga.get("required_gates") == 4, "GA gate count must remain 0/4")
    blockers = ga.get("blockers")
    require(isinstance(blockers, list), "GA blockers are missing")
    require({item.get("id") for item in blockers} == GA_BLOCKERS, "GA blockers are incomplete or combined")
    require(all(item.get("status") == "blocked" for item in blockers), "a GA blocker was authored as passed")
    excluded = set(record.get("beta_boundary", {}).get("excluded_claims", []))
    require({"native-http-tls", "multi-host-failover", "consensus", "generalized-distributed-truth"} <= excluded, "beta boundary overclaims distributed or transport capability")
    for label in ("bundle", "dependent_verdict"):
        artifact = record.get(label, {})
        require(isinstance(artifact.get("artifact_id"), int) and artifact["artifact_id"] > 0, f"{label} artifact ID is invalid")
        require("latest" not in str(artifact.get("artifact_name", "")).lower(), f"{label} latest artifact is not authoritative")
        require(bool(SHA64.fullmatch(str(artifact.get("sha256", "")))), f"{label} artifact digest is invalid")
    return record


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate_parser = commands.add_parser("generate")
    for command_parser in (generate_parser,):
        command_parser.add_argument("--bundle", required=True)
        command_parser.add_argument("--official-verdict", required=True)
        command_parser.add_argument("--independent-verdict", required=True)
        command_parser.add_argument("--expected-commit-sha", required=True)
        command_parser.add_argument("--expected-tree-sha", required=True)
        command_parser.add_argument("--expected-ref", default="refs/heads/main")
    generate_parser.add_argument("--out", required=True)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("record")
    verify_parser.add_argument("--bundle", required=True)
    verify_parser.add_argument("--official-verdict", required=True)
    verify_parser.add_argument("--independent-verdict", required=True)
    verify_parser.add_argument("--expected-commit-sha", required=True)
    verify_parser.add_argument("--expected-tree-sha", required=True)
    verify_parser.add_argument("--expected-ref", default="refs/heads/main")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "generate":
            record = generate(args)
            evidence.write_canonical_json(Path(args.out), record)
        else:
            record = verify_record(evidence.load_json(Path(args.record)))
            regenerated = generate(args)
            require(record == regenerated, "promotion record differs from live regenerated record")
        return 0
    except (evidence.EvidenceError, json.JSONDecodeError, KeyError, TypeError) as exc:
        print(f"commercial beta promotion failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
