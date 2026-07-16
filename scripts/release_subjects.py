#!/usr/bin/env python3
"""Create and verify candidate-bound AETHER release subject envelopes.

Subject envelopes turn workflow outputs into immutable evidence.  A filename is
never evidence: every envelope binds its observation and upstream artifacts to
one candidate and one canonical package digest.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import release_evidence as evidence


SUBJECT_VERSION = "aether.release-subject.v1"
SUBJECT_STATUSES = {"passed", "failed", "error", "skipped"}
SHA40 = re.compile(r"^[a-f0-9]{40}$")
SHA64 = re.compile(r"^[a-f0-9]{64}$")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise evidence.EvidenceError(message)


def load_json_object(path: Path) -> dict[str, Any]:
    payload = evidence.load_json(path)
    require(isinstance(payload, dict), f"subject observation must be an object: {path}")
    return payload


def artifact_receipt(path: Path) -> dict[str, Any]:
    payload = load_json_object(path)
    required = {
        "artifact_id",
        "artifact_name",
        "workflow_file",
        "run_id",
        "attempt",
        "head_sha",
        "status",
        "sha256",
        "byte_size",
    }
    require(required <= set(payload), f"artifact receipt is missing fields: {sorted(required - set(payload))}")
    return payload


def create_envelope(args: argparse.Namespace) -> dict[str, Any]:
    observation = load_json_object(Path(args.observation))
    candidate = {
        "repository": args.repository,
        "commit_sha": args.commit_sha,
        "tree_sha": args.tree_sha,
        "ref": args.ref,
        "dirty": False,
    }
    producer = {
        "workflow_file": args.workflow_file,
        "workflow_name": args.workflow_name,
        "job_name": args.job_name,
        "run_id": str(args.run_id),
        "attempt": args.attempt,
        "runner": args.runner,
        "host": args.host,
    }
    now = evidence.parse_time(args.generated_at) if args.generated_at else evidence.utc_now()
    valid_until = (
        evidence.parse_time(args.valid_until)
        if args.valid_until
        else now + timedelta(hours=args.valid_hours)
    )
    payload: dict[str, Any] = {
        "schema_version": SUBJECT_VERSION,
        "subject_identity": "",
        "subject_id": args.subject_id,
        "candidate": candidate,
        "producer": producer,
        "observed_status": args.status,
        "package": {"name": args.package_name, "sha256": args.package_sha256},
        "source_runs": [],
        "source_artifacts": [artifact_receipt(Path(item)) for item in (args.artifact_receipt or [])],
        "generated_at": evidence.iso(now),
        "valid_until": evidence.iso(valid_until),
        "metrics": load_json_object(Path(args.metrics)) if args.metrics else {},
        "observation": observation,
    }
    payload["subject_identity"] = evidence.identity_digest(payload, "subject_identity")
    return payload


def _verify_artifact_receipt(receipt: Any, candidate: dict[str, Any]) -> None:
    require(isinstance(receipt, dict), "source artifact receipt must be an object")
    require(isinstance(receipt.get("artifact_id"), int) and receipt["artifact_id"] > 0, "invalid source artifact ID")
    require(bool(receipt.get("artifact_name")), "source artifact name is missing")
    require(bool(receipt.get("workflow_file")), "source artifact workflow is missing")
    require(str(receipt.get("run_id", "")).isdigit(), "source artifact run ID is invalid")
    require(isinstance(receipt.get("attempt"), int) and receipt["attempt"] > 0, "source artifact attempt is invalid")
    require(
        f"-{receipt['run_id']}-{receipt['attempt']}" in receipt["artifact_name"],
        "source artifact name is not bound to its run attempt",
    )
    require(receipt.get("head_sha") == candidate["commit_sha"], "source artifact belongs to another candidate")
    require(receipt.get("status") == "passed", "source artifact producer did not pass")
    require(bool(SHA64.fullmatch(str(receipt.get("sha256", "")))), "source artifact digest is invalid")
    require(isinstance(receipt.get("byte_size"), int) and receipt["byte_size"] > 0, "source artifact is empty")


def _verify_observation(subject_id: str, observation: Any, candidate: dict[str, Any], package_sha256: str) -> None:
    require(isinstance(observation, dict), f"{subject_id} observation must be an object")
    require(observation.get("status") == "passed", f"{subject_id} observation did not pass")
    require(observation.get("candidate_commit_sha") == candidate["commit_sha"], f"{subject_id} observation commit mismatch")
    require(observation.get("candidate_tree_sha") == candidate["tree_sha"], f"{subject_id} observation tree mismatch")
    require(observation.get("package_sha256") == package_sha256, f"{subject_id} observation package mismatch")
    require(bool(observation.get("check")), f"{subject_id} observation check is missing")
    details = observation.get("details")
    require(isinstance(details, dict), f"{subject_id} observation details are missing")
    if subject_id in {
        "rust-sbom",
        "go-sbom",
        "assembled-package-sbom",
        "vulnerability-scan",
        "license-scan",
        "secret-scan",
        "pages-deployment",
        "capacity",
    }:
        source_files = details.get("source_files")
        require(isinstance(source_files, list) and source_files, f"{subject_id} source-file bindings are missing")
        for item in source_files:
            require(isinstance(item, dict), f"{subject_id} source-file binding is invalid")
            require(isinstance(item.get("artifact_id"), int) and item["artifact_id"] > 0, f"{subject_id} source artifact ID is invalid")
            evidence.safe_bundle_relative(str(item.get("path", "")))
            require(bool(SHA64.fullmatch(str(item.get("sha256", "")))), f"{subject_id} source-file digest is invalid")
            require(isinstance(item.get("byte_size"), int) and item["byte_size"] > 0, f"{subject_id} source file is empty")

    if subject_id in {"rust-sbom", "go-sbom", "assembled-package-sbom"}:
        require(details.get("format") in {"spdx-json", "cyclonedx-json"}, f"{subject_id} has an unsupported SBOM format")
        require(isinstance(details.get("document_sha256"), str) and bool(SHA64.fullmatch(details["document_sha256"])), f"{subject_id} SBOM digest is invalid")
    elif subject_id in {"vulnerability-scan", "license-scan", "secret-scan"}:
        require(isinstance(details.get("tools"), list) and details["tools"], f"{subject_id} scanner list is empty")
        require(details.get("findings", 0) == 0, f"{subject_id} contains blocking findings")
    elif subject_id == "code-scan":
        jobs = details.get("jobs")
        require(isinstance(jobs, list) and len(jobs) >= 2, "CodeQL job outcomes are incomplete")
        require(all(item.get("conclusion") == "success" for item in jobs), "CodeQL did not pass")
    elif subject_id == "transport-tls":
        require(details.get("mode") == "verify_full", "transport TLS was not verified in verify_full mode")
        require(details.get("job_conclusion") == "success", "transport TLS job did not pass")
    elif subject_id == "pages-deployment":
        require(details.get("observed_candidate_sha") == candidate["commit_sha"], "Pages displays another candidate")
        require(str(details.get("url", "")).startswith("https://"), "Pages verification did not use HTTPS")
    elif subject_id == "capacity":
        require(details.get("gate_passed") is True, "capacity gate did not pass")
        require(isinstance(details.get("metrics"), dict) and details["metrics"], "capacity metrics are missing")
        policy = details.get("policy", {})
        envelope = details.get("selected_envelope", {})
        hardware = details.get("recommended_hardware", {})
        require(envelope.get("node_class") == policy.get("node_class"), "capacity node class does not match policy")
        checks = {
            "board_size": envelope.get("maximum_recommended_pilot_board_size", 0) >= policy.get("minimum_board_size", 0),
            "operator_concurrency": envelope.get("maximum_recommended_mixed_operator_concurrency", 0) >= policy.get("minimum_mixed_operator_concurrency", 0),
            "durable_replay": envelope.get("maximum_recommended_durable_replay_size", 0) >= policy.get("minimum_durable_replay_size", 0),
            "target_latency": hardware.get("target_p95_latency_ms", float("inf")) <= policy.get("maximum_target_p95_latency_ms", 0),
            "target_replay": hardware.get("target_replay_seconds", float("inf")) <= policy.get("maximum_target_replay_seconds", 0),
        }
        require(all(checks.values()), "capacity policy threshold failed")
    elif subject_id == "customer-workflow":
        raw = details.get("raw_evidence", {})
        gates = raw.get("gates", [])
        require(raw.get("workflow_ready") is True and gates, "customer workflow did not pass")
        require(all(item.get("status") == "passed" for item in gates), "customer workflow contains a non-passing gate")
        require(details.get("workflow_passed") is True, "customer workflow did not pass")
        require(details.get("steps") == len(gates), "customer workflow steps are missing or authored")
    elif subject_id == "pilot-package-file-manifest":
        raw = details.get("raw_evidence", {})
        require(isinstance(details.get("files"), list) and details["files"], "package file manifest is empty")
        require(raw.get("files") == details["files"], "package file manifest raw evidence disagrees")
    elif subject_id in {"namespace-contention", "resource-controls", "recovery", "performance", "soak"}:
        require(details.get("gate_passed") is True, f"{subject_id} operational gate did not pass")
        require(isinstance(details.get("metrics"), dict), f"{subject_id} metrics are missing")
        raw = details.get("raw_evidence", {})
        report = raw.get("report", {})
        transcript = raw.get("transcript")
        require(isinstance(report, dict) and isinstance(transcript, str), f"{subject_id} raw evidence is missing")
        markers = details.get("required_markers", [])
        require(isinstance(markers, list) and all(marker in transcript for marker in markers), f"{subject_id} raw test markers are missing")
        require(details["metrics"].get("transcript_sha256") == evidence.sha256_bytes(transcript.encode("utf-8")), f"{subject_id} transcript digest mismatch")
        gates = report.get("gates", [])
        require(isinstance(gates, list) and gates, f"{subject_id} raw gates are missing")
        if subject_id == "namespace-contention":
            require(report.get("beta_ready") is True, "namespace operability report did not pass")
            require(all(item.get("status") in {"passed", "ci_blocking"} for item in gates), "namespace operability contains a non-passing gate")
        elif subject_id == "recovery":
            require(report.get("rollback_ready") is True, "recovery report did not pass")
            require(all(item.get("status") == "passed" for item in gates), "recovery contains a non-passing gate")
        elif subject_id == "performance":
            require(report.get("beta_ready") is True, "performance report did not pass")
            require(all(item.get("status") == "passed" for item in gates), "performance contains a non-passing gate")
    elif subject_id == "package-provenance":
        encoded = details.get("attestation_bundle_base64")
        require(isinstance(encoded, str) and encoded, "package provenance attestation bundle is missing")
        require(bool(SHA64.fullmatch(str(details.get("attestation_bundle_sha256", "")))), "package provenance bundle digest is invalid")
    else:
        raise evidence.EvidenceError(f"no semantic validator exists for release subject: {subject_id}")


def verify_envelope(
    payload: Any,
    *,
    expected_subject_id: str,
    candidate: dict[str, Any],
    package_sha256: str,
    now: datetime,
) -> dict[str, Any]:
    require(isinstance(payload, dict), f"{expected_subject_id} envelope must be an object")
    require(payload.get("schema_version") == SUBJECT_VERSION, f"{expected_subject_id} uses an unknown subject schema")
    require(payload.get("subject_id") == expected_subject_id, f"release subject ID mismatch: {expected_subject_id}")
    require(payload.get("subject_identity") == evidence.identity_digest(payload, "subject_identity"), f"{expected_subject_id} identity mismatch")
    require(payload.get("candidate") == candidate, f"{expected_subject_id} candidate mismatch")
    require(payload.get("observed_status") in SUBJECT_STATUSES, f"{expected_subject_id} status is invalid")
    require(payload.get("observed_status") == "passed", f"{expected_subject_id} did not pass")
    package = payload.get("package")
    require(isinstance(package, dict), f"{expected_subject_id} package binding is missing")
    require(package.get("sha256") == package_sha256, f"{expected_subject_id} is bound to another package")
    require(bool(SHA64.fullmatch(str(package.get("sha256", "")))), f"{expected_subject_id} package digest is invalid")
    producer = payload.get("producer")
    require(isinstance(producer, dict), f"{expected_subject_id} producer is missing")
    require(bool(producer.get("workflow_file")) and bool(producer.get("job_name")), f"{expected_subject_id} producer identity is incomplete")
    require(str(producer.get("run_id", "")).isdigit(), f"{expected_subject_id} producer run is invalid")
    require(isinstance(producer.get("attempt"), int) and producer["attempt"] > 0, f"{expected_subject_id} producer attempt is invalid")
    generated_at = evidence.parse_time(payload["generated_at"])
    valid_until = evidence.parse_time(payload["valid_until"])
    require(generated_at <= now + timedelta(minutes=5), f"{expected_subject_id} has a future timestamp")
    require(valid_until >= now, f"{expected_subject_id} expired")
    require(generated_at < valid_until, f"{expected_subject_id} validity window is invalid")
    receipts = payload.get("source_artifacts")
    require(isinstance(receipts, list), f"{expected_subject_id} source artifacts are invalid")
    seen_artifacts: set[tuple[str, int]] = set()
    for receipt in receipts:
        _verify_artifact_receipt(receipt, candidate)
        key = (str(receipt["run_id"]), receipt["artifact_id"])
        require(key not in seen_artifacts, f"{expected_subject_id} has duplicate source artifact receipts")
        seen_artifacts.add(key)
    source_runs = payload.get("source_runs")
    require(isinstance(source_runs, list), f"{expected_subject_id} source runs are invalid")
    seen_runs: set[tuple[str, int]] = set()
    for run in source_runs:
        require(isinstance(run, dict), f"{expected_subject_id} source run must be an object")
        require(bool(run.get("workflow_file")), f"{expected_subject_id} source workflow is missing")
        require(str(run.get("run_id", "")).isdigit(), f"{expected_subject_id} source run ID is invalid")
        require(isinstance(run.get("attempt"), int) and run["attempt"] > 0, f"{expected_subject_id} source attempt is invalid")
        require(run.get("head_sha") == candidate["commit_sha"], f"{expected_subject_id} source run is cross-candidate")
        require(run.get("status") == "passed", f"{expected_subject_id} source run did not pass")
        key = (str(run["run_id"]), run["attempt"])
        require(key not in seen_runs, f"{expected_subject_id} has duplicate source runs")
        seen_runs.add(key)
    for receipt in receipts:
        key = (str(receipt["run_id"]), receipt["attempt"])
        require(key in seen_runs, f"{expected_subject_id} artifact is bound to an undeclared source run")
    require(isinstance(payload.get("metrics"), dict), f"{expected_subject_id} metrics must be an object")
    _verify_observation(expected_subject_id, payload.get("observation"), candidate, package_sha256)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject-id", required=True)
    parser.add_argument("--observation", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--tree-sha", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--package-name", required=True)
    parser.add_argument("--package-sha256", required=True)
    parser.add_argument("--workflow-file", required=True)
    parser.add_argument("--workflow-name", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt", required=True, type=int)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--status", choices=sorted(SUBJECT_STATUSES), default="passed")
    parser.add_argument("--artifact-receipt", action="append")
    parser.add_argument("--metrics")
    parser.add_argument("--generated-at")
    parser.add_argument("--valid-until")
    parser.add_argument("--valid-hours", type=int, default=24)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        payload = create_envelope(args)
        if not SHA40.fullmatch(args.commit_sha) or not SHA40.fullmatch(args.tree_sha):
            raise evidence.EvidenceError("candidate commit and tree must be full lowercase SHA-1 values")
        if not SHA64.fullmatch(args.package_sha256):
            raise evidence.EvidenceError("package digest must be a lowercase SHA-256 value")
        evidence.write_canonical_json(Path(args.output), payload)
        return 0
    except (evidence.EvidenceError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"release subject creation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
