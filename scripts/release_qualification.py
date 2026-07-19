#!/usr/bin/env python3
"""Fetch exact-candidate prerequisites and build release-subject envelopes."""

from __future__ import annotations

import argparse
import base64
import io
import json
import shutil
import subprocess
import sys
import zipfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import release_evidence as evidence
import release_subjects as subjects


WORKFLOWS = {
    "ci": ".github/workflows/ci.yml",
    "supply_chain": ".github/workflows/supply-chain.yml",
    "pages": ".github/workflows/pages.yml",
    "capacity": ".github/workflows/capacity-planning.yml",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise evidence.EvidenceError(message)


def gh_api(endpoint: str, *, binary: bool = False) -> Any:
    completed = subprocess.run(
        ["gh", "api", endpoint],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise evidence.EvidenceError(
            f"GitHub API request failed for {endpoint}: "
            f"{completed.stderr.decode('utf-8', errors='replace').strip()}"
        )
    if binary:
        return completed.stdout
    return json.loads(completed.stdout)


def verify_run(repository: str, run_id: str, candidate_sha: str, workflow_file: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    require(str(run_id).isdigit(), f"invalid workflow run ID: {run_id}")
    run = gh_api(f"repos/{repository}/actions/runs/{run_id}")
    require(run.get("head_sha") == candidate_sha, f"{workflow_file} run belongs to another candidate")
    require(run.get("status") == "completed" and run.get("conclusion") == "success", f"{workflow_file} run did not pass")
    require(run.get("head_branch") == "main", f"{workflow_file} run is not a protected main run")
    path = str(run.get("path", ""))
    require(path == workflow_file or path.startswith(f"{workflow_file}@"), f"unexpected workflow for run {run_id}: {path}")
    require(isinstance(run.get("run_attempt"), int) and run["run_attempt"] > 0, f"invalid attempt for run {run_id}")
    jobs_payload = gh_api(f"repos/{repository}/actions/runs/{run_id}/attempts/{run['run_attempt']}/jobs?per_page=100")
    jobs = jobs_payload.get("jobs", [])
    require(isinstance(jobs, list), f"jobs are missing for run {run_id}")
    return run, jobs


def find_job(jobs: list[dict[str, Any]], name: str) -> dict[str, Any]:
    matches = [item for item in jobs if item.get("name") == name or str(item.get("name", "")).endswith(f" / {name}")]
    require(len(matches) == 1, f"workflow job is missing or ambiguous: {name}")
    job = matches[0]
    require(job.get("status") == "completed" and job.get("conclusion") == "success", f"workflow job did not pass: {name}")
    return {"id": job.get("id"), "name": job.get("name"), "conclusion": job.get("conclusion")}


def safe_extract(content: bytes, destination: Path) -> None:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), "artifact contains duplicate ZIP entries")
        for name in names:
            evidence.safe_bundle_relative(name)
        archive.extractall(destination)


def fetch_artifact(repository: str, run: dict[str, Any], name: str, destination: Path) -> dict[str, Any]:
    payload = gh_api(f"repos/{repository}/actions/runs/{run['id']}/artifacts?per_page=100")
    matches = [item for item in payload.get("artifacts", []) if item.get("name") == name]
    require(len(matches) == 1, f"artifact is missing or ambiguous for run {run['id']}: {name}")
    artifact = matches[0]
    require(artifact.get("expired") is False, f"artifact is expired: {name}")
    digest = str(artifact.get("digest", ""))
    require(digest.startswith("sha256:"), f"artifact digest is missing: {name}")
    archive = gh_api(f"repos/{repository}/actions/artifacts/{artifact['id']}/zip", binary=True)
    require(len(archive) == artifact.get("size_in_bytes"), f"artifact byte size mismatch: {name}")
    require(evidence.sha256_bytes(archive) == digest.removeprefix("sha256:"), f"artifact digest mismatch: {name}")
    target = destination / name
    target.mkdir(parents=True, exist_ok=True)
    safe_extract(archive, target)
    return {
        "artifact_id": artifact["id"],
        "artifact_name": name,
        "workflow_file": str(run.get("path", "")).split("@", 1)[0],
        "run_id": str(run["id"]),
        "attempt": run["run_attempt"],
        "head_sha": run["head_sha"],
        "status": "passed",
        "sha256": digest.removeprefix("sha256:"),
        "byte_size": len(archive),
    }


def fetch_inputs(args: argparse.Namespace) -> int:
    protected_main = gh_api(f"repos/{args.repository}/git/ref/heads/main")
    require(
        protected_main.get("object", {}).get("sha") == args.candidate_sha,
        "protected main advanced; restart qualification with the new candidate",
    )
    output = Path(args.output_dir).resolve()
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True)
    runs: dict[str, Any] = {}
    verified_runs: dict[str, dict[str, Any]] = {}
    jobs: dict[str, Any] = {}
    for key in WORKFLOWS:
        run, run_jobs = verify_run(
            args.repository,
            str(getattr(args, f"{key}_run_id")),
            args.candidate_sha,
            WORKFLOWS[key],
        )
        verified_runs[key] = run
        runs[key] = {
            "id": run["id"],
            "attempt": run["run_attempt"],
            "workflow_file": WORKFLOWS[key],
            "head_sha": run["head_sha"],
            "conclusion": run["conclusion"],
        }
        jobs[key] = run_jobs

    required_jobs = {
        "ci_gate": find_job(jobs["ci"], "Required CI gate"),
        "transport_tls": find_job(jobs["ci"], "Postgres verified TLS journal"),
        "supply_gate": find_job(jobs["supply_chain"], "Required Supply Chain gate"),
        "codeql_go": find_job(jobs["supply_chain"], "CodeQL (go)"),
        "codeql_python": find_job(jobs["supply_chain"], "CodeQL (python)"),
    }
    artifact_names = {
        "canonical_package": f"supply-chain-candidate-package-{args.candidate_sha}-{verified_runs['supply_chain']['id']}-{verified_runs['supply_chain']['run_attempt']}",
        "supply_chain_evidence": f"supply-chain-evidence-{args.candidate_sha}-{verified_runs['supply_chain']['id']}-{verified_runs['supply_chain']['run_attempt']}",
        "pages": f"pages-deployment-verification-{args.candidate_sha}-{verified_runs['pages']['id']}-{verified_runs['pages']['run_attempt']}",
        "capacity": f"capacity-report-{args.candidate_sha}-{verified_runs['capacity']['id']}-{verified_runs['capacity']['run_attempt']}",
    }
    receipts = {
        key: fetch_artifact(args.repository, verified_runs[run_key], artifact_names[key], output)
        for key, run_key in {
            "canonical_package": "supply_chain",
            "supply_chain_evidence": "supply_chain",
            "pages": "pages",
            "capacity": "capacity",
        }.items()
    }
    package_files = list((output / artifact_names["canonical_package"]).rglob("aether-pilot-service-windows-x86_64.zip"))
    require(len(package_files) == 1, "canonical package artifact does not contain exactly one expected package")
    package_sha = evidence.sha256_file(package_files[0])
    manifest = {
        "schema_version": "aether.release-qualification-inputs.v1",
        "repository": args.repository,
        "candidate_repository": args.candidate_repository,
        "candidate_commit_sha": args.candidate_sha,
        "candidate_tree_sha": args.candidate_tree_sha,
        "candidate_ref": args.candidate_ref,
        "package_path": package_files[0].relative_to(output).as_posix(),
        "package_sha256": package_sha,
        "runs": runs,
        "jobs": required_jobs,
        "artifacts": receipts,
    }
    evidence.write_canonical_json(output / "qualification-inputs.json", manifest)
    print(package_files[0])
    return 0


def find_one(root: Path, name: str) -> Path:
    matches = [item for item in root.rglob(name) if item.is_file()]
    require(len(matches) == 1, f"expected exactly one {name}, found {len(matches)}")
    return matches[0]


def normalized_observation(
    candidate: dict[str, Any], package_sha: str, check: str, details: dict[str, Any]
) -> dict[str, Any]:
    return {
        "status": "passed",
        "candidate_commit_sha": candidate["commit_sha"],
        "candidate_tree_sha": candidate["tree_sha"],
        "package_sha256": package_sha,
        "check": check,
        "details": details,
    }


def source_file_descriptor(path: Path, artifact_root: Path, receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_id": receipt["artifact_id"],
        "path": path.relative_to(artifact_root).as_posix(),
        "sha256": evidence.sha256_file(path),
        "byte_size": path.stat().st_size,
    }


def make_subject(
    *,
    subject_id: str,
    observation: dict[str, Any],
    candidate: dict[str, Any],
    package_name: str,
    package_sha: str,
    producer: dict[str, Any],
    receipts: list[dict[str, Any]],
    source_runs: list[dict[str, Any]],
    metrics: dict[str, Any],
    gate_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated = evidence.utc_now()
    payload = {
        "schema_version": subjects.SUBJECT_VERSION,
        "subject_identity": "",
        "subject_id": subject_id,
        "candidate": candidate,
        "producer": producer,
        "observed_status": "passed",
        "package": {"name": package_name, "sha256": package_sha},
        "source_runs": source_runs,
        "source_artifacts": receipts,
        "generated_at": evidence.iso(generated),
        "valid_until": evidence.iso(generated + timedelta(hours=24)),
        "metrics": metrics,
        "observation": observation,
    }
    payload["subject_identity"] = evidence.identity_digest(payload, "subject_identity")
    subjects.verify_envelope(
        payload,
        expected_subject_id=subject_id,
        candidate=candidate,
        package_sha256=package_sha,
        now=generated,
        gate_policy=gate_policy,
    )
    return payload


def ensure_status(payload: dict[str, Any], label: str, *, ready_key: str | None = None) -> None:
    if ready_key:
        require(payload.get(ready_key) is True, f"{label} did not pass")
    elif "status" in payload:
        require(payload.get("status") == "passed", f"{label} did not pass")


def build_subjects(args: argparse.Namespace) -> int:
    input_root = Path(args.input_dir).resolve()
    readiness_manifest_path = Path(args.readiness_manifest).resolve()
    readiness_manifest = evidence.load_json(readiness_manifest_path)
    manifest = evidence.load_json(input_root / "qualification-inputs.json")
    policy = evidence.load_json(Path(args.policy))
    evidence.validate_policy(policy)
    candidate = {
        "repository": args.candidate_repository,
        "commit_sha": args.candidate_sha,
        "tree_sha": args.candidate_tree_sha,
        "ref": args.candidate_ref,
        "dirty": False,
    }
    require(manifest.get("candidate_commit_sha") == candidate["commit_sha"], "qualification inputs are cross-candidate")
    require(manifest.get("candidate_tree_sha") == candidate["tree_sha"], "qualification input tree mismatch")
    require(manifest.get("candidate_repository") == candidate["repository"], "qualification input repository mismatch")
    package = Path(args.package).resolve()
    package_sha = evidence.sha256_file(package)
    require(package_sha == manifest.get("package_sha256"), "tested package is not the canonical Supply Chain package")
    require(readiness_manifest.get("schema_version") == "aether.release-readiness-evidence.v1", "readiness evidence schema is invalid")
    require(readiness_manifest.get("status") == "passed", "operational readiness did not pass")
    require(
        readiness_manifest.get("candidate")
        == {
            "commit_sha": candidate["commit_sha"],
            "tree_sha": candidate["tree_sha"],
            "ref": candidate["ref"],
        },
        "readiness evidence is cross-candidate",
    )
    require(readiness_manifest.get("workflow") == {"run_id": str(args.run_id), "attempt": args.attempt}, "readiness evidence run mismatch")
    require(readiness_manifest.get("package", {}).get("sha256") == package_sha, "readiness evidence tested another package")

    repo = Path(__file__).resolve().parents[1]
    readiness_output_dir = Path(args.readiness_output_dir).resolve()
    readiness_output_dir.mkdir(parents=True, exist_ok=True)

    def readiness_output(name: str) -> Path:
        descriptor = readiness_manifest.get("outputs", {}).get(name)
        require(isinstance(descriptor, dict), f"readiness output is missing: {name}")
        relative = evidence.safe_bundle_relative(str(descriptor.get("path", "")))
        require("latest" not in relative.as_posix().lower(), f"readiness output uses mutable latest path: {name}")
        path = repo / relative
        require(path.is_file(), f"readiness output file is missing: {name}")
        require(path.stat().st_size == descriptor.get("byte_size"), f"readiness output size mismatch: {name}")
        require(evidence.sha256_file(path) == descriptor.get("sha256"), f"readiness output digest mismatch: {name}")
        shutil.copy2(path, readiness_output_dir / f"{name}-{path.name}")
        return path
    output = Path(args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    producer = {
        "workflow_file": ".github/workflows/release-readiness.yml",
        "workflow_name": "Release Readiness",
        "job_name": "Release Readiness (Windows)",
        "run_id": str(args.run_id),
        "attempt": args.attempt,
        "runner": args.runner,
        "host": args.host,
    }
    receipts = manifest["artifacts"]
    observations: dict[str, tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]] = {}

    supply_root = input_root / manifest["artifacts"]["supply_chain_evidence"]["artifact_name"]
    summary = evidence.load_json(find_one(supply_root, "supply-chain-summary.json"))
    ensure_status(summary, "Supply Chain summary")
    require(summary.get("candidate_commit_sha") == candidate["commit_sha"], "Supply Chain summary is cross-candidate")
    require(summary.get("package_sha256") == package_sha, "Supply Chain summary is for another package")
    for subject_id, filename in {
        "rust-sbom": "aether-rust.cdx.json",
        "go-sbom": "aether-go.cdx.json",
        "assembled-package-sbom": "aether-package.cdx.json",
    }.items():
        document = find_one(supply_root, filename)
        raw = evidence.load_json(document)
        require(raw.get("bomFormat") == "CycloneDX", f"{subject_id} is not CycloneDX")
        document_sha = evidence.sha256_file(document)
        require(
            summary.get("sboms", {}).get(subject_id, {}).get("sha256") == document_sha,
            f"{subject_id} digest does not match the Supply Chain summary",
        )
        properties = raw.get("metadata", {}).get("properties", [])
        require(
            any(
                item.get("name") == "aether:candidate:commit_sha"
                and item.get("value") == candidate["commit_sha"]
                for item in properties
            ),
            f"{subject_id} metadata is cross-candidate",
        )
        if subject_id == "assembled-package-sbom":
            hashes = raw.get("metadata", {}).get("component", {}).get("hashes", [])
            require(
                any(
                    item.get("alg") == "SHA-256"
                    and str(item.get("content", "")).lower() == package_sha
                    for item in hashes
                ),
                "assembled package SBOM is bound to another package",
            )
        observations[subject_id] = (
            normalized_observation(candidate, package_sha, subject_id, {"format": "cyclonedx-json", "document_sha256": document_sha, "component_count": len(raw.get("components", [])), "source_files": [source_file_descriptor(document, supply_root, receipts["supply_chain_evidence"])]}),
            [receipts["supply_chain_evidence"]],
            {"component_count": len(raw.get("components", []))},
        )
    observations["license-scan"] = (
        normalized_observation(candidate, package_sha, "license-policy", {"tools": ["supply_chain.py"], "findings": len(summary.get("violations", [])), "license_count": len(summary.get("license_expressions", [])), "source_files": [source_file_descriptor(find_one(supply_root, "supply-chain-summary.json"), supply_root, receipts["supply_chain_evidence"])]}),
        [receipts["supply_chain_evidence"]],
        {"license_count": len(summary.get("license_expressions", []))},
    )
    for subject_id, filename, tools in [
        ("vulnerability-scan", "vulnerability-scan.json", ["cargo-audit", "govulncheck", "trivy"]),
        ("secret-scan", "secret-scan.json", ["gitleaks"]),
    ]:
        raw = evidence.load_json(find_one(supply_root, filename))
        ensure_status(raw, subject_id)
        require(raw.get("candidate_commit_sha") == candidate["commit_sha"], f"{subject_id} is cross-candidate")
        observations[subject_id] = (
            normalized_observation(candidate, package_sha, subject_id, {"tools": raw.get("tools", tools), "findings": 0, "source_files": [source_file_descriptor(find_one(supply_root, filename), supply_root, receipts["supply_chain_evidence"])]}),
            [receipts["supply_chain_evidence"]],
            {"findings": 0},
        )
    code_jobs = [manifest["jobs"]["codeql_go"], manifest["jobs"]["codeql_python"]]
    observations["code-scan"] = (
        normalized_observation(candidate, package_sha, "codeql", {"jobs": code_jobs}),
        [receipts["supply_chain_evidence"]],
        {"successful_jobs": len(code_jobs)},
    )
    tls_job = manifest["jobs"]["transport_tls"]
    observations["transport-tls"] = (
        normalized_observation(candidate, package_sha, "postgres-verify-full", {"mode": "verify_full", "job_conclusion": tls_job["conclusion"], "job": tls_job}),
        [],
        {"successful_jobs": 1},
    )

    pages_root = input_root / receipts["pages"]["artifact_name"]
    pages_raw = evidence.load_json(find_one(pages_root, "deployment-verification.json"))
    require(pages_raw.get("valid") is True and pages_raw.get("expected_sha") == candidate["commit_sha"], "Pages evidence did not pass for candidate")
    observations["pages-deployment"] = (
        normalized_observation(candidate, package_sha, "pages-exact-sha", {"observed_candidate_sha": pages_raw.get("observed_sha"), "url": pages_raw.get("source"), "source_files": [source_file_descriptor(find_one(pages_root, "deployment-verification.json"), pages_root, receipts["pages"])]}),
        [receipts["pages"]],
        {"attempts": len(pages_raw.get("attempt_history", []))},
    )
    capacity_root = input_root / receipts["capacity"]["artifact_name"]
    capacity_source = find_one(capacity_root, f"{receipts['capacity']['artifact_name']}.json")
    require("latest" not in capacity_source.name.lower(), "mutable capacity pointer cannot qualify a release")
    capacity_raw = evidence.load_json(capacity_source)
    capacity_policy = policy.get("capacity_acceptance", {})
    node_class = capacity_policy.get("node_class")
    matching_envelopes = [
        item for item in capacity_raw.get("single_node_envelopes", [])
        if item.get("node_class") == node_class
    ]
    require(len(matching_envelopes) == 1, "capacity report lacks one policy-selected node envelope")
    selected_envelope = matching_envelopes[0]
    recommended_hardware = capacity_raw.get("recommended_hardware", {})
    require(capacity_raw.get("node_class") == node_class, "capacity report recommends another node class")
    require(recommended_hardware.get("node_class") == node_class, "capacity hardware recommends another node class")
    capacity_acceptance = subjects.recompute_capacity_acceptance(
        capacity_policy,
        selected_envelope,
        recommended_hardware,
        capacity_raw.get("concurrency_pack"),
    )
    capacity_checks = capacity_acceptance["checks"]
    require(all(capacity_checks.values()), f"capacity acceptance failed: {sorted(name for name, passed in capacity_checks.items() if not passed)}")
    capacity_metrics = {
        "node_class": node_class,
        "limiting_factor": capacity_raw.get("current_limiting_factor"),
        "envelope_count": len(capacity_raw.get("single_node_envelopes", [])),
        "checks": capacity_checks,
        "operator_error_rate": capacity_acceptance["operator_error_rate"],
        "operator_p95_latency_ms": capacity_acceptance["policy_point"]["p95_latency_ms"],
        "service_instances_per_point": capacity_acceptance["service_instances_per_point"],
    }
    observations["capacity"] = (
        normalized_observation(candidate, package_sha, "capacity-planning", {"gate_passed": True, "metrics": capacity_metrics, "policy": capacity_policy, "selected_envelope": selected_envelope, "recommended_hardware": recommended_hardware, "concurrency_pack": capacity_raw.get("concurrency_pack"), "capacity_acceptance": capacity_acceptance, "source_files": [source_file_descriptor(capacity_source, capacity_root, receipts["capacity"])]}),
        [receipts["capacity"]],
        capacity_metrics,
    )

    performance = evidence.load_json(readiness_output("performance_beta"))
    ensure_status(performance, "performance beta", ready_key="beta_ready")
    service = evidence.load_json(readiness_output("service_operability"))
    ensure_status(service, "service operability", ready_key="beta_ready")
    rollback = evidence.load_json(readiness_output("rollback"))
    ensure_status(rollback, "rollback record", ready_key="rollback_ready")
    customer = evidence.load_json(readiness_output("customer_workflow"))
    ensure_status(customer, "customer workflow", ready_key="workflow_ready")
    require(customer.get("gates") and all(item.get("status") == "passed" for item in customer["gates"]), "customer workflow has a non-passing gate")
    security = evidence.load_json(readiness_output("security_lifecycle"))
    ensure_status(security, "security lifecycle", ready_key="beta_ready")
    commercial_policy = evidence.load_json(readiness_output("commercial_policy"))
    require(isinstance(commercial_policy, dict), "commercial policy output is invalid")
    manifest_file = readiness_output("package_file_manifest")
    package_manifest = evidence.load_json(manifest_file)
    manifest_entries = package_manifest.get("files", [])
    require(isinstance(manifest_entries, list) and manifest_entries, "package file manifest is empty")
    declared_files = {
        item.get("path"): {"bytes": item.get("bytes"), "sha256": item.get("sha256")}
        for item in manifest_entries
        if isinstance(item, dict)
    }
    require(len(declared_files) == len(manifest_entries), "package file manifest has duplicate or invalid paths")
    with zipfile.ZipFile(package) as archive:
        package_entries = [item for item in archive.infolist() if not item.is_dir()]
        require(len(package_entries) == len({item.filename for item in package_entries}), "canonical package has duplicate ZIP entries")
        observed_files = {}
        for item in package_entries:
            path = evidence.safe_bundle_relative(item.filename).as_posix()
            content = archive.read(item)
            observed_files[path] = {"bytes": len(content), "sha256": evidence.sha256_bytes(content)}
    require(declared_files == observed_files, "package file manifest does not describe the canonical package bytes")
    observations["pilot-package-file-manifest"] = (
        normalized_observation(candidate, package_sha, "package-file-manifest", {"files": manifest_entries, "manifest_sha256": evidence.sha256_file(manifest_file), "raw_evidence": package_manifest}),
        [],
        {"file_count": len(manifest_entries)},
    )
    readiness_transcript = readiness_output("readiness_transcript").read_text(encoding="utf-8")
    pilot_launch_path = readiness_output("pilot_launch_transcript")
    pilot_launch = pilot_launch_path.read_text(encoding="utf-8")
    operational = {
        "namespace-contention": (
            service,
            "namespace-contention",
            [
                "same_namespace_is_ordered_and_initializes_once ... ok",
                "blocked_namespace_does_not_delay_another_namespace_or_directory_status ... ok",
            ],
            readiness_transcript,
        ),
        "resource-controls": (
            {"gates": [{"status": "passed"}, {"status": "passed"}, {"status": "passed"}]},
            "resource-controls",
            [
                "resource_limits_are_typed_audited_and_leave_authority_unchanged ... ok",
                "audit_backpressure_is_bounded_and_visible ... ok",
                "executor_saturation_and_panics_fail_boundedly ... ok",
            ],
            readiness_transcript,
        ),
        "recovery": (rollback, "recovery", [], readiness_transcript),
        "performance": (performance, "performance-beta", [], readiness_transcript),
        "soak": (
            {"gates": [{"status": "passed"}, {"status": "passed"}]},
            "pilot-soak",
            [
                "misuse_paths_are_rejected_cleanly_and_audited ... ok",
                "soak_authenticated_pilot_service_survives_restarts ... ok",
            ],
            pilot_launch,
        ),
    }
    for subject_id, (raw, check, required_markers, transcript) in operational.items():
        require(all(marker in transcript for marker in required_markers), f"{subject_id} exact test markers are missing")
        allowed_statuses = {"passed", "ci_blocking"} if subject_id == "namespace-contention" else {"passed"}
        require(raw.get("gates") and all(item.get("status") in allowed_statuses for item in raw["gates"]), f"{subject_id} contains a non-passing gate")
        metrics = {"gate_count": len(raw.get("gates", [])), "passed_gate_count": sum(1 for item in raw.get("gates", []) if item.get("status") in allowed_statuses), "required_marker_count": len(required_markers), "transcript_sha256": evidence.sha256_bytes(transcript.encode("utf-8"))}
        observations[subject_id] = (
            normalized_observation(candidate, package_sha, check, {"gate_passed": True, "metrics": metrics, "required_markers": required_markers, "raw_evidence": {"report": raw, "transcript": transcript}}),
            [],
            metrics,
        )
    observations["customer-workflow"] = (
        normalized_observation(candidate, package_sha, "candidate-customer-workflow", {"workflow_passed": True, "workflow": customer.get("workflow"), "steps": len(customer.get("gates", [])), "raw_evidence": customer}),
        [],
        {"gate_count": len(customer.get("gates", []))},
    )
    expected_without_provenance = set(policy["future_required_bundle_subjects"]) - {"package-provenance"}
    require(set(observations) == expected_without_provenance, f"subject builder coverage mismatch: {sorted(expected_without_provenance - set(observations))}")
    for subject_id, (observation, source_receipts, metrics) in observations.items():
        bound_receipts = [receipts["canonical_package"]]
        bound_receipts.extend(
            receipt for receipt in source_receipts
            if receipt["artifact_id"] != receipts["canonical_package"]["artifact_id"]
        )
        source_key = {
            "rust-sbom": "supply_chain",
            "go-sbom": "supply_chain",
            "assembled-package-sbom": "supply_chain",
            "vulnerability-scan": "supply_chain",
            "license-scan": "supply_chain",
            "code-scan": "supply_chain",
            "secret-scan": "supply_chain",
            "transport-tls": "ci",
            "capacity": "capacity",
            "pages-deployment": "pages",
        }.get(subject_id)
        bound_runs = []
        for run_key in dict.fromkeys(["supply_chain", source_key] if source_key else ["supply_chain"]):
            source_run = manifest["runs"][run_key]
            bound_runs.append(
                {
                    "workflow_file": source_run["workflow_file"],
                    "run_id": str(source_run["id"]),
                    "attempt": source_run["attempt"],
                    "head_sha": source_run["head_sha"],
                    "status": "passed",
                }
            )
        payload = make_subject(
            subject_id=subject_id,
            observation=observation,
            candidate=candidate,
            package_name=package.name,
            package_sha=package_sha,
            producer=producer,
            receipts=bound_receipts,
            source_runs=bound_runs,
            metrics=metrics,
            gate_policy=policy,
        )
        evidence.write_canonical_json(output / f"{subject_id}.json", payload)
    shutil.copy2(readiness_manifest_path, readiness_output_dir / readiness_manifest_path.name)
    return 0


def build_provenance_subject(args: argparse.Namespace) -> int:
    package = Path(args.package).resolve()
    package_sha = evidence.sha256_file(package)
    candidate = {
        "repository": args.candidate_repository,
        "commit_sha": args.candidate_sha,
        "tree_sha": args.candidate_tree_sha,
        "ref": args.candidate_ref,
        "dirty": False,
    }
    bundle_bytes = Path(args.attestation_bundle).read_bytes()
    observation = normalized_observation(
        candidate,
        package_sha,
        "github-slsa-package-provenance",
        {
            "attestation_bundle_base64": base64.b64encode(bundle_bytes).decode("ascii"),
            "attestation_bundle_sha256": evidence.sha256_bytes(bundle_bytes),
        },
    )
    producer = {
        "workflow_file": ".github/workflows/reusable-exact-candidate-evidence.yml",
        "workflow_name": "Reusable Exact Candidate Evidence",
        "job_name": "Exact candidate evidence",
        "run_id": str(args.run_id),
        "attempt": args.attempt,
        "runner": args.runner,
        "host": args.host,
    }
    qualification_inputs = evidence.load_json(Path(args.qualification_inputs))
    require(qualification_inputs.get("candidate_commit_sha") == candidate["commit_sha"], "provenance qualification inputs are cross-candidate")
    require(qualification_inputs.get("package_sha256") == package_sha, "provenance qualification inputs bind another package")
    supply_run = qualification_inputs["runs"]["supply_chain"]
    source_run = {
        "workflow_file": supply_run["workflow_file"],
        "run_id": str(supply_run["id"]),
        "attempt": supply_run["attempt"],
        "head_sha": supply_run["head_sha"],
        "status": "passed",
    }
    payload = make_subject(
        subject_id="package-provenance",
        observation=observation,
        candidate=candidate,
        package_name=package.name,
        package_sha=package_sha,
        producer=producer,
        receipts=[qualification_inputs["artifacts"]["canonical_package"]],
        source_runs=[source_run],
        metrics={"attestation_bundle_bytes": len(bundle_bytes)},
    )
    evidence.write_canonical_json(Path(args.output), payload)
    return 0


def common_candidate(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--candidate-repository", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree-sha", required=True)
    parser.add_argument("--candidate-ref", required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("fetch-inputs")
    common_candidate(fetch)
    fetch.add_argument("--ci-run-id", required=True)
    fetch.add_argument("--supply-chain-run-id", required=True)
    fetch.add_argument("--pages-run-id", required=True)
    fetch.add_argument("--capacity-run-id", required=True)
    fetch.add_argument("--output-dir", required=True)
    fetch.set_defaults(func=fetch_inputs)
    build = commands.add_parser("build-subjects")
    common_candidate(build)
    build.add_argument("--input-dir", required=True)
    build.add_argument("--readiness-manifest", required=True)
    build.add_argument("--package", required=True)
    build.add_argument("--output-dir", required=True)
    build.add_argument("--readiness-output-dir", required=True)
    build.add_argument("--policy", default="fixtures/release/gate-policy.json")
    build.add_argument("--run-id", required=True)
    build.add_argument("--attempt", type=int, required=True)
    build.add_argument("--runner", default="Windows")
    build.add_argument("--host", default="github-windows-latest")
    build.set_defaults(func=build_subjects)
    provenance = commands.add_parser("build-provenance-subject")
    common_candidate(provenance)
    provenance.add_argument("--package", required=True)
    provenance.add_argument("--attestation-bundle", required=True)
    provenance.add_argument("--qualification-inputs", required=True)
    provenance.add_argument("--output", required=True)
    provenance.add_argument("--run-id", required=True)
    provenance.add_argument("--attempt", type=int, required=True)
    provenance.add_argument("--runner", default="Windows")
    provenance.add_argument("--host", default="github-windows-latest")
    provenance.set_defaults(func=build_provenance_subject)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        return args.func(args)
    except (evidence.EvidenceError, json.JSONDecodeError, KeyError, TypeError, zipfile.BadZipFile) as exc:
        print(f"release qualification failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
