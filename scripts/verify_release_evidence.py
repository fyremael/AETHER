#!/usr/bin/env python3
"""Fail-closed verifier for AETHER immutable release evidence."""

from __future__ import annotations

import argparse
import base64
import io
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import release_evidence as evidence
import release_subjects


SHA40 = re.compile(r"^[a-f0-9]{40}$")
SHA64 = re.compile(r"^[a-f0-9]{64}$")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise evidence.EvidenceError(message)


def verify_descriptor(root: Path, item: dict[str, Any], label: str) -> Path:
    path = root / evidence.safe_bundle_relative(item.get("path", ""))
    require(path.is_file(), f"{label} is missing: {item.get('path')}")
    require(path.stat().st_size == item.get("byte_size"), f"{label} byte size mismatch")
    require(evidence.sha256_file(path) == item.get("sha256"), f"{label} digest mismatch")
    return path


def verify_candidate(candidate: dict[str, Any]) -> None:
    require(isinstance(candidate, dict), "candidate must be an object")
    require(bool(candidate.get("repository")), "candidate repository is missing")
    require(bool(SHA40.fullmatch(str(candidate.get("commit_sha", "")))), "invalid candidate commit SHA")
    require(bool(SHA40.fullmatch(str(candidate.get("tree_sha", "")))), "invalid candidate tree SHA")
    require(bool(candidate.get("ref")), "candidate ref is missing")
    require(candidate.get("dirty") is False, "dirty candidate evidence is forbidden")


def verify_time_window(envelope: dict[str, Any], now: datetime) -> None:
    started = evidence.parse_time(envelope["started_at"])
    ended = evidence.parse_time(envelope["ended_at"])
    require(started <= ended, f"{envelope['gate_id']} ends before it starts")
    require(ended <= now + timedelta(minutes=5), f"{envelope['gate_id']} has a future timestamp")
    valid_until = envelope.get("valid_until")
    if valid_until is not None:
        require(evidence.parse_time(valid_until) >= now, f"{envelope['gate_id']} evidence expired")


def verify_attempts(envelope: dict[str, Any], gate: dict[str, Any]) -> None:
    attempts = envelope.get("attempt_history")
    require(isinstance(attempts, list) and attempts, f"{envelope['gate_id']} has no attempt history")
    require([item.get("attempt") for item in attempts] == list(range(1, len(attempts) + 1)), "attempt history is concealed or non-contiguous")
    final = attempts[-1]
    require(final.get("status") == envelope.get("observed_status"), "final attempt status mismatch")
    require(final.get("exit_code") == envelope.get("exit_code"), "final attempt exit code mismatch")
    if len(attempts) > 1:
        require(gate.get("retry_policy") == "infrastructure_only", f"semantic gate {gate['id']} retried")
        for prior in attempts[:-1]:
            require(
                prior.get("status") == "error" and prior.get("failure_class") == "infrastructure",
                f"{gate['id']} concealed a fail-then-pass retry",
            )


def verify_envelope(
    root: Path,
    envelope: dict[str, Any],
    gate: dict[str, Any],
    candidate: dict[str, Any],
    workflow: dict[str, Any],
    official: bool,
    policy: dict[str, Any],
    now: datetime,
) -> None:
    require(envelope.get("schema_version") == evidence.ENVELOPE_VERSION, "unknown evidence envelope schema")
    require(envelope.get("gate_id") == gate["id"], "evidence gate does not match policy gate")
    require(envelope.get("observed_status") in evidence.OBSERVED_STATUSES, f"unknown observed status for {gate['id']}")
    require(envelope.get("observed_status") != "skipped", f"skipped gate evidence is incomplete: {gate['id']}")
    require(envelope.get("observed_status") not in {"ready", "accepted_risk", "ci_blocking"}, "authored status used as evidence")
    require(envelope.get("candidate") == candidate, f"{gate['id']} candidate mismatch")
    require(envelope.get("workflow") == workflow, f"{gate['id']} workflow mismatch")
    require(envelope.get("official") is official, f"{gate['id']} official/local classification mismatch")
    require(envelope.get("command") == gate.get("commands"), f"{gate['id']} command mismatch")
    require(envelope.get("working_directory", ".") == gate.get("working_directory", "."), f"{gate['id']} working directory mismatch")
    require(envelope.get("evidence_id") == evidence.identity_digest(envelope, "evidence_id"), f"{gate['id']} evidence identity mismatch")
    verify_time_window(envelope, now)
    verify_attempts(envelope, gate)
    if envelope["observed_status"] == "passed":
        require(envelope.get("exit_code") == 0, f"{gate['id']} passed with non-zero exit code")
    expected_metrics = gate.get("expected_metrics", {})
    for name, expected in expected_metrics.items():
        require(envelope.get("metrics", {}).get(name) == expected, f"{gate['id']} wrong {name}")
    if gate["id"] == "performance.capacity":
        require(isinstance(envelope.get("metrics", {}).get("capacity"), dict), "Capacity artifact nesting regression")
    if gate["id"] == "delivery.pages_candidate_sha":
        require(envelope.get("metrics", {}).get("deployed_sha") == candidate["commit_sha"], "Pages deployed SHA differs from candidate SHA")
    output = verify_descriptor(root, envelope["output"], f"{gate['id']} output")
    require(output.suffix == ".log" or envelope["output"].get("media_type"), f"{gate['id']} output media type missing")
    input_names = {item.get("path") for item in envelope.get("inputs", [])}
    require(set(gate.get("inputs", [])).issubset(input_names), f"{gate['id']} input digests are incomplete")
    if official:
        require(policy["official_workflow"] in input_names, f"{gate['id']} did not digest the workflow at the candidate")


def waiver_subject_digest(waiver: dict[str, Any]) -> str:
    material = dict(waiver)
    material.pop("waiver_id", None)
    material.pop("signature", None)
    return evidence.sha256_bytes(evidence.canonical_bytes(material))


def verify_waiver(waiver: dict[str, Any], policy_gate: dict[str, Any], candidate: dict[str, Any], now: datetime) -> None:
    require(waiver.get("schema_version") == "aether.risk-waiver.v1", "unknown waiver schema")
    require(waiver.get("waiver_id") == evidence.identity_digest(waiver, "waiver_id"), "waiver identity mismatch")
    require(policy_gate.get("waivable") is True, f"waiver supplied for non-waivable gate {policy_gate['id']}")
    require(policy_gate["id"] not in evidence.NON_WAIVABLE_CORE, f"core gate {policy_gate['id']} cannot be waived")
    require(waiver.get("candidate_commit_sha") == candidate["commit_sha"], "waiver crosses candidate commit")
    require(waiver.get("candidate_tree_sha") == candidate["tree_sha"], "waiver crosses candidate tree")
    require(evidence.parse_time(waiver["approved_at"]) <= now < evidence.parse_time(waiver["expires_at"]), "waiver is future-dated or expired")
    require(bool(waiver.get("approved_by")), "waiver approver is missing")
    require(bool(waiver.get("compensating_controls")), "waiver compensating controls are missing")
    signature = waiver.get("signature", {})
    require(signature.get("scheme") == "external_attestation", "waiver signature scheme is invalid")
    require(bool(signature.get("attestation_ref")), "waiver attestation reference is missing")
    require(signature.get("subject_digest") == waiver_subject_digest(waiver), "waiver signed subject mismatch")


def verify_sbom(path: Path) -> None:
    payload = evidence.load_json(path)
    declared = payload.get("aether_lockfile_components")
    if declared is None:
        return
    components = payload.get("components", [])
    observed = {
        component.get("purl") or f"{component.get('name')}@{component.get('version')}"
        for component in components
    }
    missing = sorted(set(declared) - observed)
    require(not missing, f"SBOM missing a lockfile component: {missing}")


def verify_integrity_manifest(root: Path, manifest: dict[str, Any]) -> None:
    integrity_path = verify_descriptor(root, manifest["file_integrity_manifest"], "file-integrity manifest")
    integrity = evidence.load_json(integrity_path)
    require(integrity.get("schema_version") == "aether.bundle-file-integrity.v1", "unknown file-integrity schema")
    declared: set[str] = set()
    for item in integrity.get("files", []):
        require(item.get("path") not in declared, f"duplicate integrity entry: {item.get('path')}")
        declared.add(item.get("path"))
        verify_descriptor(root, item, f"integrity entry {item.get('path')}")
    actual = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name not in {"bundle-manifest.json", "file-integrity-manifest.json"}
    }
    require(actual == declared, f"bundle files differ from integrity manifest: missing={sorted(declared-actual)}, extra={sorted(actual-declared)}")


def run_json_command(command: list[str]) -> Any:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise evidence.EvidenceError(f"external evidence verification unavailable: {exc}") from exc
    require(
        completed.returncode == 0,
        f"external evidence verification failed: {completed.stderr.strip() or completed.stdout.strip()}",
    )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise evidence.EvidenceError("external evidence verifier returned invalid JSON") from exc


def github_api(endpoint: str) -> Any:
    return run_json_command(["gh", "api", endpoint])


def github_artifact_archive(repository: str, artifact_id: int) -> bytes:
    command = ["gh", "api", f"repos/{repository}/actions/artifacts/{artifact_id}/zip"]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise evidence.EvidenceError(f"official artifact download unavailable: {exc}") from exc
    require(
        completed.returncode == 0,
        "official artifact download failed: "
        + (completed.stderr.decode("utf-8", errors="replace").strip() or "unknown error"),
    )
    return completed.stdout


def verify_attestation(command: list[str]) -> Any:
    return run_json_command(command)


def verify_official_workflow(workflow: dict[str, Any], policy: dict[str, Any]) -> None:
    require(workflow.get("workflow_file") == policy["official_workflow"], "official workflow mismatch")
    require(workflow.get("job_id") == policy["official_job"], "official job mismatch")
    require(str(workflow.get("run_id", "")).isdigit(), "an existing workflow declaration without a successful run is not evidence")
    require(isinstance(workflow.get("attempt"), int) and workflow["attempt"] > 0, "invalid official workflow attempt")
    require(workflow.get("repository") == policy["official_repository"], "official repository mismatch")
    require(workflow.get("runner") in policy["allowed_official_runners"], "official runner is outside policy")
    require(workflow.get("host") in policy["allowed_official_hosts"], "official host is outside policy")


def expected_artifact_name(
    workflow: dict[str, Any], candidate: dict[str, Any], policy: dict[str, Any]
) -> str:
    return (
        f"{policy['official_artifact_prefix']}-{candidate['commit_sha']}-"
        f"{workflow['run_id']}-{workflow['attempt']}"
    )


def verify_github_outcome(
    bundle: Path,
    workflow: dict[str, Any],
    candidate: dict[str, Any],
    policy: dict[str, Any],
    *,
    api: Callable[[str], Any] = github_api,
    download_artifact: Callable[[str, int], bytes] = github_artifact_archive,
) -> dict[str, Any]:
    require(bundle.is_file() and bundle.suffix.lower() == ".zip", "official evidence input must be an immutable ZIP bundle")
    repository = policy["official_repository"]
    run_id = workflow["run_id"]
    attempt = workflow["attempt"]
    protected_main = api(f"repos/{repository}/git/ref/heads/main")
    require(
        protected_main.get("object", {}).get("sha") == candidate["commit_sha"],
        "protected main advanced beyond the qualified candidate",
    )
    run = api(f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}")
    require(run.get("id") == int(run_id), "declared GitHub run does not exist in the official repository")
    require(run.get("run_attempt") == attempt, "GitHub run attempt mismatch")
    require(run.get("head_sha") == candidate["commit_sha"], "GitHub run is for a different candidate")
    require(
        run.get("repository", {}).get("full_name") == repository,
        "GitHub run repository mismatch",
    )
    workflow_path = str(run.get("path", ""))
    require(
        workflow_path == policy["official_caller_workflow"]
        or workflow_path.startswith(f"{policy['official_caller_workflow']}@"),
        "GitHub run used an unexpected caller workflow",
    )

    jobs = api(f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100")
    expected_job = policy["official_job_name"]
    matching_jobs = [
        job
        for job in jobs.get("jobs", [])
        if job.get("name") == expected_job or str(job.get("name", "")).endswith(f" / {expected_job}")
    ]
    require(len(matching_jobs) == 1, "official producer job is missing or ambiguous")
    require(
        matching_jobs[0].get("status") == "completed"
        and matching_jobs[0].get("conclusion") == "success",
        "official producer job did not complete successfully",
    )

    artifact_name = expected_artifact_name(workflow, candidate, policy)
    require(workflow.get("artifact_name") == artifact_name, "declared official artifact name mismatch")
    artifacts = api(f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100")
    matching_artifacts = [
        artifact for artifact in artifacts.get("artifacts", []) if artifact.get("name") == artifact_name
    ]
    require(len(matching_artifacts) == 1, "exact official evidence artifact is missing or ambiguous")
    artifact = matching_artifacts[0]
    require(artifact.get("expired") is False, "official evidence artifact is expired")
    require(isinstance(artifact.get("size_in_bytes"), int) and artifact["size_in_bytes"] > 0, "official evidence artifact is empty")
    require(isinstance(artifact.get("id"), int), "official evidence artifact ID is missing")
    artifact_digest = str(artifact.get("digest", ""))
    require(
        artifact_digest.startswith("sha256:")
        and bool(SHA64.fullmatch(artifact_digest.removeprefix("sha256:"))),
        "official evidence artifact digest is missing or invalid",
    )
    artifact_sha = artifact.get("workflow_run", {}).get("head_sha")
    if artifact_sha is not None:
        require(artifact_sha == candidate["commit_sha"], "official artifact belongs to a different candidate")

    archive_bytes = download_artifact(repository, artifact["id"])
    require(len(archive_bytes) == artifact["size_in_bytes"], "downloaded official artifact byte size mismatch")
    require(
        evidence.sha256_bytes(archive_bytes) == artifact_digest.removeprefix("sha256:"),
        "downloaded official artifact digest mismatch",
    )
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            files = [item for item in archive.infolist() if not item.is_dir()]
            require(len(files) == 1, "official artifact must contain exactly one evidence bundle")
            item = files[0]
            evidence.safe_bundle_relative(item.filename)
            require(Path(item.filename).name == bundle.name, "official artifact contains the wrong bundle name")
            require(item.file_size == bundle.stat().st_size, "official artifact bundle byte size mismatch")
            archived_bundle = archive.read(item)
    except zipfile.BadZipFile as exc:
        raise evidence.EvidenceError("downloaded official artifact is not a valid ZIP") from exc
    require(
        evidence.sha256_bytes(archived_bundle) == evidence.sha256_file(bundle),
        "official artifact does not contain the exact input evidence bundle",
    )
    return {
        "artifact_id": artifact["id"],
        "artifact_name": artifact["name"],
        "sha256": artifact_digest.removeprefix("sha256:"),
        "byte_size": artifact["size_in_bytes"],
        "run_id": str(run_id),
        "attempt": attempt,
    }


def verify_subject_github_outcomes(
    subject_envelopes: dict[str, dict[str, Any]],
    workflow: dict[str, Any],
    candidate: dict[str, Any],
    policy: dict[str, Any],
    *,
    api: Callable[[str], Any] = github_api,
    download_artifact: Callable[[str, int], bytes] = github_artifact_archive,
) -> None:
    """Requery every prerequisite receipt and security job fail-closed."""
    repository = policy["official_repository"]
    expected_producers = {
        ".github/workflows/release-readiness.yml",
        ".github/workflows/reusable-exact-candidate-evidence.yml",
    }
    qualification_artifact_name = (
        f"release-qualification-subjects-{candidate['commit_sha']}-"
        f"{workflow['run_id']}-{workflow['attempt']}"
    )
    qualification_artifacts = api(
        f"repos/{repository}/actions/runs/{workflow['run_id']}/artifacts?per_page=100"
    )
    qualification_matches = [
        item for item in qualification_artifacts.get("artifacts", [])
        if item.get("name") == qualification_artifact_name
    ]
    require(len(qualification_matches) == 1, "qualification subject artifact is missing or ambiguous")
    qualification_artifact = qualification_matches[0]
    require(qualification_artifact.get("expired") is False, "qualification subject artifact expired")
    qualification_digest = str(qualification_artifact.get("digest", ""))
    require(qualification_digest.startswith("sha256:"), "qualification subject artifact digest is missing")
    qualification_archive = download_artifact(repository, qualification_artifact["id"])
    require(len(qualification_archive) == qualification_artifact.get("size_in_bytes"), "qualification subject artifact size mismatch")
    require(evidence.sha256_bytes(qualification_archive) == qualification_digest.removeprefix("sha256:"), "qualification subject artifact digest mismatch")
    try:
        with zipfile.ZipFile(io.BytesIO(qualification_archive)) as archive:
            files = [item for item in archive.infolist() if not item.is_dir()]
            require(len(files) == len({item.filename for item in files}), "qualification artifact has duplicate ZIP entries")
            qualification_members = {
                evidence.safe_bundle_relative(item.filename).as_posix(): archive.read(item)
                for item in files
            }
    except zipfile.BadZipFile as exc:
        raise evidence.EvidenceError("qualification subject artifact is not a valid ZIP") from exc
    readiness_manifests = [
        content for path, content in qualification_members.items()
        if "qualification-readiness/" in path
        and Path(path).name.startswith("release-readiness-evidence-")
        and Path(path).suffix == ".json"
    ]
    require(len(readiness_manifests) == 1, "qualification artifact omits or duplicates its readiness manifest")
    try:
        readiness_manifest = json.loads(readiness_manifests[0])
    except json.JSONDecodeError as exc:
        raise evidence.EvidenceError("qualification readiness manifest is invalid JSON") from exc
    require(readiness_manifest.get("schema_version") == "aether.release-readiness-evidence.v1", "qualification readiness schema is invalid")
    require(readiness_manifest.get("status") == "passed", "qualification readiness did not pass")
    require(
        readiness_manifest.get("candidate")
        == {
            "commit_sha": candidate["commit_sha"],
            "tree_sha": candidate["tree_sha"],
            "ref": candidate["ref"],
        },
        "qualification readiness is cross-candidate",
    )
    require(readiness_manifest.get("workflow") == workflow, "qualification readiness workflow binding differs")
    required_readiness_outputs = {
        "commercial_policy",
        "customer_workflow",
        "package_file_manifest",
        "performance_beta",
        "pilot_launch_transcript",
        "readiness_transcript",
        "rollback",
        "security_lifecycle",
        "service_operability",
    }
    outputs = readiness_manifest.get("outputs", {})
    require(isinstance(outputs, dict) and set(outputs) == required_readiness_outputs, "qualification readiness output catalog is incomplete")
    for output_name, descriptor in outputs.items():
        require(isinstance(descriptor, dict), f"qualification readiness descriptor is invalid: {output_name}")
        original = evidence.safe_bundle_relative(str(descriptor.get("path", "")))
        require("latest" not in original.as_posix().lower(), f"qualification readiness uses mutable latest output: {output_name}")
        expected_name = f"{output_name}-{original.name}"
        matches = [
            content for path, content in qualification_members.items()
            if "qualification-readiness/" in path and Path(path).name == expected_name
        ]
        require(len(matches) == 1, f"qualification artifact omits or duplicates readiness output: {output_name}")
        require(len(matches[0]) == descriptor.get("byte_size"), f"qualification readiness output size mismatch: {output_name}")
        require(evidence.sha256_bytes(matches[0]) == descriptor.get("sha256"), f"qualification readiness output digest mismatch: {output_name}")
    for subject_id, envelope in subject_envelopes.items():
        if subject_id == "package-provenance":
            continue
        matches = [
            content for path, content in qualification_members.items()
            if "qualification-subjects/" in path and Path(path).name == f"{subject_id}.json"
        ]
        require(len(matches) == 1, f"qualification artifact omits or duplicates subject: {subject_id}")
        require(matches[0] == evidence.canonical_bytes(envelope), f"qualification artifact subject bytes differ: {subject_id}")
    receipts: dict[tuple[str, int], dict[str, Any]] = {}
    declared_runs: dict[tuple[str, int], dict[str, Any]] = {}
    for subject_id, envelope in subject_envelopes.items():
        producer = envelope["producer"]
        require(producer["workflow_file"] in expected_producers, f"{subject_id} has an unauthorized producer workflow")
        require(producer["run_id"] == str(workflow["run_id"]), f"{subject_id} was produced by another release run")
        require(producer["attempt"] == workflow["attempt"], f"{subject_id} was produced by another release attempt")
        for receipt in envelope["source_artifacts"]:
            key = (str(receipt["run_id"]), receipt["artifact_id"])
            previous = receipts.get(key)
            require(previous is None or previous == receipt, f"ambiguous artifact receipt across subjects: {key}")
            receipts[key] = receipt
        for source_run in envelope["source_runs"]:
            key = (str(source_run["run_id"]), source_run["attempt"])
            previous_run = declared_runs.get(key)
            require(previous_run is None or previous_run == source_run, f"ambiguous source run across subjects: {key}")
            declared_runs[key] = source_run

    run_cache: dict[tuple[str, int], dict[str, Any]] = {}
    job_cache: dict[tuple[str, int], list[dict[str, Any]]] = {}
    archive_cache: dict[int, bytes] = {}
    for run_key, declared in declared_runs.items():
        run = api(f"repos/{repository}/actions/runs/{run_key[0]}/attempts/{run_key[1]}")
        require(run.get("id") == int(run_key[0]), "release subject source run does not exist")
        require(run.get("run_attempt") == run_key[1], "release subject source attempt mismatch")
        require(run.get("head_sha") == candidate["commit_sha"], "release subject source run is cross-candidate")
        require(run.get("status") == "completed" and run.get("conclusion") == "success", "release subject source run did not pass")
        require(str(run.get("path", "")).split("@", 1)[0] == declared["workflow_file"], "release subject source workflow mismatch")
        require(run.get("head_branch") == "main", "release subject source is not a protected main run")
        run_cache[run_key] = run
    for (run_id, artifact_id), receipt in receipts.items():
        attempt = receipt["attempt"]
        run_key = (run_id, attempt)
        if run_key not in run_cache:
            run_cache[run_key] = api(f"repos/{repository}/actions/runs/{run_id}/attempts/{attempt}")
        run = run_cache[run_key]
        require(run.get("id") == int(run_id), "release subject source run does not exist")
        require(run.get("run_attempt") == attempt, "release subject source attempt mismatch")
        require(run.get("head_sha") == candidate["commit_sha"], "release subject source run is cross-candidate")
        require(run.get("status") == "completed" and run.get("conclusion") == "success", "release subject source run did not pass")
        run_path = str(run.get("path", "")).split("@", 1)[0]
        require(run_path == receipt["workflow_file"], "release subject source workflow mismatch")
        require(run.get("head_branch") == "main", "release subject source is not a protected main run")
        artifacts = api(f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100")
        matches = [item for item in artifacts.get("artifacts", []) if item.get("id") == artifact_id]
        require(len(matches) == 1, "release subject artifact ID is missing or ambiguous")
        artifact = matches[0]
        require(artifact.get("name") == receipt["artifact_name"], "release subject artifact name mismatch")
        require(artifact.get("expired") is False, "release subject artifact expired")
        require(artifact.get("size_in_bytes") == receipt["byte_size"], "release subject artifact API size changed")
        require(artifact.get("digest") == f"sha256:{receipt['sha256']}", "release subject artifact API digest changed")
        archive = download_artifact(repository, artifact_id)
        require(len(archive) == receipt["byte_size"], "release subject artifact download size mismatch")
        require(evidence.sha256_bytes(archive) == receipt["sha256"], "release subject artifact download digest mismatch")
        archive_cache[artifact_id] = archive

    member_cache: dict[int, dict[str, bytes]] = {}

    def artifact_members(artifact_id: int) -> dict[str, bytes]:
        if artifact_id in member_cache:
            return member_cache[artifact_id]
        try:
            with zipfile.ZipFile(io.BytesIO(archive_cache[artifact_id])) as archive:
                files = [item for item in archive.infolist() if not item.is_dir()]
                require(len(files) == len({item.filename for item in files}), "release subject artifact has duplicate ZIP entries")
                members: dict[str, bytes] = {}
                for item in files:
                    relative = evidence.safe_bundle_relative(item.filename).as_posix()
                    require(relative not in members, "release subject artifact has ambiguous normalized paths")
                    members[relative] = archive.read(item)
        except zipfile.BadZipFile as exc:
            raise evidence.EvidenceError("release subject artifact is not a valid ZIP") from exc
        member_cache[artifact_id] = members
        return members

    for subject_id, envelope in subject_envelopes.items():
        canonical_receipts = [
            item for item in envelope["source_artifacts"]
            if item["artifact_name"].startswith("supply-chain-candidate-package-")
        ]
        require(len(canonical_receipts) == 1, f"{subject_id} lacks one canonical package artifact receipt")
        canonical_receipt = canonical_receipts[0]
        canonical_members = artifact_members(canonical_receipt["artifact_id"])
        matching_packages = [
            content for path, content in canonical_members.items()
            if Path(path).name == envelope["package"]["name"]
        ]
        require(len(matching_packages) == 1, f"{subject_id} canonical package member is missing or ambiguous")
        require(
            evidence.sha256_bytes(matching_packages[0]) == envelope["package"]["sha256"],
            f"{subject_id} canonical artifact contains different package bytes",
        )
        source_files = envelope["observation"]["details"].get("source_files", [])
        receipt_ids = {item["artifact_id"] for item in envelope["source_artifacts"]}
        verified_source_payloads: list[dict[str, Any]] = []
        for source_file in source_files:
            artifact_id = source_file["artifact_id"]
            require(artifact_id in receipt_ids, f"{subject_id} source file names an undeclared artifact")
            members = artifact_members(artifact_id)
            relative = evidence.safe_bundle_relative(source_file["path"]).as_posix()
            require(relative in members, f"{subject_id} source file is missing from its artifact")
            content = members[relative]
            require(len(content) == source_file["byte_size"], f"{subject_id} source-file byte size mismatch")
            require(evidence.sha256_bytes(content) == source_file["sha256"], f"{subject_id} source-file digest mismatch")
            try:
                payload = json.loads(content)
            except json.JSONDecodeError as exc:
                raise evidence.EvidenceError(f"{subject_id} source file is not valid JSON") from exc
            require(isinstance(payload, dict), f"{subject_id} source payload must be an object")
            verified_source_payloads.append(payload)

        if subject_id in {"rust-sbom", "go-sbom", "assembled-package-sbom"}:
            require(len(verified_source_payloads) == 1, f"{subject_id} source SBOM is ambiguous")
            sbom = verified_source_payloads[0]
            require(sbom.get("bomFormat") == "CycloneDX", f"{subject_id} source is not CycloneDX")
            properties = sbom.get("metadata", {}).get("properties", [])
            require(
                any(
                    item.get("name") == "aether:candidate:commit_sha"
                    and item.get("value") == candidate["commit_sha"]
                    for item in properties
                ),
                f"{subject_id} source SBOM is cross-candidate",
            )
            if subject_id == "assembled-package-sbom":
                hashes = sbom.get("metadata", {}).get("component", {}).get("hashes", [])
                require(
                    any(
                        item.get("alg") == "SHA-256"
                        and str(item.get("content", "")).lower() == envelope["package"]["sha256"]
                        for item in hashes
                    ),
                    "assembled package source SBOM is bound to different bytes",
                )
        elif subject_id in {"vulnerability-scan", "secret-scan"}:
            require(len(verified_source_payloads) == 1, f"{subject_id} source scan is ambiguous")
            scan = verified_source_payloads[0]
            require(scan.get("status") == "passed", f"{subject_id} source scan did not pass")
            require(scan.get("candidate_commit_sha") == candidate["commit_sha"], f"{subject_id} source scan is cross-candidate")
        elif subject_id == "license-scan":
            require(len(verified_source_payloads) == 1, "license source summary is ambiguous")
            summary = verified_source_payloads[0]
            require(summary.get("status") == "passed" and summary.get("violations") == [], "license source summary did not pass")
            require(summary.get("candidate_commit_sha") == candidate["commit_sha"], "license source summary is cross-candidate")
            require(summary.get("package_sha256") == envelope["package"]["sha256"], "license source summary is for another package")
        elif subject_id == "pages-deployment":
            require(len(verified_source_payloads) == 1, "Pages source verification is ambiguous")
            pages = verified_source_payloads[0]
            require(pages.get("valid") is True, "Pages source verification did not pass")
            require(pages.get("expected_sha") == candidate["commit_sha"] and pages.get("observed_sha") == candidate["commit_sha"], "Pages source verification is cross-candidate")
        elif subject_id == "capacity":
            require(len(verified_source_payloads) == 1, "capacity source report is ambiguous")
            verify_capacity_source_payload(
                envelope,
                verified_source_payloads[0],
                policy["capacity_acceptance"],
            )

    for subject_id, expected_names in {
        "code-scan": {"CodeQL (go)", "CodeQL (python)"},
        "transport-tls": {"Postgres verified TLS journal"},
    }.items():
        envelope = subject_envelopes.get(subject_id)
        require(envelope is not None, f"official bundle is missing {subject_id}")
        detail_jobs = envelope["observation"]["details"].get("jobs")
        if subject_id == "transport-tls":
            detail_jobs = [envelope["observation"]["details"].get("job")]
        require(isinstance(detail_jobs, list), f"{subject_id} job bindings are missing")
        observed_names = {
            str(item.get("name", "")).split(" / ")[-1]
            for item in detail_jobs
            if isinstance(item, dict)
        }
        require(observed_names == expected_names, f"{subject_id} job binding is incomplete or ambiguous")
        for declared in detail_jobs:
            expected_source_workflow = {
                "code-scan": ".github/workflows/supply-chain.yml",
                "transport-tls": ".github/workflows/ci.yml",
            }[subject_id]
            matching_runs = [
                source_run for source_run in envelope["source_runs"]
                if source_run["workflow_file"] == expected_source_workflow
            ]
            require(len(matching_runs) == 1, f"{subject_id} has no unique workflow run")
            source_run = matching_runs[0]
            run_key = (str(source_run["run_id"]), source_run["attempt"])
            if run_key not in job_cache:
                jobs_payload = api(
                    f"repos/{repository}/actions/runs/{run_key[0]}/attempts/{run_key[1]}/jobs?per_page=100"
                )
                job_cache[run_key] = jobs_payload.get("jobs", [])
            matches = [item for item in job_cache[run_key] if item.get("id") == declared.get("id")]
            require(len(matches) == 1, f"{subject_id} GitHub job ID is missing or ambiguous")
            require(matches[0].get("name") == declared.get("name"), f"{subject_id} GitHub job name changed")
            require(matches[0].get("status") == "completed" and matches[0].get("conclusion") == "success", f"{subject_id} GitHub job did not pass")


def verify_capacity_source_payload(
    envelope: dict[str, Any],
    report: dict[str, Any],
    capacity_policy: dict[str, Any],
) -> None:
    """Bind capacity subject semantics to the redownloaded artifact JSON."""

    details = envelope["observation"]["details"]
    require(
        release_subjects.same_json_value(details.get("policy"), capacity_policy),
        "capacity subject policy differs from canonical gate policy",
    )
    source_envelopes = report.get("single_node_envelopes", [])
    require(isinstance(source_envelopes, list), "capacity source envelopes are invalid")
    matching_envelopes = [
        item
        for item in source_envelopes
        if release_subjects.same_json_value(details.get("selected_envelope"), item)
    ]
    require(
        len(matching_envelopes) == 1,
        "capacity selected envelope is absent from source report",
    )
    require(
        release_subjects.same_json_value(
            details.get("recommended_hardware"), report.get("recommended_hardware")
        ),
        "capacity hardware recommendation differs from source report",
    )
    require(
        release_subjects.same_json_value(
            details.get("concurrency_pack"), report.get("concurrency_pack")
        ),
        "capacity concurrency pack differs from source report",
    )
    source_acceptance = release_subjects.recompute_capacity_acceptance(
        capacity_policy,
        details.get("selected_envelope"),
        details.get("recommended_hardware"),
        report.get("concurrency_pack"),
    )
    require(
        release_subjects.same_json_value(
            details.get("capacity_acceptance"), source_acceptance
        ),
        "capacity acceptance differs from source report",
    )


def verify_package_provenance(
    package: Path,
    subject_envelope: dict[str, Any],
    workflow: dict[str, Any],
    candidate: dict[str, Any],
    policy: dict[str, Any],
    *,
    runner: Callable[[list[str]], Any] = verify_attestation,
) -> None:
    encoded_bundle = (
        subject_envelope.get("observation", {})
        .get("details", {})
        .get("attestation_bundle_base64")
    )
    require(isinstance(encoded_bundle, str) and encoded_bundle, "package provenance subject has no attestation bundle")
    try:
        attestation_bundle = base64.b64decode(encoded_bundle, validate=True)
    except ValueError as exc:
        raise evidence.EvidenceError("package provenance subject contains invalid base64") from exc
    expected_bundle_sha = subject_envelope["observation"]["details"].get("attestation_bundle_sha256")
    require(evidence.sha256_bytes(attestation_bundle) == expected_bundle_sha, "package provenance bundle digest mismatch")
    repository = policy["official_repository"]
    signer_workflow = f"{repository}/{policy['official_attestation_workflow']}"
    with tempfile.TemporaryDirectory(prefix="aether-provenance-") as temporary:
        attestation_path = Path(temporary) / "attestation-bundle.json"
        attestation_path.write_bytes(attestation_bundle)
        command = [
            "gh",
            "attestation",
            "verify",
            str(package),
            "--repo",
            repository,
            "--bundle",
            str(attestation_path),
            "--signer-workflow",
            signer_workflow,
            "--signer-digest",
            candidate["commit_sha"],
            "--source-digest",
            candidate["commit_sha"],
            "--source-ref",
            candidate["ref"],
            "--deny-self-hosted-runners",
            "--format",
            "json",
        ]
        results = runner(command)
    require(isinstance(results, list) and results, "package provenance verification returned no result")
    package_digest = evidence.sha256_file(package)
    expected_repository_uri = f"https://github.com/{repository}"
    expected_signer_uri = (
        f"{expected_repository_uri}/{policy['official_attestation_workflow']}@{candidate['ref']}"
    )
    expected_invocation = (
        f"{expected_repository_uri}/actions/runs/{workflow['run_id']}/attempts/{workflow['attempt']}"
    )
    accepted = False
    for result in results:
        verification = result.get("verificationResult", {})
        certificate = verification.get("signature", {}).get("certificate", {})
        statement = verification.get("statement", {})
        subjects = statement.get("subject", [])
        if not any(subject.get("digest", {}).get("sha256") == package_digest for subject in subjects):
            continue
        checks = {
            "source repository": certificate.get("sourceRepositoryURI") == expected_repository_uri,
            "source digest": certificate.get("sourceRepositoryDigest") == candidate["commit_sha"],
            "source ref": certificate.get("sourceRepositoryRef") == candidate["ref"],
            "workflow repository": certificate.get("githubWorkflowRepository") == repository,
            "workflow digest": certificate.get("githubWorkflowSHA") == candidate["commit_sha"],
            "workflow ref": certificate.get("githubWorkflowRef") == candidate["ref"],
            "signer URI": certificate.get("buildSignerURI") == expected_signer_uri,
            "signer digest": certificate.get("buildSignerDigest") == candidate["commit_sha"],
            "run invocation": certificate.get("runInvocationURI") == expected_invocation,
            "runner environment": certificate.get("runnerEnvironment") == "github-hosted",
            "predicate type": statement.get("predicateType") == "https://slsa.dev/provenance/v1",
        }
        if all(checks.values()):
            accepted = True
            break
    require(accepted, "package provenance is not bound to the exact official run and candidate")


def extract_bundle(bundle: Path, destination: Path) -> Path:
    require("latest" not in bundle.name.lower(), "latest pointer is not an authoritative release input")
    if bundle.is_dir():
        target = destination / "bundle"
        shutil.copytree(bundle, target)
        return target
    require(bundle.suffix.lower() == ".zip", "evidence bundle must be a directory or zip")
    with zipfile.ZipFile(bundle) as archive:
        names = archive.namelist()
        require(len(names) == len(set(names)), "bundle contains duplicate zip entries")
        for name in names:
            evidence.safe_bundle_relative(name)
        archive.extractall(destination)
    manifests = list(destination.rglob("bundle-manifest.json"))
    require(len(manifests) == 1, "bundle must contain exactly one manifest")
    return manifests[0].parent


def verify_package_file_manifest(package: Path, subject_envelope: dict[str, Any]) -> None:
    files = subject_envelope["observation"]["details"].get("files", [])
    declared = {
        item.get("path"): {"bytes": item.get("bytes"), "sha256": item.get("sha256")}
        for item in files
        if isinstance(item, dict)
    }
    require(len(declared) == len(files), "package file manifest has duplicate or invalid paths")
    try:
        with zipfile.ZipFile(package) as archive:
            entries = [item for item in archive.infolist() if not item.is_dir()]
            require(len(entries) == len({item.filename for item in entries}), "canonical package has duplicate ZIP entries")
            observed: dict[str, dict[str, Any]] = {}
            for item in entries:
                relative = evidence.safe_bundle_relative(item.filename).as_posix()
                content = archive.read(item)
                require(relative not in observed, "canonical package has ambiguous normalized paths")
                observed[relative] = {
                    "bytes": len(content),
                    "sha256": evidence.sha256_bytes(content),
                }
    except zipfile.BadZipFile as exc:
        raise evidence.EvidenceError("canonical release package is not a valid ZIP") from exc
    require(declared == observed, "package file manifest does not match canonical package bytes")


def verify_bundle(
    bundle: Path,
    *,
    expected_commit_sha: str | None = None,
    expected_tree_sha: str | None = None,
    expected_ref: str | None = None,
    require_official: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    with tempfile.TemporaryDirectory(prefix="aether-verify-") as temporary:
        root = extract_bundle(bundle.resolve(), Path(temporary))
        manifest_path = root / "bundle-manifest.json"
        require(manifest_path.is_file(), "bundle manifest is missing")
        manifest = evidence.load_json(manifest_path)
        require(manifest.get("schema_version") == evidence.BUNDLE_VERSION, "unknown evidence bundle schema")
        require(manifest.get("bundle_id") == evidence.identity_digest(manifest, "bundle_id"), "bundle identity mismatch")
        require(manifest.get("verifier") == {"version": evidence.VERIFIER_VERSION, "algorithm": evidence.ALGORITHM}, "unknown verifier version")
        candidate = manifest.get("candidate")
        verify_candidate(candidate)
        if expected_commit_sha:
            require(candidate["commit_sha"] == expected_commit_sha, "bundle commit SHA does not match expected candidate")
        if expected_tree_sha:
            require(candidate["tree_sha"] == expected_tree_sha, "bundle tree SHA does not match expected candidate")
        if expected_ref:
            require(candidate["ref"] == expected_ref, "bundle ref does not match expected candidate")
        official = manifest.get("official") is True
        if require_official:
            require(official, "diagnostic/local bundle cannot be used as official evidence")
        verify_integrity_manifest(root, manifest)
        policy_path = verify_descriptor(root, manifest["policy"], "gate policy")
        policy = evidence.load_json(policy_path)
        evidence.validate_policy(policy)
        workflow = manifest.get("workflow")
        require(isinstance(workflow, dict), "workflow identity is missing")
        if official:
            verify_official_workflow(workflow, policy)
        gates = {gate["id"]: gate for gate in policy["gates"]}
        envelopes: list[dict[str, Any]] = []
        seen_gate_ids: set[str] = set()
        seen_evidence_ids: set[str] = set()
        for item in manifest.get("evidence", []):
            path = verify_descriptor(root, item, f"evidence fragment {item.get('gate_id')}")
            envelope = evidence.load_json(path)
            gate_id = envelope.get("gate_id")
            require(gate_id in gates, f"unknown evidence gate: {gate_id}")
            require(gate_id not in seen_gate_ids, f"duplicate gate evidence: {gate_id}")
            require(envelope.get("evidence_id") not in seen_evidence_ids, "duplicate evidence identity")
            seen_gate_ids.add(gate_id)
            seen_evidence_ids.add(envelope.get("evidence_id"))
            require(item.get("gate_id") == gate_id, "manifest/envelope gate mismatch")
            require(item.get("evidence_id") == envelope.get("evidence_id"), "manifest/envelope identity mismatch")
            require(item.get("observed_status") == envelope.get("observed_status"), "manifest concealed evidence status")
            verify_envelope(root, envelope, gates[gate_id], candidate, workflow, official, policy, now)
            envelopes.append(envelope)
        require(seen_gate_ids == set(gates), f"missing evidence gates: {sorted(set(gates) - seen_gate_ids)}")
        waiver_payloads: list[dict[str, Any]] = []
        for item in manifest.get("waivers", []):
            waiver = evidence.load_json(verify_descriptor(root, item, "waiver"))
            gate_id = waiver.get("gate_id")
            require(gate_id in gates, f"waiver names unknown gate: {gate_id}")
            verify_waiver(waiver, gates[gate_id], candidate, now)
            waiver_payloads.append(waiver)
        package_path = verify_descriptor(root, manifest["package"], "release package")
        package_digest = evidence.sha256_file(package_path)
        require(package_digest == manifest.get("package_attestation_subject_sha256"), "package digest does not match attested subject")
        for item in manifest.get("sboms", []):
            verify_sbom(verify_descriptor(root, item, "SBOM"))
        available_subjects: set[str] = set()
        subject_paths: dict[str, Path] = {}
        subject_envelopes: dict[str, dict[str, Any]] = {}
        expected_subjects = set(policy.get("future_required_bundle_subjects", []))
        observed_subjects: set[str] = set()
        for subject in manifest.get("subjects", []):
            subject_id = subject.get("id")
            require(subject_id in expected_subjects, f"unknown bundle subject: {subject_id}")
            require(subject_id not in observed_subjects, f"duplicate bundle subject: {subject_id}")
            observed_subjects.add(subject_id)
            require(subject.get("status") in {"present", "missing"}, f"invalid subject status: {subject_id}")
            if subject["status"] == "present":
                subject_paths[subject_id] = verify_descriptor(
                    root, subject.get("file", {}), f"bundle subject {subject_id}"
                )
                subject_envelopes[subject_id] = release_subjects.verify_envelope(
                    evidence.load_json(subject_paths[subject_id]),
                    expected_subject_id=subject_id,
                    candidate=candidate,
                    package_sha256=package_digest,
                    now=now,
                    gate_policy=policy,
                )
                available_subjects.add(subject_id)
            else:
                require("file" not in subject, f"missing subject {subject_id} names a file")
        require(observed_subjects == expected_subjects, f"bundle subject catalog is incomplete: {sorted(expected_subjects-observed_subjects)}")
        if "pilot-package-file-manifest" in subject_envelopes:
            verify_package_file_manifest(
                package_path,
                subject_envelopes["pilot-package-file-manifest"],
            )
        official_artifact = None
        if official:
            require(
                "package-provenance" in subject_paths,
                "official bundle is missing signed package provenance",
            )
            verify_package_provenance(
                package_path,
                subject_envelopes["package-provenance"],
                workflow,
                candidate,
                policy,
            )
            verify_subject_github_outcomes(
                subject_envelopes,
                workflow,
                candidate,
                policy,
            )
            official_artifact = verify_github_outcome(bundle, workflow, candidate, policy)
        verdict, blockers = evidence.compute_verdict(
            policy,
            envelopes,
            waiver_payloads,
            candidate,
            official,
            available_subjects=available_subjects,
        )
        require(manifest.get("computed_verdict") == verdict, "authored bundle verdict differs from computed verdict")
        require(manifest.get("blockers") == blockers, "authored blockers differ from computed blockers")
        return {
            "schema_version": "aether.release-evidence-verdict.v1",
            "bundle_id": manifest["bundle_id"],
            "candidate": candidate,
            "policy_id": policy["policy_id"],
            "official": official,
            "computed_verdict": verdict,
            "blockers": blockers,
            "evidence_ids": sorted(seen_evidence_ids),
            "package_sha256": package_digest,
            "verifier": manifest["verifier"],
            "workflow": {
                "run_id": str(workflow["run_id"]),
                "attempt": workflow["attempt"],
            },
            "official_repository": policy["official_repository"],
            "official_artifact": official_artifact,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle")
    parser.add_argument("--expected-commit-sha")
    parser.add_argument("--expected-tree-sha")
    parser.add_argument("--expected-ref")
    parser.add_argument("--require-official", action="store_true")
    parser.add_argument("--require-passed", action="store_true")
    parser.add_argument("--out")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        verdict = verify_bundle(
            Path(args.bundle),
            expected_commit_sha=args.expected_commit_sha,
            expected_tree_sha=args.expected_tree_sha,
            expected_ref=args.expected_ref,
            require_official=args.require_official,
        )
        if args.out:
            evidence.write_canonical_json(Path(args.out), verdict)
        else:
            sys.stdout.buffer.write(evidence.canonical_bytes(verdict))
        if args.require_passed and verdict["computed_verdict"] != "passed":
            return 3
        return 0
    except (evidence.EvidenceError, json.JSONDecodeError, KeyError, TypeError, zipfile.BadZipFile) as exc:
        print(f"release evidence verification failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
