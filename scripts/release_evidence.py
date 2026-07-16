#!/usr/bin/env python3
"""Capture and assemble candidate-bound AETHER release evidence.

The module intentionally uses only the Python standard library so a fresh
checkout can verify a downloaded bundle without installing project packages.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


ENVELOPE_VERSION = "aether.release-evidence-envelope.v1"
BUNDLE_VERSION = "aether.release-evidence-bundle.v1"
POLICY_VERSION = "aether.release-gate-policy.v1"
VERIFIER_VERSION = "aether-release-evidence-verifier-v3"
ALGORITHM = "sha256-canonical-json-v1"
OBSERVED_STATUSES = {"passed", "failed", "error", "skipped"}
NON_WAIVABLE_CORE = {
    "semantic.policy_noninterference",
    "semantic.trace_handle_identity",
    "storage.transactional_schema_append",
    "semantic.full_acceptance",
    "quality.rust_fmt_clippy_all_targets",
    "quality.go_boundary",
    "quality.python_boundary",
}


class EvidenceError(ValueError):
    pass


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def canonical_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def path_digest(path: Path) -> tuple[str, int]:
    if path.is_file():
        return sha256_file(path), path.stat().st_size
    if not path.is_dir():
        raise EvidenceError(f"input path does not exist: {path}")
    digest = hashlib.sha256()
    size = 0
    files = sorted(item for item in path.rglob("*") if item.is_file())
    for item in files:
        relative = item.relative_to(path).as_posix().encode("utf-8")
        content_digest = sha256_file(item).encode("ascii")
        digest.update(relative)
        digest.update(b"\0")
        digest.update(content_digest)
        digest.update(b"\n")
        size += item.stat().st_size
    return digest.hexdigest(), size


def descriptor(path: Path, *, name: str, display_path: str, media_type: str | None = None) -> dict[str, Any]:
    digest, size = path_digest(path)
    result: dict[str, Any] = {
        "name": name,
        "path": display_path,
        "sha256": digest,
        "byte_size": size,
    }
    if media_type is not None:
        result["media_type"] = media_type
    return result


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_canonical_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload))


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"invalid RFC3339 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise EvidenceError(f"timestamp lacks timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def run_git(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout.strip()


def candidate_identity(root: Path, ref: str | None = None) -> dict[str, Any]:
    status = run_git(root, "status", "--porcelain", "--untracked-files=all")
    if status:
        raise EvidenceError("candidate worktree is dirty; commit or remove every change before capture")
    repository = run_git(root, "config", "--get", "remote.origin.url") or root.name
    commit_sha = run_git(root, "rev-parse", "HEAD")
    tree_sha = run_git(root, "rev-parse", "HEAD^{tree}")
    resolved_ref = ref or os.environ.get("GITHUB_REF")
    if not resolved_ref:
        resolved_ref = run_git(root, "symbolic-ref", "--quiet", "--short", "HEAD") or "detached"
    return {
        "repository": repository,
        "commit_sha": commit_sha,
        "tree_sha": tree_sha,
        "ref": resolved_ref,
        "dirty": False,
    }


def tool_version(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"unavailable: {exc}"
    return completed.stdout.strip().splitlines()[0] if completed.stdout.strip() else "unknown"


def tool_versions() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "rustc": tool_version(["rustc", "--version"]),
        "cargo": tool_version(["cargo", "--version"]),
        "go": tool_version(["go", "version"]),
        "git": tool_version(["git", "--version"]),
        "verifier": VERIFIER_VERSION,
    }


def workflow_identity(args: argparse.Namespace) -> dict[str, Any]:
    workflow_file = args.workflow_file or os.environ.get("AETHER_WORKFLOW_FILE") or "local"
    run_id = args.run_id or os.environ.get("GITHUB_RUN_ID") or "local"
    attempt = int(args.attempt or os.environ.get("GITHUB_RUN_ATTEMPT") or 1)
    job_id = args.job_id or os.environ.get("AETHER_JOB_ID") or "local"
    runner = args.runner or os.environ.get("RUNNER_OS") or platform.system()
    host = args.host or os.environ.get("RUNNER_NAME") or platform.node() or "local"
    repository = args.repository or os.environ.get("GITHUB_REPOSITORY") or "local"
    artifact_name = args.artifact_name or os.environ.get("AETHER_ARTIFACT_NAME") or "local"
    return {
        "workflow_file": workflow_file,
        "run_id": str(run_id),
        "attempt": attempt,
        "job_id": job_id,
        "runner": runner,
        "host": host,
        "repository": repository,
        "artifact_name": artifact_name,
        "tool_versions": tool_versions(),
    }


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("schema_version") != POLICY_VERSION:
        raise EvidenceError(f"unknown gate policy schema: {policy.get('schema_version')!r}")
    gates = policy.get("gates")
    if not isinstance(gates, list) or not gates:
        raise EvidenceError("gate policy requires a non-empty gates list")
    required_identity = {
        "official_repository": str,
        "official_workflow": str,
        "official_attestation_workflow": str,
        "official_caller_workflow": str,
        "official_job": str,
        "official_job_name": str,
        "official_artifact_prefix": str,
        "allowed_official_runners": list,
        "allowed_official_hosts": list,
    }
    for field, expected_type in required_identity.items():
        value = policy.get(field)
        if not isinstance(value, expected_type) or not value:
            raise EvidenceError(f"gate policy requires non-empty {field}")
    seen: set[str] = set()
    for gate in gates:
        gate_id = gate.get("id")
        if not isinstance(gate_id, str) or not gate_id:
            raise EvidenceError("every policy gate requires an id")
        if gate_id in seen:
            raise EvidenceError(f"duplicate gate policy id: {gate_id}")
        seen.add(gate_id)
        if "status" in gate:
            raise EvidenceError(f"policy gate {gate_id} contains authored status")
        if gate.get("retry_policy") not in {"never", "infrastructure_only"}:
            raise EvidenceError(f"gate {gate_id} has invalid retry policy")
        if not isinstance(gate.get("commands"), list) or not gate["commands"]:
            raise EvidenceError(f"gate {gate_id} requires exact commands")
    non_waivable = set(policy.get("non_waivable_gate_ids", []))
    if not NON_WAIVABLE_CORE.issubset(non_waivable):
        missing = sorted(NON_WAIVABLE_CORE - non_waivable)
        raise EvidenceError(f"policy made core gates waivable or omitted: {missing}")
    if contains_authored_outcome(policy):
        raise EvidenceError("gate policy contains authored readiness outcome")


def contains_authored_outcome(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"observed_status", "computed_verdict", "beta_ready"}:
                return True
            if key == "status" and item in {"ready", "accepted_risk", "ci_blocking"}:
                return True
            if contains_authored_outcome(item):
                return True
    elif isinstance(value, list):
        return any(contains_authored_outcome(item) for item in value)
    elif value in {"accepted_risk", "ci_blocking"}:
        return True
    return False


def input_descriptors(root: Path, paths: Iterable[str]) -> list[dict[str, Any]]:
    items = []
    for relative in sorted(paths):
        source = (root / relative).resolve()
        try:
            source.relative_to(root.resolve())
        except ValueError as exc:
            raise EvidenceError(f"input escapes repository: {relative}") from exc
        items.append(descriptor(source, name=relative, display_path=relative))
    return items


def execute_gate(
    root: Path,
    gate: dict[str, Any],
    candidate: dict[str, Any],
    workflow: dict[str, Any],
    official: bool,
    output_root: Path,
    validity_hours: int,
) -> dict[str, Any]:
    gate_id = gate["id"]
    working_directory = gate.get("working_directory", ".")
    cwd = (root / working_directory).resolve()
    log_path = output_root / "outputs" / f"{gate_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = utc_now()
    exit_code = 0
    status = "passed"
    failure_class = "none"
    log_parts: list[str] = []
    try:
        for command_text in gate["commands"]:
            command = shlex.split(command_text, posix=os.name != "nt")
            log_parts.append(f"$ {command_text}\n")
            completed = subprocess.run(
                command,
                cwd=cwd,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            log_parts.append(completed.stdout)
            exit_code = completed.returncode
            if exit_code != 0:
                status = "failed"
                failure_class = "semantic"
                break
    except (OSError, subprocess.SubprocessError) as exc:
        status = "error"
        failure_class = "infrastructure"
        exit_code = 127
        log_parts.append(f"infrastructure error: {exc}\n")
    ended = utc_now()
    log_path.write_text("".join(log_parts), encoding="utf-8", newline="\n")
    relative_log = log_path.relative_to(output_root).as_posix()
    attempt = {
        "attempt": 1,
        "started_at": iso(started),
        "ended_at": iso(ended),
        "exit_code": exit_code,
        "status": status,
        "failure_class": failure_class,
    }
    declared_inputs = list(gate.get("inputs", []))
    if workflow["workflow_file"] != "local" and workflow["workflow_file"] not in declared_inputs:
        declared_inputs.append(workflow["workflow_file"])
    envelope: dict[str, Any] = {
        "schema_version": ENVELOPE_VERSION,
        "evidence_id": "",
        "gate_id": gate_id,
        "official": official,
        "candidate": candidate,
        "workflow": workflow,
        "command": gate["commands"],
        "working_directory": working_directory,
        "started_at": iso(started),
        "ended_at": iso(ended),
        "exit_code": exit_code,
        "attempt_history": [attempt],
        "inputs": input_descriptors(root, declared_inputs),
        "observed_status": status,
        "metrics": {},
        "output": descriptor(
            log_path,
            name=f"{gate_id}-log",
            display_path=relative_log,
            media_type="text/plain",
        ),
        "valid_until": iso(ended + timedelta(hours=validity_hours)),
    }
    envelope["evidence_id"] = identity_digest(envelope, "evidence_id")
    return envelope


def identity_digest(payload: dict[str, Any], identity_field: str) -> str:
    material = dict(payload)
    material.pop(identity_field, None)
    return sha256_bytes(canonical_bytes(material))


def capture(args: argparse.Namespace) -> int:
    root = repo_root()
    policy_path = (root / args.policy).resolve()
    policy = load_json(policy_path)
    validate_policy(policy)
    candidate = candidate_identity(root, args.ref)
    workflow = workflow_identity(args)
    official = bool(args.official)
    if official:
        if workflow["workflow_file"] != policy["official_workflow"]:
            raise EvidenceError("official capture workflow does not match gate policy")
        if workflow["job_id"] != policy["official_job"]:
            raise EvidenceError("official capture job does not match gate policy")
        if not workflow["run_id"].isdigit():
            raise EvidenceError("official capture requires an immutable workflow run id")
        if workflow["repository"] != policy["official_repository"]:
            raise EvidenceError("official capture repository does not match gate policy")
        if workflow["runner"] not in policy["allowed_official_runners"]:
            raise EvidenceError("official capture runner does not match gate policy")
        if workflow["host"] not in policy["allowed_official_hosts"]:
            raise EvidenceError("official capture host does not match gate policy")
        expected_artifact = (
            f"{policy['official_artifact_prefix']}-{candidate['commit_sha']}-"
            f"{workflow['run_id']}-{workflow['attempt']}"
        )
        if workflow["artifact_name"] != expected_artifact:
            raise EvidenceError("official capture artifact name does not match candidate and run")
    run_component = safe_component(workflow["run_id"])
    output_root = Path(args.output_dir) if args.output_dir else (
        root
        / "artifacts"
        / "release"
        / "evidence"
        / candidate["commit_sha"]
        / run_component
        / str(workflow["attempt"])
    )
    if output_root.exists() and any(output_root.iterdir()):
        raise EvidenceError(f"capture output directory is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    envelopes_dir = output_root / "envelopes"
    selected = set(args.gate or [])
    failures = False
    for gate in sorted(policy["gates"], key=lambda item: item["id"]):
        if selected and gate["id"] not in selected:
            continue
        envelope = execute_gate(
            root,
            gate,
            candidate,
            workflow,
            official,
            output_root,
            int(policy.get("validity_hours", 24)),
        )
        write_canonical_json(envelopes_dir / f"{gate['id']}.json", envelope)
        print(f"{gate['id']}: {envelope['observed_status']}")
        failures |= envelope["observed_status"] != "passed"
    capture_manifest = {
        "schema_version": "aether.release-evidence-capture.v1",
        "candidate": candidate,
        "workflow": workflow,
        "official": official,
        "policy_sha256": sha256_file(policy_path),
    }
    write_canonical_json(output_root / "capture-manifest.json", capture_manifest)
    print(output_root)
    return 1 if failures and args.enforce else 0


def safe_component(value: str) -> str:
    result = "".join(character if character.isalnum() or character in "._-" else "_" for character in value)
    return result or "unknown"


def safe_bundle_relative(value: str) -> Path:
    path = Path(value.replace("\\", "/"))
    if path.is_absolute() or ".." in path.parts:
        raise EvidenceError(f"unsafe bundle path: {value}")
    return path


def copy_into(staging: Path, source: Path, relative: str) -> dict[str, Any]:
    target = staging / safe_bundle_relative(relative)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    media_type = "application/json" if target.suffix == ".json" else "application/octet-stream"
    return file_descriptor(target, staging, media_type)


def file_descriptor(path: Path, base: Path, media_type: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "sha256": sha256_file(path),
        "byte_size": path.stat().st_size,
        "media_type": media_type,
    }


def load_envelopes(evidence_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    paths = sorted((evidence_dir / "envelopes").glob("*.json"))
    if not paths:
        raise EvidenceError(f"no evidence envelopes found under {evidence_dir}")
    return [(path, load_json(path)) for path in paths]


def waiver_satisfies(
    waiver: dict[str, Any], gate: dict[str, Any], candidate: dict[str, Any], now: datetime
) -> bool:
    if not gate.get("waivable", False) or gate["id"] in NON_WAIVABLE_CORE:
        return False
    return (
        waiver.get("gate_id") == gate["id"]
        and waiver.get("candidate_commit_sha") == candidate["commit_sha"]
        and waiver.get("candidate_tree_sha") == candidate["tree_sha"]
        and parse_time(waiver["approved_at"]) <= now < parse_time(waiver["expires_at"])
    )


def compute_verdict(
    policy: dict[str, Any],
    envelopes: list[dict[str, Any]],
    waivers: list[dict[str, Any]],
    candidate: dict[str, Any],
    official: bool,
    *,
    available_subjects: set[str],
) -> tuple[str, list[str]]:
    by_gate = {envelope.get("gate_id"): envelope for envelope in envelopes}
    blockers: list[str] = []
    now = max((parse_time(item["ended_at"]) for item in envelopes), default=utc_now())
    for gate in sorted(policy["gates"], key=lambda item: item["id"]):
        evidence = by_gate.get(gate["id"])
        if evidence is None:
            blockers.append(f"missing evidence: {gate['id']}")
            continue
        if evidence.get("observed_status") == "passed":
            continue
        if any(waiver_satisfies(waiver, gate, candidate, now) for waiver in waivers):
            continue
        blockers.append(f"gate not passed: {gate['id']}={evidence.get('observed_status')}")
    if not official:
        blockers.append("bundle is local/diagnostic, not official workflow evidence")
    for subject in sorted(set(policy.get("future_required_bundle_subjects", [])) - available_subjects):
        blockers.append(f"required bundle subject missing: {subject}")
    return ("passed" if not blockers else "blocked", sorted(blockers))


def deterministic_zip(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(item for item in source.rglob("*") if item.is_file()):
            info = zipfile.ZipInfo(path.relative_to(source).as_posix())
            info.date_time = (1980, 1, 1, 0, 0, 0)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def assemble(args: argparse.Namespace) -> int:
    root = repo_root()
    policy_path = (root / args.policy).resolve()
    policy = load_json(policy_path)
    validate_policy(policy)
    evidence_dir = Path(args.evidence_dir).resolve()
    loaded = load_envelopes(evidence_dir)
    envelopes = [payload for _, payload in loaded]
    candidate = envelopes[0]["candidate"]
    workflow = envelopes[0]["workflow"]
    official = bool(envelopes[0]["official"])
    if any(item["candidate"] != candidate or item["workflow"] != workflow for item in envelopes):
        raise EvidenceError("evidence envelopes do not bind one candidate and workflow")
    package_path = Path(args.package).resolve()
    if not package_path.is_file():
        raise EvidenceError(f"package does not exist: {package_path}")
    package_digest = sha256_file(package_path)
    attested_digest = args.package_attestation_sha256 or package_digest
    run_id = safe_component(workflow["run_id"])
    name = f"aether-release-evidence-{candidate['commit_sha']}-{run_id}-{workflow['attempt']}"
    destination_root = Path(args.output_dir).resolve()
    destination_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aether-evidence-") as temporary:
        staging = Path(temporary) / name
        staging.mkdir(parents=True)
        policy_descriptor = copy_into(staging, policy_path, "policy/gate-policy.json")
        integrity_paths: list[Path] = [staging / policy_descriptor["path"]]
        evidence_descriptors = []
        for source, envelope in sorted(loaded, key=lambda item: item[1]["gate_id"]):
            relative = f"evidence/{envelope['gate_id']}.json"
            item = copy_into(staging, source, relative)
            integrity_paths.append(staging / item["path"])
            evidence_descriptors.append(
                {
                    "gate_id": envelope["gate_id"],
                    "evidence_id": envelope["evidence_id"],
                    "path": item["path"],
                    "sha256": item["sha256"],
                    "byte_size": item["byte_size"],
                    "observed_status": envelope["observed_status"],
                }
            )
            source_output = evidence_dir / safe_bundle_relative(envelope["output"]["path"])
            output_item = copy_into(staging, source_output, envelope["output"]["path"])
            integrity_paths.append(staging / output_item["path"])
        package_item = copy_into(staging, package_path, f"package/{package_path.name}")
        integrity_paths.append(staging / package_item["path"])
        waiver_payloads = []
        waiver_items = []
        for waiver_arg in sorted(args.waiver or []):
            source = Path(waiver_arg).resolve()
            waiver = load_json(source)
            waiver_payloads.append(waiver)
            item = copy_into(staging, source, f"waivers/{source.name}")
            waiver_items.append(item)
            integrity_paths.append(staging / item["path"])
        sbom_items = []
        for sbom_arg in sorted(args.sbom or []):
            source = Path(sbom_arg).resolve()
            item = copy_into(staging, source, f"sboms/{source.name}")
            sbom_items.append(item)
            integrity_paths.append(staging / item["path"])
        supplied_subjects: dict[str, Path] = {}
        for subject_arg in args.subject or []:
            if "=" not in subject_arg:
                raise EvidenceError("--subject must use <subject-id>=<path>")
            subject_id, source_text = subject_arg.split("=", 1)
            if subject_id in supplied_subjects:
                raise EvidenceError(f"duplicate bundle subject: {subject_id}")
            supplied_subjects[subject_id] = Path(source_text).resolve()
        # Imported lazily because release_subjects reuses canonical helpers
        # from this module.
        import release_subjects

        subject_items = []
        validated_subjects: set[str] = set()
        for subject_id in sorted(policy.get("future_required_bundle_subjects", [])):
            source = supplied_subjects.get(subject_id)
            if source is None:
                subject_items.append({"id": subject_id, "status": "missing"})
                continue
            release_subjects.verify_envelope(
                load_json(source),
                expected_subject_id=subject_id,
                candidate=candidate,
                package_sha256=package_digest,
                now=utc_now(),
            )
            item = copy_into(staging, source, f"subjects/{subject_id}/{source.name}")
            integrity_paths.append(staging / item["path"])
            subject_items.append({"id": subject_id, "status": "present", "file": item})
            validated_subjects.add(subject_id)
        integrity_payload = {
            "schema_version": "aether.bundle-file-integrity.v1",
            "files": [
                file_descriptor(path, staging, "application/json" if path.suffix == ".json" else "application/octet-stream")
                for path in sorted(integrity_paths)
            ],
        }
        integrity_path = staging / "file-integrity-manifest.json"
        write_canonical_json(integrity_path, integrity_payload)
        integrity_item = file_descriptor(integrity_path, staging, "application/json")
        verdict, blockers = compute_verdict(
            policy,
            envelopes,
            waiver_payloads,
            candidate,
            official,
            available_subjects=validated_subjects,
        )
        manifest: dict[str, Any] = {
            "schema_version": BUNDLE_VERSION,
            "bundle_id": "",
            "official": official,
            "candidate": candidate,
            "policy": policy_descriptor,
            "workflow": workflow,
            "evidence": sorted(evidence_descriptors, key=lambda item: item["gate_id"]),
            "waivers": waiver_items,
            "package": package_item,
            "package_attestation_subject_sha256": attested_digest,
            "file_integrity_manifest": integrity_item,
            "sboms": sbom_items,
            "subjects": subject_items,
            "computed_verdict": verdict,
            "blockers": blockers,
            "verifier": {"version": VERIFIER_VERSION, "algorithm": ALGORITHM},
        }
        manifest["bundle_id"] = identity_digest(manifest, "bundle_id")
        write_canonical_json(staging / "bundle-manifest.json", manifest)
        archive = destination_root / f"{name}.zip"
        deterministic_zip(staging, archive)
    print(archive)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser("capture", help="run policy gates and emit envelopes")
    capture_parser.add_argument("--policy", default="fixtures/release/gate-policy.json")
    capture_parser.add_argument("--output-dir")
    capture_parser.add_argument("--gate", action="append")
    capture_parser.add_argument("--ref")
    capture_parser.add_argument("--workflow-file")
    capture_parser.add_argument("--run-id")
    capture_parser.add_argument("--attempt", type=int)
    capture_parser.add_argument("--job-id")
    capture_parser.add_argument("--runner")
    capture_parser.add_argument("--host")
    capture_parser.add_argument("--repository")
    capture_parser.add_argument("--artifact-name")
    capture_parser.add_argument("--official", action="store_true")
    capture_parser.add_argument("--enforce", action="store_true")
    capture_parser.set_defaults(func=capture)

    assemble_parser = subparsers.add_parser("assemble", help="build a deterministic evidence bundle")
    assemble_parser.add_argument("--policy", default="fixtures/release/gate-policy.json")
    assemble_parser.add_argument("--evidence-dir", required=True)
    assemble_parser.add_argument("--package", required=True)
    assemble_parser.add_argument("--package-attestation-sha256")
    assemble_parser.add_argument("--sbom", action="append")
    assemble_parser.add_argument("--subject", action="append")
    assemble_parser.add_argument("--waiver", action="append")
    assemble_parser.add_argument("--output-dir", required=True)
    assemble_parser.set_defaults(func=assemble)
    return parser


def main() -> int:
    try:
        args = build_parser().parse_args()
        return args.func(args)
    except EvidenceError as exc:
        print(f"release evidence error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
