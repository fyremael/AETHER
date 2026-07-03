#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PACKAGE_REQUIRED_FILES = [
    Path("bin/aether_pilot_service.exe"),
    Path("bin/aetherctl.exe"),
    Path("config/pilot-service.json"),
    Path("config/pilot-operator.token"),
    Path("rotate-pilot-token.ps1"),
    Path("backup-pilot-state.ps1"),
    Path("restore-pilot-state.ps1"),
    Path("docs/PILOT_OPERATIONS_PLAYBOOK.md"),
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def normalize_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def display_path(path: Path, root: Path) -> str:
    try:
        return normalize_path(path.relative_to(root))
    except ValueError:
        return normalize_path(path)


def find_powershell() -> str | None:
    return shutil.which("pwsh") or shutil.which("powershell")


def run_command(
    command: list[str],
    root: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_seconds,
        check=False,
    )
    elapsed = time.perf_counter() - started
    return {
        "command": command,
        "exit_code": completed.returncode,
        "duration_seconds": round(elapsed, 3),
        "output_tail": "\n".join(completed.stdout.splitlines()[-120:]),
    }


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_sbom(package_root: Path, artifact_dir: Path, root: Path) -> dict[str, Any]:
    files = []
    for path in sorted(package_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(package_root)
        files.append(
            {
                "path": normalize_path(relative),
                "bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "package_root": display_path(package_root, root),
        "files": files,
    }
    sbom_path = artifact_dir / "pilot-package-sbom.json"
    checksums_path = artifact_dir / "pilot-package-sha256.txt"
    write_json(sbom_path, payload)
    write_text(
        checksums_path,
        "".join(f"{item['sha256']}  {item['path']}\n" for item in files),
    )
    return {
        "sbom_path": display_path(sbom_path, root),
        "checksums_path": display_path(checksums_path, root),
        "file_count": len(files),
    }


def copy_package(package_root: Path, artifact_dir: Path) -> Path:
    target = artifact_dir / "security-package-under-test"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(package_root, target)
    return target


def rotate_package_token(package_root: Path, timeout_seconds: int) -> dict[str, Any]:
    powershell = find_powershell()
    if powershell is None:
        return {
            "status": "blocked",
            "blockers": ["PowerShell is required to run the packaged token rotation helper."],
        }
    token_path = package_root / "config" / "pilot-operator.token"
    rotate_script = package_root / "rotate-pilot-token.ps1"
    if not token_path.exists() or not rotate_script.exists():
        return {
            "status": "blocked",
            "blockers": ["Package token file or rotate-pilot-token.ps1 is missing."],
        }
    before = token_path.read_text(encoding="utf-8").strip()
    command = [
        powershell,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(rotate_script),
        "-TokenPath",
        str(token_path),
    ]
    result = run_command(command, package_root, timeout_seconds)
    after = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""
    backups = list(token_path.parent.glob("pilot-operator.token.*.bak"))
    passed = result["exit_code"] == 0 and before and after and before != after and len(after) >= 48 and bool(backups)
    return {
        "status": "passed" if passed else "blocked",
        "command": command,
        "duration_seconds": result["duration_seconds"],
        "output_tail": result["output_tail"],
        "backup_count": len(backups),
        "blockers": []
        if passed
        else [
            "Token rotation did not change the token, produce a backup, and leave a sufficiently long secret."
        ],
    }


def file_contains_all(path: Path, needles: list[str]) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, [f"missing file: {normalize_path(path)}"]
    text = path.read_text(encoding="utf-8", errors="replace")
    missing = [needle for needle in needles if needle not in text]
    return not missing, missing


def build_gate(
    gate_id: str,
    title: str,
    passed: bool,
    evidence: list[str],
    blockers: list[str] | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "id": gate_id,
        "title": title,
        "status": "passed" if passed else "blocked",
        "evidence": evidence,
        "blockers": [] if passed else blockers or [f"{title} is incomplete."],
    }
    if details:
        payload.update(details)
    return payload


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    generated_at = args.generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    package_root = Path(args.package_root)
    artifact_dir = Path(args.artifact_dir)
    if not package_root.is_absolute():
        package_root = root / package_root
    if not artifact_dir.is_absolute():
        artifact_dir = root / artifact_dir
    artifact_dir.mkdir(parents=True, exist_ok=True)

    gates: list[dict[str, Any]] = []
    missing_package_files = [
        normalize_path(path)
        for path in PACKAGE_REQUIRED_FILES
        if not (package_root / path).exists()
    ]
    gates.append(
        build_gate(
            "package_security_surface_present",
            "Package security lifecycle files are present",
            package_root.exists() and not missing_package_files,
            [display_path(package_root / path, root) for path in PACKAGE_REQUIRED_FILES],
            blockers=[f"missing package file: {path}" for path in missing_package_files],
        )
    )

    package_copy = copy_package(package_root, artifact_dir) if package_root.exists() else package_root
    rotation = rotate_package_token(package_copy, args.timeout_seconds)
    gates.append(
        build_gate(
            "package_token_rotation",
            "Package-local token rotation helper changes and backs up the token",
            rotation["status"] == "passed",
            [display_path(package_copy / "rotate-pilot-token.ps1", root)],
            blockers=rotation.get("blockers", []),
            details={
                "duration_seconds": rotation.get("duration_seconds"),
                "backup_count": rotation.get("backup_count", 0),
                "output_tail": rotation.get("output_tail", ""),
            },
        )
    )

    cargo = shutil.which("cargo")
    if cargo is None:
        command_gate = build_gate(
            "token_command_tests",
            "Token command and auth reload tests pass",
            False,
            ["cargo is not on PATH"],
            blockers=["cargo is required for token command and auth reload regression tests."],
        )
    else:
        token_command = run_command(
            [cargo, "test", "-p", "aether_api", "token_command", "--", "--nocapture"],
            root,
            args.timeout_seconds,
        )
        auth_reload = run_command(
            [
                cargo,
                "test",
                "-p",
                "aether_api",
                "--test",
                "http_service",
                "http_service_exposes_status_and_supports_auth_reload",
                "--",
                "--exact",
                "--nocapture",
            ],
            root,
            args.timeout_seconds,
        )
        passed = token_command["exit_code"] == 0 and auth_reload["exit_code"] == 0
        command_gate = build_gate(
            "token_command_tests",
            "Token command and auth reload tests pass",
            passed,
            ["crates/aether_api/src/deployment.rs", "crates/aether_api/tests/http_service.rs"],
            blockers=[] if passed else ["Token command or auth reload regression test failed."],
            details={
                "commands": [token_command["command"], auth_reload["command"]],
                "duration_seconds": round(
                    token_command["duration_seconds"] + auth_reload["duration_seconds"], 3
                ),
                "output_tail": "\n".join([token_command["output_tail"], auth_reload["output_tail"]]).strip(),
            },
        )
    gates.append(command_gate)

    playbook_ok, playbook_missing = file_contains_all(
        root / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md",
        ["External Secret-Manager Playbook", "token_command", "cloud secret-manager CLIs", "restart is the reload boundary"],
    )
    deployment_ok, deployment_missing = file_contains_all(
        root / "docs" / "PILOT_DEPLOYMENT.md",
        ["token_file", "token_command", "secret-manager", "revoked token"],
    )
    gates.append(
        build_gate(
            "secret_manager_contract_documented",
            "External secret-manager token command contract is documented",
            playbook_ok and deployment_ok,
            ["docs/PILOT_OPERATIONS_PLAYBOOK.md", "docs/PILOT_DEPLOYMENT.md"],
            blockers=[f"playbook missing marker: {item}" for item in playbook_missing]
            + [f"deployment guide missing marker: {item}" for item in deployment_missing],
        )
    )

    sbom = build_sbom(package_root, artifact_dir, root) if package_root.exists() else {"file_count": 0}
    gates.append(
        build_gate(
            "package_sbom_and_checksums",
            "Package SBOM and checksum manifest are generated",
            sbom.get("file_count", 0) >= len(PACKAGE_REQUIRED_FILES),
            [sbom.get("sbom_path", "sbom unavailable"), sbom.get("checksums_path", "checksums unavailable")],
            blockers=["Package SBOM/checksum manifest did not include the expected file set."],
            details={"file_count": sbom.get("file_count", 0)},
        )
    )

    beta_ready = all(gate["status"] == "passed" for gate in gates)
    return {
        "generated_at": generated_at,
        "package_root": display_path(package_root, root),
        "artifact_dir": display_path(artifact_dir, root),
        "beta_ready": beta_ready,
        "gates": gates,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# AETHER Security And Key Lifecycle Gate",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Package root: `{payload['package_root']}`",
        f"- Artifact dir: `{payload['artifact_dir']}`",
        f"- Beta ready: `{payload['beta_ready']}`",
        "",
        "| Gate | Status | Evidence | Blockers |",
        "| --- | --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        evidence = "<br>".join(f"`{item}`" for item in gate.get("evidence", []))
        blockers = "<br>".join(gate.get("blockers", [])) or "none"
        lines.append(f"| `{gate['title']}` | `{gate['status']}` | {evidence} | {blockers} |")
    lines.extend(["", "## Command Output Tails", ""])
    for gate in payload["gates"]:
        output = gate.get("output_tail")
        if not output:
            continue
        lines.extend([f"### {gate['title']}", "", "```text", output, "```", ""])
    return "\n".join(lines)


def cmd_run(args: argparse.Namespace) -> int:
    payload = build_report(args)
    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), render_markdown(payload))
    if args.enforce and not payload["beta_ready"]:
        print("security and key lifecycle gate is not ready", file=sys.stderr)
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate AETHER security and key lifecycle beta gate")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--package-root", required=True)
    run.add_argument("--artifact-dir", required=True)
    run.add_argument("--out-json", required=True)
    run.add_argument("--out-md", required=True)
    run.add_argument("--timeout-seconds", type=int, default=120)
    run.add_argument("--generated-at")
    run.add_argument("--enforce", action="store_true")
    run.set_defaults(func=cmd_run)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
