#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request


PASSING_STATUSES = {"passed", "ci_blocking"}
POSTGRES_CI_REQUIRED_MARKERS = [
    "postgres-journal",
    "scripts/ci-postgres-tls.sh",
    "Postgres transport security matrix",
    "cargo test -p aether_storage --lib",
    "cargo test -p aether_storage --test postgres_tls",
    "cargo test -p aether_api --test http_service http_service_postgres_namespaces",
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


def command_result(
    command: list[str], root: Path, timeout_seconds: int, env: dict[str, str] | None = None
) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=root,
        env=env,
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
        "output_tail": "\n".join(completed.stdout.splitlines()[-80:]),
        "status": "passed" if completed.returncode == 0 else "failed",
    }


def normalize_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def display_path(path: Path, root: Path) -> str:
    try:
        return normalize_path(path.relative_to(root))
    except ValueError:
        return normalize_path(path)


def find_powershell() -> str | None:
    return shutil.which("pwsh") or shutil.which("powershell")


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def http_json(
    method: str,
    url: str,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    timeout_seconds: int = 10,
) -> tuple[int, Any]:
    encoded = None
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        encoded = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=encoded, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else None
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw
        return exc.code, payload


def wait_for_health(base_url: str, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            status, payload = http_json("GET", f"{base_url}/health", timeout_seconds=2)
            if status == 200 and isinstance(payload, dict):
                return
        except Exception as exc:  # noqa: BLE001 - health polling reports the final failure.
            last_error = exc
        time.sleep(0.5)
    if last_error:
        raise RuntimeError(f"service did not become healthy: {last_error}")
    raise RuntimeError("service did not become healthy")


def test_datom(element: int, value: str) -> dict[str, Any]:
    return {
        "entity": 1,
        "attribute": 1,
        "value": {"String": value},
        "op": "Assert",
        "element": element,
        "replica": 1,
        "causal_context": {"frontier": []},
        "provenance": {
            "author_principal": "release-readiness",
            "agent_id": "service-v2-operability",
            "tool_id": "package-proof",
            "session_id": "release-readiness",
            "source_ref": {"uri": "service-v2-package-proof", "digest": None},
            "parent_datom_ids": [],
            "confidence": 1.0,
            "trust_domain": "release",
            "schema_version": "v1",
        },
        "policy": None,
    }


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run_package_backup_restore_drill(
    package_root: Path,
    artifact_dir: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    if os.name != "nt":
        return {
            "status": "unavailable",
            "blockers": ["Current-run package backup/restore drill is Windows-only because the pilot package contains .exe binaries."],
            "next_actions": ["Run release-readiness on the Windows package host."],
        }

    binary = package_root / "bin" / "aether_pilot_service.exe"
    config_path = package_root / "config" / "pilot-service.json"
    token_path = package_root / "config" / "pilot-operator.token"
    backup_script = package_root / "backup-pilot-state.ps1"
    restore_script = package_root / "restore-pilot-state.ps1"
    missing = [
        path
        for path in [binary, config_path, token_path, backup_script, restore_script]
        if not path.exists()
    ]
    if missing:
        return {
            "status": "diagnostic",
            "blockers": [f"packaged path missing: {normalize_path(path)}" for path in missing],
            "next_actions": ["Build the pilot package before running the Service v2 proof collector."],
        }

    source_package_root = package_root
    artifact_dir.mkdir(parents=True, exist_ok=True)
    working_package_root = artifact_dir / "package-under-test"
    if working_package_root.exists():
        shutil.rmtree(working_package_root)
    shutil.copytree(source_package_root, working_package_root)
    package_root = working_package_root
    binary = package_root / "bin" / "aether_pilot_service.exe"
    config_path = package_root / "config" / "pilot-service.json"
    token_path = package_root / "config" / "pilot-operator.token"
    backup_script = package_root / "backup-pilot-state.ps1"
    restore_script = package_root / "restore-pilot-state.ps1"

    powershell = find_powershell()
    if powershell is None:
        return {
            "status": "failed",
            "blockers": ["PowerShell is not available for package backup/restore helper execution."],
            "next_actions": ["Install pwsh or Windows PowerShell on the release host."],
        }

    stdout_path = artifact_dir / "package-service.stdout.txt"
    stderr_path = artifact_dir / "package-service.stderr.txt"
    snapshot_dir = artifact_dir / "snapshot"
    transcript: list[str] = []
    port = free_tcp_port()
    base_url = f"http://127.0.0.1:{port}"

    config = load_json(config_path)
    config["bind_addr"] = f"127.0.0.1:{port}"
    write_json(config_path, config)
    token = token_path.read_text(encoding="utf-8").strip()
    for relative_dir in ("data", "logs"):
        cleanup_dir = package_root / relative_dir
        cleanup_dir.mkdir(parents=True, exist_ok=True)
        for child in cleanup_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)

    service: subprocess.Popen[str] | None = None

    def start_service() -> subprocess.Popen[str]:
        stdout = stdout_path.open("a", encoding="utf-8", newline="\n")
        stderr = stderr_path.open("a", encoding="utf-8", newline="\n")
        try:
            proc = subprocess.Popen(
                [str(binary), "--config", str(config_path)],
                cwd=package_root,
                stdout=stdout,
                stderr=stderr,
                text=True,
            )
        finally:
            stdout.close()
            stderr.close()
        wait_for_health(base_url, timeout_seconds=min(timeout_seconds, 90))
        return proc

    try:
        service = start_service()
        status, _ = http_json(
            "POST",
            f"{base_url}/v1/append",
            token=token,
            body={"datoms": [test_datom(1, "service-v2-package-alpha")]},
        )
        if status != 200:
            raise RuntimeError(f"first append returned HTTP {status}")
        status, before_snapshot = http_json("GET", f"{base_url}/v1/history", token=token)
        if status != 200 or len(before_snapshot.get("datoms", [])) != 1:
            raise RuntimeError(f"expected one datom before snapshot, got {before_snapshot}")
        stop_process(service)
        service = None

        backup = command_result(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(backup_script),
                "-SnapshotDir",
                str(snapshot_dir),
                "-ConfirmServiceStopped",
            ],
            root=package_root,
            timeout_seconds=timeout_seconds,
        )
        transcript.append("backup:\n" + backup["output_tail"])
        if backup["status"] != "passed":
            raise RuntimeError("backup helper failed")
        manifest_path = snapshot_dir / "manifest.json"
        if not manifest_path.exists():
            raise RuntimeError("backup did not write manifest.json")

        service = start_service()
        status, _ = http_json(
            "POST",
            f"{base_url}/v1/append",
            token=token,
            body={"datoms": [test_datom(2, "service-v2-package-beta")]},
        )
        if status != 200:
            raise RuntimeError(f"second append returned HTTP {status}")
        status, before_restore = http_json("GET", f"{base_url}/v1/history", token=token)
        if status != 200 or len(before_restore.get("datoms", [])) != 2:
            raise RuntimeError(f"expected two datoms before restore, got {before_restore}")
        stop_process(service)
        service = None

        restore = command_result(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(restore_script),
                "-SnapshotDir",
                str(snapshot_dir),
                "-ConfirmServiceStopped",
            ],
            root=package_root,
            timeout_seconds=timeout_seconds,
        )
        transcript.append("restore:\n" + restore["output_tail"])
        if restore["status"] != "passed":
            raise RuntimeError("restore helper failed")

        service = start_service()
        status, after_restore = http_json("GET", f"{base_url}/v1/history", token=token)
        datoms = after_restore.get("datoms", []) if isinstance(after_restore, dict) else []
        if status != 200 or len(datoms) != 1:
            raise RuntimeError(f"expected one restored datom, got {after_restore}")
        restored_value = datoms[0].get("value")
        if restored_value != {"String": "service-v2-package-alpha"}:
            raise RuntimeError(f"unexpected restored value: {restored_value}")

        return {
            "status": "passed",
            "duration_seconds": round(time.perf_counter() - started, 3),
            "artifact_dir": normalize_path(artifact_dir),
            "source_package_root": normalize_path(source_package_root),
            "package_under_test": normalize_path(working_package_root),
            "snapshot_manifest": normalize_path(manifest_path),
            "blockers": [],
            "next_actions": [],
            "output_tail": "\n".join(transcript),
        }
    except Exception as exc:  # noqa: BLE001 - converted into a gate payload.
        return {
            "status": "failed",
            "duration_seconds": round(time.perf_counter() - started, 3),
            "artifact_dir": normalize_path(artifact_dir),
            "source_package_root": normalize_path(source_package_root),
            "package_under_test": normalize_path(working_package_root),
            "blockers": [str(exc)],
            "next_actions": ["Fix the packaged service backup/restore drill before commercial beta."],
            "output_tail": "\n".join(transcript),
        }
    finally:
        if service is not None:
            stop_process(service)


def file_contains_all(path: Path, needles: list[str]) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, [f"missing file: {path}"]
    text = path.read_text(encoding="utf-8")
    missing = [needle for needle in needles if needle not in text]
    return not missing, missing


def pack_status_from_results(results: list[dict[str, Any]], group_name: str) -> str:
    matches = [item for item in results if item.get("persona") == group_name]
    if not matches:
        return "skipped"
    statuses = {str(item.get("status", "missing")) for item in matches}
    if "failed" in statuses:
        return "failed"
    if "passed" in statuses:
        return "passed"
    if "skipped" in statuses:
        return "skipped"
    return "missing"


def hardening_latest_status(root: Path, hardening_json: Path | None) -> dict[str, str]:
    if hardening_json is None or not hardening_json.exists():
        return {"admin": "missing", "operator": "missing"}
    payload = load_json(hardening_json)
    results = payload.get("results", [])
    return {
        "admin": pack_status_from_results(results, "admin"),
        "operator": pack_status_from_results(results, "operator"),
    }


def promotion_blocking_status(config_path: Path) -> dict[str, bool]:
    payload = load_json(config_path)
    groups = payload.get("groups", {})
    return {
        "admin": bool(groups.get("admin", {}).get("blocking")),
        "operator": bool(groups.get("operator", {}).get("blocking")),
    }


def collect_service_v2_evidence(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    generated_at = args.generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    hardening_path = Path(args.hardening_json) if args.hardening_json else None
    if hardening_path and not hardening_path.is_absolute():
        hardening_path = root / hardening_path
    package_root = Path(args.package_root) if args.package_root else None
    if package_root and not package_root.is_absolute():
        package_root = root / package_root
    artifact_dir = Path(args.artifact_dir) if args.artifact_dir else root / "artifacts" / "qa" / "release-readiness" / "service-v2-package-proof"
    if not artifact_dir.is_absolute():
        artifact_dir = root / artifact_dir

    gates: list[dict[str, Any]] = []

    cargo = shutil.which("cargo")
    if cargo is None:
        gates.append(
            {
                "id": "sqlite_namespace_restart_replay",
                "title": "SQLite namespace restart/replay drill",
                "status": "failed",
                "required_for_commercial_beta": True,
                "evidence": ["cargo is not on PATH"],
                "blockers": ["Cannot run the SQLite restart/replay drill without cargo."],
                "next_actions": ["Install the Rust toolchain before release-readiness."],
            }
        )
    else:
        sqlite_command = [
            cargo,
            "test",
            "-p",
            "aether_api",
            "--test",
            "http_service",
            "http_service_sqlite_namespaces_preserve_history_across_restart",
            "--",
            "--exact",
            "--nocapture",
        ]
        sqlite = command_result(sqlite_command, root, args.timeout_seconds)
        gates.append(
            {
                "id": "sqlite_namespace_restart_replay",
                "title": "SQLite namespace restart/replay drill",
                "status": sqlite["status"],
                "required_for_commercial_beta": True,
                "command": sqlite["command"],
                "duration_seconds": sqlite["duration_seconds"],
                "exit_code": sqlite["exit_code"],
                "evidence": [
                    "crates/aether_api/tests/http_service.rs",
                    "http_service_sqlite_namespaces_preserve_history_across_restart",
                ],
                "output_tail": sqlite["output_tail"],
                "blockers": []
                if sqlite["status"] == "passed"
                else ["SQLite namespace restart/replay drill failed."],
                "next_actions": []
                if sqlite["status"] == "passed"
                else ["Fix the failing HTTP namespace restart/replay test."],
            }
        )

    postgres_url = os.environ.get(args.postgres_env)
    if postgres_url and cargo is not None:
        env = os.environ.copy()
        env[args.postgres_env] = postgres_url
        storage_command = [
            cargo,
            "test",
            "-p",
            "aether_storage",
            "postgres_journal",
            "--",
            "--nocapture",
        ]
        http_command = [
            cargo,
            "test",
            "-p",
            "aether_api",
            "--test",
            "http_service",
            "http_service_postgres_namespaces_preserve_history_across_restart_when_configured",
            "--",
            "--exact",
            "--nocapture",
        ]
        storage = command_result(storage_command, root, args.timeout_seconds, env=env)
        http = command_result(http_command, root, args.timeout_seconds, env=env)
        status = "passed" if storage["status"] == "passed" and http["status"] == "passed" else "failed"
        gates.append(
            {
                "id": "postgres_journal_restart_replay",
                "title": "Postgres journal restart/replay drill",
                "status": status,
                "required_for_commercial_beta": True,
                "commands": [storage["command"], http["command"]],
                "duration_seconds": round(storage["duration_seconds"] + http["duration_seconds"], 3),
                "exit_codes": [storage["exit_code"], http["exit_code"]],
                "evidence": [
                    "crates/aether_storage/src/lib.rs",
                    "crates/aether_api/tests/http_service.rs",
                    args.postgres_env,
                ],
                "output_tail": "\n".join([storage["output_tail"], http["output_tail"]]).strip(),
                "blockers": []
                if status == "passed"
                else ["Postgres storage or HTTP namespace restart/replay drill failed."],
                "next_actions": []
                if status == "passed"
                else ["Fix the failing Postgres journal or HTTP namespace drill."],
            }
        )
    else:
        postgres_ci_ok, postgres_ci_missing = file_contains_all(
            root / ".github" / "workflows" / "ci.yml",
            POSTGRES_CI_REQUIRED_MARKERS,
        )
        if args.accept_ci_postgres and postgres_ci_ok:
            status = "ci_blocking"
            blockers: list[str] = []
            next_actions = [
                "Keep the blocking CI Postgres job green and use a live AETHER_POSTGRES_TEST_URL run for final beta-candidate signoff."
            ]
        else:
            status = "unavailable"
            blockers = [
                f"{args.postgres_env} is not set for this run, so live Postgres restart/replay evidence was not captured locally."
            ]
            if args.accept_ci_postgres and not postgres_ci_ok:
                blockers.extend(
                    f"CI Postgres proof is missing required coverage marker: {item}"
                    for item in postgres_ci_missing
                )
            next_actions = [
                "Run release-readiness in an environment with AETHER_POSTGRES_TEST_URL or rely on the blocking CI Postgres job until local beta evidence is required."
            ]
        gates.append(
            {
                "id": "postgres_journal_restart_replay",
                "title": "Postgres journal restart/replay drill",
                "status": status,
                "required_for_commercial_beta": True,
                "evidence": [
                    "crates/aether_storage/src/lib.rs",
                    "crates/aether_api/tests/http_service.rs",
                    ".github/workflows/ci.yml",
                ],
                "blockers": blockers,
                "next_actions": next_actions,
            }
        )

    ci_path = root / ".github" / "workflows" / "ci.yml"
    ci_ok, ci_missing = file_contains_all(
        ci_path,
        [
            "container-smoke",
            "Boot image and verify authenticated status",
            "docker stop",
            "docker start",
            "X-Aether-Namespace",
            "/v1/status",
            "/v1/history",
            "test \"$forbidden_status\" = \"403\"",
        ],
    )
    gates.append(
        {
            "id": "container_boot_auth_status_restart",
            "title": "Container boot/auth/status/restart smoke",
            "status": "ci_blocking" if ci_ok else "failed",
            "required_for_commercial_beta": True,
            "evidence": [".github/workflows/ci.yml"],
            "blockers": []
            if ci_ok
            else [f"CI container smoke is missing required coverage marker: {item}" for item in ci_missing],
            "next_actions": [
                "Keep this CI job blocking and publish its artifact/status alongside beta release evidence."
            ]
            if ci_ok
            else ["Restore the container boot/auth/status/restart coverage in CI."],
        }
    )

    if package_root:
        package_drill = run_package_backup_restore_drill(
            package_root=package_root,
            artifact_dir=artifact_dir,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        package_drill = {
            "status": "diagnostic",
            "blockers": ["No --package-root was supplied, so current-run package backup/restore evidence was not captured."],
            "next_actions": ["Run Service v2 proof after the pilot package build and pass --package-root."],
        }
    gates.append(
        {
            "id": "package_backup_restore_restart",
            "title": "Package backup/restore through restart",
            "status": package_drill["status"],
            "required_for_commercial_beta": True,
            "evidence": [
                "scripts/build-pilot-package.ps1",
                display_path(package_root, root) if package_root and package_root.exists() else "no package root supplied",
                package_drill.get("package_under_test", "package proof copy unavailable"),
                package_drill.get("snapshot_manifest", "snapshot manifest unavailable"),
            ],
            "duration_seconds": package_drill.get("duration_seconds"),
            "output_tail": package_drill.get("output_tail", ""),
            "blockers": package_drill.get("blockers", []),
            "next_actions": package_drill.get("next_actions", []),
        }
    )

    promotion = promotion_blocking_status(root / ".github" / "hardening-promotion-state.json")
    promotion_status = "passed" if promotion["admin"] and promotion["operator"] else "blocked"
    gates.append(
        {
            "id": "admin_operator_hardening_promoted",
            "title": "Admin and operator hardening packs are blocking",
            "status": promotion_status,
            "required_for_commercial_beta": True,
            "evidence": [".github/hardening-promotion-state.json", "docs/QA_HARDENING_PROGRAM.md"],
            "blockers": []
            if promotion_status == "passed"
            else [
                "admin hardening is not blocking" if not promotion["admin"] else "",
                "operator hardening is not blocking" if not promotion["operator"] else "",
            ],
            "next_actions": []
            if promotion_status == "passed"
            else [
                "Wait for the configured scheduled green streaks, then promote admin first and operator second."
            ],
        }
    )

    for gate in gates:
        gate["blockers"] = [item for item in gate.get("blockers", []) if item]

    beta_required = [gate for gate in gates if gate.get("required_for_commercial_beta")]
    beta_ready = all(gate.get("status") in PASSING_STATUSES for gate in beta_required)
    return {
        "generated_at": generated_at,
        "target": "commercial_beta",
        "beta_ready": beta_ready,
        "postgres_env": args.postgres_env,
        "hardening_json": str(hardening_path) if hardening_path else None,
        "package_root": normalize_path(package_root) if package_root else None,
        "artifact_dir": normalize_path(artifact_dir),
        "gates": gates,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# AETHER Service v2 Operability Proof",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Target: `{payload['target']}`",
        f"- Beta ready: `{payload['beta_ready']}`",
        f"- Postgres env: `{payload['postgres_env']}`",
        f"- Package root: `{payload.get('package_root') or 'none'}`",
        f"- Artifact dir: `{payload.get('artifact_dir') or 'none'}`",
        "",
        "| Gate | Status | Required for beta | Evidence | Blockers | Next actions |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        evidence = "<br>".join(f"`{item}`" for item in gate.get("evidence", []))
        blockers = "<br>".join(gate.get("blockers", [])) or "none"
        next_actions = "<br>".join(gate.get("next_actions", [])) or "none"
        lines.append(
            f"| `{gate['title']}` | `{gate['status']}` | "
            f"`{gate['required_for_commercial_beta']}` | {evidence} | {blockers} | {next_actions} |"
        )

    lines.extend(["", "## Command Output Tails", ""])
    for gate in payload["gates"]:
        output = gate.get("output_tail")
        if not output:
            continue
        lines.extend(
            [
                f"### {gate['title']}",
                "",
                "```text",
                output,
                "```",
                "",
            ]
        )

    lines.extend(
        [
            "## Interpretation",
            "",
            "- `passed` means this release-readiness run captured direct local evidence.",
            "- `ci_blocking` means the repository carries a blocking CI job for the evidence, but this local run did not rerun that external service path.",
            "- `diagnostic` and `unavailable` keep beta blockers visible without weakening design-partner alpha.",
            "",
        ]
    )
    return "\n".join(lines)


def cmd_run(args: argparse.Namespace) -> int:
    payload = collect_service_v2_evidence(args)
    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), render_markdown(payload))
    failed = [gate for gate in payload["gates"] if gate["status"] == "failed"]
    if failed:
        for gate in failed:
            print(f"service v2 gate failed: {gate['id']}", file=sys.stderr)
        return 2
    if args.enforce_beta and not payload["beta_ready"]:
        print("Service v2 commercial beta proof is not ready", file=sys.stderr)
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect AETHER Service v2 operability evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--out-json", required=True)
    run.add_argument("--out-md", required=True)
    run.add_argument("--hardening-json")
    run.add_argument("--package-root")
    run.add_argument("--artifact-dir")
    run.add_argument("--postgres-env", default="AETHER_POSTGRES_TEST_URL")
    run.add_argument("--accept-ci-postgres", action="store_true")
    run.add_argument("--timeout-seconds", type=int, default=120)
    run.add_argument("--generated-at")
    run.add_argument("--enforce-beta", action="store_true")
    run.set_defaults(func=cmd_run)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
