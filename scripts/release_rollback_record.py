#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PASSING_SERVICE_V2_STATUSES = {"passed", "ci_blocking"}


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


def git_commit(root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:  # noqa: BLE001 - absence of git should not hide the record.
        return "<git unavailable>"


def file_contains_all(path: Path, needles: list[str]) -> tuple[bool, list[str]]:
    if not path.exists():
        return False, [f"missing file: {normalize_path(path)}"]
    text = path.read_text(encoding="utf-8", errors="replace")
    missing = [needle for needle in needles if needle not in text]
    return not missing, missing


def service_v2_gate_status(payload: dict[str, Any], gate_id: str) -> str:
    for gate in payload.get("gates", []):
        if gate.get("id") == gate_id:
            return str(gate.get("status", "missing"))
    return "missing"


def build_gate(
    gate_id: str,
    title: str,
    passed: bool,
    evidence: list[str],
    blockers: list[str] | None = None,
    next_actions: list[str] | None = None,
) -> dict[str, Any]:
    if passed:
        normalized_blockers: list[str] = []
        normalized_next_actions: list[str] = []
    else:
        normalized_blockers = blockers or [f"{title} is incomplete."]
        normalized_next_actions = next_actions or [f"Complete {title.lower()} before commercial beta."]
    return {
        "id": gate_id,
        "title": title,
        "status": "passed" if passed else "blocked",
        "evidence": evidence,
        "blockers": normalized_blockers,
        "next_actions": normalized_next_actions,
    }


def build_record(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    generated_at = args.generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    service_v2_path = Path(args.service_v2_json)
    package_root = Path(args.package_root)
    package_zip = Path(args.package_zip)
    if not service_v2_path.is_absolute():
        service_v2_path = root / service_v2_path
    if not package_root.is_absolute():
        package_root = root / package_root
    if not package_zip.is_absolute():
        package_zip = root / package_zip

    service_v2 = load_json(service_v2_path) if service_v2_path.exists() else {"gates": []}
    commit = git_commit(root)
    timestamp_id = generated_at.replace("+00:00", "Z").replace(":", "")
    candidate_id = f"{commit[:12]}-{timestamp_id}"

    gates: list[dict[str, Any]] = []

    package_files = [
        package_root / "bin" / "aether_pilot_service.exe",
        package_root / "bin" / "aetherctl.exe",
        package_root / "config" / "pilot-service.json",
        package_root / "backup-pilot-state.ps1",
        package_root / "restore-pilot-state.ps1",
        package_root / "rotate-pilot-token.ps1",
        package_root / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md",
    ]
    missing_package_files = [path for path in package_files if not path.exists()]
    gates.append(
        build_gate(
            "package_artifacts_present",
            "Package artifacts and recovery helpers are present",
            package_root.exists() and package_zip.exists() and not missing_package_files,
            [
                display_path(package_root, root) if package_root.exists() else normalize_path(package_root),
                display_path(package_zip, root) if package_zip.exists() else normalize_path(package_zip),
            ],
            blockers=[f"missing package path: {normalize_path(path)}" for path in missing_package_files]
            + ([] if package_zip.exists() else [f"missing package zip: {normalize_path(package_zip)}"]),
            next_actions=["Build the pilot package before generating the rollback record."],
        )
    )

    package_restore_status = service_v2_gate_status(service_v2, "package_backup_restore_restart")
    gates.append(
        build_gate(
            "package_backup_restore_verified",
            "Package backup/restore proof passed",
            package_restore_status == "passed",
            [
                display_path(service_v2_path, root) if service_v2_path.exists() else normalize_path(service_v2_path),
                "package_backup_restore_restart",
            ],
            blockers=[f"Service v2 package backup/restore status is {package_restore_status}."],
            next_actions=["Run Service v2 operability proof after package build."],
        )
    )

    sqlite_status = service_v2_gate_status(service_v2, "sqlite_namespace_restart_replay")
    postgres_status = service_v2_gate_status(service_v2, "postgres_journal_restart_replay")
    gates.append(
        build_gate(
            "restart_replay_verified",
            "SQLite and Postgres restart/replay evidence exists",
            sqlite_status == "passed" and postgres_status in PASSING_SERVICE_V2_STATUSES,
            [
                display_path(service_v2_path, root) if service_v2_path.exists() else normalize_path(service_v2_path),
                "sqlite_namespace_restart_replay",
                "postgres_journal_restart_replay",
            ],
            blockers=[
                f"SQLite restart/replay status is {sqlite_status}.",
                f"Postgres restart/replay status is {postgres_status}.",
            ],
            next_actions=["Restore Service v2 restart/replay evidence before beta signoff."],
        )
    )

    playbook_path = root / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md"
    deployment_path = root / "docs" / "PILOT_DEPLOYMENT.md"
    playbook_ok, playbook_missing = file_contains_all(
        playbook_path,
        ["in-place upgrade", "rollback", "Restore the database snapshot", "Re-run the launch pack"],
    )
    package_playbook_path = package_root / "docs" / "PILOT_OPERATIONS_PLAYBOOK.md"
    package_playbook_ok, package_playbook_missing = file_contains_all(
        package_playbook_path,
        ["in-place upgrade", "rollback", "Restore the database snapshot", "Re-run the launch pack"],
    )
    gates.append(
        build_gate(
            "upgrade_rollback_playbook_packaged",
            "Upgrade and rollback playbook is packaged",
            playbook_ok and package_playbook_ok,
            [display_path(playbook_path, root), display_path(package_playbook_path, root)],
            blockers=[
                f"repo playbook missing marker: {item}" for item in playbook_missing
            ]
            + [f"packaged playbook missing marker: {item}" for item in package_playbook_missing],
            next_actions=["Update and repackage the pilot operations playbook."],
        )
    )

    postgres_doc_ok, postgres_doc_missing = file_contains_all(
        deployment_path,
        ["For Postgres deployments", "export and restore", "normal Postgres tooling", "journal schema"],
    )
    gates.append(
        build_gate(
            "postgres_export_restore_boundary_documented",
            "Postgres export/restore boundary is documented",
            postgres_doc_ok,
            [display_path(deployment_path, root)],
            blockers=[f"deployment doc missing marker: {item}" for item in postgres_doc_missing],
            next_actions=["Document the Postgres journal export/restore boundary before beta."],
        )
    )

    gates.append(
        build_gate(
            "versioned_rollback_record_written",
            "Versioned rollback record is generated",
            True,
            ["this record"],
        )
    )

    rollback_ready = all(gate["status"] == "passed" for gate in gates)
    return {
        "generated_at": generated_at,
        "candidate_id": candidate_id,
        "commit": commit,
        "rollback_ready": rollback_ready,
        "package_root": normalize_path(package_root),
        "package_zip": normalize_path(package_zip),
        "service_v2_json": normalize_path(service_v2_path),
        "rollback_policy": {
            "primary": "Rollback is binary/config first. Restore data only when the attempted upgrade performed a destructive migration.",
            "sqlite": "Use the packaged backup/restore helpers for package-local SQLite pilot state.",
            "postgres": "Use normal Postgres operator tooling to export and restore the configured journal schema; AETHER does not treat SQL-derived state as authority.",
        },
        "gates": gates,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# AETHER Release Rollback Record",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Candidate: `{payload['candidate_id']}`",
        f"- Commit: `{payload['commit']}`",
        f"- Rollback ready: `{payload['rollback_ready']}`",
        f"- Package root: `{payload['package_root']}`",
        f"- Package zip: `{payload['package_zip']}`",
        f"- Service v2 proof: `{payload['service_v2_json']}`",
        "",
        "## Rollback Policy",
        "",
        f"- Primary: {payload['rollback_policy']['primary']}",
        f"- SQLite: {payload['rollback_policy']['sqlite']}",
        f"- Postgres: {payload['rollback_policy']['postgres']}",
        "",
        "## Gates",
        "",
        "| Gate | Status | Evidence | Blockers | Next actions |",
        "| --- | --- | --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        evidence = "<br>".join(f"`{item}`" for item in gate["evidence"])
        blockers = "<br>".join(gate["blockers"]) or "none"
        next_actions = "<br>".join(gate["next_actions"]) or "none"
        lines.append(
            f"| `{gate['title']}` | `{gate['status']}` | {evidence} | {blockers} | {next_actions} |"
        )
    lines.append("")
    return "\n".join(lines)


def cmd_render(args: argparse.Namespace) -> int:
    payload = build_record(args)
    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), render_markdown(payload))
    if args.enforce and not payload["rollback_ready"]:
        print("release rollback record is not ready", flush=True)
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render AETHER release rollback record")
    subparsers = parser.add_subparsers(dest="command", required=True)

    render = subparsers.add_parser("render")
    render.add_argument("--service-v2-json", required=True)
    render.add_argument("--package-root", required=True)
    render.add_argument("--package-zip", required=True)
    render.add_argument("--out-json", required=True)
    render.add_argument("--out-md", required=True)
    render.add_argument("--generated-at")
    render.add_argument("--enforce", action="store_true")
    render.set_defaults(func=cmd_render)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
