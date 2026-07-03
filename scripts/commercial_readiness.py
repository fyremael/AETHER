#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_STATUSES = {"ready", "accepted_risk", "diagnostic", "observational", "blocked", "missing"}
ALLOWED_GATE_CLASSES = {"blocking", "diagnostic", "observational", "future"}
READY_STATUSES = {"ready", "accepted_risk"}


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


def validate_ledger(payload: dict[str, Any], root: Path) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != 1:
        errors.append("schema_version must be 1")

    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages:
        errors.append("stages must be a non-empty list")
        return errors

    stage_ids: set[str] = set()
    for stage_index, stage in enumerate(stages):
        stage_prefix = f"stages[{stage_index}]"
        stage_id = stage.get("id")
        if not stage_id:
            errors.append(f"{stage_prefix}.id is required")
            continue
        if stage_id in stage_ids:
            errors.append(f"duplicate stage id: {stage_id}")
        stage_ids.add(stage_id)

        for field in ("label", "intent", "claim_boundary"):
            if not stage.get(field):
                errors.append(f"{stage_id}.{field} is required")

        gates = stage.get("gates")
        if not isinstance(gates, list) or not gates:
            errors.append(f"{stage_id}.gates must be a non-empty list")
            continue

        gate_ids: set[str] = set()
        for gate_index, gate in enumerate(gates):
            gate_prefix = f"{stage_id}.gates[{gate_index}]"
            gate_id = gate.get("id")
            if not gate_id:
                errors.append(f"{gate_prefix}.id is required")
                continue
            if gate_id in gate_ids:
                errors.append(f"duplicate gate id in {stage_id}: {gate_id}")
            gate_ids.add(gate_id)

            for field in ("title", "owner"):
                if not gate.get(field):
                    errors.append(f"{stage_id}.{gate_id}.{field} is required")

            gate_class = gate.get("gate_class")
            if gate_class not in ALLOWED_GATE_CLASSES:
                errors.append(
                    f"{stage_id}.{gate_id}.gate_class must be one of {sorted(ALLOWED_GATE_CLASSES)}"
                )

            status = gate.get("status")
            if status not in ALLOWED_STATUSES:
                errors.append(
                    f"{stage_id}.{gate_id}.status must be one of {sorted(ALLOWED_STATUSES)}"
                )

            evidence = gate.get("evidence")
            if not isinstance(evidence, list) or not evidence:
                errors.append(f"{stage_id}.{gate_id}.evidence must be a non-empty list")
            else:
                for evidence_index, item in enumerate(evidence):
                    item_prefix = f"{stage_id}.{gate_id}.evidence[{evidence_index}]"
                    if not item.get("label"):
                        errors.append(f"{item_prefix}.label is required")
                    path = item.get("path")
                    if path:
                        evidence_path = root / path
                        if not evidence_path.exists():
                            errors.append(f"{item_prefix}.path does not exist: {path}")
                    elif not item.get("url") and not item.get("command"):
                        errors.append(f"{item_prefix} needs path, url, or command")

            blockers = gate.get("blockers", [])
            if gate_class == "blocking" and status not in READY_STATUSES and not blockers:
                errors.append(f"{stage_id}.{gate_id} is blocking and not ready but has no blockers")

            next_actions = gate.get("next_actions", [])
            if status not in READY_STATUSES and not next_actions:
                errors.append(f"{stage_id}.{gate_id} is not ready but has no next_actions")

    target = payload.get("current_target_stage")
    if target not in stage_ids:
        errors.append(f"current_target_stage does not name an existing stage: {target}")

    return errors


def gate_is_ready(gate: dict[str, Any]) -> bool:
    return gate.get("status") in READY_STATUSES


def stage_status(stage: dict[str, Any]) -> str:
    blocking_gates = [
        gate for gate in stage.get("gates", []) if gate.get("gate_class") == "blocking"
    ]
    if all(gate_is_ready(gate) for gate in blocking_gates):
        return "ready"
    if any(gate.get("status") in {"blocked", "missing"} for gate in blocking_gates):
        return "blocked"
    return "diagnostic"


def build_summary(
    payload: dict[str, Any], ledger_path: Path, root: Path, generated_at: str | None = None
) -> dict[str, Any]:
    stages = []
    for stage in payload["stages"]:
        gates = stage.get("gates", [])
        blocking = [gate for gate in gates if gate.get("gate_class") == "blocking"]
        ready_count = sum(1 for gate in blocking if gate_is_ready(gate))
        stages.append(
            {
                "id": stage["id"],
                "label": stage["label"],
                "intent": stage["intent"],
                "claim_boundary": stage["claim_boundary"],
                "status": stage_status(stage),
                "blocking_ready": ready_count,
                "blocking_total": len(blocking),
                "gates": gates,
            }
        )

    current_target = payload["current_target_stage"]
    current_stage = next(stage for stage in stages if stage["id"] == current_target)
    return {
        "generated_at": generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "ledger_path": str(ledger_path.relative_to(root)) if ledger_path.is_relative_to(root) else str(ledger_path),
        "review_cadence": payload.get("review_cadence", ""),
        "current_target_stage": current_target,
        "current_target_status": current_stage["status"],
        "stages": stages,
    }


def format_evidence_item(item: dict[str, Any]) -> str:
    label = item.get("label", "evidence")
    if item.get("path"):
        return f"[{label}]({item['path']})"
    if item.get("url"):
        return f"[{label}]({item['url']})"
    return f"`{item.get('command', label)}`"


def format_list(items: list[str]) -> str:
    if not items:
        return "none"
    return "<br>".join(items)


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# AETHER Commercial Release Readiness",
        "",
        f"- Generated: `{summary['generated_at']}`",
        f"- Ledger: `{summary['ledger_path']}`",
        f"- Review cadence: {summary.get('review_cadence') or 'unspecified'}",
        f"- Current target: `{summary['current_target_stage']}`",
        f"- Current target status: `{summary['current_target_status']}`",
        "",
        "## Stage Summary",
        "",
        "| Stage | Status | Blocking gates | Claim boundary |",
        "| --- | --- | --- | --- |",
    ]
    for stage in summary["stages"]:
        lines.append(
            f"| `{stage['label']}` | `{stage['status']}` | "
            f"`{stage['blocking_ready']}/{stage['blocking_total']}` | {stage['claim_boundary']} |"
        )

    for stage in summary["stages"]:
        lines.extend(
            [
                "",
                f"## {stage['label']}",
                "",
                stage["intent"],
                "",
                f"Claim boundary: {stage['claim_boundary']}",
                "",
                "| Gate | Owner | Class | Status | Evidence | Blockers | Next actions |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for gate in stage["gates"]:
            evidence = "<br>".join(format_evidence_item(item) for item in gate.get("evidence", []))
            lines.append(
                f"| `{gate['title']}` | `{gate['owner']}` | `{gate['gate_class']}` | "
                f"`{gate['status']}` | {evidence} | {format_list(gate.get('blockers', []))} | "
                f"{format_list(gate.get('next_actions', []))} |"
            )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `ready` means every blocking gate for that stage is backed by current tracked evidence.",
            "- `blocked` means at least one blocking gate has explicit blockers and must not be sold past its claim boundary.",
            "- `diagnostic` and `observational` gates are visible evidence, not release blockers, until promoted.",
            "",
        ]
    )
    return "\n".join(lines)


def cmd_render(args: argparse.Namespace) -> int:
    root = repo_root()
    ledger_path = Path(args.ledger)
    if not ledger_path.is_absolute():
        ledger_path = root / ledger_path
    payload = load_json(ledger_path)
    errors = validate_ledger(payload, root)
    if errors:
        for error in errors:
            print(f"commercial readiness ledger error: {error}", file=sys.stderr)
        return 2

    summary = build_summary(
        payload=payload,
        ledger_path=ledger_path,
        root=root,
        generated_at=args.generated_at,
    )
    write_json(Path(args.out_json), summary)
    write_text(Path(args.out_md), render_markdown(summary))

    if args.enforce_current_target and summary["current_target_status"] != "ready":
        print(
            "current commercial target is not ready: "
            f"{summary['current_target_stage']}={summary['current_target_status']}",
            file=sys.stderr,
        )
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render AETHER commercial readiness ledger")
    subparsers = parser.add_subparsers(dest="command", required=True)

    render = subparsers.add_parser("render")
    render.add_argument("--ledger", required=True)
    render.add_argument("--out-json", required=True)
    render.add_argument("--out-md", required=True)
    render.add_argument("--generated-at")
    render.add_argument("--enforce-current-target", action="store_true")
    render.set_defaults(func=cmd_render)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
