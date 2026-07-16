#!/usr/bin/env python3
"""Render commercial claim policy without treating it as observed evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ALLOWED_GATE_CLASSES = {"blocking", "diagnostic", "observational", "future"}
FORBIDDEN_OUTCOME_FIELDS = {"status", "evidence", "blockers", "observed_status", "beta_ready"}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def validate_ledger(payload: dict[str, Any], _root: Path) -> list[str]:
    errors: list[str] = []
    if payload.get("schema_version") != 2:
        errors.append("schema_version must be 2")
    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages:
        return errors + ["stages must be a non-empty list"]
    stage_ids: set[str] = set()
    for stage_index, stage in enumerate(stages):
        stage_id = stage.get("id")
        if not stage_id:
            errors.append(f"stages[{stage_index}].id is required")
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
            gate_id = gate.get("id")
            prefix = f"{stage_id}.gates[{gate_index}]"
            if not gate_id:
                errors.append(f"{prefix}.id is required")
                continue
            if gate_id in gate_ids:
                errors.append(f"duplicate gate id in {stage_id}: {gate_id}")
            gate_ids.add(gate_id)
            for field in ("title", "owner", "requirement"):
                if not gate.get(field):
                    errors.append(f"{stage_id}.{gate_id}.{field} is required")
            if gate.get("gate_class") not in ALLOWED_GATE_CLASSES:
                errors.append(
                    f"{stage_id}.{gate_id}.gate_class must be one of {sorted(ALLOWED_GATE_CLASSES)}"
                )
            forbidden = sorted(FORBIDDEN_OUTCOME_FIELDS.intersection(gate))
            if forbidden:
                errors.append(
                    f"{stage_id}.{gate_id} contains authored outcome fields: {forbidden}"
                )
            requirement = gate.get("evidence_requirement")
            if not isinstance(requirement, dict):
                errors.append(f"{stage_id}.{gate_id}.evidence_requirement is required")
                continue
            gate_ids_required = requirement.get("gate_ids")
            subjects = requirement.get("bundle_subjects")
            if not isinstance(gate_ids_required, list) or not gate_ids_required:
                errors.append(f"{stage_id}.{gate_id}.evidence_requirement.gate_ids is required")
            if not isinstance(subjects, list):
                errors.append(f"{stage_id}.{gate_id}.evidence_requirement.bundle_subjects must be a list")
    target = payload.get("current_target_stage")
    if target not in stage_ids:
        errors.append(f"current_target_stage does not name an existing stage: {target}")
    return errors


def build_summary(
    payload: dict[str, Any], ledger_path: Path, root: Path, generated_at: str | None = None
) -> dict[str, Any]:
    return {
        "schema_version": "aether.commercial-claim-policy-summary.v2",
        "generated_at": generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "ledger_path": str(ledger_path.relative_to(root))
        if ledger_path.is_relative_to(root)
        else str(ledger_path),
        "review_cadence": payload.get("review_cadence", ""),
        "current_target_stage": payload["current_target_stage"],
        "readiness_source": "immutable_evidence_bundle_only",
        "computed_status": "not_computed_from_policy",
        "stages": payload["stages"],
    }


def format_list(items: list[str]) -> str:
    return "<br>".join(f"`{item}`" for item in items) or "none"


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# AETHER Commercial Claim Policy",
        "",
        f"- Generated: `{summary['generated_at']}`",
        f"- Policy ledger: `{summary['ledger_path']}`",
        f"- Current target: `{summary['current_target_stage']}`",
        "- Readiness source: `immutable evidence bundle only`",
        "- Computed status: `not computed from policy`",
        "",
        "This document defines requirements, owners, and claim boundaries. It",
        "contains no observed gate outcomes and cannot promote a release.",
    ]
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
                "| Requirement | Owner | Class | Evidence gate IDs | Bundle subjects |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for gate in stage["gates"]:
            requirement = gate["evidence_requirement"]
            lines.append(
                f"| **{gate['title']}** — {gate['requirement']} | `{gate['owner']}` | "
                f"`{gate['gate_class']}` | {format_list(requirement['gate_ids'])} | "
                f"{format_list(requirement['bundle_subjects'])} |"
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Policy declarations are not evidence.",
            "- Only `passed` candidate-bound envelopes satisfy evidence gates.",
            "- Waivers are separate signed, scoped, expiring facts and never rewrite failures.",
            "- Commercial beta and GA remain separate claim computations.",
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
            print(f"commercial claim policy error: {error}", file=sys.stderr)
        return 2
    summary = build_summary(payload, ledger_path, root, args.generated_at)
    write_json(Path(args.out_json), summary)
    write_text(Path(args.out_md), render_markdown(summary))
    if args.enforce_current_target:
        print(
            "--enforce-current-target is no longer valid: claim policy contains no observed outcomes",
            file=sys.stderr,
        )
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
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
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
