#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_DOCS = [
    Path("docs/COMMERCIALIZATION/AI_SUPPORT_RESOLUTION_DESK.md"),
    Path("examples/demo-05-ai-support-resolution-desk.md"),
    Path("crates/aether_api/examples/demo_05_ai_support_resolution_desk.rs"),
    Path("python/notebooks/06_ai_support_resolution_desk.ipynb"),
    Path("site/support-resolution-desk.html"),
]

REQUIRED_DEMO_MARKERS = [
    "AETHER Demo 05: AI Support Resolution Desk",
    "Active support cases on the desk",
    "Retrieved evidence from the support-memory sidecar",
    "Published candidate resolutions",
    "Which resolution is actually ready",
    "Who owns the case now",
    "Fenced stale recommendations at Current",
    "Why the current selected resolution is true",
    "case/501",
    "apply-migration-credit",
    "lead-ana",
    "root tuple",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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


def missing_paths(root: Path, paths: list[Path]) -> list[str]:
    return [normalize_path(path) for path in paths if not (root / path).exists()]


def missing_text_markers(text: str, markers: list[str]) -> list[str]:
    return [marker for marker in markers if marker not in text]


def run_command(command: list[str], root: Path, timeout_seconds: int) -> dict[str, Any]:
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
        "output": completed.stdout,
        "output_tail": "\n".join(completed.stdout.splitlines()[-120:]),
    }


def build_gate(
    gate_id: str,
    title: str,
    passed: bool,
    evidence: list[str],
    blockers: list[str] | None = None,
    next_actions: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": gate_id,
        "title": title,
        "status": "passed" if passed else "blocked",
        "evidence": evidence,
        "blockers": [] if passed else blockers or [f"{title} is incomplete."],
        "next_actions": [] if passed else next_actions or [f"Complete {title.lower()}."],
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    root = repo_root()
    generated_at = args.generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    gates: list[dict[str, Any]] = []

    missing_docs = missing_paths(root, REQUIRED_DOCS)
    gates.append(
        build_gate(
            "workflow_pack_present",
            "AI support resolution desk workflow pack is present",
            not missing_docs,
            [normalize_path(path) for path in REQUIRED_DOCS],
            blockers=[f"missing path: {path}" for path in missing_docs],
            next_actions=["Restore the tracked support-desk docs, demo, notebook, and site entry."],
        )
    )

    narrative_path = root / "docs/COMMERCIALIZATION/AI_SUPPORT_RESOLUTION_DESK.md"
    demo_path = root / "examples/demo-05-ai-support-resolution-desk.md"
    narrative_text = narrative_path.read_text(encoding="utf-8", errors="replace") if narrative_path.exists() else ""
    demo_text = demo_path.read_text(encoding="utf-8", errors="replace") if demo_path.exists() else ""
    narrative_missing = missing_text_markers(
        narrative_text,
        ["Run The Exemplar", "Truth Boundary", "How To Present It", "Technical Appendix"],
    )
    demo_missing = missing_text_markers(
        demo_text,
        ["Screen-Share Flow", "What You Should See", "Why It Matters", "cargo run -p aether_api --example demo_05_ai_support_resolution_desk"],
    )
    gates.append(
        build_gate(
            "customer_onboarding_flow_documented",
            "Customer onboarding flow is documented",
            not narrative_missing and not demo_missing,
            [
                "docs/COMMERCIALIZATION/AI_SUPPORT_RESOLUTION_DESK.md",
                "examples/demo-05-ai-support-resolution-desk.md",
            ],
            blockers=[f"narrative missing marker: {item}" for item in narrative_missing]
            + [f"demo missing marker: {item}" for item in demo_missing],
            next_actions=["Restore the buyer-facing support-desk walkthrough sections."],
        )
    )

    command = ["cargo", "run", "-p", "aether_api", "--example", "demo_05_ai_support_resolution_desk", "--release"]
    demo = run_command(command, root, args.timeout_seconds)
    missing_markers = missing_text_markers(demo["output"], REQUIRED_DEMO_MARKERS)
    demo_passed = demo["exit_code"] == 0 and not missing_markers
    gates.append(
        build_gate(
            "demo_execution_passed",
            "Support desk demo executes and prints acceptance markers",
            demo_passed,
            ["crates/aether_api/examples/demo_05_ai_support_resolution_desk.rs"],
            blockers=([f"demo exited {demo['exit_code']}"] if demo["exit_code"] != 0 else [])
            + [f"demo output missing marker: {item}" for item in missing_markers],
            next_actions=["Fix the support-desk demo or update the acceptance markers intentionally."],
        )
        | {
            "command": command,
            "duration_seconds": demo["duration_seconds"],
            "exit_code": demo["exit_code"],
            "output_tail": demo["output_tail"],
        }
    )

    workflow_ready = all(gate["status"] == "passed" for gate in gates)
    return {
        "generated_at": generated_at,
        "workflow": "ai_support_resolution_desk",
        "workflow_ready": workflow_ready,
        "gates": gates,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# AETHER Customer Workflow Acceptance",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Workflow: `{payload['workflow']}`",
        f"- Workflow ready: `{payload['workflow_ready']}`",
        "",
        "| Gate | Status | Evidence | Blockers | Next actions |",
        "| --- | --- | --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        evidence = "<br>".join(f"`{item}`" for item in gate.get("evidence", []))
        blockers = "<br>".join(gate.get("blockers", [])) or "none"
        next_actions = "<br>".join(gate.get("next_actions", [])) or "none"
        lines.append(
            f"| `{gate['title']}` | `{gate['status']}` | {evidence} | {blockers} | {next_actions} |"
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
    return "\n".join(lines)


def cmd_run(args: argparse.Namespace) -> int:
    payload = build_report(args)
    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), render_markdown(payload))
    if args.enforce and not payload["workflow_ready"]:
        print("customer workflow acceptance is not ready", file=sys.stderr)
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run AETHER customer workflow acceptance")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--out-json", required=True)
    run.add_argument("--out-md", required=True)
    run.add_argument("--timeout-seconds", type=int, default=180)
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
