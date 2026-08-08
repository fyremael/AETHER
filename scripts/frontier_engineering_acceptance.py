#!/usr/bin/env python3
"""Validate the frontier engineering control-room workflow pack."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_PATHS = [
    Path("crates/aether_api/examples/demo_07_frontier_engineering_control_room.rs"),
    Path("examples/demo-07-frontier-engineering-control-room.md"),
    Path("docs/COMMERCIALIZATION/FRONTIER_ENGINEERING_CONTROL_ROOM.md"),
    Path("scripts/run-demo-07.cmd"),
]

REQUIRED_MARKERS = [
    "AETHER Demo 07: Frontier Engineering Control Room",
    "GOVERNED CHANGE CAMPAIGN",
    "24 recursively dependent work packages",
    "3 competing change candidates",
    "12 exact prerequisite receipts across 4 gate lanes",
    "WORK GRAPH BEFORE REPAIR",
    "STALE RUNNER EVIDENCE FENCED",
    "IDENTITY AND POLICY REJECTIONS",
    "PROMOTION AT CURRENT",
    "candidate-c-release-control-repair",
    "ROUTING UPDATES ACCEPTED",
    "ROUTING UPDATES RETAINED",
    "EVIDENCE ANCHOR",
    "WHY THE PROMOTION IS TRUE",
    "root tuple",
    "functional control-room proof, not autonomous software delivery",
]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def normalize(path: Path) -> str:
    return str(path).replace("\\", "/")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Frontier Engineering Acceptance",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Ready: `{payload['ready']}`",
        f"- Exit code: `{payload['execution']['exit_code']}`",
        f"- Duration seconds: `{payload['execution']['duration_seconds']}`",
        "",
        "| Gate | Status |",
        "| --- | --- |",
    ]
    for gate in payload["gates"]:
        lines.append(f"| `{gate['id']}` | `{gate['status']}` |")
    lines.extend(
        [
            "",
            "## Output Tail",
            "",
            "```text",
            payload["execution"]["output_tail"],
            "```",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    root = repo_root()
    missing_paths = [normalize(path) for path in REQUIRED_PATHS if not (root / path).exists()]
    command = [
        "cargo",
        "run",
        "-p",
        "aether_api",
        "--example",
        "demo_07_frontier_engineering_control_room",
        "--release",
    ]
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=args.timeout_seconds,
        check=False,
    )
    duration = round(time.perf_counter() - started, 3)
    missing_markers = [marker for marker in REQUIRED_MARKERS if marker not in completed.stdout]
    gates = [
        {
            "id": "workflow_pack_present",
            "status": "passed" if not missing_paths else "blocked",
            "missing": missing_paths,
        },
        {
            "id": "release_demo_execution",
            "status": "passed" if completed.returncode == 0 else "blocked",
            "exit_code": completed.returncode,
        },
        {
            "id": "frontier_engineering_markers",
            "status": "passed" if not missing_markers else "blocked",
            "missing": missing_markers,
        },
    ]
    ready = all(gate["status"] == "passed" for gate in gates)
    payload = {
        "generated_at": args.generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "workflow": "frontier_engineering_control_room",
        "ready": ready,
        "gates": gates,
        "execution": {
            "command": command,
            "exit_code": completed.returncode,
            "duration_seconds": duration,
            "output_tail": "\n".join(completed.stdout.splitlines()[-120:]),
        },
    }
    write_json(Path(args.out_json), payload)
    write_markdown(Path(args.out_md), payload)
    if args.enforce and not ready:
        print("frontier engineering acceptance is not ready", file=sys.stderr)
        return 3
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--out-json", required=True)
    result.add_argument("--out-md", required=True)
    result.add_argument("--timeout-seconds", type=int, default=300)
    result.add_argument("--generated-at")
    result.add_argument("--enforce", action="store_true")
    return result


if __name__ == "__main__":
    sys.exit(run(parser().parse_args()))
