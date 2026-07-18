#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


def duration_ms(duration: dict[str, Any]) -> float:
    return float(duration.get("secs", 0)) * 1000.0 + float(duration.get("nanos", 0)) / 1_000_000.0


def measurement_key(measurement: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(measurement.get("group", "")),
        str(measurement.get("workload", "")),
        str(measurement.get("scale", "")),
    )


def find_measurement(report: dict[str, Any], threshold: dict[str, Any]) -> dict[str, Any] | None:
    expected = (
        str(threshold["group"]),
        str(threshold["workload"]),
        str(threshold["scale"]),
    )
    for measurement in report.get("report", {}).get("measurements", []):
        if measurement_key(measurement) == expected:
            return measurement
    return None


def drift_status(path: Path) -> str:
    if not path.exists():
        return "missing"
    text = path.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"Gated overall:\s+`([^`]+)`", text)
    return match.group(1) if match else "missing"


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
    thresholds_path = Path(args.thresholds)
    bundle_path = Path(args.bundle)
    if not thresholds_path.is_absolute():
        thresholds_path = root / thresholds_path
    if not bundle_path.is_absolute():
        bundle_path = root / bundle_path

    thresholds = load_json(thresholds_path)
    bundle = load_json(bundle_path) if bundle_path.exists() else {}
    gates: list[dict[str, Any]] = []

    host_id = bundle.get("host_manifest", {}).get("host_id")
    suite_id = bundle.get("run", {}).get("suite_id")
    configured_hosts = thresholds.get("allowed_host_ids", [])
    threshold_schema_version = thresholds.get("schema_version")
    expected_suite_id = thresholds.get("suite_id")
    allowed_host_ids = (
        configured_hosts
        if isinstance(configured_hosts, list)
        and configured_hosts
        and all(isinstance(item, str) and item for item in configured_hosts)
        and len(configured_hosts) == len(set(configured_hosts))
        else []
    )
    policy_valid = (
        threshold_schema_version == 2
        and bool(allowed_host_ids)
        and isinstance(expected_suite_id, str)
        and bool(expected_suite_id)
    )
    gates.append(
        build_gate(
            "bundle_identity",
            "Performance bundle matches an approved beta host and suite",
            policy_valid and host_id in allowed_host_ids and suite_id == expected_suite_id,
            [normalize_path(bundle_path)],
            blockers=[
                f"expected schema 2, one of hosts {allowed_host_ids}, and suite {expected_suite_id}; got schema {threshold_schema_version}, host {host_id}, and suite {suite_id}"
            ],
            details={
                "host_id": host_id,
                "allowed_host_ids": allowed_host_ids,
                "suite_id": suite_id,
                "threshold_schema_version": threshold_schema_version,
            },
        )
    )

    for drift in thresholds.get("drift_reports", []):
        report_path = Path(drift["path"])
        if not report_path.is_absolute():
            report_path = root / report_path
        status = drift_status(report_path)
        allowed = set(drift.get("allowed_gated_overall", []))
        gates.append(
            build_gate(
                f"drift_{drift['suite']}",
                f"{drift['suite']} gated drift is within beta tolerance",
                status in allowed,
                [normalize_path(report_path)],
                blockers=[f"gated drift status {status} is not in {sorted(allowed)}"],
                details={"gated_overall": status, "allowed": sorted(allowed)},
            )
        )

    for threshold in thresholds.get("latency_thresholds", []):
        measurement = find_measurement(bundle, threshold)
        if measurement is None:
            gates.append(
                build_gate(
                    f"latency_{threshold['id']}",
                    f"{threshold['workload']} latency is present",
                    False,
                    [normalize_path(bundle_path)],
                    blockers=[
                        f"missing measurement {threshold['group']} / {threshold['workload']} / {threshold['scale']}"
                    ],
                )
            )
            continue
        mean_ms = duration_ms(measurement["latency"]["mean"])
        max_mean_ms = float(threshold["max_mean_ms"])
        gates.append(
            build_gate(
                f"latency_{threshold['id']}",
                f"{threshold['workload']} mean latency stays under beta ceiling",
                mean_ms <= max_mean_ms,
                [normalize_path(bundle_path)],
                blockers=[f"mean {mean_ms:.3f} ms exceeds ceiling {max_mean_ms:.3f} ms"],
                details={
                    "group": threshold["group"],
                    "workload": threshold["workload"],
                    "scale": threshold["scale"],
                    "mean_ms": round(mean_ms, 3),
                    "max_mean_ms": max_mean_ms,
                    "why": threshold.get("why", ""),
                },
            )
        )

    beta_ready = all(gate["status"] == "passed" for gate in gates)
    return {
        "generated_at": generated_at,
        "thresholds": normalize_path(thresholds_path),
        "bundle": normalize_path(bundle_path),
        "beta_ready": beta_ready,
        "gates": gates,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# AETHER Performance Beta Gate",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Thresholds: `{payload['thresholds']}`",
        f"- Bundle: `{payload['bundle']}`",
        f"- Beta ready: `{payload['beta_ready']}`",
        "",
        "| Gate | Status | Measurement | Threshold | Evidence | Blockers |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        measurement = (
            f"{gate.get('mean_ms')} ms"
            if gate.get("mean_ms") is not None
            else gate.get("gated_overall", gate.get("suite_id", "-"))
        )
        threshold = (
            f"{gate.get('max_mean_ms')} ms"
            if gate.get("max_mean_ms") is not None
            else ", ".join(gate.get("allowed", [])) or "-"
        )
        evidence = "<br>".join(f"`{item}`" for item in gate.get("evidence", []))
        blockers = "<br>".join(gate.get("blockers", [])) or "none"
        lines.append(
            f"| `{gate['title']}` | `{gate['status']}` | `{measurement}` | `{threshold}` | {evidence} | {blockers} |"
        )
    lines.append("")
    return "\n".join(lines)


def cmd_run(args: argparse.Namespace) -> int:
    payload = build_report(args)
    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), render_markdown(payload))
    if args.enforce and not payload["beta_ready"]:
        print("performance beta gate is not ready", file=sys.stderr)
        return 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate AETHER commercial beta performance thresholds")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--thresholds", required=True)
    run.add_argument("--bundle", required=True)
    run.add_argument("--out-json", required=True)
    run.add_argument("--out-md", required=True)
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
