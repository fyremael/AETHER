#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


POLICY_SCHEMA_VERSION = 2
APPROVED_BETA_HOST_IDS = {
    "dev-chad-windows-native",
    "github-windows-latest",
}
REQUIRED_SUITE_ID = "full_stack"
KNOWN_DRIFT_STATUSES = {"ok", "warn"}
REQUIRED_DRIFT_SURFACES = {
    "core_kernel": "artifacts/performance/latest-drift-core_kernel.md",
    "service_in_process": "artifacts/performance/latest-drift-service_in_process.md",
}
REQUIRED_LATENCY_SURFACES = {
    "core_restart_replay": (
        "core_kernel",
        "Durable restart current replay",
        "1,000 entities",
    ),
    "service_restart_replay": (
        "service_in_process",
        "Durable restart coordination replay",
        "128 tasks",
    ),
    "service_coordination_run": (
        "service_in_process",
        "Kernel service coordination run",
        "128 tasks",
    ),
    "http_status": (
        "http_pilot_boundary",
        "HTTP service status endpoint",
        "pilot boundary",
    ),
    "http_history": (
        "http_pilot_boundary",
        "HTTP history endpoint",
        "25 datoms",
    ),
    "http_coordination_report": (
        "http_pilot_boundary",
        "HTTP coordination report endpoint",
        "pilot coordination",
    ),
    "http_coordination_delta": (
        "http_pilot_boundary",
        "HTTP coordination delta endpoint",
        "4 changed rows",
    ),
}


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


def is_trimmed_identifier(value: Any) -> bool:
    return type(value) is str and bool(value) and value == value.strip()


def validate_threshold_policy(thresholds: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(thresholds, dict):
        return ["threshold policy must be an object"]

    schema_version = thresholds.get("schema_version")
    if type(schema_version) is not int or schema_version != POLICY_SCHEMA_VERSION:
        errors.append(f"schema_version must be integer {POLICY_SCHEMA_VERSION}")

    configured_hosts = thresholds.get("allowed_host_ids")
    if not isinstance(configured_hosts, list) or not configured_hosts:
        errors.append("allowed_host_ids must be a non-empty list")
    elif not all(is_trimmed_identifier(item) for item in configured_hosts):
        errors.append("allowed_host_ids must contain only non-empty trimmed strings")
    elif len(configured_hosts) != len(set(configured_hosts)):
        errors.append("allowed_host_ids must not contain duplicates")
    elif set(configured_hosts) != APPROVED_BETA_HOST_IDS:
        errors.append(
            f"allowed_host_ids must be exactly {sorted(APPROVED_BETA_HOST_IDS)}"
        )

    suite_id = thresholds.get("suite_id")
    if not is_trimmed_identifier(suite_id) or suite_id != REQUIRED_SUITE_ID:
        errors.append(f"suite_id must be {REQUIRED_SUITE_ID}")

    drift_reports = thresholds.get("drift_reports")
    drift_by_suite: dict[str, dict[str, Any]] = {}
    if not isinstance(drift_reports, list) or not drift_reports:
        errors.append("drift_reports must be a non-empty list")
    else:
        for index, drift in enumerate(drift_reports):
            prefix = f"drift_reports[{index}]"
            if not isinstance(drift, dict):
                errors.append(f"{prefix} must be an object")
                continue
            suite = drift.get("suite")
            path = drift.get("path")
            allowed = drift.get("allowed_gated_overall")
            if not is_trimmed_identifier(suite):
                errors.append(f"{prefix}.suite must be a non-empty trimmed string")
            elif suite in drift_by_suite:
                errors.append(f"duplicate drift suite: {suite}")
            else:
                drift_by_suite[suite] = drift
            if not is_trimmed_identifier(path):
                errors.append(f"{prefix}.path must be a non-empty trimmed string")
            if not isinstance(allowed, list) or not allowed:
                errors.append(f"{prefix}.allowed_gated_overall must be non-empty")
            elif not all(is_trimmed_identifier(item) for item in allowed):
                errors.append(
                    f"{prefix}.allowed_gated_overall must contain trimmed strings"
                )
            elif len(allowed) != len(set(allowed)):
                errors.append(f"{prefix}.allowed_gated_overall has duplicates")
            elif not set(allowed).issubset(KNOWN_DRIFT_STATUSES):
                errors.append(
                    f"{prefix}.allowed_gated_overall contains an unknown status"
                )
        for suite, expected_path in REQUIRED_DRIFT_SURFACES.items():
            drift = drift_by_suite.get(suite)
            if drift is None:
                errors.append(f"required drift suite is missing: {suite}")
            elif drift.get("path") != expected_path:
                errors.append(f"required drift path changed for {suite}")

    latency_thresholds = thresholds.get("latency_thresholds")
    latency_by_id: dict[str, dict[str, Any]] = {}
    measurement_keys: set[tuple[str, str, str]] = set()
    if not isinstance(latency_thresholds, list) or not latency_thresholds:
        errors.append("latency_thresholds must be a non-empty list")
    else:
        for index, threshold in enumerate(latency_thresholds):
            prefix = f"latency_thresholds[{index}]"
            if not isinstance(threshold, dict):
                errors.append(f"{prefix} must be an object")
                continue
            threshold_id = threshold.get("id")
            if not is_trimmed_identifier(threshold_id):
                errors.append(f"{prefix}.id must be a non-empty trimmed string")
            elif threshold_id in latency_by_id:
                errors.append(f"duplicate latency threshold id: {threshold_id}")
            else:
                latency_by_id[threshold_id] = threshold
            fields = (
                threshold.get("group"),
                threshold.get("workload"),
                threshold.get("scale"),
            )
            if not all(is_trimmed_identifier(item) for item in fields):
                errors.append(
                    f"{prefix} group, workload, and scale must be non-empty trimmed strings"
                )
            elif fields in measurement_keys:
                errors.append(f"duplicate latency measurement surface: {fields}")
            else:
                measurement_keys.add(fields)
            ceiling = threshold.get("max_mean_ms")
            if (
                type(ceiling) not in (int, float)
                or not math.isfinite(ceiling)
                or ceiling <= 0
            ):
                errors.append(f"{prefix}.max_mean_ms must be finite and positive")
            if not is_trimmed_identifier(threshold.get("why")):
                errors.append(f"{prefix}.why must be a non-empty trimmed string")
        for threshold_id, expected_surface in REQUIRED_LATENCY_SURFACES.items():
            threshold = latency_by_id.get(threshold_id)
            if threshold is None:
                errors.append(f"required latency threshold is missing: {threshold_id}")
                continue
            observed_surface = (
                threshold.get("group"),
                threshold.get("workload"),
                threshold.get("scale"),
            )
            if observed_surface != expected_surface:
                errors.append(f"required latency surface changed for {threshold_id}")

    return errors


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

    policy_errors = validate_threshold_policy(thresholds)
    gates.append(
        build_gate(
            "policy_integrity",
            "Performance beta threshold policy is complete and valid",
            not policy_errors,
            [normalize_path(thresholds_path)],
            blockers=policy_errors,
            details={"schema_version": thresholds.get("schema_version") if isinstance(thresholds, dict) else None},
        )
    )
    if policy_errors:
        return {
            "generated_at": generated_at,
            "thresholds": normalize_path(thresholds_path),
            "bundle": normalize_path(bundle_path),
            "beta_ready": False,
            "gates": gates,
        }

    host_id = bundle.get("host_manifest", {}).get("host_id")
    suite_id = bundle.get("run", {}).get("suite_id")
    allowed_host_ids = thresholds["allowed_host_ids"]
    threshold_schema_version = thresholds["schema_version"]
    expected_suite_id = thresholds["suite_id"]
    gates.append(
        build_gate(
            "bundle_identity",
            "Performance bundle matches an approved beta host and suite",
            host_id in allowed_host_ids and suite_id == expected_suite_id,
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
