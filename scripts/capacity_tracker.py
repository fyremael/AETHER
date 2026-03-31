#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


def emit_outputs(outputs: dict[str, str], github_output: str | None) -> None:
    if github_output:
        with open(github_output, "a", encoding="utf-8", newline="\n") as handle:
            for key, value in outputs.items():
                handle.write(f"{key}={value}\n")
    else:
        for key, value in outputs.items():
            print(f"{key}={value}")


def github_request(
    token: str,
    repo: str,
    path: str,
    *,
    method: str = "GET",
    query: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
) -> Any:
    query_string = ""
    if query:
        query_string = "?" + urllib.parse.urlencode(query)
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}{query_string}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aether-capacity-tracker",
        },
    )
    with urllib.request.urlopen(request) as response:
        if response.headers.get("Content-Type", "").startswith("application/json"):
            return json.load(response)
        return response.read()


def load_config(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    required = {"workflow_file", "base_branch", "artifact_name", "tracker_issue_title"}
    missing = required.difference(payload)
    if missing:
        raise SystemExit(f"invalid capacity tracking config {path}: missing {sorted(missing)}")
    return payload


def build_summary_payload(args: argparse.Namespace, report: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    envelopes = {item["node_class"]: item for item in report["single_node_envelopes"]}
    current = envelopes[report["node_class"]]
    ceilings = {item["category"]: item["status"] for item in report["ceiling_signals"]}
    return {
        "generated_at": args.generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "event_name": args.event_name,
        "ref_name": args.ref_name,
        "sha": args.sha,
        "workflow_file": config["workflow_file"],
        "host_id": report["host_id"],
        "recommended_node_class": report["node_class"],
        "current_limiting_factor": report["current_limiting_factor"],
        "current_pilot_board": current["maximum_recommended_pilot_board_size"],
        "current_mixed_concurrency": current["maximum_recommended_mixed_operator_concurrency"],
        "current_durable_replay": current["maximum_recommended_durable_replay_size"],
        "confidence_level": report["confidence_level"],
        "ceiling_statuses": ceilings,
    }


def build_summary_markdown(summary: dict[str, Any], previous: dict[str, Any] | None, changes: list[str]) -> str:
    lines = [
        "# Capacity planning tracker",
        "",
        f"- Generated: `{summary['generated_at']}`",
        f"- Workflow run: `{summary['run_id']}` attempt `{summary['run_attempt']}`",
        f"- Event: `{summary['event_name']}` on `{summary['ref_name']}`",
        f"- Commit: `{summary['sha']}`",
        f"- Host: `{summary['host_id']}`",
        f"- Recommended default node class: `{summary['recommended_node_class']}`",
        f"- Current limiting factor: `{summary['current_limiting_factor']}`",
        f"- Safe single-node pilot envelope: `{summary['current_pilot_board']}` tasks, `{summary['current_mixed_concurrency']}` mixed concurrent operators, `{summary['current_durable_replay']}` durable replay entities",
        f"- Confidence: `{summary['confidence_level']}`",
        "",
        "## Ceiling status",
        "",
        "| Ceiling | Status |",
        "| --- | --- |",
    ]
    for category, status in sorted(summary["ceiling_statuses"].items()):
        lines.append(f"| `{category}` | `{status}` |")
    if previous:
        lines.extend(
            [
                "",
                "## Previous scheduled baseline",
                "",
                f"- Prior node class: `{previous['recommended_node_class']}`",
                f"- Prior limiting factor: `{previous['current_limiting_factor']}`",
                f"- Prior pilot envelope: `{previous['current_pilot_board']}` tasks / `{previous['current_mixed_concurrency']}` mixed concurrent operators / `{previous['current_durable_replay']}` durable replay entities",
            ]
        )
    lines.extend(["", "## Material change check", ""])
    if changes:
        for change in changes:
            lines.append(f"- {change}")
    else:
        lines.append("- No material capacity drift was detected relative to the previous scheduled summary.")
    lines.append("")
    return "\n".join(lines)


def cmd_write_summary(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    report = load_json(Path(args.capacity_json))
    summary = build_summary_payload(args, report, config)
    markdown = build_summary_markdown(summary, None, [])
    write_json(Path(args.out_json), summary)
    write_text(Path(args.out_md), markdown)
    return 0


def load_previous_summary(
    token: str,
    repo: str,
    workflow_file: str,
    base_branch: str,
    current_run_id: str,
    artifact_name: str,
) -> dict[str, Any] | None:
    runs = github_request(
        token,
        repo,
        f"/actions/workflows/{workflow_file}/runs",
        query={
            "branch": base_branch,
            "event": "schedule",
            "status": "completed",
            "per_page": "15",
        },
    )
    for run in runs.get("workflow_runs", []):
        run_id = str(run["id"])
        if run_id == str(current_run_id):
            continue
        artifacts = github_request(
            token,
            repo,
            f"/actions/runs/{run_id}/artifacts",
            query={"per_page": "100"},
        )
        artifact = next(
            (
                item
                for item in artifacts.get("artifacts", [])
                if item.get("name") == artifact_name and not item.get("expired", False)
            ),
            None,
        )
        if artifact is None:
            continue
        try:
            archive_bytes = github_download_artifact(token, artifact["archive_download_url"])
        except urllib.error.HTTPError:
            continue
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            member = next(
                (
                    name
                    for name in archive.namelist()
                    if name.endswith("capacity-summary.json")
                ),
                None,
            )
            if member is None:
                continue
            with archive.open(member) as handle:
                return json.load(handle)
    return None


def github_download_artifact(token: str, artifact_url: str) -> bytes:
    request = urllib.request.Request(
        artifact_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aether-capacity-tracker",
        },
    )
    with urllib.request.urlopen(request) as response:
        return response.read()


def detect_changes(current: dict[str, Any], previous: dict[str, Any] | None, threshold_pct: float) -> list[str]:
    if previous is None:
        return ["No previous scheduled capacity summary was available; this run establishes the current baseline."]
    changes: list[str] = []
    if current["recommended_node_class"] != previous["recommended_node_class"]:
        changes.append(
            f"Recommended default node class changed from `{previous['recommended_node_class']}` to `{current['recommended_node_class']}`."
        )
    if current["current_limiting_factor"] != previous["current_limiting_factor"]:
        changes.append(
            f"Current limiting factor changed from `{previous['current_limiting_factor']}` to `{current['current_limiting_factor']}`."
        )
    for key, label in [
        ("current_pilot_board", "pilot board envelope"),
        ("current_mixed_concurrency", "mixed concurrency envelope"),
        ("current_durable_replay", "durable replay envelope"),
    ]:
        old = max(int(previous[key]), 1)
        new = int(current[key])
        pct = abs(new - old) / old * 100.0
        if pct >= threshold_pct:
            direction = "up" if new > old else "down"
            changes.append(
                f"Current {label} moved {direction} by {pct:.0f}% (`{old}` -> `{new}`)."
            )
    for category, status in current["ceiling_statuses"].items():
        if previous["ceiling_statuses"].get(category) != status:
            changes.append(
                f"Ceiling status for `{category}` changed from `{previous['ceiling_statuses'].get(category, 'missing')}` to `{status}`."
            )
    return changes


def find_issue_by_title(token: str, repo: str, title: str) -> dict[str, Any] | None:
    issues = github_request(
        token,
        repo,
        "/issues",
        query={"state": "open", "per_page": "100"},
    )
    for issue in issues:
        if issue.get("pull_request"):
            continue
        if issue["title"] == title:
            return issue
    return None


def upsert_issue(token: str, repo: str, title: str, body: str) -> dict[str, Any]:
    existing = find_issue_by_title(token, repo, title)
    if existing is None:
        return github_request(
            token,
            repo,
            "/issues",
            method="POST",
            payload={"title": title, "body": body},
        )
    return github_request(
        token,
        repo,
        f"/issues/{existing['number']}",
        method="PATCH",
        payload={"title": title, "body": body},
    )


def create_follow_up_issue(
    token: str,
    repo: str,
    current: dict[str, Any],
    changes: list[str],
) -> None:
    if not changes:
        return
    date_prefix = str(current["generated_at"]).split("T", 1)[0]
    title = f"Capacity drift: {date_prefix} {current['host_id']}"
    if find_issue_by_title(token, repo, title) is not None:
        return
    body = "\n".join(
        [
            "# Capacity drift follow-up",
            "",
            f"- Host: `{current['host_id']}`",
            f"- Recommended node class: `{current['recommended_node_class']}`",
            f"- Current limiting factor: `{current['current_limiting_factor']}`",
            "",
            "## Changes",
            "",
        ]
        + [f"- {change}" for change in changes]
    )
    github_request(
        token,
        repo,
        "/issues",
        method="POST",
        payload={"title": title, "body": body},
    )


def cmd_sync_issue(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    current = load_json(Path(args.summary_json))
    token = args.github_token
    previous = load_previous_summary(
        token,
        args.repo,
        config["workflow_file"],
        config["base_branch"],
        str(current["run_id"]),
        config["artifact_name"],
    )
    changes = detect_changes(
        current,
        previous,
        float(config.get("material_change_threshold_pct", 25)),
    )
    markdown = build_summary_markdown(current, previous, changes)
    write_text(Path(args.summary_md), markdown)
    tracker = upsert_issue(token, args.repo, config["tracker_issue_title"], markdown)
    material_change = any(
        not change.startswith("No previous scheduled capacity summary")
        for change in changes
    )
    if material_change:
        create_follow_up_issue(token, args.repo, current, changes)
    emit_outputs(
        {
            "tracker_issue_number": str(tracker["number"]),
            "material_change": "true" if material_change else "false",
        },
        args.github_output,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    write_summary = subparsers.add_parser("write-summary")
    write_summary.add_argument("--config", required=True)
    write_summary.add_argument("--capacity-json", required=True)
    write_summary.add_argument("--out-json", required=True)
    write_summary.add_argument("--out-md", required=True)
    write_summary.add_argument("--run-id", required=True)
    write_summary.add_argument("--run-attempt", required=True)
    write_summary.add_argument("--event-name", required=True)
    write_summary.add_argument("--ref-name", required=True)
    write_summary.add_argument("--sha", required=True)
    write_summary.add_argument("--generated-at")
    write_summary.set_defaults(func=cmd_write_summary)

    sync_issue = subparsers.add_parser("sync-issue")
    sync_issue.add_argument("--config", required=True)
    sync_issue.add_argument("--summary-json", required=True)
    sync_issue.add_argument("--summary-md", required=True)
    sync_issue.add_argument("--repo", required=True)
    sync_issue.add_argument("--github-token", required=True)
    sync_issue.add_argument("--github-output")
    sync_issue.set_defaults(func=cmd_sync_issue)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
