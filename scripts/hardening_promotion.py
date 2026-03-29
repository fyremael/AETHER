#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from copy import deepcopy
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


def load_config(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    if "groups" not in payload or "promotion_order" not in payload:
        raise SystemExit(f"invalid hardening promotion config: {path}")
    return payload


def parse_pack_status_args(values: list[str]) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise SystemExit(f"invalid --pack-status value: {item}")
        pack, status = item.split("=", 1)
        statuses[pack.strip()] = status.strip()
    return statuses


def cmd_ci_outputs(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    groups = config["groups"]
    outputs: dict[str, str] = {}
    blocking_groups: list[str] = []
    for name in config["promotion_order"]:
        blocking = bool(groups[name]["blocking"])
        outputs[f"{name}_blocking"] = "true" if blocking else "false"
        if blocking:
            blocking_groups.append(name)
    outputs["blocking_groups"] = ",".join(blocking_groups)
    emit_outputs(outputs, args.github_output)
    return 0


def cmd_write_run_metrics(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    statuses = parse_pack_status_args(args.pack_status)
    ordered_statuses = {
        name: statuses.get(name, "missing")
        for name in config["promotion_order"]
    }
    payload = {
        "generated_at": args.generated_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "event_name": args.event_name,
        "ref_name": args.ref_name,
        "sha": args.sha,
        "workflow_file": config["workflow_file"],
        "pack_statuses": ordered_statuses,
    }
    markdown_lines = [
        "# QA Hardening Promotion Run",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Workflow run: `{payload['run_id']}` attempt `{payload['run_attempt']}`",
        f"- Event: `{payload['event_name']}` on `{payload['ref_name']}`",
        f"- Commit: `{payload['sha']}`",
        "",
        "| Group | Status |",
        "| --- | --- |",
    ]
    for name in config["promotion_order"]:
        markdown_lines.append(f"| `{name}` | `{ordered_statuses[name]}` |")
    markdown_lines.append("")

    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), "\n".join(markdown_lines))
    return 0


def github_request_json(
    token: str, repo: str, path: str, query: dict[str, str] | None = None
) -> Any:
    query_string = ""
    if query:
        query_string = "?" + urllib.parse.urlencode(query)
    url = f"https://api.github.com/repos/{repo}{path}{query_string}"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aether-hardening-promotion",
        },
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def github_download_artifact(token: str, artifact_url: str) -> bytes:
    request = urllib.request.Request(
        artifact_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aether-hardening-promotion",
        },
    )
    with urllib.request.urlopen(request) as response:
        return response.read()


def load_prior_run_metrics(
    token: str,
    repo: str,
    workflow_file: str,
    base_branch: str,
    current_run_id: str,
) -> list[dict[str, Any]]:
    runs_payload = github_request_json(
        token,
        repo,
        f"/actions/workflows/{workflow_file}/runs",
        {
            "branch": base_branch,
            "event": "schedule",
            "status": "completed",
            "per_page": "20",
        },
    )
    metrics: list[dict[str, Any]] = []
    for run in runs_payload.get("workflow_runs", []):
        run_id = str(run["id"])
        if run_id == str(current_run_id):
            continue
        artifacts = github_request_json(
            token,
            repo,
            f"/actions/runs/{run_id}/artifacts",
            {"per_page": "100"},
        )
        artifact = next(
            (
                item
                for item in artifacts.get("artifacts", [])
                if item.get("name") == "qa-hardening-metrics"
                and not item.get("expired", False)
            ),
            None,
        )
        if artifact is None:
            continue
        try:
            archive_bytes = github_download_artifact(
                token, artifact["archive_download_url"]
            )
        except urllib.error.HTTPError:
            continue
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            json_member = next(
                (
                    name
                    for name in archive.namelist()
                    if name.endswith("promotion-run.json")
                ),
                None,
            )
            if json_member is None:
                continue
            with archive.open(json_member) as handle:
                metrics.append(json.load(handle))
    return metrics


def compute_streaks(
    promotion_order: list[str], metrics: list[dict[str, Any]]
) -> dict[str, int]:
    streaks = {name: 0 for name in promotion_order}
    for name in promotion_order:
        count = 0
        for metric in metrics:
            if metric.get("pack_statuses", {}).get(name) == "passed":
                count += 1
            else:
                break
        streaks[name] = count
    return streaks


def build_tracker_markdown(
    config: dict[str, Any],
    current_metrics: dict[str, Any],
    streaks: dict[str, int],
    next_group: str | None,
) -> str:
    minimum = config["minimum_consecutive_scheduled_green_runs"]
    lines = [
        "# QA hardening promotion tracker",
        "",
        "This issue is maintained by automation and tracks when hardening packs",
        "are eligible to move from diagnostic-only sweeps into blocking CI.",
        "",
        f"- Workflow: `{config['workflow_file']}`",
        f"- Promotion threshold: `{minimum}` consecutive scheduled green runs plus one successful local/manual validation",
        f"- Last evaluated run: `{current_metrics['run_id']}` on `{current_metrics['ref_name']}` via `{current_metrics['event_name']}`",
        f"- Commit: `{current_metrics['sha']}`",
        "",
        "## Current run",
        "",
        "| Group | Current status | Scheduled green streak | Manual validation | Blocking CI |",
        "| --- | --- | --- | --- | --- |",
    ]
    for name in config["promotion_order"]:
        group = config["groups"][name]
        manual = "yes" if group["manual_validation"]["confirmed"] else "no"
        blocking = "yes" if group["blocking"] else "no"
        lines.append(
            f"| `{name}` | `{current_metrics['pack_statuses'].get(name, 'missing')}` | "
            f"`{streaks[name]}/{minimum}` | `{manual}` | `{blocking}` |"
        )
    lines.extend(["", "## Promotion order", ""])
    for index, name in enumerate(config["promotion_order"], start=1):
        group = config["groups"][name]
        prefix = "next" if next_group == name else f"{index}."
        lines.append(
            f"{prefix} `{name}`: {group['label']} -> {group['workflow_target']}"
        )
    lines.append("")
    if next_group:
        group = config["groups"][next_group]
        lines.extend(
            [
                "## Next eligible promotion",
                "",
                (
                    f"`{next_group}` is eligible for promotion. The scheduled green streak "
                    f"has reached `{streaks[next_group]}/{minimum}` and manual validation is already confirmed."
                ),
                "",
                f"- Target: `{group['workflow_target']}`",
                f"- Proposed branch prefix: `codex/promote-hardening-{next_group}-<run-id>`",
                "",
            ]
        )
    else:
        next_pending = next(
            (
                name
                for name in config["promotion_order"]
                if not config["groups"][name]["blocking"]
            ),
            None,
        )
        lines.extend(
            [
                "## Next eligible promotion",
                "",
                (
                    "No new promotion is ready yet."
                    if next_pending is None
                    else f"`{next_pending}` remains the next candidate, but it has not met the promotion threshold yet."
                ),
                "",
            ]
        )
    return "\n".join(lines)


def build_promotion_issue_markdown(
    config: dict[str, Any],
    group_name: str,
    streaks: dict[str, int],
) -> str:
    group = config["groups"][group_name]
    minimum = config["minimum_consecutive_scheduled_green_runs"]
    manual = group["manual_validation"]
    return textwrap.dedent(
        f"""\
        The `{group_name}` hardening pack is ready to move into blocking `{group['workflow_target']}`.

        Promotion evidence:

        - scheduled green streak: `{streaks[group_name]}/{minimum}`
        - manual/local validation: `{manual['confirmed_at']}` via `{manual['evidence']}`
        - workflow target: `{group['workflow_target']}`

        Expected repo action:

        - update `.github/hardening-promotion-state.json` to set `{group_name}` blocking to `true`
        - allow the resulting PR to prove the new blocking job in `CI`
        - keep the remaining packs in diagnostic mode until they satisfy the same criteria
        """
    )


def cmd_evaluate_streaks(args: argparse.Namespace) -> int:
    config = load_config(Path(args.config))
    current_metrics = load_json(Path(args.current_metrics))
    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(f"environment variable {args.token_env} is required")

    prior_metrics = load_prior_run_metrics(
        token=token,
        repo=args.repo,
        workflow_file=config["workflow_file"],
        base_branch=config["base_branch"],
        current_run_id=str(current_metrics["run_id"]),
    )

    metrics_for_streak = []
    if (
        current_metrics.get("event_name") == "schedule"
        and current_metrics.get("ref_name") == config["base_branch"]
    ):
        metrics_for_streak.append(current_metrics)
    metrics_for_streak.extend(prior_metrics)

    streaks = compute_streaks(config["promotion_order"], metrics_for_streak)
    next_group = None
    for name in config["promotion_order"]:
        group = config["groups"][name]
        if group["blocking"]:
            continue
        if (
            group["manual_validation"]["confirmed"]
            and streaks[name] >= config["minimum_consecutive_scheduled_green_runs"]
        ):
            next_group = name
        break

    tracker_markdown = build_tracker_markdown(
        config=config,
        current_metrics=current_metrics,
        streaks=streaks,
        next_group=next_group,
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "current_run": current_metrics,
        "scheduled_green_streaks": streaks,
        "next_eligible_group": next_group,
        "tracker_issue_title": config["tracker_issue_title"],
        "promotion_issue_title": (
            f"Promote {next_group} hardening checks into blocking CI"
            if next_group
            else ""
        ),
        "promotion_issue_body": (
            build_promotion_issue_markdown(config, next_group, streaks)
            if next_group
            else ""
        ),
    }

    write_json(Path(args.out_json), payload)
    write_text(Path(args.out_md), tracker_markdown)

    outputs = {
        "tracker_issue_title": config["tracker_issue_title"],
        "next_eligible_group": next_group or "",
        "promotion_issue_title": payload["promotion_issue_title"],
        "should_open_promotion_issue": "true" if next_group else "false",
    }
    emit_outputs(outputs, args.github_output)
    return 0


def cmd_apply_promotion(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_config(config_path)
    group_name = args.group
    if group_name not in config["groups"]:
        raise SystemExit(f"unknown group: {group_name}")
    updated = deepcopy(config)
    updated["groups"][group_name]["blocking"] = True
    updated["groups"][group_name]["promoted_at"] = args.promoted_at or datetime.now(
        timezone.utc
    ).replace(microsecond=0).isoformat()
    write_json(config_path, updated)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AETHER hardening promotion helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ci_outputs = subparsers.add_parser("ci-outputs")
    ci_outputs.add_argument("--config", required=True)
    ci_outputs.add_argument("--github-output")
    ci_outputs.set_defaults(func=cmd_ci_outputs)

    write_run = subparsers.add_parser("write-run-metrics")
    write_run.add_argument("--config", required=True)
    write_run.add_argument("--out-json", required=True)
    write_run.add_argument("--out-md", required=True)
    write_run.add_argument("--run-id", required=True)
    write_run.add_argument("--run-attempt", required=True)
    write_run.add_argument("--event-name", required=True)
    write_run.add_argument("--ref-name", required=True)
    write_run.add_argument("--sha", required=True)
    write_run.add_argument("--generated-at")
    write_run.add_argument(
        "--pack-status", action="append", default=[], metavar="GROUP=STATUS"
    )
    write_run.set_defaults(func=cmd_write_run_metrics)

    evaluate = subparsers.add_parser("evaluate-streaks")
    evaluate.add_argument("--config", required=True)
    evaluate.add_argument("--repo", required=True)
    evaluate.add_argument("--token-env", default="GITHUB_TOKEN")
    evaluate.add_argument("--current-metrics", required=True)
    evaluate.add_argument("--out-json", required=True)
    evaluate.add_argument("--out-md", required=True)
    evaluate.add_argument("--github-output")
    evaluate.set_defaults(func=cmd_evaluate_streaks)

    apply_promotion = subparsers.add_parser("apply-promotion")
    apply_promotion.add_argument("--config", required=True)
    apply_promotion.add_argument("--group", required=True)
    apply_promotion.add_argument("--promoted-at")
    apply_promotion.set_defaults(func=cmd_apply_promotion)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
