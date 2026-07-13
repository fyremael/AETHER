from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request


def iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def verify_payload(payload: dict[str, Any], expected_sha: str) -> tuple[bool, str]:
    observed = str(payload.get("source_sha", ""))
    valid = (
        payload.get("schema_version") == "aether-pages-source-v1"
        and observed == expected_sha
    )
    return valid, observed


def read_site_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_payload(url: str, timeout_seconds: float) -> dict[str, Any]:
    with request.urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the exact source SHA embedded in a built or deployed Pages site."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--site-file", type=Path)
    source.add_argument("--url")
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--attempts", type=int, default=6)
    parser.add_argument("--delay-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    attempts = 1 if args.site_file else max(1, args.attempts)
    history: list[dict[str, Any]] = []
    valid = False
    observed_sha = ""

    for index in range(1, attempts + 1):
        entry: dict[str, Any] = {"attempt": index, "checked_at": iso_now()}
        try:
            payload = (
                read_site_file(args.site_file)
                if args.site_file
                else fetch_payload(args.url, args.timeout_seconds)
            )
            valid, observed_sha = verify_payload(payload, args.expected_sha)
            entry.update(
                {
                    "outcome": "match" if valid else "sha_mismatch",
                    "observed_sha": observed_sha,
                }
            )
        except (OSError, ValueError, json.JSONDecodeError, error.URLError) as exc:
            entry.update({"outcome": "fetch_failed", "error": str(exc)})
        history.append(entry)
        if valid:
            break
        if index < attempts:
            time.sleep(max(0.0, args.delay_seconds))

    result = {
        "schema_version": "aether-pages-verification-v1",
        "source": str(args.site_file) if args.site_file else args.url,
        "expected_sha": args.expected_sha,
        "observed_sha": observed_sha or None,
        "valid": valid,
        "attempt_history": history,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.out)
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
