#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir/.." rev-parse --show-toplevel)"
candidate="${1:-$(git -C "$repo_root" rev-parse HEAD)}"
auth_provider="${2:-oauth2}"

if [[ ! "$candidate" =~ ^[0-9a-f]{40}$ ]]; then
  echo "candidate must be a full lowercase commit SHA" >&2
  exit 2
fi
if ! command -v colab >/dev/null 2>&1; then
  echo "colab CLI is missing; install with: uv tool install google-colab-cli==0.6.0" >&2
  exit 2
fi

tree="$(git -C "$repo_root" rev-parse "${candidate}^{tree}")"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
session="aether-${candidate:0:8}-${timestamp,,}"
output_dir="$repo_root/artifacts/colab/runs/$candidate/$timestamp"
archive="$output_dir/aether-colab-diagnostic.zip"
summary="$output_dir/summary.json"
receipt="$output_dir/receipt.json"
mkdir -p "$output_dir"

session_may_exist=0
stop_session() {
  if [[ "$session_may_exist" == "1" ]]; then
    colab --auth="$auth_provider" stop -s "$session" >/dev/null 2>&1 || true
    session_may_exist=0
  fi
}
trap stop_session EXIT INT TERM

colab --auth="$auth_provider" whoami >/dev/null
session_may_exist=1
colab --auth="$auth_provider" run \
  --keep \
  -s "$session" \
  --timeout 3600 \
  "$repo_root/scripts/colab_runtime_diagnostic.py" \
  "$candidate" \
  > >(tee "$output_dir/colab-run.stdout.txt") \
  2> >(tee "$output_dir/colab-run.stderr.txt" >&2)

colab --auth="$auth_provider" download -s "$session" \
  /content/aether-colab-diagnostic.zip "$archive"
colab --auth="$auth_provider" download -s "$session" \
  /content/aether-colab-diagnostic/summary.json "$summary"
stop_session
colab --auth="$auth_provider" sessions >"$output_dir/sessions-after.txt"
if grep -Fq -- "$session" "$output_dir/sessions-after.txt"; then
  echo "Colab session remained active after explicit stop: $session" >&2
  exit 1
fi

python3 - "$summary" "$candidate" "$tree" "$session" "$archive" "$receipt" <<'PY'
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

summary_path, expected_commit, expected_tree, session, archive_path, receipt_path = sys.argv[1:]
summary_bytes = Path(summary_path).read_bytes()
summary = json.loads(summary_bytes)
if summary.get("qualification_status") != "diagnostic_only":
    raise SystemExit("Colab output attempted to acquire qualification authority")
if summary.get("policy", {}).get("commercial_beta_authority") is not False:
    raise SystemExit("Colab output attempted to acquire commercial beta authority")
if summary.get("candidate") != {
    "commit_sha": expected_commit,
    "tree_sha": expected_tree,
}:
    raise SystemExit("Colab output candidate identity mismatch")

archive_bytes = Path(archive_path).read_bytes()
receipt = {
    "schema_version": "aether.colab-diagnostic-receipt.v1",
    "qualification_status": "diagnostic_only",
    "commercial_beta_authority": False,
    "candidate": {"commit_sha": expected_commit, "tree_sha": expected_tree},
    "session": session,
    "artifacts": {
        "archive": {
            "path": str(Path(archive_path)),
            "byte_size": len(archive_bytes),
            "sha256": hashlib.sha256(archive_bytes).hexdigest(),
        },
        "summary": {
            "path": str(Path(summary_path)),
            "byte_size": len(summary_bytes),
            "sha256": hashlib.sha256(summary_bytes).hexdigest(),
        },
    },
}
Path(receipt_path).write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
PY

echo "Colab diagnostic complete: $receipt"
