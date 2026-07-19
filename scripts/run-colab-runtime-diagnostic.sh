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
teardown_session() {
  if [[ "$session_may_exist" == "1" ]]; then
    local teardown_failed=0
    if ! colab --auth="$auth_provider" stop -s "$session" \
      >"$output_dir/colab-stop.stdout.txt" \
      2>"$output_dir/colab-stop.stderr.txt"; then
      teardown_failed=1
    fi
    if colab --auth="$auth_provider" sessions >"$output_dir/sessions-after.txt"; then
      if grep -Fq -- "$session" "$output_dir/sessions-after.txt"; then
        teardown_failed=1
      else
        session_may_exist=0
        teardown_failed=0
      fi
    else
      teardown_failed=1
    fi
    if [[ "$teardown_failed" == "1" ]]; then
      echo "failed to prove Colab session teardown: $session" >&2
      return 1
    fi
  fi
}

cleanup_session() {
  local status=$?
  trap - EXIT INT TERM
  if ! teardown_session && [[ "$status" == "0" ]]; then
    status=1
  fi
  exit "$status"
}
trap cleanup_session EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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
teardown_session

python3 "$repo_root/scripts/verify_colab_runtime_diagnostic.py" \
  --summary "$summary" \
  --candidate "$candidate" \
  --tree "$tree" \
  --session "$session" \
  --archive "$archive" \
  --receipt "$receipt"

echo "Colab diagnostic complete: $receipt"
