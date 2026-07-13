from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_inventory(
    root: Path,
    *,
    required_paths: list[str],
    required_globs: list[str],
) -> dict[str, object]:
    files = []
    directories = []
    if root.exists():
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(root).as_posix()
            if path.is_dir():
                directories.append(relative)
            elif path.is_file():
                files.append(
                    {
                        "path": relative,
                        "bytes": path.stat().st_size,
                        "sha256": sha256_file(path),
                    }
                )

    missing_paths = [
        required for required in required_paths if not (root / required).is_file()
    ]
    missing_globs = [pattern for pattern in required_globs if not list(root.glob(pattern))]
    return {
        "schema_version": "capacity-artifact-inventory-v1",
        "candidate_sha": os.environ.get("GITHUB_SHA"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "root": str(root),
        "required_paths": sorted(required_paths),
        "required_globs": sorted(required_globs),
        "missing_paths": sorted(missing_paths),
        "missing_globs": sorted(missing_globs),
        "directories": directories,
        "files": files,
        "valid": not missing_paths and not missing_globs,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record and assert the downloaded Capacity Planning artifact layout."
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--required", action="append", default=[])
    parser.add_argument("--required-glob", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inventory = build_inventory(
        args.root,
        required_paths=args.required,
        required_globs=args.required_glob,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.out)
    if not inventory["valid"]:
        print(
            "capacity artifact layout is invalid: "
            f"missing paths={inventory['missing_paths']}, "
            f"missing globs={inventory['missing_globs']}"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
