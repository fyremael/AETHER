#!/usr/bin/env python3
"""Select the fast PR boundaries affected by a set of changed paths."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


RELEASE_CONTROL_PREFIXES = (
    ".github/workflows/",
    "fixtures/release/",
    "schemas/release/",
    "scripts/",
    "requirements-release.txt",
)


def select_scopes(paths: Iterable[str]) -> dict[str, bool]:
    normalized = {Path(path).as_posix().removeprefix("./") for path in paths if path}
    shared = any(path.startswith(RELEASE_CONTROL_PREFIXES) for path in normalized)
    return {
        "rust": shared
        or any(
            path in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml"}
            or path.startswith("crates/")
            for path in normalized
        ),
        "go": shared
        or any(path in {"go.mod", "go.sum"} or path.startswith("go/") for path in normalized),
        "python": shared
        or any(
            path in {"pyproject.toml", "requirements.txt"}
            or path.startswith(("python/", "notebooks/"))
            for path in normalized
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths-file", required=True)
    parser.add_argument("--github-output", required=True)
    args = parser.parse_args()
    selected = select_scopes(Path(args.paths_file).read_text(encoding="utf-8").splitlines())
    with Path(args.github_output).open("a", encoding="utf-8") as output:
        for name, enabled in selected.items():
            output.write(f"{name}={str(enabled).lower()}\n")
    print(selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
