#!/usr/bin/env python3
"""Select fast language boundaries and protected-main product integration."""

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

PRODUCT_INTEGRATION_EXEMPT_FILES = frozenset(
    {
        ".github/dependabot.yml",
        ".github/hardening-promotion-state.json",
        ".github/workflows/release-readiness.yml",
        ".github/workflows/reusable-exact-candidate-evidence.yml",
        "fixtures/release/gate-policy.json",
        "python/tests/test_release_evidence.py",
        "python/tests/test_release_preflight.py",
        "python/tests/test_release_subjects.py",
        "requirements-release.txt",
        "scripts/release_evidence.py",
        "scripts/release_preflight.py",
        "scripts/release_qualification.py",
        "scripts/release_subjects.py",
        "scripts/run-release-readiness.ps1",
        "scripts/verify_release_evidence.py",
    }
)
PRODUCT_INTEGRATION_EXEMPT_PREFIXES = (
    "docs/",
    "schemas/release/",
)


def product_integration_required(paths: set[str]) -> bool:
    """Fail closed unless every changed path is explicitly non-product."""

    if not paths:
        return True
    return not all(
        path in PRODUCT_INTEGRATION_EXEMPT_FILES
        or path.startswith(PRODUCT_INTEGRATION_EXEMPT_PREFIXES)
        for path in paths
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
        "product_integration": product_integration_required(normalized),
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
