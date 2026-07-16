from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SITE_ROOT = REPO_ROOT / "site"
RUSTDOC_ROOT = REPO_ROOT / "target" / "doc"
RUSTDOC_RESERVED = {"search.desc", "search-index.js", "settings.html", "src", "static.files"}
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")


def copy_tree(source: Path, destination: Path) -> None:
    shutil.copytree(source, destination, dirs_exist_ok=True)


def discover_crate_docs(api_root: Path) -> list[str]:
    crates = []
    for child in sorted(api_root.iterdir()):
        if child.name in RUSTDOC_RESERVED:
            continue
        if child.is_dir() and (child / "index.html").exists():
            crates.append(child.name)
    return crates


def write_api_index(api_root: Path) -> None:
    index_path = api_root / "index.html"
    if index_path.exists():
        return

    crates = discover_crate_docs(api_root)
    links = "\n".join(
        f'          <li><a href="./{crate}/index.html"><code>{crate}</code></a></li>'
        for crate in crates
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>AETHER API Reference</title>
    <meta
      name="description"
      content="Generated Rust API reference for the AETHER workspace."
    />
    <link rel="stylesheet" href="../styles.css" />
  </head>
  <body>
    <div class="page-shell">
      <main class="hero compact-hero">
        <p class="eyebrow">AETHER API Reference</p>
        <h1>Generated crate docs for the semantic kernel.</h1>
        <p class="lede">
          The workspace is published crate by crate because this repository uses
          a virtual workspace root. Choose a crate below or return to the
          documentation portal.
        </p>
        <div class="hero-actions">
          <a class="button button-primary" href="../index.html">Open docs portal</a>
          <a
            class="button button-secondary"
            href="https://github.com/fyremael/AETHER/blob/main/docs/ARCHITECTURE.md"
            >Read architecture guide</a
          >
        </div>
      </main>

      <section class="section">
        <div class="section-heading">
          <p class="kicker">Crates</p>
          <h2>Workspace API surfaces</h2>
        </div>
        <div class="showcase-card">
          <ul class="api-crate-list">
{links}
          </ul>
        </div>
      </section>
    </div>
  </body>
</html>
"""
    index_path.write_text(html, encoding="utf-8")


def resolve_candidate_sha(explicit: str | None = None) -> str:
    candidate = explicit or os.environ.get("GITHUB_SHA")
    if not candidate:
        candidate = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
        ).strip()
    candidate = candidate.lower()
    if not FULL_SHA.fullmatch(candidate):
        raise ValueError("Pages candidate SHA must be a full 40-character commit SHA")
    return candidate


def workspace_version() -> str:
    cargo = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    return str(cargo["workspace"]["package"]["version"])


def write_source_identity(out_dir: Path, *, candidate_sha: str, version: str) -> None:
    metadata = {
        "schema_version": "aether-pages-source-v1",
        "source_sha": candidate_sha,
        "source_ref": os.environ.get("GITHUB_REF"),
        "version": version,
    }
    (out_dir / "source-version.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    index_path = out_dir / "index.html"
    index = index_path.read_text(encoding="utf-8")
    marker = "      </footer>"
    if marker not in index:
        raise ValueError("site index is missing the footer source-identity insertion point")
    identity = (
        "        <p class=\"source-identity\">\n"
        f"          AETHER {html.escape(version)} · source "
        f"<code>{candidate_sha}</code>\n"
        "        </p>\n"
    )
    index_path.write_text(index.replace(marker, identity + marker, 1), encoding="utf-8")


def build_site(
    out_dir: Path,
    *,
    candidate_sha: str | None = None,
    version: str | None = None,
) -> None:
    if not SITE_ROOT.exists():
        raise FileNotFoundError(f"site assets directory not found: {SITE_ROOT}")
    if not RUSTDOC_ROOT.exists():
        raise FileNotFoundError(
            f"Rust API docs not found at {RUSTDOC_ROOT}. Run `cargo doc --workspace --no-deps` first."
        )

    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    copy_tree(SITE_ROOT, out_dir)
    copy_tree(RUSTDOC_ROOT, out_dir / "api")
    write_api_index(out_dir / "api")
    write_source_identity(
        out_dir,
        candidate_sha=resolve_candidate_sha(candidate_sha),
        version=version or workspace_version(),
    )
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble the GitHub Pages site from static portal assets and generated rustdoc."
    )
    parser.add_argument(
        "--out-dir",
        default="_site",
        help="Output directory for the assembled Pages site.",
    )
    parser.add_argument("--candidate-sha", help="Full source commit SHA embedded in the site.")
    parser.add_argument("--version", help="Displayed AETHER version; defaults to Cargo workspace version.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    build_site(out_dir, candidate_sha=args.candidate_sha, version=args.version)
    print(f"Pages site assembled at {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
