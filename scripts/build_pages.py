from __future__ import annotations

import argparse
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SITE_ROOT = REPO_ROOT / "site"
RUSTDOC_ROOT = REPO_ROOT / "target" / "doc"
RUSTDOC_RESERVED = {"search.desc", "search-index.js", "settings.html", "src", "static.files"}


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


def build_site(out_dir: Path) -> None:
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    build_site(out_dir)
    print(f"Pages site assembled at {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
