from __future__ import annotations

import argparse
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SITE_ROOT = REPO_ROOT / "site"
RUSTDOC_ROOT = REPO_ROOT / "target" / "doc"


def copy_tree(source: Path, destination: Path) -> None:
    shutil.copytree(source, destination, dirs_exist_ok=True)


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
