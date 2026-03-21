from __future__ import annotations

import argparse
import contextlib
import http.server
import socket
import socketserver
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

from build_pages import REPO_ROOT, build_site


SITE_OUT_DEFAULT = REPO_ROOT / "artifacts" / "pages-preview-presentation"
PUBLISH_DEFAULT = REPO_ROOT / "site" / "assets" / "presentation"


EXPORTS = [
    ("showcase.html", "#social-card-fabric", "social-fabric-card.png"),
    ("showcase.html", "#social-card-pilot", "social-pilot-card.png"),
    ("showcase.html", "#social-card-proof", "social-proof-card.png"),
    ("showcase.html", "#cover-operational-truth", "cover-operational-truth.png"),
    ("showcase.html", "#cover-semantic-fabric", "cover-semantic-fabric.png"),
    ("showcase.html", "#cover-pilot-proof", "cover-pilot-proof.png"),
    ("showcase.html", "#shot-demo-proof", "screenshot-demo-proof.png"),
    ("showcase.html", "#shot-report-summary", "screenshot-report-summary.png"),
    ("showcase.html", "#shot-audit-proof", "screenshot-audit-proof.png"),
]


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def find_free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def serve_directory(directory: Path, port: int) -> socketserver.TCPServer:
    handler = lambda *args, **kwargs: QuietHandler(*args, directory=str(directory), **kwargs)
    httpd: socketserver.TCPServer = socketserver.TCPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def export_assets(site_dir: Path, publish_dir: Path) -> None:
    publish_dir.mkdir(parents=True, exist_ok=True)
    port = find_free_port()
    server = serve_directory(site_dir, port)
    base_url = f"http://127.0.0.1:{port}"

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": 1600, "height": 1200}, device_scale_factor=2)

            for html_path, selector, filename in EXPORTS:
                page.goto(f"{base_url}/{html_path}", wait_until="networkidle")
                locator = page.locator(selector)
                locator.scroll_into_view_if_needed()
                locator.screenshot(path=str(publish_dir / filename))

            browser.close()
    finally:
        server.shutdown()
        server.server_close()


def write_manifest(publish_dir: Path) -> None:
    lines = [
        "# AETHER Presentation Assets",
        "",
        "Generated export set for social, slides, and customer-facing proof surfaces.",
        "",
        "## Files",
        "",
    ]
    for _, _, filename in EXPORTS:
        lines.append(f"- `{filename}`")
    (publish_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build exportable presentation assets from the AETHER Pages showcase surfaces."
    )
    parser.add_argument(
        "--site-out",
        default=str(SITE_OUT_DEFAULT),
        help="Temporary assembled Pages site directory.",
    )
    parser.add_argument(
        "--publish-dir",
        default=str(PUBLISH_DEFAULT),
        help="Directory to write exported PNG assets.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    site_out = Path(args.site_out)
    if not site_out.is_absolute():
        site_out = REPO_ROOT / site_out
    publish_dir = Path(args.publish_dir)
    if not publish_dir.is_absolute():
        publish_dir = REPO_ROOT / publish_dir

    build_site(site_out)
    export_assets(site_out, publish_dir)
    write_manifest(publish_dir)
    print(f"Presentation assets written to {publish_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
