from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_REPO_URL = "https://github.com/fyremael/AETHER"
DEFAULT_COLAB_ROOT = Path("/content/AETHER")


@dataclass
class NotebookService:
    repo_root: Path
    base_url: str
    process: subprocess.Popen[str]


def bootstrap_repo(
    repo_url: str = DEFAULT_REPO_URL,
    *,
    repo_root: str | os.PathLike[str] | None = None,
) -> Path:
    root = _discover_repo_root()
    if root is None:
        root = Path(repo_root) if repo_root is not None else DEFAULT_COLAB_ROOT
        if not _looks_like_repo(root):
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(root)],
                check=True,
            )
    _ensure_python_path(root)
    return root


def ensure_rust_toolchain() -> None:
    if shutil.which("cargo") is not None:
        return

    subprocess.run(
        [
            "bash",
            "-lc",
            "curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal",
        ],
        check=True,
    )
    cargo_bin = Path.home() / ".cargo" / "bin"
    os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    rustup = cargo_bin / ("rustup.exe" if os.name == "nt" else "rustup")
    if rustup.exists():
        subprocess.run([str(rustup), "default", "stable"], check=True)


def start_http_service(
    repo_root: str | os.PathLike[str],
    *,
    host: str = "127.0.0.1",
    port: int | None = None,
) -> NotebookService:
    root = Path(repo_root)
    _ensure_python_path(root)

    service_port = port or _free_port()
    base_url = f"http://{host}:{service_port}"
    env = os.environ.copy()
    cargo_bin = Path.home() / ".cargo" / "bin"
    if cargo_bin.exists():
        env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"

    process = subprocess.Popen(
        [
            "cargo",
            "run",
            "-p",
            "aether_api",
            "--example",
            "http_kernel_service",
            "--",
            f"{host}:{service_port}",
        ],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    _wait_for_health(base_url, process)
    return NotebookService(repo_root=root, base_url=base_url, process=process)


def stop_http_service(service: NotebookService) -> None:
    service.process.terminate()
    try:
        service.process.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10.0)


def pretty_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _discover_repo_root() -> Path | None:
    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if _looks_like_repo(candidate):
            return candidate
    if _looks_like_repo(DEFAULT_COLAB_ROOT):
        return DEFAULT_COLAB_ROOT
    return None


def _looks_like_repo(path: Path) -> bool:
    return (path / "Cargo.toml").exists() and (path / "python").exists()


def _ensure_python_path(repo_root: Path) -> None:
    python_root = repo_root / "python"
    if str(python_root) not in sys.path:
        sys.path.insert(0, str(python_root))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_health(
    base_url: str,
    process: subprocess.Popen[str],
    timeout_seconds: float = 180.0,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(
                "AETHER HTTP example exited before it became healthy:\n"
                f"{output}"
            )
        try:
            with request.urlopen(f"{base_url}/health", timeout=1.0) as response:
                if response.status == 200:
                    return
        except (error.URLError, TimeoutError):
            time.sleep(1.0)

    output = process.stdout.read() if process.stdout else ""
    raise RuntimeError(
        "AETHER HTTP example did not become healthy in time:\n"
        f"{output}"
    )
