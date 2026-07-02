from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request


DEFAULT_REPO_URL = "https://github.com/fyremael/AETHER"
DEFAULT_COLAB_ROOT = Path("/content/AETHER")
PILOT_EXAMPLE_NAME = "pilot_http_kernel_service"

_PILOT_SERVICE_CACHE: dict[tuple[str, str, int | None, str, str], "NotebookService"] = {}


@dataclass
class NotebookService:
    repo_root: Path
    base_url: str
    process: subprocess.Popen[str]
    bearer_token: str | None = None
    namespace: str | None = None
    scratch_dir: Path | None = None
    config_path: Path | None = None


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


def start_pilot_service(
    repo_root: str | os.PathLike[str],
    *,
    host: str = "127.0.0.1",
    port: int | None = None,
    namespace: str = "notebook",
    bearer_token: str = "notebook-pilot-token",
    reuse: bool = True,
    prefer_existing_binary: bool = True,
    verbose: bool = True,
) -> NotebookService:
    root = Path(repo_root).resolve()
    _ensure_python_path(root)

    cache_key = (str(root), host, port, namespace, bearer_token)
    if reuse:
        cached = _PILOT_SERVICE_CACHE.get(cache_key)
        if cached is not None and _service_is_healthy(cached):
            if verbose:
                print(f"Reusing AETHER pilot service at {cached.base_url}")
            return cached
        if cached is not None:
            stop_http_service(cached)

    service_port = port or _free_port()
    base_url = f"http://{host}:{service_port}"
    scratch_dir = Path(tempfile.mkdtemp(prefix="aether-notebook-pilot-"))
    config_dir = scratch_dir / "config"
    data_root = scratch_dir / "namespaces"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)

    token_path = config_dir / "pilot-operator.token"
    token_path.write_text(bearer_token, encoding="utf-8")
    config_path = config_dir / "pilot-service.json"
    config_path.write_text(
        json.dumps(
            {
                "config_version": "pilot-v2-colab",
                "schema_version": "v1",
                "service_mode": "single_node",
                "bind_addr": f"{host}:{service_port}",
                "storage": {
                    "kind": "sqlite",
                    "data_root": str(data_root),
                },
                "audit_log_path": str(scratch_dir / "audit.jsonl"),
                "auth": {
                    "revoked_token_ids": [],
                    "revoked_principal_ids": [],
                    "tokens": [
                        {
                            "principal": "notebook-operator",
                            "principal_id": "principal:notebook-operator",
                            "token_id": "token:notebook-operator",
                            "scopes": ["append", "query", "explain", "ops"],
                            "policy_context": {
                                "capabilities": ["executor", "memory_reader"],
                                "visibilities": [],
                            },
                            "token_file": token_path.name,
                            "namespaces": [namespace],
                            "revoked": False,
                        }
                    ],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["AETHER_PILOT_CONFIG"] = str(config_path)
    cargo_bin = Path.home() / ".cargo" / "bin"
    if cargo_bin.exists():
        env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"

    command = _pilot_service_command(
        root,
        prefer_existing_binary=prefer_existing_binary,
    )
    if verbose:
        if _is_cargo_run_command(command):
            print(
                "Starting AETHER pilot service through Cargo. "
                "The first notebook launch may compile Rust; later launches reuse the built binary."
            )
        else:
            print(f"Starting AETHER pilot service from {command[0]}")

    try:
        process = subprocess.Popen(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        _wait_for_health(base_url, process)
    except Exception:
        shutil.rmtree(scratch_dir, ignore_errors=True)
        raise

    service = NotebookService(
        repo_root=root,
        base_url=base_url,
        process=process,
        bearer_token=bearer_token,
        namespace=namespace,
        scratch_dir=scratch_dir,
        config_path=config_path,
    )
    if reuse:
        _PILOT_SERVICE_CACHE[cache_key] = service
    return service


def stop_http_service(service: NotebookService) -> None:
    for key, cached in list(_PILOT_SERVICE_CACHE.items()):
        if cached is service:
            _PILOT_SERVICE_CACHE.pop(key, None)
    _stop_process(service)
    if service.scratch_dir is not None:
        shutil.rmtree(service.scratch_dir, ignore_errors=True)


def _stop_process(service: NotebookService) -> None:
    if service.process.poll() is not None:
        return
    service.process.terminate()
    try:
        service.process.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        service.process.kill()
        service.process.wait(timeout=10.0)


def pretty_json(value: Any, title: str | None = None) -> None:
    """Print a notebook-friendly summary followed by raw JSON."""

    if title:
        print(f"\n=== {title} ===")
    for line in describe_value(value):
        print(line)
    print("Raw JSON:")
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def describe_value(value: Any) -> list[str]:
    if isinstance(value, dict):
        if _looks_like_health(value):
            return [f"Health check: status={value.get('status', '<missing>')}."]
        if _looks_like_service_status(value):
            return _describe_service_status(value)
        if "datoms" in value and isinstance(value["datoms"], list):
            return _describe_history(value)
        if "rows" in value and isinstance(value["rows"], list):
            return _describe_query_result(value)
        if "query" in value or "queries" in value:
            return _describe_document_run(value)
        if "trace" in value and isinstance(value["trace"], dict):
            return _describe_trace(value["trace"])
        if _looks_like_coordination_report(value):
            return _describe_coordination_report(value)
        if _looks_like_coordination_delta(value):
            return _describe_coordination_delta(value)
        if "entries" in value and isinstance(value["entries"], list):
            return _describe_audit(value)
        if "matches" in value and "facts" in value:
            return _describe_vector_search(value)
        if "reference" in value and isinstance(value["reference"], dict):
            return _describe_artifact_reference(value["reference"])
        return [f"Object with {len(value)} top-level fields: {', '.join(sorted(value.keys()))}."]
    if isinstance(value, list):
        return _describe_list(value)
    return [f"Value: {value!r}."]


def _looks_like_health(value: dict[str, Any]) -> bool:
    return set(value.keys()) == {"status"} or (
        "status" in value and len(value) == 1
    )


def _looks_like_service_status(value: dict[str, Any]) -> bool:
    return (
        "service_mode" in value
        and "storage" in value
        and "principals" in value
    )


def _describe_service_status(value: dict[str, Any]) -> list[str]:
    storage = value.get("storage") or {}
    principals = value.get("principals") or []
    principal_bits = []
    for principal in principals[:3]:
        principal_bits.append(
            f"{principal.get('principal', '<unknown>')} "
            f"token={principal.get('token_id', '<none>')} "
            f"scopes={principal.get('scopes', [])}"
        )
    if len(principals) > 3:
        principal_bits.append(f"... {len(principals) - 3} more principal(s)")
    return [
        "Service status: authenticated pilot boundary is reachable.",
        f"Mode={value.get('service_mode')} namespace={value.get('effective_namespace')} "
        f"config={value.get('config_version')} schema={value.get('schema_version')}.",
        f"Storage backend={storage.get('backend')} data_root={storage.get('data_root')} "
        f"audit={storage.get('audit_log_path')}.",
        f"Active namespaces={value.get('active_namespace_count')} "
        f"known={value.get('namespaces', [])}.",
        f"Principals: {'; '.join(principal_bits) if principal_bits else 'none reported'}.",
    ]


def _describe_history(value: dict[str, Any]) -> list[str]:
    datoms = value["datoms"]
    op_counts: dict[str, int] = {}
    latest = None
    for datom in datoms:
        op = str(datom.get("op", "<missing>"))
        op_counts[op] = op_counts.get(op, 0) + 1
        element = datom.get("element")
        if isinstance(element, int):
            latest = max(latest or element, element)
    return [
        f"Journal history: {len(datoms)} datom(s), latest element={latest}.",
        f"Operation mix: {_format_counts(op_counts)}.",
    ]


def _describe_query_result(value: dict[str, Any]) -> list[str]:
    rows = value["rows"]
    lines = [f"Query result: {len(rows)} row(s)."]
    tuple_ids = [row.get("tuple_id") for row in rows if isinstance(row, dict)]
    tuple_ids = [tuple_id for tuple_id in tuple_ids if tuple_id is not None]
    if tuple_ids:
        lines.append(f"Tuple ids: {_preview(tuple_ids)}.")
    values = [row.get("values") for row in rows if isinstance(row, dict)]
    if values:
        lines.append(f"Row values preview: {_preview(values)}.")
    return lines


def _describe_document_run(value: dict[str, Any]) -> list[str]:
    lines = ["Document run: parsed, planned, evaluated, and returned by the service."]
    if isinstance(value.get("derived"), list):
        lines.append(f"Derived tuples materialized: {len(value['derived'])}.")
    query = value.get("query")
    if isinstance(query, dict) and isinstance(query.get("rows"), list):
        lines.append(f"Default query rows: {len(query['rows'])}.")
    queries = value.get("queries")
    if isinstance(queries, list):
        lines.append(f"Named queries returned: {len(queries)}.")
        for query_entry in queries[:5]:
            if not isinstance(query_entry, dict):
                continue
            result = query_entry.get("result") or {}
            rows = result.get("rows") if isinstance(result, dict) else None
            row_count = len(rows) if isinstance(rows, list) else "unknown"
            lines.append(f"- {query_entry.get('name', '<unnamed>')}: {row_count} row(s).")
    return lines


def _describe_trace(trace: dict[str, Any]) -> list[str]:
    tuples = trace.get("tuples") or []
    source_ids: set[Any] = set()
    for item in tuples:
        metadata = item.get("metadata", {}) if isinstance(item, dict) else {}
        for source_id in metadata.get("source_datom_ids", []) or []:
            source_ids.add(source_id)
    return [
        f"Explanation trace: root tuple={trace.get('root')} with {len(tuples)} tuple(s).",
        f"Source datoms referenced: {_preview(sorted(source_ids)) if source_ids else 'none reported'}.",
    ]


def _looks_like_coordination_report(value: dict[str, Any]) -> bool:
    return "history_len" in value and "current_authorized" in value and "claimable" in value


def _describe_coordination_report(value: dict[str, Any]) -> list[str]:
    sections = [
        "pre_heartbeat_authorized",
        "as_of_authorized",
        "live_heartbeats",
        "current_authorized",
        "claimable",
        "accepted_outcomes",
        "rejected_outcomes",
    ]
    counts = {
        section: len(value.get(section, []))
        for section in sections
    }
    return [
        f"Coordination report: history={value.get('history_len')} datom(s).",
        f"Section counts: {_format_counts(counts)}.",
        "Trace summary is included when at least one visible report row can be explained.",
    ]


def _looks_like_coordination_delta(value: dict[str, Any]) -> bool:
    return "left_history_len" in value and "right_history_len" in value


def _describe_coordination_delta(value: dict[str, Any]) -> list[str]:
    sections = [
        "current_authorized",
        "claimable",
        "live_heartbeats",
        "accepted_outcomes",
        "rejected_outcomes",
    ]
    lines = [
        "Coordination delta: compares two explicit semantic cuts.",
        f"History lengths: left={value.get('left_history_len')} right={value.get('right_history_len')}.",
    ]
    for section in sections:
        delta = value.get(section) or {}
        if not isinstance(delta, dict):
            continue
        lines.append(
            f"- {section}: added={len(delta.get('added', []))} "
            f"removed={len(delta.get('removed', []))} "
            f"changed={len(delta.get('changed', []))}."
        )
    return lines


def _describe_audit(value: dict[str, Any]) -> list[str]:
    entries = value["entries"]
    lines = [f"Audit log: {len(entries)} entries visible to this token."]
    for entry in entries[-3:]:
        if not isinstance(entry, dict):
            continue
        context = entry.get("context") or {}
        lines.append(
            f"- {entry.get('method')} {entry.get('path')} status={entry.get('status')} "
            f"principal={entry.get('principal')} namespace={context.get('namespace')} "
            f"intent={context.get('selected_report') or context.get('query_goal') or context.get('command_source')}."
        )
    return lines


def _describe_vector_search(value: dict[str, Any]) -> list[str]:
    matches = value.get("matches") or []
    facts = value.get("facts") or []
    lines = [
        f"Vector sidecar search: {len(matches)} match(es), {len(facts)} projected semantic fact(s)."
    ]
    if matches:
        first = matches[0]
        lines.append(
            f"Top match: vector={first.get('vector_id')} score={first.get('score')} "
            f"entity={first.get('entity')}."
        )
    return lines


def _describe_artifact_reference(reference: dict[str, Any]) -> list[str]:
    return [
        "Artifact sidecar reference: external memory metadata recovered under policy.",
        f"sidecar={reference.get('sidecar_id')} artifact={reference.get('artifact_id')} "
        f"uri={reference.get('uri')}.",
    ]


def _describe_list(value: list[Any]) -> list[str]:
    lines = [f"List output: {len(value)} item(s)."]
    if value and all(isinstance(item, dict) and "result" in item for item in value):
        lines.append("Named query list:")
        for item in value[:5]:
            result = item.get("result") or {}
            rows = result.get("rows") if isinstance(result, dict) else None
            row_count = len(rows) if isinstance(rows, list) else "unknown"
            lines.append(f"- {item.get('name', '<unnamed>')}: {row_count} row(s).")
    elif value:
        lines.append(f"Preview: {_preview(value)}.")
    return lines


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


def _preview(value: Any, limit: int = 3) -> str:
    if isinstance(value, list):
        shown = value[:limit]
        suffix = "" if len(value) <= limit else f", ... +{len(value) - limit}"
        return f"{shown}{suffix}"
    return repr(value)


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


def _pilot_service_command(
    repo_root: Path,
    *,
    prefer_existing_binary: bool,
) -> list[str]:
    if prefer_existing_binary and os.environ.get("AETHER_NOTEBOOK_FORCE_CARGO_RUN") != "1":
        binary = _example_binary(repo_root, PILOT_EXAMPLE_NAME)
        if binary is not None:
            return [str(binary)]

    return [
        "cargo",
        "run",
        "-p",
        "aether_api",
        "--example",
        PILOT_EXAMPLE_NAME,
    ]


def _example_binary(repo_root: Path, example_name: str) -> Path | None:
    suffix = ".exe" if os.name == "nt" else ""
    for profile in ("debug", "release"):
        binary = repo_root / "target" / profile / "examples" / f"{example_name}{suffix}"
        if binary.exists() and _binary_is_fresh(repo_root, binary):
            return binary
    return None


def _binary_is_fresh(repo_root: Path, binary: Path) -> bool:
    binary_mtime = binary.stat().st_mtime
    source_paths = [repo_root / "Cargo.toml", repo_root / "Cargo.lock"]
    source_paths.extend((repo_root / "crates").glob("**/*.rs"))
    for source_path in source_paths:
        if source_path.exists() and source_path.stat().st_mtime > binary_mtime:
            return False
    return True


def _is_cargo_run_command(command: list[str]) -> bool:
    return len(command) >= 2 and command[0] == "cargo" and command[1] == "run"


def _service_is_healthy(service: NotebookService) -> bool:
    if service.process.poll() is not None:
        return False
    try:
        with request.urlopen(f"{service.base_url}/health", timeout=0.5) as response:
            return response.status == 200
    except (error.URLError, TimeoutError):
        return False


def _wait_for_health(
    base_url: str,
    process: subprocess.Popen[str],
    timeout_seconds: float = 180.0,
    poll_interval_seconds: float = 0.25,
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
            time.sleep(poll_interval_seconds)

    output = process.stdout.read() if process.stdout else ""
    raise RuntimeError(
        "AETHER HTTP example did not become healthy in time:\n"
        f"{output}"
    )
