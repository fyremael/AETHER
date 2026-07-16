#!/usr/bin/env python3
"""Generate and validate AETHER CycloneDX SBOM and supply-chain evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import subprocess
import sys
import tomllib
import urllib.parse
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from cyclonedx.schema import SchemaVersion
from cyclonedx.validation.json import JsonStrictValidator
from packageurl import PackageURL


SCHEMA_VERSION = "aether.supply-chain-evidence.v1"
TOOL_VERSION = "aether-supply-chain-v1"
CDX_VERSION = "1.5"


class SupplyChainError(ValueError):
    pass


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def canonical_bytes(payload: Any) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_json(command: list[str], cwd: Path) -> Any:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return json.loads(completed.stdout)


def run_text(command: list[str], cwd: Path) -> str:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout


def parse_json_stream(content: str) -> list[Any]:
    decoder = json.JSONDecoder()
    index = 0
    values = []
    while index < len(content):
        while index < len(content) and content[index].isspace():
            index += 1
        if index >= len(content):
            break
        value, index = decoder.raw_decode(content, index)
        values.append(value)
    return values


def serial(candidate_sha: str, kind: str) -> str:
    value = uuid.uuid5(uuid.NAMESPACE_URL, f"https://aether.dev/sbom/{candidate_sha}/{kind}")
    return f"urn:uuid:{value}"


def normalized_timestamp(value: str | None) -> str:
    if value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        raise SupplyChainError("generated-at timestamp must include a timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def cdx_base(candidate_sha: str, kind: str, generated_at: str, root_component: dict[str, Any]) -> dict[str, Any]:
    return {
        "$schema": "http://cyclonedx.org/schema/bom-1.5.schema.json",
        "bomFormat": "CycloneDX",
        "specVersion": CDX_VERSION,
        "serialNumber": serial(candidate_sha, kind),
        "version": 1,
        "metadata": {
            "timestamp": generated_at,
            "tools": [{"vendor": "AETHER", "name": "supply_chain.py", "version": TOOL_VERSION}],
            "component": root_component,
            "properties": [
                {"name": "aether:candidate:commit_sha", "value": candidate_sha},
                {"name": "aether:sbom:subject", "value": kind},
            ],
        },
        "components": [],
        "dependencies": [],
    }


def license_choice(expression: str) -> list[dict[str, str]]:
    normalized = {
        "MIT/Apache-2.0": "MIT OR Apache-2.0",
        "Unlicense/MIT": "Unlicense OR MIT",
    }.get(expression, expression)
    return [{"expression": normalized}]


def hash_choice(digest: str) -> list[dict[str, str]]:
    return [{"alg": "SHA-256", "content": digest.lower()}]


def cargo_purl(name: str, version: str) -> str:
    return str(PackageURL(type="cargo", name=name, version=version))


def go_purl(module: str, version: str) -> str:
    parts = module.split("/")
    return str(
        PackageURL(
            type="golang",
            namespace="/".join(parts[:-1]) or None,
            name=parts[-1],
            version=version,
        )
    )


def generic_purl(name: str, version: str) -> str:
    return str(PackageURL(type="generic", name=name, version=version))


def rust_sbom(root: Path, candidate_sha: str, generated_at: str) -> tuple[dict[str, Any], set[str]]:
    lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    locked = {
        (item["name"], item["version"]): item
        for item in lock.get("package", [])
    }
    metadata = run_json(["cargo", "metadata", "--format-version", "1", "--locked"], root)
    packages = {(item["name"], item["version"]): item for item in metadata["packages"]}
    workspace_ids = set(metadata["workspace_members"])
    root_component = {
        "type": "application",
        "bom-ref": f"pkg:generic/aether-rust-workspace@{candidate_sha}",
        "name": "aether-rust-workspace",
        "version": candidate_sha,
        "purl": generic_purl("aether-rust-workspace", candidate_sha),
        "licenses": license_choice("Apache-2.0 OR MIT"),
        "hashes": hash_choice(sha256_file(root / "Cargo.lock")),
    }
    bom = cdx_base(candidate_sha, "rust", generated_at, root_component)
    refs: dict[str, str] = {}
    licenses: set[str] = set()
    for key, item in sorted(packages.items()):
        name, version = key
        reference = cargo_purl(name, version)
        refs[item["id"]] = reference
        license_expression = item.get("license") or "NOASSERTION"
        licenses.add(license_expression)
        lock_item = locked.get(key)
        if lock_item and lock_item.get("checksum"):
            digest = lock_item["checksum"]
        else:
            digest = sha256_file(Path(item["manifest_path"]))
        component = {
            "type": "application" if item["id"] in workspace_ids else "library",
            "bom-ref": reference,
            "name": name,
            "version": version,
            "purl": reference,
            "licenses": license_choice(license_expression),
            "hashes": hash_choice(digest),
            "properties": [
                {
                    "name": "aether:cargo:lockfile_component",
                    "value": "true" if key in locked else "false",
                }
            ],
        }
        bom["components"].append(component)
    resolve = metadata.get("resolve") or {"nodes": []}
    for node in sorted(resolve.get("nodes", []), key=lambda value: value["id"]):
        reference = refs.get(node["id"])
        if not reference:
            continue
        depends_on = sorted(
            refs[dependency["pkg"]]
            for dependency in node.get("deps", [])
            if dependency["pkg"] in refs
        )
        bom["dependencies"].append({"ref": reference, "dependsOn": depends_on})
    bom["dependencies"].append(
        {
            "ref": root_component["bom-ref"],
            "dependsOn": sorted(refs[item] for item in workspace_ids if item in refs),
        }
    )
    return bom, licenses


def h1_hex(value: str | None) -> str | None:
    if not value or not value.startswith("h1:"):
        return None
    try:
        return base64.b64decode(value[3:]).hex()
    except ValueError:
        return None


def detect_license(directory: Path | None, root_module: bool = False) -> str:
    if root_module:
        return "Apache-2.0 OR MIT"
    if directory is None or not directory.is_dir():
        return "NOASSERTION"
    candidates = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.name.lower().startswith(("license", "copying"))
    )
    identifiers: set[str] = set()
    for path in candidates:
        content = path.read_text(encoding="utf-8", errors="ignore").lower()
        if "apache license" in content and "version 2.0" in content:
            identifiers.add("Apache-2.0")
        if "permission is hereby granted, free of charge" in content:
            identifiers.add("MIT")
        if "redistribution and use in source and binary forms" in content:
            identifiers.add("BSD-3-Clause" if "neither the name" in content else "BSD-2-Clause")
        if "permission to use, copy, modify, and/or distribute" in content:
            identifiers.add("ISC")
        if "mozilla public license version 2.0" in content:
            identifiers.add("MPL-2.0")
    if not identifiers:
        return "NOASSERTION"
    return " AND ".join(sorted(identifiers))


def go_sum_components(go_sum: Path) -> set[tuple[str, str]]:
    result = set()
    for line in go_sum.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        version = parts[1].removesuffix("/go.mod")
        result.add((parts[0], version))
    return result


def go_sbom(root: Path, candidate_sha: str, generated_at: str) -> tuple[dict[str, Any], set[str]]:
    go_root = root / "go"
    listed = parse_json_stream(run_text(["go", "list", "-m", "-json", "all"], go_root))
    active = {(item["Path"], item.get("Version") or "0.1.0"): item for item in listed}
    locked = go_sum_components(go_root / "go.sum")
    all_components = set(active) | locked
    downloads: dict[tuple[str, str], dict[str, Any]] = {}
    for module, version in sorted(all_components):
        if module == "github.com/fyremael/aether/go":
            continue
        completed = subprocess.run(
            ["go", "mod", "download", "-json", f"{module}@{version}"],
            cwd=go_root,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.stdout.strip():
            downloads[(module, version)] = json.loads(completed.stdout)
    root_module = "github.com/fyremael/aether/go"
    root_version = "0.1.0"
    root_ref = go_purl(root_module, root_version)
    root_component = {
        "type": "application",
        "bom-ref": root_ref,
        "name": root_module,
        "version": root_version,
        "purl": root_ref,
        "licenses": license_choice("Apache-2.0 OR MIT"),
        "hashes": hash_choice(sha256_file(go_root / "go.sum")),
    }
    bom = cdx_base(candidate_sha, "go", generated_at, root_component)
    refs = {(root_module, root_version): root_ref}
    licenses: set[str] = {"Apache-2.0 OR MIT"}
    for module, version in sorted(all_components):
        reference = go_purl(module, version)
        refs[(module, version)] = reference
        download = downloads.get((module, version), {})
        directory = Path(download["Dir"]) if download.get("Dir") else None
        root_item = module == root_module
        expression = detect_license(go_root if root_item else directory, root_item)
        licenses.add(expression)
        digest = h1_hex(download.get("Sum"))
        if digest is None:
            digest = sha256_file(go_root / "go.mod") if root_item else hashlib.sha256(f"{module}@{version}".encode()).hexdigest()
        bom["components"].append(
            {
                "type": "application" if root_item else "library",
                "bom-ref": reference,
                "name": module,
                "version": version,
                "purl": reference,
                "licenses": license_choice(expression),
                "hashes": hash_choice(digest),
                "properties": [
                    {
                        "name": "aether:go:sum_component",
                        "value": "true" if (module, version) in locked else "false",
                    }
                ],
            }
        )
    graph = run_text(["go", "mod", "graph"], go_root).splitlines()
    edges: dict[str, set[str]] = {}
    for line in graph:
        parts = line.split()
        if len(parts) != 2:
            continue
        parent, child = parts
        parent_module, _, parent_version = parent.partition("@")
        child_module, _, child_version = child.partition("@")
        parent_version = parent_version or root_version
        child_version = child_version or root_version
        parent_ref = refs.get((parent_module, parent_version))
        child_ref = refs.get((child_module, child_version))
        if parent_ref and child_ref:
            edges.setdefault(parent_ref, set()).add(child_ref)
    for reference in sorted(set(refs.values())):
        bom["dependencies"].append({"ref": reference, "dependsOn": sorted(edges.get(reference, set()))})
    return bom, licenses


def package_sbom(package_zip: Path, candidate_sha: str, generated_at: str) -> tuple[dict[str, Any], set[str]]:
    version = "0.1.0"
    root_ref = generic_purl("aether-pilot-service", version)
    root_component = {
        "type": "application",
        "bom-ref": root_ref,
        "name": "aether-pilot-service",
        "version": version,
        "purl": root_ref,
        "licenses": license_choice("Apache-2.0 OR MIT"),
        "hashes": hash_choice(sha256_file(package_zip)),
        "properties": [{"name": "aether:candidate:commit_sha", "value": candidate_sha}],
    }
    bom = cdx_base(candidate_sha, "assembled-package", generated_at, root_component)
    dependencies = []
    with zipfile.ZipFile(package_zip) as archive:
        for info in sorted(archive.infolist(), key=lambda value: value.filename):
            if info.is_dir():
                continue
            content = archive.read(info.filename)
            digest = hashlib.sha256(content).hexdigest()
            name = urllib.parse.quote(info.filename, safe="")
            reference = generic_purl(f"aether-pilot-service-{name}", version)
            dependencies.append(reference)
            bom["components"].append(
                {
                    "type": "file",
                    "bom-ref": reference,
                    "name": info.filename,
                    "version": version,
                    "purl": reference,
                    "licenses": license_choice("Apache-2.0 OR MIT"),
                    "hashes": hash_choice(digest),
                }
            )
    bom["dependencies"].append({"ref": root_ref, "dependsOn": sorted(dependencies)})
    return bom, {"Apache-2.0 OR MIT"}


def validate_cyclonedx(payload: dict[str, Any]) -> None:
    error = JsonStrictValidator(SchemaVersion.V1_5).validate_str(
        json.dumps(payload, sort_keys=True), all_errors=False
    )
    if error is not None:
        raise SupplyChainError(f"CycloneDX validation failed: {error}")


def component_keys(payload: dict[str, Any]) -> set[str]:
    return {
        component.get("purl") or f"{component.get('name')}@{component.get('version')}"
        for component in payload.get("components", [])
    }


def validate_completeness(root: Path, rust: dict[str, Any], go: dict[str, Any], package: dict[str, Any], package_zip: Path) -> None:
    rust_expected = {
        cargo_purl(item["name"], item["version"])
        for item in tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8")).get("package", [])
    }
    rust_missing = rust_expected - component_keys(rust)
    if rust_missing:
        raise SupplyChainError(f"Rust SBOM missing Cargo.lock components: {sorted(rust_missing)}")
    go_expected = {go_purl(module, version) for module, version in go_sum_components(root / "go" / "go.sum")}
    go_missing = go_expected - component_keys(go)
    if go_missing:
        raise SupplyChainError(f"Go SBOM missing go.sum components: {sorted(go_missing)}")
    with zipfile.ZipFile(package_zip) as archive:
        package_expected = {info.filename for info in archive.infolist() if not info.is_dir()}
    package_observed = {component["name"] for component in package.get("components", [])}
    package_missing = package_expected - package_observed
    if package_missing:
        raise SupplyChainError(f"package SBOM missing files: {sorted(package_missing)}")


def validate_license_policy(
    licenses: Iterable[str],
    components: Iterable[dict[str, Any]],
    policy: dict[str, Any],
    generated_at: str,
) -> list[str]:
    allowed = set(policy.get("allowed_spdx_expressions", []))
    exceptions = {item["expression"] for item in policy.get("exceptions", [])}
    violations = []
    normalized_licenses = {
        license_choice(expression)[0]["expression"] for expression in licenses
    }
    for expression in sorted(normalized_licenses):
        if expression == "NOASSERTION":
            continue
        if expression not in allowed and expression not in exceptions:
            violations.append(f"license expression not allowed: {expression}")
        for denied in policy.get("denied_spdx_identifiers", []):
            if re.search(rf"\b{re.escape(denied)}\b", expression):
                violations.append(f"denied license identifier: {denied} in {expression}")
    unknown_exceptions = {
        item["purl"]: item
        for item in policy.get("unknown_component_exceptions", [])
    }
    generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    for component in components:
        expression = component.get("licenses", [{}])[0].get("expression")
        if expression != "NOASSERTION":
            continue
        purl = component.get("purl", "")
        exception = unknown_exceptions.get(purl)
        if policy.get("allow_unknown", False):
            continue
        if not exception:
            violations.append(f"unknown license without component exception: {purl}")
            continue
        expiry = datetime.fromisoformat(exception["expires_at"].replace("Z", "+00:00"))
        if expiry <= generated:
            violations.append(f"expired unknown-license exception: {purl}")
        if not exception.get("owner") or not exception.get("reason"):
            violations.append(f"incomplete unknown-license exception: {purl}")
    return sorted(set(violations))


def validate_pinned_delivery_inputs(
    root: Path,
    policy: dict[str, Any],
) -> list[str]:
    allowed_actions = set(policy.get("actions", []))
    allowed_images = set(policy.get("container_images", []))
    violations: list[str] = []
    workflow_root = root / ".github" / "workflows"
    for path in sorted(workflow_root.glob("*.y*ml")):
        content = path.read_text(encoding="utf-8")
        if not re.search(r"(?m)^permissions:\s*$", content):
            violations.append(f"workflow lacks explicit top-level permissions: {path.name}")
        if re.search(r"(?m)^permissions:\s*write-all\s*$", content):
            violations.append(f"workflow uses write-all permissions: {path.name}")
        for match in re.finditer(r"(?m)^\s*(?:-\s*)?uses:\s*([^\s#]+)", content):
            reference = match.group(1)
            if reference.startswith("./"):
                local = (root / reference).resolve()
                if not local.is_file():
                    violations.append(f"missing local workflow action: {reference}")
                continue
            if not re.fullmatch(r"[^@\s]+@[a-f0-9]{40}", reference):
                violations.append(f"mutable action reference in {path.name}: {reference}")
            elif reference not in allowed_actions:
                violations.append(f"action is not allowlisted in {path.name}: {reference}")
        for match in re.finditer(r"(?m)^\s*image:\s*([^\s#]+)", content):
            image = match.group(1)
            if "@sha256:" not in image:
                violations.append(f"mutable service image in {path.name}: {image}")
            elif image not in allowed_images:
                violations.append(f"service image is not allowlisted in {path.name}: {image}")
    dockerfile = root / "Dockerfile"
    if dockerfile.is_file():
        for match in re.finditer(
            r"(?mi)^FROM\s+([^\s]+)(?:\s+AS\s+[^\s]+)?\s*$",
            dockerfile.read_text(encoding="utf-8"),
        ):
            image = match.group(1)
            if "@sha256:" not in image:
                violations.append(f"mutable Docker base image: {image}")
            elif image not in allowed_images:
                violations.append(f"Docker base image is not allowlisted: {image}")
    return sorted(set(violations))


def generate(args: argparse.Namespace) -> int:
    root = repo_root()
    candidate_sha = args.candidate_sha
    if not re.fullmatch(r"[a-f0-9]{40}", candidate_sha):
        raise SupplyChainError("candidate SHA must be a full lowercase commit SHA")
    generated_at = normalized_timestamp(args.generated_at)
    package_zip = Path(args.package_zip).resolve()
    if not package_zip.is_file():
        raise SupplyChainError(f"package zip does not exist: {package_zip}")
    out = Path(args.out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    rust, rust_licenses = rust_sbom(root, candidate_sha, generated_at)
    go, go_licenses = go_sbom(root, candidate_sha, generated_at)
    package, package_licenses = package_sbom(package_zip, candidate_sha, generated_at)
    for payload in (rust, go, package):
        validate_cyclonedx(payload)
    validate_completeness(root, rust, go, package, package_zip)
    license_policy = json.loads((root / args.license_policy).read_text(encoding="utf-8"))
    delivery_policy = json.loads((root / args.allowed_actions).read_text(encoding="utf-8"))
    all_components = rust["components"] + go["components"] + package["components"]
    violations = validate_license_policy(
        rust_licenses | go_licenses | package_licenses,
        all_components,
        license_policy,
        generated_at,
    )
    violations.extend(validate_pinned_delivery_inputs(root, delivery_policy))
    violations = sorted(set(violations))
    paths = {
        "rust-sbom": out / "aether-rust.cdx.json",
        "go-sbom": out / "aether-go.cdx.json",
        "assembled-package-sbom": out / "aether-package.cdx.json",
    }
    write_json(paths["rust-sbom"], rust)
    write_json(paths["go-sbom"], go)
    write_json(paths["assembled-package-sbom"], package)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "candidate_commit_sha": candidate_sha,
        "generated_at": generated_at,
        "tool_version": TOOL_VERSION,
        "package_sha256": sha256_file(package_zip),
        "status": "passed" if not violations else "failed",
        "license_policy": args.license_policy,
        "allowed_actions_policy": args.allowed_actions,
        "scanner_versions": delivery_policy.get("scanners", {}),
        "license_expressions": sorted(rust_licenses | go_licenses | package_licenses),
        "violations": violations,
        "sboms": {
            name: {
                "path": path.name,
                "sha256": sha256_file(path),
                "component_count": len(payload["components"]),
                "dependency_count": len(payload["dependencies"]),
            }
            for (name, path), payload in zip(paths.items(), (rust, go, package), strict=True)
        },
    }
    write_json(out / "supply-chain-summary.json", summary)
    if violations:
        raise SupplyChainError("; ".join(violations))
    print(out)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--candidate-sha", required=True)
    generate_parser.add_argument("--package-zip", required=True)
    generate_parser.add_argument("--out-dir", required=True)
    generate_parser.add_argument("--generated-at")
    generate_parser.add_argument("--license-policy", default="fixtures/release/license-policy.json")
    generate_parser.add_argument(
        "--allowed-actions",
        default="fixtures/release/allowed-actions.json",
    )
    generate_parser.set_defaults(func=generate)
    return parser


def main() -> int:
    try:
        args = build_parser().parse_args()
        return args.func(args)
    except (SupplyChainError, subprocess.SubprocessError, OSError, json.JSONDecodeError) as exc:
        print(f"supply-chain error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
