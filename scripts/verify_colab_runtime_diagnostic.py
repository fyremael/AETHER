from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from pathlib import Path


SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_receipt(
    *,
    summary_path: Path,
    expected_commit: str,
    expected_tree: str,
    session: str,
    archive_path: Path,
) -> dict:
    if SHA_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("candidate must be a full lowercase commit SHA")
    if SHA_PATTERN.fullmatch(expected_tree) is None:
        raise ValueError("tree must be a full lowercase SHA")

    summary_bytes = summary_path.read_bytes()
    summary = json.loads(summary_bytes)
    if summary.get("qualification_status") != "diagnostic_only":
        raise ValueError("Colab output attempted to acquire qualification authority")
    if summary.get("policy", {}).get("commercial_beta_authority") is not False:
        raise ValueError("Colab output attempted to acquire commercial beta authority")
    if summary.get("candidate") != {
        "commit_sha": expected_commit,
        "tree_sha": expected_tree,
    }:
        raise ValueError("Colab output candidate identity mismatch")

    archive_bytes = archive_path.read_bytes()
    with zipfile.ZipFile(archive_path) as archive:
        summary_entries = [
            entry
            for entry in archive.infolist()
            if entry.filename == "summary.json" and not entry.is_dir()
        ]
        if len(summary_entries) != 1:
            raise ValueError(
                "diagnostic archive must contain exactly one top-level summary.json"
            )
        inner_summary_bytes = archive.read(summary_entries[0])
    if inner_summary_bytes != summary_bytes:
        raise ValueError("downloaded summary bytes differ from the archive summary bytes")

    return {
        "schema_version": "aether.colab-diagnostic-receipt.v1",
        "qualification_status": "diagnostic_only",
        "commercial_beta_authority": False,
        "candidate": {"commit_sha": expected_commit, "tree_sha": expected_tree},
        "session": session,
        "artifacts": {
            "archive": {
                "path": str(archive_path),
                "byte_size": len(archive_bytes),
                "sha256": sha256(archive_bytes),
                "summary_entry": "summary.json",
                "summary_sha256": sha256(inner_summary_bytes),
            },
            "summary": {
                "path": str(summary_path),
                "byte_size": len(summary_bytes),
                "sha256": sha256(summary_bytes),
            },
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--tree", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    receipt = build_receipt(
        summary_path=args.summary,
        expected_commit=args.candidate,
        expected_tree=args.tree,
        session=args.session,
        archive_path=args.archive,
    )
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
