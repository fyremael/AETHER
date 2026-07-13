from __future__ import annotations

import importlib.util
import json
import tempfile
import tomllib
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "supply_chain.py"


def load_module():
    spec = importlib.util.spec_from_file_location("supply_chain", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SupplyChainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_module()

    def test_package_sbom_is_strict_cyclonedx_and_covers_every_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "package.zip"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("bin/aether.exe", b"binary")
                archive.writestr("docs/LICENSE", b"license")
            bom, licenses = self.module.package_sbom(
                package,
                "a" * 40,
                "2026-07-12T00:00:00Z",
            )
            self.module.validate_cyclonedx(bom)
            self.assertEqual(
                {component["name"] for component in bom["components"]},
                {"bin/aether.exe", "docs/LICENSE"},
            )
            self.assertEqual(licenses, {"Apache-2.0 OR MIT"})
            self.assertTrue(all(component.get("purl") for component in bom["components"]))
            self.assertTrue(all(component.get("hashes") for component in bom["components"]))

    def test_completeness_rejects_missing_cargo_lock_component(self) -> None:
        lock = tomllib.loads((REPO_ROOT / "Cargo.lock").read_text(encoding="utf-8"))
        rust_components = [
            {"purl": self.module.cargo_purl(item["name"], item["version"])}
            for item in lock["package"]
        ]
        removed = rust_components.pop()
        go_components = [
            {"purl": self.module.go_purl(module, version)}
            for module, version in self.module.go_sum_components(REPO_ROOT / "go" / "go.sum")
        ]
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "package.zip"
            with zipfile.ZipFile(package, "w") as archive:
                archive.writestr("file.txt", b"content")
            with self.assertRaisesRegex(ValueError, "Cargo.lock"):
                self.module.validate_completeness(
                    REPO_ROOT,
                    {"components": rust_components},
                    {"components": go_components},
                    {"components": [{"name": "file.txt"}]},
                    package,
                )
        self.assertTrue(removed["purl"].startswith("pkg:cargo/"))

    def test_unknown_license_requires_scoped_unexpired_exception(self) -> None:
        component = {
            "purl": "pkg:golang/example.invalid/unlicensed@v1.0.0",
            "licenses": [{"expression": "NOASSERTION"}],
        }
        policy = {
            "allowed_spdx_expressions": ["MIT"],
            "denied_spdx_identifiers": [],
            "allow_unknown": False,
            "exceptions": [],
            "unknown_component_exceptions": [],
        }
        violations = self.module.validate_license_policy(
            {"NOASSERTION"}, [component], policy, "2026-07-12T00:00:00Z"
        )
        self.assertIn("without component exception", violations[0])
        policy["unknown_component_exceptions"] = [
            {
                "purl": component["purl"],
                "reason": "reviewed upstream gap",
                "owner": "security",
                "expires_at": "2026-08-01T00:00:00Z",
            }
        ]
        self.assertEqual(
            self.module.validate_license_policy(
                {"NOASSERTION"}, [component], policy, "2026-07-12T00:00:00Z"
            ),
            [],
        )

    def test_tracked_license_policy_has_explicit_unknown_exception(self) -> None:
        policy = json.loads(
            (REPO_ROOT / "fixtures" / "release" / "license-policy.json").read_text(
                encoding="utf-8"
            )
        )
        exception = policy["unknown_component_exceptions"][0]
        self.assertEqual(
            exception["purl"],
            "pkg:golang/github.com/mattn/go-localereader@v0.0.1",
        )
        self.assertGreater(
            datetime.fromisoformat(exception["expires_at"].replace("Z", "+00:00")),
            datetime(2026, 7, 12, tzinfo=timezone.utc),
        )

    def test_tracked_actions_and_images_are_immutable_and_allowlisted(self) -> None:
        policy = json.loads(
            (REPO_ROOT / "fixtures" / "release" / "allowed-actions.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            self.module.validate_pinned_delivery_inputs(REPO_ROOT, policy),
            [],
        )

    def test_container_runtime_declares_native_tls_dependencies(self) -> None:
        dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("ca-certificates", dockerfile)
        self.assertIn("libssl3", dockerfile)

    def test_mutable_action_image_and_write_all_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            (workflows / "unsafe.yml").write_text(
                "permissions: write-all\n"
                "jobs:\n"
                "  unsafe:\n"
                "    services:\n"
                "      db:\n"
                "        image: postgres:16\n"
                "    steps:\n"
                "      - uses: actions/checkout@v4\n",
                encoding="utf-8",
            )
            (root / "Dockerfile").write_text("FROM debian:bookworm-slim\n", encoding="utf-8")
            violations = self.module.validate_pinned_delivery_inputs(
                root,
                {"actions": [], "container_images": []},
            )
        self.assertTrue(any("write-all" in item for item in violations))
        self.assertTrue(any("mutable action" in item for item in violations))
        self.assertTrue(any("mutable service image" in item for item in violations))
        self.assertTrue(any("mutable Docker base" in item for item in violations))


if __name__ == "__main__":
    unittest.main()
