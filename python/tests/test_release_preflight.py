from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("release_preflight", REPO_ROOT / "scripts" / "release_preflight.py")
assert SPEC and SPEC.loader
release_preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_preflight)
QUALIFICATION_SPEC = importlib.util.spec_from_file_location(
    "release_qualification", REPO_ROOT / "scripts" / "release_qualification.py"
)
assert QUALIFICATION_SPEC and QUALIFICATION_SPEC.loader
release_qualification = importlib.util.module_from_spec(QUALIFICATION_SPEC)
QUALIFICATION_SPEC.loader.exec_module(release_qualification)


class ReleasePreflightTests(unittest.TestCase):
    def test_static_contract_validates_policy_dependencies_schemas_and_workflows(self) -> None:
        result = release_preflight.validate_static(REPO_ROOT)
        self.assertEqual(result["subject_count"], 18)
        self.assertGreaterEqual(result["schema_count"], 3)

    def test_required_dependency_parser_is_exact(self) -> None:
        names = release_preflight.dependency_names(REPO_ROOT / "requirements-release.txt")
        self.assertTrue({"pytest", "jsonschema"}.issubset(names))

    def test_tooling_file_allowlist_entries_do_not_match_lookalike_paths(self) -> None:
        rules = ["scripts/release_preflight.py", ".github/workflows/"]
        self.assertTrue(release_qualification.tooling_path_allowed("scripts/release_preflight.py", rules))
        self.assertTrue(release_qualification.tooling_path_allowed(".github/workflows/ci.yml", rules))
        self.assertFalse(release_qualification.tooling_path_allowed("scripts/release_preflight.py.bak", rules))
        self.assertFalse(release_qualification.tooling_path_allowed(".github/workflows-old/ci.yml", rules))

    def test_current_release_control_files_are_explicitly_allowlisted(self) -> None:
        policy = release_preflight.evidence.load_json(
            REPO_ROOT / "fixtures" / "release" / "gate-policy.json"
        )
        rules = policy["qualification_tooling_allowed_paths"]
        release_control_files = {
            "python/tests/test_release_evidence.py",
            "python/tests/test_release_preflight.py",
            "python/tests/test_release_subjects.py",
            "scripts/release_evidence.py",
            "scripts/release_preflight.py",
            "scripts/release_qualification.py",
            "scripts/release_subjects.py",
        }
        self.assertTrue(
            all(release_qualification.tooling_path_allowed(path, rules) for path in release_control_files)
        )
        self.assertFalse(
            release_qualification.tooling_path_allowed("scripts/release_subjects.py.bak", rules)
        )

    def test_projected_gate_sources_are_explicit_and_fail_closed(self) -> None:
        self.assertEqual(
            release_qualification.projected_gate_source({"id": "security.supply_chain"}),
            ("supply_chain", "supply_gate"),
        )
        self.assertEqual(
            release_qualification.projected_gate_source({"id": "semantic.full_acceptance"}),
            ("ci", "ci_gate"),
        )
        with self.assertRaises(release_preflight.evidence.EvidenceError):
            release_qualification.projected_gate_source({"id": "future.unmapped_gate"})

    def test_projected_evidence_keeps_old_product_and_new_tooling_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = {
                "schema_version": "aether.release-qualification-inputs.v2",
                "candidate_repository": "fyremael/AETHER",
                "candidate_commit_sha": "a" * 40,
                "candidate_tree_sha": "b" * 40,
                "candidate_ref": "refs/heads/main",
                "qualification_tooling": {
                    "repository": "fyremael/AETHER",
                    "commit_sha": "c" * 40,
                    "tree_sha": "d" * 40,
                    "ref": "refs/heads/main",
                    "dirty": False,
                },
                "runs": {
                    "ci": {"id": 11, "attempt": 1, "workflow_file": ".github/workflows/ci.yml", "head_sha": "a" * 40},
                    "supply_chain": {"id": 12, "attempt": 1, "workflow_file": ".github/workflows/supply-chain.yml", "head_sha": "a" * 40},
                },
                "jobs": {
                    "ci_gate": {"id": 21, "name": "Required CI gate", "conclusion": "success"},
                    "supply_gate": {"id": 22, "name": "Required Supply Chain gate", "conclusion": "success"},
                },
            }
            manifest_path = root / "qualification-inputs.json"
            release_preflight.evidence.write_canonical_json(manifest_path, manifest)
            output = root / "projected"
            args = argparse.Namespace(
                qualification_inputs=str(manifest_path),
                policy="fixtures/release/gate-policy.json",
                repository="fyremael/AETHER",
                output_dir=str(output),
                run_id="42",
                attempt=1,
                runner="Windows",
                host="github-windows-latest",
            )
            self.assertEqual(release_qualification.project_gate_evidence(args), 0)
            envelope = release_preflight.evidence.load_json(next((output / "envelopes").glob("*.json")))
            self.assertEqual(envelope["candidate"]["commit_sha"], "a" * 40)
            self.assertEqual(envelope["qualification_tooling"]["commit_sha"], "c" * 40)


if __name__ == "__main__":
    unittest.main()
