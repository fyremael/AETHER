from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_SHA = "a" * 40


def load_script(name: str):
    path = REPO_ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PagesIdentityTests(unittest.TestCase):
    def test_source_identity_is_visible_and_machine_verifiable(self) -> None:
        builder = load_script("build_pages.py")
        verifier = load_script("verify_pages_deployment.py")
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir)
            (output / "index.html").write_text(
                "<html><body><footer>docs\n      </footer></body></html>",
                encoding="utf-8",
            )
            builder.write_source_identity(
                output, candidate_sha=EXAMPLE_SHA, version="0.1.0"
            )

            metadata = json.loads(
                (output / "source-version.json").read_text(encoding="utf-8")
            )
            self.assertEqual(metadata["source_sha"], EXAMPLE_SHA)
            self.assertIn(EXAMPLE_SHA, (output / "index.html").read_text(encoding="utf-8"))
            self.assertEqual(verifier.verify_payload(metadata, EXAMPLE_SHA), (True, EXAMPLE_SHA))
            self.assertEqual(verifier.verify_payload(metadata, "b" * 40), (False, EXAMPLE_SHA))

    def test_pages_workflow_verifies_the_deployed_candidate(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "pages.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("--candidate-sha \"${{ github.sha }}\"", workflow)
        self.assertIn("source-version.json", workflow)
        self.assertIn("verify_pages_deployment.py", workflow)
        self.assertIn("pages-deployment-verification", workflow)


if __name__ == "__main__":
    unittest.main()
