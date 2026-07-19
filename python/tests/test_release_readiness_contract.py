from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]


class ReleaseReadinessContractTests(unittest.TestCase):
    def test_canonical_package_staging_creates_missing_parent_before_copy(self) -> None:
        runner = (REPO_ROOT / "scripts" / "run-release-readiness.ps1").read_text(
            encoding="utf-8"
        )
        resolve_parent = "$pilotPackageParent = Split-Path -Parent $pilotPackageZip"
        create_parent = (
            "New-Item -ItemType Directory -Force $pilotPackageParent | Out-Null"
        )
        copy_package = (
            "Copy-Item -LiteralPath $CandidatePackageZip -Destination $pilotPackageZip"
        )
        self.assertIn(resolve_parent, runner)
        self.assertIn(create_parent, runner)
        self.assertIn(copy_package, runner)
        self.assertLess(runner.index(resolve_parent), runner.index(create_parent))
        self.assertLess(runner.index(create_parent), runner.index(copy_package))


if __name__ == "__main__":
    unittest.main()
