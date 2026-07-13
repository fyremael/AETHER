# Supply-Chain Security

AETHER treats package identity, dependency identity, license policy, scanner
outcomes, and workflow identity as release inputs. A file/checksum inventory is
still produced for operations, but it is named
`pilot-package-file-manifest.json` and is not called an SBOM.

## Standard SBOMs

`scripts/supply_chain.py` generates strict CycloneDX 1.5 JSON for:

- every Rust component represented by `Cargo.lock` and `cargo metadata`;
- every Go module represented by `go.sum` and `go list -m`;
- every file in the final assembled pilot package.

Every component carries a name, version, package URL, license expression, and
SHA-256 hash. Rust and Go SBOMs include dependency graphs. Generation fails if
a lockfile component or packaged file is absent, CycloneDX validation fails,
the license policy rejects a component, or a delivery input is mutable or not
allowlisted.

Run the local generator after building the package:

```powershell
python -m pip install -r requirements-release.txt
./scripts/build-pilot-package.ps1
$sha = (git rev-parse HEAD).Trim()
python scripts/supply_chain.py generate `
  --candidate-sha $sha `
  --package-zip artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip `
  --out-dir artifacts/release/supply-chain
```

The explicit license and workflow policies are
`fixtures/release/license-policy.json` and
`fixtures/release/allowed-actions.json`. Unknown licenses fail closed unless a
component-specific exception names an owner, reason, and unexpired deadline.

## Blocking workflow gates

`.github/workflows/supply-chain.yml` builds one candidate package and runs:

- `cargo-audit` against `Cargo.lock`;
- `govulncheck` against all Go packages;
- Trivy against the assembled package filesystem;
- Gitleaks across repository history;
- CodeQL for the supported Go and Python languages;
- strict CycloneDX completeness and license-policy verification; and
- GitHub Sigstore-backed package provenance and SBOM attestations.

Scanner versions and action identities are pinned and recorded in the allowed
actions policy and SBOM summary. All third-party actions use immutable commit
SHAs; service and Docker base images use immutable digests. Dependabot covers
Cargo, Go modules, release Python dependencies, and GitHub Actions.

The allowlist includes the Trivy composite action's own SHA-pinned
`setup-trivy` dependency. Nested third-party actions are part of the same
delivery boundary as actions named directly by a workflow.

The security baseline requires Rust 1.86 or newer and Go 1.26.5 or newer. The
July 2026 gate upgrade deliberately raised the Rust MSRV and Go workflow patch
level so patched Postgres/Tokio/Rustls and Go standard-library versions could
replace vulnerable locked components; older toolchains are not a supported
beta boundary.

The exact-candidate evidence workflow embeds all three SBOMs, the license
verdict, and the signed package provenance bundle. Vulnerability, code, secret,
recovery, performance, and soak subjects remain fail-closed requirements until
the exact workflow downloads their successful SHA-bound outputs.

## Repository controls

Repository administrators must configure the hosting platform to match the
tracked workflow policy:

- require pull-request review and the CI, Supply Chain, and exact-candidate
  checks on protected `main`;
- disallow force pushes and branch deletion;
- restrict Actions to GitHub-authored actions plus the exact allowlisted SHAs;
- protect the release environment with required reviewers;
- keep secret scanning and push protection enabled; and
- retain artifact attestations and uploaded evidence for the release-retention
  period.

These hosted controls cannot be proven by a repository file. Until a current
repository-settings evidence record verifies them, `service.beta_boundary`
remains blocked even when local generation passes.

The desired hosted boundary is machine-readable in
`.github/repository-controls.json`. Capture and verify the live settings with:

```powershell
python scripts/verify_repository_controls.py `
  --out artifacts/release/repository-controls.json
```

The command is read-only, binds the evidence to the checked-out commit, and
fails closed when branch protection, aggregate required checks, Actions
restrictions, security analysis, or deployment-environment policy drifts.
