# Pilot Deployment

This document describes the hardened deployment path for the current AETHER pilot service.

The goal is not full platform ops maturity. The goal is a repeatable, packageable, single-node service deployment with durable storage, policy-bound auth, secret-backed token handling, and CI-built artifacts.

## What Changed

The pilot service is now exposed as a real release binary:

```text
cargo build -p aether_api --bin aether_pilot_service --release
```

That binary starts only from an explicit deployment config:

```text
aether_pilot_service --config <path>
```

or

```text
AETHER_PILOT_CONFIG=<path> aether_pilot_service
```

This is intentional. The hardened service no longer relies on a baked-in default token.

## Config Model

The deployment config is JSON. The tracked template lives at:

- `fixtures/deployment/pilot-service.template.json`

The package builder copies that template into the package `config/` directory and keeps all data paths relative to the config file so the package can be moved as a unit.

The config defines:

- bind address
- SQLite journal path
- audit log path
- one or more auth principals
- per-principal scopes
- optional semantic policy context
- a secret source for each token

Each token must come from exactly one source:

- `token`
- `token_env`
- `token_file`

For the pilot deployment path, `token_file` is the recommended default.

## Secret Handling

The package builder writes a cryptographically strong bearer token to:

- `config/pilot-operator.token`

That token is not printed to stdout by the service at startup.

Instead, the service prints:

- principal name
- granted scopes
- token source type
- effective semantic policy

If you need to rotate a token manually, run:

```text
double-click scripts/new-pilot-token.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File scripts/new-pilot-token.ps1 -OutputPath <token-file>
```

Then restart the service with the updated secret file.

## Building A Package

Windows operator path:

```text
double-click scripts/build-pilot-package.cmd
```

Technical path:

```bash
powershell -ExecutionPolicy Bypass -File scripts/build-pilot-package.ps1
```

By default that produces:

- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64/`
- `artifacts/pilot/packages/aether-pilot-service-windows-x86_64.zip`

The package contains:

- `bin/aether_pilot_service.exe`
- `config/pilot-service.json`
- `config/pilot-operator.token`
- `data/`
- `logs/`
- `docs/PILOT_DEPLOYMENT.md`
- `run-pilot-service.ps1`
- `run-pilot-service.cmd`

## Running The Packaged Service

Inside the package directory:

```text
double-click run-pilot-service.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File .\run-pilot-service.ps1
```

The service will:

- open the SQLite journal from `data/coordination.sqlite`
- open the sidecar catalog adjacent to that journal
- write audit JSONL to `logs/audit.jsonl`
- enforce bearer-token auth plus token-bound semantic policy ceilings

## CI Posture

The package path is now part of CI. Mainline GitHub Actions builds the pilot package and uploads it as an artifact. The same CI run also executes the full launch-validation pack so drift, soak, and stress regressions become hard gates rather than advisory side workflows.

## What This Still Is Not

This deployment path is still:

- single-node
- SQLite-backed
- coarse-scope auth at the HTTP layer
- operator-validated rather than auto-remediating

It is a hardened pilot delivery path, not yet a full production platform envelope.
