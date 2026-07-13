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

- config version
- schema version
- service mode
- bind address
- HTTP transport boundary (`loopback_plaintext` or `trusted_tls_ingress`)
- bounded namespace worker, queue, and audit-writer capacities
- either the legacy SQLite `database_path` or the Service v2 tagged `storage` object
- audit log path
- one or more auth principals
- explicit principal IDs and token IDs
- optional config-backed revoked token or principal IDs
- per-principal scopes
- optional semantic policy context
- optional allowed namespaces for each token; missing namespace bindings default to `default`
- a secret source for each token

Service v2 storage examples:

```json
{ "kind": "sqlite", "data_root": "../data" }
```

```json
{
  "kind": "postgres",
  "database_url_env": "AETHER_DATABASE_URL",
  "schema": "aether",
  "sidecar_path": "../data/sidecars.sqlite",
  "tls": {
    "mode": "verify_full",
    "ca_certificate_paths": ["../secrets/postgres-ca.pem"],
    "client_certificate_path": "../secrets/aether-client.pem",
    "client_private_key_path": "../secrets/aether-client-key.pem",
    "disable_system_roots": true
  }
}
```

SQLite remains the package default. Postgres is journal-first only; sidecar
catalogs remain local SQLite files in this slice.

`verify_full` is the Postgres default and cannot downgrade. `verify_ca` is an
explicit weaker mode for deployments that cannot match a DNS name. Plaintext
requires `development_plaintext` and is accepted only for literal loopback or
Unix-socket development endpoints. Client certificate and key must be supplied
together. TLS status exposes only mode, CA count, client-certificate presence,
and system-root use.

The default HTTP transport is:

```json
{ "mode": "loopback_plaintext" }
```

It rejects non-loopback binds. To place AETHER behind a TLS-terminating ingress,
declare the boundary explicitly:

```json
{
  "mode": "trusted_tls_ingress",
  "external_https_origin": "https://aether.example.com",
  "ingress": "production-edge-gateway"
}
```

The ingress must block direct client access to the backend HTTP listener.
Native TLS is not claimed by the current Rust HTTP binary.

Synchronous kernel and storage operations run on a bounded executor. The
package defaults are:

```json
{
  "namespace_workers": 8,
  "namespace_queue": 64,
  "audit_queue": 1024
}
```

These values live under `concurrency`. When worker plus queue capacity is
exhausted, AETHER returns `503 namespace_busy` with `Retry-After`. Operations
remain serialized inside one namespace but can proceed concurrently across
namespaces. Audit disk writes use the bounded single-writer queue; an overload
is recorded as `audit_write_failed` in the in-memory audit surface.

For Postgres deployments, restore discipline belongs to the database operator:
export and restore the configured journal schema with normal Postgres tooling,
then keep the local sidecar catalog and audit log with the service package or
its host volume. AETHER does not treat Postgres as a SQL rule engine, a global
`AsOf` clock, or an authoritative sidecar catalog.

Each token must come from exactly one source:

- `token`
- `token_env`
- `token_file`
- `token_command`

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

For deeper integration, the service can also fetch a token at startup with `token_command`. That is the preferred bridge to external secret-manager CLIs or local broker scripts when operators do not want bearer tokens stored directly in the package.

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
- `bin/aetherctl.exe`
- `config/pilot-service.json`
- `config/pilot-operator.token`
- `data/`
- `logs/`
- `docs/PILOT_DEPLOYMENT.md`
- `docs/PILOT_OPERATIONS_PLAYBOOK.md`
- `run-pilot-service.ps1`
- `run-pilot-service.cmd`
- `run-aether-ops.ps1`
- `run-aether-ops.cmd`
- `rotate-pilot-token.ps1`
- `rotate-pilot-token.cmd`
- `backup-pilot-state.ps1`
- `backup-pilot-state.cmd`
- `restore-pilot-state.ps1`
- `restore-pilot-state.cmd`

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
- expose `GET /v1/status` for live status and `POST /v1/admin/auth/reload` for explicit auth reload

## Running The Packaged Operator Cockpit

Inside the package directory:

```text
double-click run-aether-ops.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File .\run-aether-ops.ps1
```

That launcher:

- targets `http://127.0.0.1:3000`
- reads `config/pilot-operator.token`
- starts `aetherctl tui` as the live pilot operations cockpit

The v1 cockpit is intentionally read-only. It is for observing health,
coordination state, cut diffs, audit entries, history, and tuple proof traces
from the running authenticated service.

## Backup And Restore

Inside the package directory:

```text
double-click backup-pilot-state.cmd
```

exports a timestamped snapshot containing:

- `config/pilot-service.json`
- package-local token files
- the SQLite journal
- the adjacent sidecar catalog
- the audit JSONL log
- a `manifest.json` describing the captured paths

To restore from a snapshot:

```text
double-click restore-pilot-state.cmd
```

or:

```bash
powershell -ExecutionPolicy Bypass -File .\restore-pilot-state.ps1 -SnapshotDir <snapshot-dir>
```

The restore helper can back up the current package state before applying the
selected snapshot.

## CI Posture

The package path is now part of CI. Mainline GitHub Actions builds the pilot package and uploads it as an artifact. The same CI run also executes the full launch-validation pack so drift, soak, and stress regressions become hard gates rather than advisory side workflows.

Release-readiness also emits a hardening gate-state summary. That summary
shows which persona packs are blocking versus diagnostic and records the latest
known pass/fail/skipped evidence without bypassing the configured promotion
threshold.

## Playbooks

Use [PILOT_OPERATIONS_PLAYBOOK.md](./PILOT_OPERATIONS_PLAYBOOK.md) for the step-by-step operator path:

- first deployment
- token rotation
- external secret-manager startup
- in-place upgrade
- rollback
- restart/replay validation

## What This Still Is Not

This deployment path is still:

- single-node
- SQLite-backed
- coarse-scope auth at the HTTP layer
- operator-validated rather than auto-remediating

It is a hardened pilot delivery path, not yet a full production platform envelope.
