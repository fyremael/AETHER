# Pilot Operations Playbook

This playbook is the operator-facing companion to [PILOT_DEPLOYMENT.md](./PILOT_DEPLOYMENT.md).

It describes the safe, repeatable motions for:

- first deployment
- token rotation
- external secret-manager startup
- in-place upgrade
- rollback
- restart and replay verification

The current scope is still a single-node pilot service. The goal is disciplined service operation, not full platform orchestration.

## Deployment Playbook

1. Build a fresh package:

   ```text
   powershell -ExecutionPolicy Bypass -File scripts/build-pilot-package.ps1
   ```

2. Expand the generated ZIP onto the target host.
3. Confirm the package contains:
   - `bin/aether_pilot_service.exe`
   - `config/pilot-service.json`
   - `config/pilot-operator.token`
   - `rotate-pilot-token.ps1`
   - `docs/PILOT_DEPLOYMENT.md`
4. Start the service with `run-pilot-service.cmd`.
5. Confirm:
   - the service binds to the configured address
   - `logs/audit.jsonl` is created
   - `data/coordination.sqlite` is created
   - `/health` responds
6. Run the launch pack before exposing the pilot to stakeholders:

   ```text
   powershell -ExecutionPolicy Bypass -File scripts/run-pilot-launch-validation.ps1
   ```

## Token Rotation Playbook

Default file-backed rotation path:

1. Stop the service.
2. Rotate the token:

   ```text
   .\rotate-pilot-token.cmd
   ```

3. Distribute the new bearer token to approved operators.
4. Restart the service.
5. Verify:
   - the old token is rejected
   - the new token succeeds
   - audit entries continue to append normally

The packaged rotation script backs up the previous token file before writing the new one.

## External Secret-Manager Playbook

The pilot service now supports a command-based token source:

- `token_command`

This allows the deployment config to fetch a bearer token from an external secret manager or broker at startup.

Example shape:

```json
{
  "principal": "pilot-operator",
  "scopes": ["append", "query", "explain", "ops"],
  "policy_context": {
    "capabilities": ["executor"],
    "visibilities": []
  },
  "token_command": [
    "pwsh",
    "-NoProfile",
    "-File",
    "../scripts/fetch-pilot-token.ps1"
  ]
}
```

Operational rules:

- the command must print only the bearer token to stdout
- a non-zero exit code aborts service startup
- empty stdout is rejected
- the command is evaluated at startup, so restart is the reload boundary

Good fits for `token_command`:

- cloud secret-manager CLIs
- vault brokers
- local secure-wrapper scripts

Avoid commands that print banners, prompts, or status lines to stdout.

## Upgrade Playbook

1. Run the launch pack on the candidate revision.
2. Build a fresh package.
3. Stop the existing service.
4. Snapshot:
   - `data/coordination.sqlite`
   - adjacent sidecar catalog
   - `logs/audit.jsonl`
   - current config files
5. Replace:
   - `bin/`
   - packaged docs
   - package-local helper scripts
6. Preserve:
   - `data/`
   - `logs/`
   - active token source unless intentionally rotated
7. Start the new binary.
8. Verify:
   - `/health`
   - authenticated query
   - authenticated explain
   - audit append
   - expected restart/replay timings remain within drift budget

## Rollback Playbook

Use rollback when the new binary fails launch validation or shows unacceptable drift.

1. Stop the service.
2. Restore the previous:
   - `bin/`
   - packaged helper scripts
   - config set if it changed with the release
3. Restore the database snapshot only if the upgrade attempted a destructive migration.
4. Restart the prior version.
5. Re-run the launch pack to confirm the rollback baseline.

For the current pilot path, journal and sidecar schemas are intentionally simple, so rollback should usually be binary/config level rather than data rollback.

## Restart And Replay Check

The performance report now includes durable restart/replay workloads. Treat these as the canonical restart health indicators:

- `Durable restart current replay`
- `Durable restart coordination replay`

These workloads should be reviewed during:

- release acceptance
- host migration
- storage-path changes
- secret-source changes that alter startup behavior

## Evidence To Preserve

For each significant deployment or upgrade, keep:

- launch transcript
- latest performance report
- latest drift report
- package hash or ZIP artifact
- config provenance
- token-source mode used for the deployment

The pilot service is meant to be explainable operationally as well as semantically. Preserve the evidence that explains the deployment decision too.
