# Go Boundary

This directory now contains the first real Go operator shell for AETHER.

Scope for Go in v1:

- CLI and admin commands
- deployment-oriented process wrappers
- service lifecycle integration
- narrow API boundary consumers

Implemented today:

- `cmd/aetherctl`, a real CLI over the stable HTTP boundary
- `cmd/aetherctl tui`, a pilot-focused read-mostly operator cockpit for live service health, coordination state, audit entries, history, and tuple proof traces
- `internal/client`, a typed Go HTTP client for health, history, audit, pilot coordination reports, document runs, and tuple explanation
- request-level policy-context support for document execution, with authenticated tokens able to impose the maximum semantic visibility that requests may narrow but not exceed
- explain, report, and history calls now follow the same token-bound effective policy as document execution on authenticated services
- Go unit coverage via `go test ./...`

Current commands:

```bash
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 health
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 history
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 run --file ./document.aether
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 run --file ./document.aether --capabilities executor --visibilities ops
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 explain --tuple-id 7 --capabilities executor
go run ./cmd/aetherctl --base-url http://127.0.0.1:3000 --token-file ./pilot-operator.token tui --refresh 2s
```

`aetherctl tui` is the live pilot operations entrance. It is intentionally narrow:

- read-only in v1
- backed only by the authenticated HTTP service
- focused on the current coordination pilot rather than arbitrary AETHER workflows

Auth handling for `aetherctl` now supports:

- `--token`
- `--token-file`
- `AETHER_TOKEN` as the fallback when neither flag is supplied

Current test command:

```bash
go test ./...
```

Out of scope:

- authoritative semantic execution
- resolver duplication
- rule-engine ownership
