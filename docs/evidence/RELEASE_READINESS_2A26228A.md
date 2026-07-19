# Release Readiness failure: `2a26228a3b134f12c6be0f0405def043caca4a55`

Candidate `2a26228a3b134f12c6be0f0405def043caca4a55` is permanently
disqualified for commercial beta. It must not be rerun, reinterpreted, or used
to author a successful release-evidence bundle.

## Immutable coordinates

- Candidate tree: `40784bf663db284bb1d2490dd45d579a134ed6e1`
- Candidate ref: `refs/heads/main`
- CI: run `29681719182`, attempt 1, passed
- Supply Chain: run `29681719149`, attempt 1, passed
- Pages: run `29681719158`, attempt 1, passed
- Capacity Planning: run `29683314717`, attempt 1, passed
- Capacity artifact: `8441592931`
- Capacity artifact archive SHA-256:
  `69cc940acb30ca56f6185925813977ff3666faf26d9831e0ae786f69234c6570`
- Release Readiness: run `29684445133`, attempt 1, failed
- Release Readiness failure artifact: `8441824686`
- Failure artifact API size: `50664326` bytes
- Failure artifact archive SHA-256:
  `6f808ef7e518d1302ba8056a615bfe4609e7dbc0a7aa3a9fba7398261b949f1f`
- Failure artifact expiry: `2026-10-17T11:03:08Z`

## Failure bytes

- `service-v2-operability-20260719-110935.json`: 5,148 bytes, SHA-256
  `25939a96855599f05a60b22ef98b451c639f50a7bb951de05ca89ab6653f03e1`
- `release-readiness-20260719-110935.txt`: 122,934 bytes, SHA-256
  `4f2557354c420b1e380b28a2c96e31dae449481212ca1b0ae91459b5e1ae2890`
- `release-readiness-evidence-2a26228a3b134f12c6be0f0405def043caca4a55-29684445133-1.json`:
  2,068 bytes, SHA-256
  `3e5aa73263bbd577842126cc562d3089df8932deef9a2d5cb2bf5fcd4fd1600e`
- `performance-beta-20260719-110935.json`: 5,594 bytes, SHA-256
  `25ff1edb398f4cee1dcc0e24f8c326575902e87c9e686fe6b1e511370cfefe26`

## Root cause and disposition

The canonical Supply Chain package was staged, expanded, digest-matched, and
tested successfully. Release Readiness then failed the required
`package_backup_restore_restart` Service v2 gate. The package backup helper
correctly requires `-ConfirmServiceStopped` and independently refuses a
reachable configured endpoint. The operability harness had already terminated
and waited for the package service process, but it invoked both the backup and
restore helpers without the required acknowledgement.

The focused repair adds that acknowledgement only to the two helper calls after
the existing process-stop boundary and preserves the helpers' independent
reachable-endpoint refusal. Controlled alpha remains unchanged. A successful
repair requires a reviewed merge and an entirely new protected candidate with
new exact-SHA CI, Supply Chain, Pages, Capacity Planning, Release Readiness,
clean-room byte verification, and independent review.
