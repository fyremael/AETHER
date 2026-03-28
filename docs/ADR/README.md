# ADR

Architecture decision records for AETHER should live in this directory.

Seed documents already captured at the repository root:

- `IMPLEMENTATION_DECISION.md`
- `REPO_LAYOUT.md`
- `INTERFACES.md`

As implementation work proceeds, promote durable decisions into numbered ADR documents here.

The durable parts of `IMPLEMENTATION_DECISION.md` are now represented in the
numbered ADR set as the repository has matured. The root document remains as
historical context, but the numbered ADRs are the maintained architecture
record.

## Numbering

Use zero-padded ascending numbers:

- `0001-...`
- `0002-...`

The first formal ADR in this repository is:

- `docs/ADR/0001-authority-partitions-and-federated-cuts.md`

The current numbered ADR set is:

- `docs/ADR/0001-authority-partitions-and-federated-cuts.md`
- `docs/ADR/0002-governed-incident-blackboard-is-demo-packaging.md`
- `docs/ADR/0003-rust-is-mainline-kernel-language.md`
- `docs/ADR/0004-aether-dsl-is-canonical-semantics-surface.md`
- `docs/ADR/0005-recursion-compiles-through-scc-and-semi-naive-execution.md`
- `docs/ADR/0006-go-is-a-shell-not-the-core-runtime.md`
- `docs/ADR/0007-sidecars-remain-subordinate-to-semantic-control.md`
