# STATUS

## Current state

The repository has been upgraded from a pure spec package to an implementation scaffold.

Completed:

- Rust workspace root created
- canonical Rust crates added under `crates/`
- Go and Python boundary directories created
- initial docs placeholders added for ADRs, roadmap, and limitations

Not yet completed:

- semantic invariants beyond basic type scaffolding
- deterministic resolver behavior for all merge classes
- real rule parsing, planning, and semi-naive evaluation
- end-to-end acceptance tests from `TESTPLAN.md`

## Immediate focus

Milestone `M0` remains the next implementation target:

- element IDs
- schema typing
- datom journal
- in-memory store
- temporal replay
