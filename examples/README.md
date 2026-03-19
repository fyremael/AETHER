# Examples

This directory holds worked examples and end-to-end walkthroughs for the AETHER kernel.

Start here:

- `demo-01-temporal-dependency-horizon.md` is the first public showcase: temporal replay plus recursive closure over the same journal.
- `transitive-closure.md` shows the first real recursive vertical slice, from datoms through resolution, compilation, and fixed-point evaluation.

For non-technical Windows operators:

- double-click `scripts/run-demo-01.cmd`

Until the DSL parser lands, examples use the Rust AST surface directly. That is a temporary authoring choice, not a change in the long-term semantic center of gravity.
