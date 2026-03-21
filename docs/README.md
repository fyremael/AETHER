# Documentation Center

This directory is the operating manual for the AETHER repository.

The root documents still matter. `SPEC.md`, `RULES.md`, `INTERFACES.md`, and `REPO_LAYOUT.md` remain governing texts. What lives here is the connective tissue around them: the guides that help people understand the current implementation, operate the demonstrations, contribute safely, and keep the documentation itself honest.

The GitHub Pages site is the published front door for this material. It hosts the generated Rust API reference and a curated landing page that points readers back into the handbook documents in the repository.

## Reading Paths

### If you are new to AETHER

Read these in order:

1. `README.md`
2. `examples/demo-03-coordination-situation-room.md`
3. `docs/ARCHITECTURE.md`
4. `docs/STATUS.md`
5. `docs/KNOWN_LIMITATIONS.md`

That path gets you from thesis to running system to current boundary.

### If you are implementing features

Read these in order:

1. `docs/ARCHITECTURE.md`
2. `docs/GLOSSARY.md`
3. `docs/DEVELOPER_WORKFLOW.md`
4. `CONTRIBUTING.md`
5. `docs/DOCUMENTATION_STANDARD.md`

That path is the shortest route to “make a change without accidentally moving the center of gravity.”

### If you are operating demos or presenting the project

Start here:

1. `docs/OPERATIONS.md`
2. `examples/README.md`
3. `scripts/README.md`

### If you are evaluating AETHER as a product or platform

Read:

1. `docs/COMMERCIALIZATION/README.md`
2. `docs/COMMERCIALIZATION/VISION.md`
3. `docs/COMMERCIALIZATION/PRODUCT_NARRATIVE.md`
4. `docs/COMMERCIALIZATION/BUYER_USE_CASE_MATRIX.md`
5. `docs/PILOT_COORDINATION.md`

That path moves from category ambition to buyer story to the actual pilot proof.

### If you are working on the coordination pilot

Read:

1. `docs/PILOT_COORDINATION.md`
2. `docs/PILOT_LAUNCH.md`
3. `docs/STATUS.md`
4. `docs/KNOWN_LIMITATIONS.md`
5. `docs/PERFORMANCE.md`

Those documents explain the durable pilot contract, report artifacts, drift workflow, and the remaining hardening gaps.
They are also the place to look for the current audit semantics on the authenticated pilot service.

### If you need performance numbers

Read:

1. `docs/PERFORMANCE.md`
2. `scripts/README.md`
3. `TESTPLAN.md`

That path explains the operator-facing report, baseline capture, drift comparison, the benchmark harness, the stress workloads, and the target interpretation.

### If you need the long-range picture

Read:

1. `docs/STATUS.md`
2. `docs/ROADMAP.md`
3. `docs/KNOWN_LIMITATIONS.md`

That set answers “what exists now, what does not yet exist, and what comes next.”

## Documentation Map

### Repository root

| Document | Purpose |
| --- | --- |
| `README.md` | Public front door and architectural stance |
| `SPEC.md` | Governing system thesis, data model, and milestone plan |
| `RULES.md` | Rule-language and recursive semantics expectations |
| `INTERFACES.md` | Crate boundaries and trait-shape guidance |
| `REPO_LAYOUT.md` | Required repository structure |
| `TESTPLAN.md` | Test intent and acceptance direction |
| `CONTRIBUTING.md` | Contribution expectations and review contract |

### Docs directory

| Document | Purpose |
| --- | --- |
| `docs/ARCHITECTURE.md` | How the current kernel is shaped and how data moves through it |
| `docs/DEVELOPER_WORKFLOW.md` | Day-to-day engineering loop, verification contract, and definition of done |
| `docs/OPERATIONS.md` | Operator-facing guidance for demos, reports, and presentations |
| `docs/PILOT_COORDINATION.md` | Scope, exit gates, and run path for the current coordination pilot |
| `docs/PILOT_LAUNCH.md` | Launch-readiness contract and validation pack for the current design-partner pilot |
| `docs/PERFORMANCE.md` | Performance workloads, runner commands, benchmark harness, and interpretation guidance |
| `docs/COMMERCIALIZATION/README.md` | Hub for commercialization, product framing, buyer narrative, and messaging |
| `docs/GLOSSARY.md` | Canonical vocabulary for the codebase and its semantics |
| `docs/DOCUMENTATION_STANDARD.md` | Documentation quality bar, update triggers, and maintenance rules |
| `docs/STATUS.md` | What is implemented now |
| `docs/ROADMAP.md` | What is planned next |
| `docs/KNOWN_LIMITATIONS.md` | Where the current implementation is intentionally incomplete |
| `docs/ADR/README.md` | ADR policy and location for numbered architecture decisions |

## Principles

The documentation system follows a few non-negotiable rules:

- say what the system does today before describing what it may do later
- distinguish governing specification from current implementation guide
- optimize for precision, not hype
- make the operator path as explicit as the developer path
- make the product story as disciplined as the implementation story
- treat process documentation as product surface, not internal trivia

## Maintenance

Documentation quality is not a cleanup phase after implementation. It is part of the implementation.

The working rule is simple: if a change affects behavior, architecture, workflow, operator experience, or public expectations, the relevant documentation should change in the same commit.

The full maintenance standard lives in `docs/DOCUMENTATION_STANDARD.md`.
