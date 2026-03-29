# AETHER Education Set

This is the gentle-entry curriculum for AETHER.

It is written for readers who do not want to begin with compiler passes,
runtime strata, or service boundaries. The goal is to make the system feel
graspable before it feels technical.

The method is simple:

- plain talk before jargon
- pictures before abstractions
- worked examples before formal claims
- technical terms only after the reader already understands the shape

Each lesson follows the same teaching rhythm:

1. ask the human question first
2. show the concrete example
3. name the formal concept only after the idea is already familiar

## Reading Order

Read these in order:

1. `docs/EDUCATION/WHAT_AETHER_IS.md`
2. `docs/EDUCATION/TIME_CUTS_AND_MEMORY.md`
3. `docs/EDUCATION/RECURSIVE_CLOSURE.md`
4. `docs/EDUCATION/COORDINATION_AUTHORITY_AND_PROOF.md`
5. `docs/EDUCATION/FEDERATED_TRUTH.md`

That sequence moves from the simplest question, "What sort of thing is this?",
to the larger question, "How does a fabric of exact local truths scale?"

## Interactive Notebook Path

If you want runnable onboarding instead of prose-first onboarding, use:

1. `python/notebooks/README.md`
2. `python/notebooks/01_aether_onramp.ipynb`
3. `python/notebooks/02_time_cuts_and_memory.ipynb`
4. `python/notebooks/03_recursive_closure_and_explain.ipynb`
5. `python/notebooks/04_governed_incident_blackboard.ipynb`
6. `python/notebooks/05_policy_and_sidecars.ipynb`

That path stays inside the current Python boundary and HTTP example service,
so readers can touch the real v1 surfaces while they learn.

## What This Set Is For

Use this set when you need to explain AETHER to:

- new teammates
- curious operators
- technical buyers who are not systems specialists
- design partners
- investors who want the concept clearly before the diligence layer

## What This Set Avoids

It does not try to replace:

- `docs/ARCHITECTURE.md`
- `docs/COMMERCIALIZATION/DISTRIBUTED_TRUTH.md`
- `docs/FEDERATED_TRUTH_IMPLEMENTATION_PLAN.md`
- `SPEC.md`

Those documents are the deeper layers.
This set is the on-ramp.

## Teaching Promise

By the end of these notes, a reader should be able to say:

- what AETHER remembers
- what AETHER derives
- why AETHER can explain itself
- how AETHER decides who may act
- why AETHER avoids pretending the world shares one giant truth

If those five things are clear, the rest of the system becomes much easier to
learn.
