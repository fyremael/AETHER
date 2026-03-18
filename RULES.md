# RULES.md — AETHER DSL and Recursive Semantics

## 1. Rule-language stance

The rule layer is a first-class subsystem.

It is not:

- a helper around host-language loops,
- a one-off reachability module,
- a set of hand-coded graph traversals,
- an afterthought bolted onto a non-recursive query engine.

## 2. Canonical authoring surface

The canonical authoring surface is the **AETHER DSL**.

The DSL must be able to express:

- schema declarations,
- attribute merge classes,
- extensional facts,
- queries,
- recursive rules,
- temporal views,
- policy/capability annotations.

Host-language AST builders may exist, but they are secondary.

## 3. Minimal v1 rule form

A rule has the form:

\[
H(\bar{x}) \leftarrow B_1(\bar{x}_1), \dots, B_m(\bar{x}_m), \neg C_1(\bar{y}_1), \dots, \neg C_n(\bar{y}_n)
\]

v1 supports:

- extensional predicates from resolved state,
- intensional predicates from rule heads,
- monotone recursion,
- stratified negation,
- bounded aggregation per stratum.

## 4. Required DSL sections

A program should be able to declare:

- schema,
- predicates,
- rules,
- materialization directives,
- optional explain directives.

Illustrative shape:

```text
schema {
  attr task.status: ScalarLWW<TaskStatus>
  attr task.depends_on: RefSet<Task>
}

predicates {
  task_depends_on(Task, Task)
  depends_transitive(Task, Task)
}

rules {
  depends_transitive(x, y) <- task_depends_on(x, y)
  depends_transitive(x, z) <- depends_transitive(x, y), task_depends_on(y, z)
}
```

Exact syntax is open, but the semantics are not.

## 5. Compilation pipeline

Every rule set must pass through:

1. parse/build AST,
2. variable-safety validation,
3. schema/type validation,
4. predicate dependency graph construction,
5. SCC decomposition,
6. stratification check,
7. semi-naive delta-plan generation,
8. phase-graph lowering,
9. materialization registration.

## 6. Evaluation model

Let `I_k` be the accumulated intensional facts at iteration `k`.

\[
I_{k+1} = I_k \cup \Delta I_{k+1}
\]

with

\[
\Delta I_{k+1} = T(I_k) \setminus I_k
\]

Termination occurs when `ΔI` is empty.

## 7. Required derivation metadata

Each derived tuple must record:

- tuple ID,
- predicate,
- rule ID,
- source datom IDs,
- parent derived tuple IDs,
- stratum,
- SCC ID,
- iteration.

## 8. Mandatory v1 rule workloads

The implementation must directly support:

- transitive dependency closure,
- provenance closure,
- artifact ancestry,
- capability inheritance,
- task readiness,
- reachability over claim/lease/task graphs.

## 9. Explainability requirement

The rule subsystem must expose:

- plan explanation,
- SCC explanation,
- derivation explanation for a tuple,
- iteration counts and delta sizes.

## 10. Deferred semantics

Not in v1:

- non-stratified negation,
- weighted/probabilistic rules,
- arbitrary lattice-valued confidence propagation,
- full differential maintenance over unbounded update streams.
