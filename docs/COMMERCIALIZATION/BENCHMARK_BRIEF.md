# Benchmark Brief

This brief is the short, field-ready readout of the current AETHER benchmark
surface.

The goal is not to drown the reader in microbenchmark tables. The goal is to
show what the current kernel and pilot can already do, where the dev host and
WSL differ, and where the release gate is calm versus merely acceptable.

The numbers below come from the current host-aware benchmark matrix and drift
artifacts:

- `artifacts/performance/matrix/latest.md`
- `artifacts/performance/latest-drift-core_kernel.md`
- `artifacts/performance/latest-drift-service_in_process.md`

The comparison hosts are:

- `CHAD (Windows native)`
- `CHAD (WSL Ubuntu)`

## Release Readout

- The current release-readiness pass completed successfully.
- The accepted regression gate remains the native Windows dev host for:
  - `core_kernel`
  - `service_in_process`
- The `service_in_process` gate is currently `ok`.
- The `core_kernel` gate is currently `warn`, not `fail`, driven by `Compiler SCC planning | recursive width 64`.
- HTTP pilot boundary and replicated-partition workloads are measured and reported, but they remain observational rather than fail-level release gates.

## Chart 1: Core Kernel Throughput

```text
Higher is better

Resolver current      Win  326.6K | ##############################
                      WSL  549.2K | ##################################################

Resolver AsOf         Win  718.0K | #############################
                      WSL 1240.0K | ###################################################

Recursive closure 128 Win   27.6K | #############
                      WSL   55.6K | ###########################

Tuple explain 128     Win   37.4K | ###############################
                      WSL   37.6K | ################################
```

Interpretation:
AETHER’s semantic center is real and fast enough to profile meaningfully. The
most interesting split is not “kernel versus boundary,” but “Windows native
versus WSL Ubuntu” on the same machine. WSL materially outperforms native
Windows on state resolution and recursive closure, while tuple explanation is
nearly identical across both. That is a good sign: the explain path looks
stable, while the heavier resolver/runtime paths appear more sensitive to host
and toolchain environment than to semantic instability.

## Chart 2: Durable Replay And Service Path

```text
Higher is better

Durable current replay     Win   63.0K | ############################
                           WSL  109.1K | ##################################################

Coordination run           Win   36.3K | ##############################
                           WSL   48.3K | ########################################

Durable coordination replay
                           Win   15.7K | ##############
                           WSL   44.3K | ########################################
```

Interpretation:
The replay-heavy path is where host differences become most visible. That does
not weaken the kernel claim. It sharpens it. AETHER is already measuring the
difference between in-process semantic work and durable restart/replay work,
which is exactly the distinction operators and advisors care about. The Windows
pilot remains solid and release-gated; WSL shows that the same semantic kernel
can move much faster under a different host/runtime profile.

## Chart 3: HTTP Pilot Boundary

```text
Higher is better

Coordination report   Win   1.78K | ################################
                      WSL   1.80K | ################################

Delta report          Win     868 | ###############################
                      WSL     901 | ################################

History               Win 288.1K  | ###############################################
                      WSL 306.7K  | ##################################################

Tuple explain         Win 104.3K  | ###########################################
                      WSL 121.2K  | ##################################################
```

Interpretation:
The pilot HTTP boundary is already in the right shape for a design-partner
surface. The meaningful paths, coordination report, delta report, history, and
tuple explanation, are broadly comparable across Windows and WSL. That is more
important than raw `/health` vanity numbers. It suggests that the operator and
service-facing semantics are being carried cleanly through the HTTP seam rather
than collapsing under host-specific request overhead.

## Chart 4: Replicated Prototype, Reads Versus Write Paths

```text
Lower latency is better

Leader append admission     Win   46.3 ms | ######################
                            WSL 1908.5 ms | ##################################################

Follower catch-up           Win   36.3 ms | #################
                            WSL  521.6 ms | ##################################################

Federated history read      Win   22.7 ms | ###############################
                            WSL    3.6 ms | #####

Federated run report        Win   19.7 ms | ########################
                            WSL    4.1 ms | #####

Manual promotion            Win   23.6 ms | ###########################
                            WSL   20.2 ms | #######################
```

Interpretation:
This is the most honest “prototype versus product” chart in the brief. The
replicated authority-partition slice already behaves like a real measured
system, not a thought experiment, but it is not yet smooth across all paths.
The federated read/report path is very fast on WSL, while the write-side leader
append and follower catch-up paths are much rougher there. That is exactly why
this suite is observational today. The prototype is semantically useful and
operationally real, but its variance still needs work before it should become a
release gate.

## Chart 5: Windows Accepted Drift

```text
Delta from accepted Windows baseline

Journal append 10K        +0.56% | +
Journal append 50K       -10.63% | ----------
Resolver current         -14.06% | --------------
Resolver AsOf            -10.06% | ----------
Durable restart current   -0.72% | -
Compiler SCC width 16     -8.46% | --------
Compiler SCC width 64    -19.60% | -------------------   WARN
Recursive closure 64     -14.65% | ---------------
Recursive closure 128    -10.89% | ----------
Tuple explain 128         -7.22% | -------
```

Interpretation:
The release gate is doing its job. It is not panicking, and it is not asleep.
Most gated core workloads remain inside the accepted budget, with one
warn-level outlier in wider SCC planning. That is the right operational
outcome: the release remains green, but the system tells us where attention
belongs. Just as important, both structural footprint estimates stayed flat at
`+0.00%`, which means semantic output size did not quietly inflate while
throughput moved around.

## What This Means

Three conclusions matter right now:

1. The semantic kernel is measurable, stable, and already differentiated by the
   kinds of workloads we care about: replay, recursive closure, proof, and
   governed coordination.
2. The single-node pilot boundary is strong enough to benchmark honestly, not
   just demonstrate visually.
3. The replicated partition slice is real enough to instrument and compare, but
   still exploratory enough that we should keep it out of fail-level release
   gating for now.

That is a healthy place to be. The current numbers support the present claim we
want to make:

**AETHER is a release-ready single-node semantic kernel and pilot service, with
a measured replicated-truth prototype that is promising but not yet promoted to
the same operational bar.**

## Caveats

- These are still single-machine measurements.
- The accepted release baselines are currently native Windows on the canonical
  dev host.
- WSL and GitHub-runner data are comparative, not yet accepted release
  references.
- Memory figures remain structural lower-bound estimates, not allocator-exact
  telemetry.
- HTTP and replicated-partition suites are intentionally measured ahead of full
  gating so we can learn their variance before we overstate their stability.
