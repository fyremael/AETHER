# Colab Tutorials

This directory holds the interactive onboarding lane for AETHER.

These notebooks are designed for:

- first-time developers
- curious operators
- design partners who want to run the examples instead of only reading them
- SDK users who want a guided path across the current Python boundary

Each notebook is independent and Colab-friendly.
When opened in Colab, the setup cells clone the repository if needed, install
Rust if `cargo` is not present, and start the real
`crates/aether_api/examples/http_kernel_service.rs` boundary.

The first run in a fresh Colab session takes a few minutes because it installs
the Rust toolchain and compiles the example service.

## Notebook Series

| Notebook | Best use | Open in Colab |
| --- | --- | --- |
| `01_aether_onramp.ipynb` | Fastest first runnable introduction | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/01_aether_onramp.ipynb) |
| `02_time_cuts_and_memory.ipynb` | Learn replay, history, and `AsOf` cuts | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/02_time_cuts_and_memory.ipynb) |
| `03_recursive_closure_and_explain.ipynb` | Learn recursion, fixed point, and tuple explanation | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/03_recursive_closure_and_explain.ipynb) |
| `04_governed_incident_blackboard.ipynb` | Product-facing governed board walkthrough | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/04_governed_incident_blackboard.ipynb) |
| `05_policy_and_sidecars.ipynb` | Learn policy narrowing plus artifact/vector sidecars | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/05_policy_and_sidecars.ipynb) |
| `06_ai_support_resolution_desk.ipynb` | ML-facing support application walkthrough | [Open](https://colab.research.google.com/github/fyremael/AETHER/blob/main/python/notebooks/06_ai_support_resolution_desk.ipynb) |

## Suggested Order

Work through the notebooks in numeric order.

That progression mirrors the wider education path:

1. what AETHER is
2. how time cuts work
3. how recursive closure and proof work
4. how governed coordination feels in a product story
5. how policy and sidecar memory stay subordinate to semantic control
6. how a buyer-relevant support application can be built honestly on that proof

## Local Use

If you are running locally instead of in Colab:

1. open Jupyter from the repository root
2. open a notebook from this directory
3. run the setup cell

The setup helper will reuse the local checkout instead of cloning the repo.
