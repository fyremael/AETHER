# Learning Tuple Schema

This document defines the first AETHER-Learn tuple vocabulary.

## Routing proof tuples

- TaskTuple: task id, phase, family, budget, deadline, context.
- ProposalTuple: task id, worker id, predicted utility, predicted cost, confidence.
- RoutingDecisionTuple: task id, router id, candidates, selected worker, exploration score, router hash.
- RoutingOutcomeTuple: task id, selected worker, realized utility, realized cost, latency, failure mode.
- RouterDeltaTuple: delta id, router id, task id, selected worker, reward, cumulative regret, evidence.
- PromotionTuple: artifact id, decision, accepted scope, reason.

## General rule

Large payloads stay outside the semantic journal and enter by reference. Each learning tuple should preserve source, context, cut or timestamp, and provenance.
