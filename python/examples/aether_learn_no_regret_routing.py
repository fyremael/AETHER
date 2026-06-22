#!/usr/bin/env python3
from __future__ import annotations

import argparse, csv, json, math, random
from pathlib import Path

WORKERS = {
    "fast_cheap_worker": (0.25, 90, {"simple": .88, "math": .35, "code": .38, "ambiguous": .45, "adversarial": .20}),
    "accurate_expensive_worker": (1.00, 520, {"simple": .96, "math": .78, "code": .75, "ambiguous": .70, "adversarial": .52}),
    "math_specialist_worker": (0.62, 260, {"simple": .70, "math": .91, "code": .46, "ambiguous": .56, "adversarial": .42}),
    "code_specialist_worker": (0.66, 280, {"simple": .68, "math": .48, "code": .90, "ambiguous": .58, "adversarial": .44}),
}
FAMILIES = ["simple", "math", "code", "ambiguous", "adversarial"]


def phase(step: int, horizon: int) -> str:
    if step < horizon // 3:
        return "A"
    if step < 2 * horizon // 3:
        return "B"
    return "C"


def distribution(ph: str):
    if ph == "A":
        return [("simple", .70), ("math", .10), ("code", .10), ("ambiguous", .10)]
    if ph == "B":
        return [("simple", .15), ("math", .40), ("code", .35), ("ambiguous", .10)]
    return [("simple", .20), ("math", .25), ("code", .25), ("ambiguous", .15), ("adversarial", .15)]


def draw_family(rng: random.Random, ph: str) -> str:
    roll, acc = rng.random(), 0.0
    for fam, prob in distribution(ph):
        acc += prob
        if roll <= acc:
            return fam
    return distribution(ph)[-1][0]


def expected(worker_id: str, fam: str) -> float:
    cost, latency, skill = WORKERS[worker_id]
    success = skill.get(fam, .2)
    task_value = success * 1.0 + (1.0 - success) * -.35
    return task_value - .35 * cost - .0008 * latency


def observe(worker_id: str, fam: str, rng: random.Random):
    cost, latency, skill = WORKERS[worker_id]
    ok = rng.random() < skill.get(fam, .2)
    seen_latency = max(5, int(rng.gauss(latency, latency * .10)))
    reward = (1.0 if ok else -.35) - .35 * cost - .0008 * seen_latency
    return round(reward, 6), seen_latency, ("none" if ok else f"missed_{fam}")


class UCB:
    def __init__(self, arms: int):
        self.n = [0] * arms
        self.v = [0.0] * arms
        self.t = 0

    def choose(self, exploration: float):
        for i, count in enumerate(self.n):
            if count == 0:
                return i, 999.0
        scores = [value + exploration * math.sqrt(math.log(self.t) / count) for value, count in zip(self.v, self.n)]
        i = max(range(len(scores)), key=scores.__getitem__)
        return i, round(scores[i] - self.v[i], 6)

    def update(self, arm: int, reward: float):
        self.t += 1
        self.n[arm] += 1
        self.v[arm] += (reward - self.v[arm]) / self.n[arm]


def put(ledger, tuple_type: str, **fields):
    ledger.append({"tuple_type": tuple_type, **fields})


def run(horizon: int, exploration: float, seed: int):
    rng = random.Random(seed)
    ids = list(WORKERS)
    states = {fam: UCB(len(ids)) for fam in FAMILIES}
    ledger, reward_sum, oracle_sum = [], 0.0, 0.0
    phase_reward = {"A": 0.0, "B": 0.0, "C": 0.0}
    phase_regret = {"A": 0.0, "B": 0.0, "C": 0.0}
    phase_count = {"A": 0, "B": 0, "C": 0}

    for step in range(horizon):
        ph = phase(step, horizon)
        fam = draw_family(rng, ph)
        task_id = f"task-{step:04d}"
        put(ledger, "TaskTuple", task_id=task_id, phase=ph, family=fam, budget=1.25, deadline_ms=750, visibility_context="research-demo")

        for wid in ids:
            put(ledger, "ProposalTuple", task_id=task_id, worker_id=wid, predicted_utility=round(expected(wid, fam) + rng.gauss(0, .05), 6), predicted_cost=WORKERS[wid][0], confidence=round(WORKERS[wid][2].get(fam, .2), 6))

        arm, bonus = states[fam].choose(exploration)
        wid = ids[arm]
        put(ledger, "RoutingDecisionTuple", task_id=task_id, router_id="ucb1-router-v0", candidates=ids, selected_worker=wid, selected_arm_index=arm, exploration_bonus=bonus, router_hash=f"ucb1-e{exploration:.3f}")

        reward, latency, failure = observe(wid, fam, rng)
        states[fam].update(arm, reward)
        put(ledger, "RoutingOutcomeTuple", task_id=task_id, selected_worker=wid, realized_utility=reward, realized_cost=WORKERS[wid][0], latency_ms=latency, failure_mode=failure)

        oracle = max(expected(worker_id, fam) for worker_id in ids)
        reward_sum += reward
        oracle_sum += oracle
        regret = oracle_sum - reward_sum
        phase_reward[ph] += reward
        phase_regret[ph] += oracle - reward
        phase_count[ph] += 1
        update_id = f"router-update-{step:04d}"
        put(ledger, "RouterUpdateTuple", update_id=update_id, router_id="ucb1-router-v0", task_id=task_id, selected_worker=wid, reward=reward, cumulative_regret=round(regret, 6), evidence=f"outcome:{task_id}:{failure}")
        put(ledger, "PromotionTuple", artifact_id=update_id, decision=("accepted_local" if reward > -.2 else "kept_for_evidence"), accepted_scope="demo-partition", reason=("positive realized utility" if reward > -.2 else "negative outcome retained for learning"))

    return {
        "summary": {
            "horizon": horizon,
            "seed": seed,
            "exploration": exploration,
            "cumulative_reward": round(reward_sum, 6),
            "oracle_reward": round(oracle_sum, 6),
            "cumulative_regret": round(oracle_sum - reward_sum, 6),
            "learned_values": {fam: {ids[i]: round(v, 6) for i, v in enumerate(state.v)} for fam, state in states.items()},
            "selection_counts": {fam: {ids[i]: state.n[i] for i in range(len(ids))} for fam, state in states.items()},
            "phase_avg_reward": {ph: round(phase_reward[ph] / max(1, phase_count[ph]), 6) for ph in ["A", "B", "C"]},
            "phase_regret": {ph: round(phase_regret[ph], 6) for ph in ["A", "B", "C"]},
        },
        "ledger": ledger,
    }


def write(result, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(result["summary"], indent=2) + "\n", encoding="utf-8")
    (output_dir / "ledger.jsonl").write_text("\n".join(json.dumps(row, sort_keys=True) for row in result["ledger"]) + "\n", encoding="utf-8")
    with (output_dir / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        rows = csv.writer(handle)
        rows.writerow(["metric", "value"])
        for key, value in result["summary"].items():
            rows.writerow([key, json.dumps(value, sort_keys=True) if isinstance(value, dict) else value])


def main():
    parser = argparse.ArgumentParser(description="Run the AETHER-Learn no-regret routing proof.")
    parser.add_argument("--horizon", type=int, default=240)
    parser.add_argument("--exploration", type=float, default=.85)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--output-dir", type=Path, default=Path("target/aether-learn/no-regret-routing"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.horizon < 12:
        raise SystemExit("--horizon must be at least 12")
    result = run(args.horizon, args.exploration, args.seed)
    write(result, args.output_dir)
    if args.json:
        print(json.dumps(result["summary"], indent=2))
    else:
        s = result["summary"]
        print("AETHER-Learn no-regret routing proof")
        print(f"  cumulative reward: {s['cumulative_reward']}")
        print(f"  oracle reward: {s['oracle_reward']}")
        print(f"  cumulative regret: {s['cumulative_regret']}")
        print(f"  phase average reward: {s['phase_avg_reward']}")
        print(f"  selection counts: {s['selection_counts']}")
        print(f"  artifacts: {args.output_dir}")


if __name__ == "__main__":
    main()
