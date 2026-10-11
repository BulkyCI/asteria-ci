"""Phase 9: the a2a_* run lists and profiles, the sender loss trace, the
all-to-all cost pairing and the oracle-headroom gate."""

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analyze  # noqa: E402
import profiles  # noqa: E402
import summarize  # noqa: E402

CELLS = ("ep64-2m", "ep64-256k", "ep16-2m", "ep16-256k")
CALIBRATION = {
    "switch_queue": {c: {"congestion_thresholds": [0.1, 0.2, 0.4], "loss_thresholds": [0.01, 0.02, 0.04]} for c in CELLS},
    "per_receiver": {c: {"congestion_thresholds": [0.2, 0.4, 0.8], "trim_thresholds": [0.03, 0.06, 0.12],
                         "loss_thresholds": {"123240": [0.01, 0.02, 0.04]}} for c in CELLS},
}
TUNING = {
    "graded_per_connection": {"gamma": 1.0},
    "graded_per_receiver": {"gamma": 0.5},
    "graded_per_receiver_mix": {"gamma": 0.25, "sender_pool_weight": 0.75, "loss_interval_ns": 123240},
    "graded_per_receiver_shared": {"gamma": 0.25, "loss_interval_ns": 492960},
}


class Stages(unittest.TestCase):
    def counts(self, calibration, tuning):
        return {s: len(profiles.a2a_stage_runs(s, calibration, tuning)) for s in profiles.A2A_STAGES}

    def test_counts(self):
        self.assertEqual(
            self.counts(None, None),
            {"a2a_oracle_calibration": 12, "a2a_gate": 96, "a2a_calibration": 24, "a2a_tuning": 84, "a2a_comparison": 192},
        )
        self.assertEqual(
            self.counts(CALIBRATION, TUNING),
            {"a2a_oracle_calibration": 12, "a2a_gate": 192, "a2a_calibration": 24, "a2a_tuning": 84, "a2a_comparison": 576},
        )
        admitted = {**TUNING, "admitted": ["ep64-2m-loss1e2_up", "ep16-256k-structural"]}
        # 2 cells by 6 arms by 3 seeds, and 2 healthy pairs by 6 arms by 3 seeds.
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_comparison", None, admitted)), 72)

    def test_keys_unique_and_prefixed(self):
        for stage in profiles.A2A_STAGES:
            runs = profiles.a2a_stage_runs(stage, CALIBRATION, TUNING)
            self.assertEqual(len({r["key"] for r in runs}), len(runs))
            self.assertTrue(all(r["key"].split("-")[0] == stage for r in runs))

    def test_faults(self):
        onset = {"start_ns": profiles.DEGRADE_AT_NS, "spine": 3}
        self.assertEqual(profiles.a2a_fault("loss1e3_down"),
                         [{"model": "gray", "error_rate": 1e-3, **onset, "leaf": 1, "direction": "down"}])
        self.assertEqual(profiles.a2a_fault("blackhole_up"),
                         [{"model": "silent", **onset, "leaf": 0, "direction": "up"}])
        self.assertEqual(profiles.a2a_fault("halfrate_up"),
                         [{"model": "gray", "rate": "200Gbps", **onset, "leaf": 0, "direction": "up"}])
        self.assertEqual(profiles.a2a_fault("structural"),
                         [{"model": "gray", "rate": "200Gbps", "start_ns": 0, "spine": 3}])

    def test_profiles(self):
        runs = {r["key"]: r for r in profiles.a2a_stage_runs("a2a_gate", CALIBRATION, None)}
        oracle = profiles.a2a_stage_profile(runs["a2a_gate-ep16-256k-loss1e2_up-graded_switch_queue-s301"], CALIBRATION)
        workload = oracle["workload"]
        self.assertEqual(workload["group"], {"leaves": [0, 1]})
        self.assertEqual(oracle["steps"], workload["iterations"])
        self.assertEqual(workload["compute_ns"] % 1000, 0)
        balancing = oracle["network"]["load_balancing"]
        self.assertEqual(balancing["grade_cost_source"], "switch_queue")
        self.assertEqual(balancing["gamma"], 0.5)
        self.assertEqual(balancing["loss_thresholds"], [0.01, 0.02, 0.04])
        self.assertEqual(balancing["congestion_thresholds"], [0.1, 0.2, 0.4])
        self.assertNotIn("sender_pool_weight", balancing)
        mix = next(r for r in profiles.a2a_stage_runs("a2a_comparison", CALIBRATION, TUNING)
                   if r["arm"] == "graded_per_receiver_mix")
        balancing = profiles.a2a_stage_profile(mix, CALIBRATION)["network"]["load_balancing"]
        self.assertEqual((balancing["sender_pool_weight"], balancing["loss_interval_ns"]), (0.75, 123240))
        self.assertEqual(balancing["loss_thresholds"], [0.01, 0.02, 0.04])
        self.assertEqual(balancing["trim_thresholds"], [0.03, 0.06, 0.12])

    def test_calibration_runs_use_defaults_and_trace(self):
        runs = profiles.a2a_stage_runs("a2a_calibration", CALIBRATION, None)
        mix = next(r for r in runs if r["arm"] == "graded_per_receiver_mix")
        balancing = profiles.a2a_stage_profile(mix, CALIBRATION)["network"]["load_balancing"]
        self.assertNotIn("congestion_thresholds", balancing)
        self.assertEqual(balancing["loss_thresholds"], profiles.A2A_LOSS_THRESHOLDS_PLACEHOLDER)
        self.assertEqual(balancing["sender_pool_weight"], 0.75)
        self.assertEqual(balancing["loss_interval_ns"], 2 * profiles.BASE_RTT_NS)


class Losses(unittest.TestCase):
    def test_totals_and_detection(self):
        rows = [
            (100, 0, 3, 10, 0), (100, 1, 3, 10, 1), (100, 0, 2, 9, 0),
            (200, 0, 3, 10, 2), (200, 1, 3, 10, 2),
            (300, 0, 3, 10, 0), (300, 1, 2, 7, 1),
            (400, 1, 3, 10, 20),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            path.write_text("time_ns,rank,spine,sent,lost,grade\n" + "".join(f"{t},{r},{s},{n},{lo},3\n" for t, r, s, n, lo in rows))
            result = summarize.spine_losses(path, 3, 50)
        self.assertEqual(result["spines"], {"2": {"sent": 16, "lost": 1}, "3": {"sent": 60, "lost": 25}})
        self.assertEqual(result["cumulative_lost_reached_ns"], {"1": 100, "5": 200, "20": 400})
        self.assertEqual(result["cumulative_lost_reached_after_onset_ns"], {"1": 50, "5": 150, "20": 350})


def run(arm, group, size, condition, seed, stage="a2a_gate", setting=None):
    return {"key": f"{stage}-{group}-{size}-{condition}-{arm}-s{seed}", "stage": stage, "arm": arm, "setting": setting,
            "k": 63, "group": group, "size": size, "condition": condition, "seed": seed}


class Pairing(unittest.TestCase):
    def write(self, root, runs, iterations):
        for r in runs:
            values = iterations[r["key"]]
            directory = root / r["key"]
            directory.mkdir(parents=True)
            timing = {"complete": values is not None,
                      "iteration_completion_ns": {str(i + 1): v for i, v in enumerate(values or [])}}
            (directory / "summary.json").write_text(json.dumps({"all_to_all": timing}))
        listing = root / "runs.json"
        listing.write_text(json.dumps(runs))
        return listing

    def test_pairs_by_arm_group_size_and_seed_across_stages(self):
        runs, values = [], {}

        def add(r, v):
            runs.append(r)
            values[r["key"]] = v

        # Healthy pairs; decoys at the other size and on another seed.
        add(run("uniform", "ep64", "2m", "healthy", 301), [100, 100, 100, 100])
        add(run("uniform", "ep64", "256k", "healthy", 301), [10, 10, 10, 10])
        add(run("uniform", "ep64", "2m", "healthy", 302), [200, 200, 200, 200])
        add(run("reps", "ep64", "2m", "healthy", 301, "a2a_comparison"), [90, 90, 90, 90])
        add(run("uniform", "ep64", "2m", "loss1e2_up", 301), [100, 150, 150, 200])
        add(run("reps", "ep64", "2m", "loss1e2_up", 301, "a2a_comparison"), [90, 100, 110, 140])
        add(run("reps", "ep64", "256k", "loss1e2_up", 301, "a2a_comparison"), None)  # no healthy pair
        add(run("uniform", "ep64", "2m", "loss1e2_up", 302), None)  # incomplete
        with tempfile.TemporaryDirectory() as directory:
            listing = self.write(Path(directory), runs, values)
            rows = analyze.a2a_rows([listing, listing], Path(directory))
        costs = {r["key"]: r for r in analyze.a2a_costs(rows)}
        uniform = costs["a2a_gate-ep64-2m-loss1e2_up-uniform-s301"]
        self.assertEqual(uniform["cost_ns"], 50)
        # p99 by linear interpolation: 150 + 0.97 * 50 against 100.
        self.assertAlmostEqual(uniform["tail_cost_ns"], 98.5)
        self.assertEqual(costs["a2a_comparison-ep64-2m-loss1e2_up-reps-s301"]["cost_ns"], 20)
        self.assertIsNone(costs["a2a_comparison-ep64-256k-loss1e2_up-reps-s301"]["cost_ns"])
        self.assertEqual(costs["a2a_gate-ep64-2m-loss1e2_up-uniform-s302"]["cost_ns"], float("inf"))
        cell = next(c for c in analyze.a2a_cost_cells(rows) if (c["size"], c["condition"]) == ("2m", "loss1e2_up"))
        self.assertAlmostEqual(cell["arms"]["reps"]["recovered"][0], 0.6)
        self.assertEqual(cell["arms"]["reps"]["stages"], ["a2a_comparison"])

    def test_duplicate_pair_is_refused(self):
        rows = [
            {**run("uniform", "ep64", "2m", "healthy", 301), "iterations_ns": [1]},
            {**run("uniform", "ep64", "2m", "healthy", 301, "a2a_comparison"), "iterations_ns": [1]},
        ]
        with self.assertRaises(ValueError):
            analyze.a2a_costs(rows)


def gate_rows(uniform_costs, oracle_costs, healthy=1000):
    rows = []
    for arm, costs in (("uniform", uniform_costs), ("graded_switch_queue", oracle_costs)):
        for seed, cost in zip((301, 302, 303), costs):
            rows.append({**run(arm, "ep64", "2m", "healthy", seed), "iterations_ns": [healthy, healthy]})
            rows.append({**run(arm, "ep64", "2m", "halfrate_up", seed),
                         "iterations_ns": None if cost is None else [healthy + cost, healthy + cost]})
    return rows


class Gate(unittest.TestCase):
    def gate(self, uniform_costs, oracle_costs):
        (row,) = analyze.a2a_gate(analyze.a2a_cost_cells(gate_rows(uniform_costs, oracle_costs)))
        return row

    def test_admitted(self):
        row = self.gate([400, 420, 440], [100, 120, 140])
        self.assertEqual((row["half_rule"], row["seed_rule"], row["admitted"]), (True, True, True))
        self.assertAlmostEqual(row["oracle_over_uniform"], 0.285714, places=5)
        self.assertIn("admitted", analyze.gate_line(row))

    def test_half_rule_fails(self):
        # The oracle beats uniform on every seed beyond the noise but keeps
        # more than half the cost.
        row = self.gate([400, 410, 420], [250, 260, 270])
        self.assertEqual((row["half_rule"], row["seed_rule"], row["admitted"]), (False, True, False))

    def test_seed_rule_fails(self):
        # Half the mean cost, but slower than uniform on seed 303.
        row = self.gate([400, 400, 100], [100, 100, 200])
        self.assertEqual((row["half_rule"], row["seed_rule"], row["admitted"]), (True, False, False))

    def test_no_uniform_cost(self):
        row = self.gate([-10, 0, 10], [-50, -40, -30])
        self.assertFalse(row["half_rule"])
        self.assertFalse(row["admitted"])

    def test_incomplete_oracle_is_dropped_and_missing_seed_unreadable(self):
        self.assertFalse(self.gate([400, 420, 440], [100, None, 140])["admitted"])
        rows = [r for r in gate_rows([400, 420, 440], [100, 120, 140]) if r["seed"] != 303 or r["arm"] == "uniform"]
        (row,) = analyze.a2a_gate(analyze.a2a_cost_cells(rows))
        self.assertIsNone(row["admitted"])
        self.assertIn("unreadable", analyze.gate_line(row))


if __name__ == "__main__":
    unittest.main()
