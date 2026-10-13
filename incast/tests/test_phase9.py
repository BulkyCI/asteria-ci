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
            {"a2a_oracle_calibration": 12, "a2a_gate": 96, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 207,
             "a2a_gate_reps": 192, "a2a_candidates": 384, "a2a_receiver": 288},
        )
        self.assertEqual(
            self.counts(CALIBRATION, TUNING),
            {"a2a_oracle_calibration": 12, "a2a_gate": 192, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 591,
             "a2a_gate_reps": 192, "a2a_candidates": 576, "a2a_receiver": 288},
        )
        admitted = {**TUNING, "admitted": ["ep64-2m-loss1e2_up", "ep16-256k-structural"]}
        # 2 cells by 6 arms by 3 seeds, and 2 healthy pairs by 6 arms by 3
        # seeds; plus the soft oracle reading, 4 loss cells and a healthy
        # pair by 3 seeds.
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_comparison", None, admitted)), 87)

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
        self.assertEqual(balancing["held_exploration"], "none")
        self.assertNotIn("held_sentinel_interval_ns", balancing)
        self.assertNotIn("pool_decay_interval_ns", balancing)
        self.assertNotIn("loss_hold_clean_sentinels", balancing)
        mix = next(r for r in profiles.a2a_stage_runs("a2a_comparison", CALIBRATION, TUNING)
                   if r["arm"] == "graded_per_receiver_mix")
        balancing = profiles.a2a_stage_profile(mix, CALIBRATION)["network"]["load_balancing"]
        self.assertEqual((balancing["sender_pool_weight"], balancing["loss_interval_ns"]), (0.75, 123240))
        self.assertEqual(balancing["loss_thresholds"], [0.01, 0.02, 0.04])
        self.assertEqual(balancing["trim_thresholds"], [0.03, 0.06, 0.12])
        self.assertEqual(balancing["held_exploration"], "sentinel")
        self.assertEqual(balancing["held_sentinel_interval_ns"], profiles.A2A_SENTINEL_INTERVAL_NS)
        self.assertEqual(balancing["pool_decay_interval_ns"], 2 * profiles.BASE_RTT_NS)
        self.assertEqual(balancing["loss_hold_clean_sentinels"], profiles.A2A_LOSS_HOLD_CLEAN_SENTINELS)
        conn = next(r for r in profiles.a2a_stage_runs("a2a_comparison", CALIBRATION, TUNING)
                    if r["arm"] == "graded_per_connection")
        balancing = profiles.a2a_stage_profile(conn, CALIBRATION)["network"]["load_balancing"]
        self.assertEqual(balancing["held_exploration"], "sentinel")
        self.assertNotIn("pool_decay_interval_ns", balancing)

    def test_tuning_sweeps_sentinel_and_pool_decay(self):
        runs = profiles.a2a_stage_runs("a2a_tuning", None, None)
        settings = {(r["arm"], json.dumps(r["setting"], sort_keys=True)) for r in runs}
        self.assertEqual(len(settings), 96)
        for run_ in runs:
            balancing = profiles.a2a_stage_profile(run_, None)["network"]["load_balancing"]
            self.assertEqual(balancing["held_sentinel_interval_ns"], run_["setting"]["held_sentinel_interval_ns"])
            self.assertEqual("pool_decay_interval_ns" in balancing, balancing.get("sender_pool_weight", 0) > 0)
        sentinels = {r["setting"]["held_sentinel_interval_ns"] for r in runs}
        self.assertEqual(sentinels, {24648, 123240, 1_000_000})
        decays = {r["setting"].get("pool_decay_interval_ns") for r in runs if r["arm"] == "graded_per_receiver_mix"}
        self.assertEqual(decays, {24648, 123240})
        self.assertEqual({r["setting"]["loss_hold_clean_sentinels"] for r in runs}, {256, 1024})
        # The loss hold crosses gamma only: 4 arms by 3 gammas at n = 256,
        # the other dimensions at their defaults.
        held = [(r["arm"], r["setting"]) for r in runs if r["setting"]["loss_hold_clean_sentinels"] == 256]
        self.assertEqual(len({(arm, json.dumps(x, sort_keys=True)) for arm, x in held}), 12)
        for _, setting in held:
            self.assertEqual(setting["held_sentinel_interval_ns"], profiles.A2A_SENTINEL_INTERVAL_NS)
            self.assertEqual(setting.get("pool_decay_interval_ns", profiles.A2A_POOL_DECAY_INTERVAL_NS),
                             profiles.A2A_POOL_DECAY_INTERVAL_NS)
            self.assertEqual(setting.get("sender_pool_weight", 0.75), 0.75)
            self.assertEqual(setting.get("loss_interval_ns", 24648), 24648)
        for run_ in runs:
            balancing = profiles.a2a_stage_profile(run_, None)["network"]["load_balancing"]
            self.assertEqual(balancing["loss_hold_clean_sentinels"], run_["setting"]["loss_hold_clean_sentinels"])

    def test_tuning_mix_is_the_mix_arm_alone(self):
        full = profiles.a2a_stage_runs("a2a_tuning", CALIBRATION, None)
        runs = profiles.a2a_stage_runs("a2a_tuning_mix", CALIBRATION, None)
        # The mix arm's 45 settings, each with its healthy pair.
        self.assertEqual(len(runs), 90)
        self.assertEqual({r["arm"] for r in runs}, {"graded_per_receiver_mix"})
        self.assertEqual({(r["group"], r["size"], r["condition"], r["seed"]) for r in runs},
                         {("ep64", "256k", c, 201) for c in ("loss1e2_up", "healthy")})
        mix = [r for r in full if r["arm"] == "graded_per_receiver_mix"]
        self.assertEqual([r["key"].replace("a2a_tuning-", "a2a_tuning_mix-", 1) for r in mix],
                         [r["key"] for r in runs])
        self.assertEqual([r["setting"] for r in mix], [r["setting"] for r in runs])

    def test_pool_hold_propagates_in_the_mix_arm_only(self):
        for stage in profiles.A2A_STAGES:
            for run_ in profiles.a2a_stage_runs(stage, CALIBRATION, TUNING):
                balancing = profiles.a2a_stage_profile(run_, CALIBRATION)["network"]["load_balancing"]
                if run_["arm"] == "graded_per_receiver_mix":
                    self.assertIs(balancing["pool_hold_propagates"], True)
                    self.assertGreater(balancing["sender_pool_weight"], 0)
                else:
                    self.assertNotIn("pool_hold_propagates", balancing)

    def test_tuning_cell_is_the_headline(self):
        runs = profiles.a2a_stage_runs("a2a_tuning", None, None)
        self.assertEqual({(r["group"], r["size"]) for r in runs}, {("ep64", "256k")})
        self.assertEqual({r["condition"] for r in runs}, {"loss1e2_up", "healthy"})
        runs = profiles.a2a_stage_runs("a2a_tuning", {"headline": "ep16-2m-loss1e3_down"}, None)
        self.assertEqual(len(runs), 192)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         {("ep16", "2m", "loss1e3_down"), ("ep16", "2m", "healthy")})
        for bad in ("loss1e2_up", "ep64-1m-loss1e2_up", "ep64-256k-healthy", "ep32-2m-loss1e2_up"):
            with self.assertRaises(ValueError):
                profiles.a2a_stage_runs("a2a_tuning", {"headline": bad}, None)

    def test_committed_calibration_headline(self):
        path = Path(profiles.__file__).resolve().parent / "a2a_calibration.json"
        calibration = json.loads(path.read_text())
        self.assertEqual(profiles.a2a_headline(calibration), ("ep64", "256k", "loss1e2_up"))
        self.assertIn("headline_reason", calibration)
        runs = profiles.a2a_stage_runs("a2a_tuning", calibration, None)
        self.assertEqual(len(runs), 192)
        self.assertEqual({(r["group"], r["size"]) for r in runs}, {("ep64", "256k")})

    def test_soft_oracle_reading(self):
        runs = [r for r in profiles.a2a_stage_runs("a2a_comparison", CALIBRATION, {"admitted": []})
                if r["arm"] == profiles.A2A_SOFT_ORACLE]
        self.assertEqual(len(runs), 15)
        self.assertEqual({(r["group"], r["size"]) for r in runs}, {("ep64", "2m")})
        self.assertEqual({r["condition"] for r in runs},
                         {"loss1e3_up", "loss1e2_up", "loss1e3_down", "loss1e2_down", "healthy"})
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertIn("a2a_comparison-ep64-2m-loss1e2_up-graded_switch_queue_soft-s301", {r["key"] for r in runs})
        soft = profiles.a2a_stage_profile(runs[0], CALIBRATION)["network"]["load_balancing"]
        oracle = profiles.a2a_balancing(profiles.A2A_ORACLE, "ep64", "2m", CALIBRATION, profiles.A2A_ORACLE_SETTING)
        self.assertEqual(soft["loss_thresholds"], [0.0005, 0.002, 0.008])
        self.assertEqual(soft["held_exploration"], "none")
        self.assertEqual({k: v for k, v in soft.items() if k != "loss_thresholds"},
                         {k: v for k, v in oracle.items() if k != "loss_thresholds"})

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


class Candidates(unittest.TestCase):
    def test_runs_and_settings(self):
        tuning = json.loads((Path(profiles.__file__).parent / "a2a_tuning.json").read_text())
        calibration = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_candidates", calibration, tuning)
        # The gate's 28 cells, whatever a2a_tuning.json admits, and 4
        # healthy pairs by 6 arms by 3 seeds.
        self.assertEqual(len(runs), (28 + 4) * 6 * 3)
        self.assertEqual({r["arm"] for r in runs}, {"reps_hold_auto", "reps_hold_host", "reps_flow_hold",
                                                    "mrc_hold_auto", "shared_floor1", "mix_floor1"})
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs if r["condition"] != "healthy"},
                         set(profiles.a2a_gate_cells()))
        self.assertLessEqual({r["condition"] for r in runs}, {"healthy", *profiles.A2A_FAULTS})
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, calibration)["network"]["load_balancing"]
                  for r in runs if (r["group"], r["size"], r["condition"]) == ("ep64", "256k", "loss1e2_up")}
        mix = tuning["graded_per_receiver_mix"]
        thresholds = calibration["per_receiver"]["ep64-256k"]["loss_thresholds"][str(mix["loss_interval_ns"])]
        hold = {"mode": "spray_uniform", "loss_thresholds": thresholds, "held_exploration": "sentinel",
                "loss_interval_ns": mix["loss_interval_ns"], "loss_hold_clean_sentinels": 1024,
                "held_sentinel_interval_ns": 1_000_000, "sentinel_mode": "duplicate",
                "hold_release": "sentinels"}
        self.assertEqual(by_arm["reps_hold_auto"], {**hold, "selector": "reps_hold", "hold_scope": "auto"})
        self.assertEqual(by_arm["reps_hold_host"], {**hold, "selector": "reps_hold", "hold_scope": "host"})
        self.assertEqual(by_arm["reps_flow_hold"], {**hold, "selector": "reps_flow_hold"})
        # MRC's skip_base_rtts and probe_timeouts at the defaults the mrc arm runs at.
        self.assertEqual(by_arm["mrc_hold_auto"], {**hold, "selector": "mrc_hold", "hold_scope": "auto"})
        for arm in profiles.A2A_REPS_HOLD_ARMS:
            self.assertNotIn("hold_rule", by_arm[arm])
        self.assertEqual(by_arm["mix_floor1"], {**by_arm["mix_floor1"], "congestion_grade_floor": 1, **mix})
        self.assertNotIn("pool_hold_propagates", by_arm["mix_floor1"])
        self.assertEqual(by_arm["shared_floor1"]["congestion_grade_floor"], 1)
        self.assertNotIn("pool_hold_propagates", by_arm["shared_floor1"])
        self.assertEqual(by_arm["shared_floor1"]["sender_pool_weight"], 1.0)


class Receiver(unittest.TestCase):
    def test_runs_and_settings(self):
        tuning = json.loads((Path(profiles.__file__).parent / "a2a_tuning.json").read_text())
        calibration = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_receiver", calibration, tuning)
        self.assertEqual(len(runs), (28 + 4) * 3 * 3)
        self.assertEqual({r["arm"] for r in runs}, {"reps_hold_auto_rx", "reps_hold_auto", "reps_flow_hold_rx"})
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, calibration)["network"]["load_balancing"]
                  for r in runs if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep64", "2m", "healthy", 301)}
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", calibration, tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep64", "2m", "healthy", 301)}
        receiver = {"path_seq": True, "receiver_hold": True, "receiver_hold_clean": 1024}
        self.assertEqual(by_arm["reps_hold_auto"], candidates["reps_hold_auto"])
        self.assertEqual(by_arm["reps_hold_auto_rx"], {**candidates["reps_hold_auto"], **receiver})
        self.assertEqual(by_arm["reps_flow_hold_rx"], {**candidates["reps_flow_hold"], **receiver})
        self.assertEqual(by_arm["reps_hold_auto_rx"]["sentinel_mode"], "duplicate")


class ReceiverAndTimeouts(unittest.TestCase):
    def test_receiver_reaction(self):
        onset = analyze.DEGRADE_AT_NS
        header = "time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n"
        rows = [
            (onset - 10, 9, 0, 3, 100, 1, "leaf"),  # before the onset
            (onset + 100, 9, 1, 3, 100, 1, "leaf"),  # another sender leaf
            (onset + 200, 9, 0, 2, 100, 1, "leaf"),  # another spine
            (onset + 300, 9, 0, 3, 100, 0, "none"),
            (onset + 400, 9, 0, 3, 100, 1, "host"),
            (onset + 500, 1, 0, 3, 100, 1, "leaf"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ns3").mkdir()
            (root / "ns3" / "receiver_holds.csv").write_text(
                header + "".join(",".join(map(str, r)) + "\n" for r in rows))
            self.assertEqual(analyze.receiver_reaction(root, "loss1e2_up"),
                             {"receiver_reaction_ns": 400, "receiver_basis": "sender_leaf 0"})
            # Down: the receivers of leaf 1 (rank 9) alone, any sender leaf.
            self.assertEqual(analyze.receiver_reaction(root, "loss1e2_down")["receiver_reaction_ns"], 100)
            self.assertEqual(analyze.receiver_reaction(root, "structural")["receiver_reaction_ns"],
                             onset - 10)
            self.assertEqual(analyze.receiver_reaction(root / "none", "loss1e2_up"),
                             {"receiver_reaction_ns": None, "receiver_basis": None})

    def test_rto_counts_per_iteration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ns3").mkdir()
            timing = {"complete": True, "iteration_start_ns": {"1": 100, "2": 1000, "3": 5000}}
            (root / "summary.json").write_text(json.dumps({"all_to_all": timing}))
            self.assertEqual(analyze.rto_counts(root), {"rto_events": None, "rto_per_iteration": None})
            times = (50, 100, 999, 1000, 4000, 6000, 7000)
            (root / "ns3" / "rto_events.csv").write_text(
                "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n"
                + "".join(f"{t},0,1,3,0,4096\n" for t in times))
            self.assertEqual(analyze.rto_counts(root), {"rto_events": 7, "rto_per_iteration": [3, 2, 2]})

    def test_candidates_carry_the_new_measures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for condition in ("healthy", "loss1e2_up"):
                r = run("reps_hold_auto_rx", "ep64", "2m", condition, 301, "a2a_receiver")
                runs.append(r)
                d = root / r["key"]
                (d / "ns3").mkdir(parents=True)
                timing = {"complete": True, "iteration_completion_ns": {"1": 10, "2": 10},
                          "iteration_start_ns": {"1": 0, "2": 10}}
                (d / "summary.json").write_text(json.dumps({"all_to_all": timing}))
                (d / "ns3" / "rto_events.csv").write_text(
                    "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n5,0,1,3,1,1\n")
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_candidates(analyze.a2a_rows([listing], root, "a2a_receiver"), root)
            entry = cell["arms"]["reps_hold_auto_rx"]
            self.assertEqual(entry["rto_events"], [1])
            self.assertEqual(entry["rto_per_iteration"], [[1, 0]])
            self.assertEqual(entry["receiver_reaction_ns"], [None])
            self.assertIn("RTOs mean", analyze.candidates_markdown([cell]))


class Reaction(unittest.TestCase):
    def trace(self, root, header, rows):
        (root / "ns3").mkdir(parents=True)
        (root / "ns3" / "spine_losses.csv").write_text(header + "\n" + "".join(",".join(map(str, r)) + "\n" for r in rows))

    def test_share_below_a_sixteenth_after_onset(self):
        onset = analyze.DEGRADE_AT_NS
        interval = 100_000
        first = onset // interval  # the interval holding the onset
        rows = []
        for index, share3 in ((first - 2, 0), (first, 10), (first + 1, 7), (first + 2, 5)):
            end = (index + 1) * interval
            # host 0 (leaf 0) sends 100 per interval, spine 3 share3 of them;
            # host 9 (leaf 1) sends only on spine 3.
            rows += [(end, 0, 3, share3, 0, 3), (end, 0, 1, 100 - share3, 0, 3), (end, 9, 3, 50, 0, 3)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.trace(root, "time_ns,rank,spine,sent,lost,grade", rows)
            result = analyze.reaction(root, "loss1e2_up", interval)
            self.assertEqual(result, {"reaction_ns": (first + 3) * interval - onset, "share_basis": "hosts of leaf 0"})
            # Down: without destination_leaf, the hosts off leaf 1 (host 0 alone here).
            self.assertEqual(analyze.reaction(root, "loss1e2_down", interval)["share_basis"], "hosts off leaf 1")
            self.assertIsNone(analyze.reaction(root / "none", "loss1e2_up", interval)["reaction_ns"])

    def test_destination_leaf_rows(self):
        end = (analyze.DEGRADE_AT_NS // 1000 + 2) * 1000
        rows = [(end, 0, 1, 3, 1, 0, 3), (end, 0, 1, 0, 99, 0, 3), (end, 0, 2, 3, 50, 0, 3), (end, 0, 2, 0, 50, 0, 3)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.trace(root, "time_ns,rank,destination_leaf,spine,sent,lost,grade", rows)
            result = analyze.reaction(root, "loss1e2_down", 1000)
        self.assertEqual(result, {"reaction_ns": end - analyze.DEGRADE_AT_NS, "share_basis": "destination_leaf 1"})

    def test_early_cost(self):
        onset = analyze.DEGRADE_AT_NS

        def write(root, starts, completions, complete=True):
            root.mkdir()
            timing = {"complete": complete,
                      "iteration_completion_ns": {str(i + 1): c for i, c in enumerate(completions)},
                      "iteration_start_ns": {str(i + 1): s for i, s in enumerate(starts)}}
            (root / "summary.json").write_text(json.dumps({"all_to_all": timing}))
            return root

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            starts = [0, onset - 1, onset + 100_000, onset + 400_000, onset + 600_000]
            healthy = write(root / "h", starts, [100, 100, 100, 100, 100])
            faulted = write(root / "f", starts, [100, 150, 170, 120, 900])
            self.assertEqual(analyze.early_cost(faulted, healthy, "loss1e2_up"), 70 + 20)
            # Structural's onset is 0: iteration 1 alone starts within 500 us of it.
            self.assertEqual(analyze.early_cost(faulted, healthy, "structural"), 0)
            incomplete = write(root / "i", starts, [100], complete=False)
            self.assertEqual(analyze.early_cost(incomplete, healthy, "loss1e2_up"), float("inf"))
            self.assertIsNone(analyze.early_cost(faulted, None, "loss1e2_up"))


class GateReps(unittest.TestCase):
    def rows(self, oracle_costs, reps_costs, healthy=1000):
        rows = []
        for arm, costs in (("reps_oracle", oracle_costs), ("reps", reps_costs)):
            for seed, cost in zip((301, 302, 303), costs):
                rows.append({**run(arm, "ep64", "256k", "healthy", seed, "a2a_gate_reps"),
                             "iterations_ns": [healthy, healthy]})
                rows.append({**run(arm, "ep64", "256k", "loss1e2_up", seed, "a2a_gate_reps"),
                             "iterations_ns": None if cost is None else [healthy + cost, healthy + cost]})
        (row,) = analyze.a2a_gate_reps(analyze.a2a_cost_cells(rows))
        return row

    def test_verdicts(self):
        row = self.rows([100, 110, 120], [300, 310, 320])
        self.assertEqual((row["reps_oracle_mean_ns"], row["reps_mean_ns"], row["verdict"]), (110, 310, "data_helps"))
        self.assertEqual(self.rows([100, 300, 120], [300, 310, 320])["verdict"], "seed_rule_not_met")
        # An incomplete run costs infinity, so it is read, not unreadable.
        self.assertEqual(self.rows([100, None, 120], [300, 310, 320])["verdict"], "seed_rule_not_met")
        (empty,) = analyze.a2a_gate_reps([{"group": "ep64", "size": "256k", "condition": "loss1e2_up", "arms": {}}])
        self.assertIsNone(empty["verdict"])
        self.assertIn("| data_helps |", analyze.gate_reps_markdown([row]))

    def test_stage_profiles(self):
        runs = profiles.a2a_stage_runs("a2a_gate_reps", CALIBRATION, TUNING)
        self.assertEqual({r["arm"] for r in runs}, {"reps_oracle", "reps"})
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual(len({(r["group"], r["size"], r["condition"]) for r in runs}), 32)
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, CALIBRATION)["network"]["load_balancing"] for r in runs}
        self.assertEqual(by_arm["reps_oracle"],
                         {"mode": "spray_uniform", "selector": "reps_oracle", "grade_cost_source": "switch_queue"})
        self.assertEqual(by_arm["reps"], {"mode": "ev_hash", "selector": "reps"})


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


class Iterations(unittest.TestCase):
    def test_per_group_and_size(self):
        runs = profiles.a2a_stage_runs("a2a_gate", None, None)
        by_cell = {(r["group"], r["size"]): (r["iterations"], r["compute_ns"]) for r in runs}
        self.assertEqual(by_cell[("ep64", "2m")], (12, 2_070_000))
        self.assertEqual(by_cell[("ep16", "256k")], (40, 62_000))
        run_ = next(r for r in runs if (r["group"], r["size"]) == ("ep64", "2m"))
        document = profiles.a2a_stage_profile(run_, None)
        self.assertEqual((document["steps"], document["workload"]["iterations"]), (12, 12))


def write_calibration_run(root, key, reports, losses=None):
    """A run directory with a summary, spine_reports.csv rows of (time,
    rank, sequence, spine, arrivals, mark, trim, loss) and, when given,
    spine_losses.csv rows of (time, rank, spine, sent, lost)."""
    ns3 = root / key / "ns3"
    ns3.mkdir(parents=True)
    (root / key / "summary.json").write_text("{}")
    (ns3 / "spine_reports.csv").write_text(
        "time_ns,rank,sequence,spine,arrivals,mark_fraction,trim_fraction,grade,loss_fraction\n"
        + "".join(f"{t},{r},{q},{s},{a},{m},{tr},3,{lo}\n" for t, r, q, s, a, m, tr, lo in reports))
    if losses is not None:
        (ns3 / "spine_losses.csv").write_text(
            "time_ns,rank,spine,sent,lost,grade\n" + "".join(f"{t},{r},{s},{n},{lo},3\n" for t, r, s, n, lo in losses))


class A2aCalibrate(unittest.TestCase):
    INTERVAL = 2 * profiles.BASE_RTT_NS

    def reports(self, hot_mark, hot_trim=0.0, hot_loss=0.0):
        # 100 reports of eight spines; spine 3 marks, trims and loses above
        # the others, who sit at the median.
        return [(i * 1000, 0, i, s, 100, 0.1 + (hot_mark if s == 3 else 0), 0.01 + (hot_trim if s == 3 else 0),
                 hot_loss if s == 3 else 0.0) for i in range(100) for s in range(8)]

    def losses(self, sent, lost_at=(), spines=8, hosts=(0, 8), intervals=40):
        """Every host of hosts sends `sent` per spine per two-base-RTT
        interval; lost_at lists the (host, interval, spine) that lose one."""
        return [((i + 1) * self.INTERVAL, h, s, sent, 1 if (h, i, s) in lost_at else 0)
                for h in hosts for i in range(intervals) for s in range(spines)]

    def calibrate(self, runs, existing=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            listed = []
            for r, reports, losses in runs:
                write_calibration_run(root, r["key"], reports, losses)
                listed.append(r)
            listing = root / "runs.json"
            listing.write_text(json.dumps(listed + [run("graded_switch_queue", "ep64", "2m", "healthy", 999,
                                                         "a2a_oracle_calibration")]))  # no summary: skipped
            return analyze.a2a_calibrate([listing], root, existing or {})

    def test_scopes_cells_and_rules(self):
        intervals = [str(i) for i in profiles.A2A_LOSS_INTERVALS_NS]
        self.assertEqual(intervals, ["24648", "123240", "492960"])
        oracle = run("graded_switch_queue", "ep16", "256k", "healthy", 101, "a2a_oracle_calibration")
        connection = run("graded_per_connection", "ep64", "2m", "healthy", 101, "a2a_calibration")
        mix = run("graded_per_receiver_mix", "ep64", "2m", "healthy", 101, "a2a_calibration")
        result = self.calibrate([
            (oracle, self.reports(0.02, hot_loss=0.0), None),
            (connection, self.reports(0.5, 0.05), None),
            # 50 sends per spine per interval, none lost: a zero p99, so
            # (f/4, f/2, f) for one loss f, 1/50, 1/250 and 1/1000 at the
            # three intervals, and one loss grades 0.
            (mix, self.reports(0.03, 0.02), self.losses(50)),
        ], existing={"switch_queue": {"ep64-2m": {"kept": True}}})
        sq = result["switch_queue"]
        self.assertEqual(sq["ep64-2m"], {"kept": True})
        self.assertEqual(sq["ep16-256k"]["congestion_thresholds"], [0.02, 0.04, 0.08])
        self.assertNotIn("trim_thresholds", sq["ep16-256k"])
        # Ground-truth loss: none, so the 1e-4 floor of the congestion rule;
        # no count floor at one loss in 100 arrivals.
        self.assertEqual(sq["ep16-256k"]["loss_thresholds"], [0.0001, 0.0002, 0.0004])
        self.assertTrue(sq["ep16-256k"]["loss_calibration"]["floored"])
        self.assertIsNone(sq["ep16-256k"]["loss_calibration"]["one_loss_fraction"])
        self.assertEqual(sq["ep16-256k"]["loss_calibration"]["rule"], "p99, 1e-4 floor, no count floor")
        self.assertEqual(result["per_receiver"]["ep64-2m"]["loss_calibration"]["24648"]["rule"],
                         "p99 zero: one loss grades 0")
        pooled = result["per_receiver"]["ep64-2m"]
        self.assertEqual(pooled["congestion_thresholds"], [0.03, 0.06, 0.12])
        self.assertEqual(pooled["trim_thresholds"], [0.02, 0.04, 0.08])
        self.assertEqual(pooled["loss_thresholds"], {"24648": [0.005, 0.01, 0.02], "123240": [0.001, 0.002, 0.004],
                                                     "492960": [0.00025, 0.0005, 0.001]})
        per_connection = result["per_connection"]["ep64-2m"]
        self.assertTrue(per_connection["capped"])
        self.assertEqual(per_connection["congestion_thresholds"], [0.25, 0.5, 1.0])
        self.assertEqual(per_connection["trim_thresholds"], [0.05, 0.1, 0.2])
        self.assertEqual(per_connection["loss_thresholds"], pooled["loss_thresholds"])
        # Leaf 0's host sends 50 x 20 = 1000 per 40-base-RTT interval on
        # spine 3: one expected loss at 1e-3 keeps the 1e-3 headline.
        self.assertEqual(result["headline_rule"]["expected_losses_at_1e3"], 1.0)
        self.assertEqual(result["headline"], "ep64-2m-loss1e3_up")
        # The listing round-trips through the stages.
        balancing = profiles.a2a_balancing("graded_per_receiver_mix", "ep64", "2m", result,
                                           {"gamma": 0.5, "sender_pool_weight": 0.75, "loss_interval_ns": 123240})
        self.assertEqual(balancing["loss_thresholds"], [0.001, 0.002, 0.004])
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_tuning", result, None)), 192)

    def test_one_loss_grades_0(self):
        # A zero p99: (f/4, f/2, f), rounded down so one loss reaches f.
        zero = analyze.loss_thresholds(0.0, [133, 133, 140])
        self.assertEqual(zero["rule"], "p99 zero: one loss grades 0")
        self.assertTrue(zero["floored"])
        self.assertEqual(zero["loss_thresholds"], [0.001879, 0.003759, 0.007518])
        self.assertLessEqual(zero["loss_thresholds"][2], 1 / 133)
        # A p99 above zero but below one loss takes the same rule.
        below = analyze.loss_thresholds(0.001, [100])
        self.assertEqual(below["rule"], "p99 below one loss: one loss grades 0")
        self.assertEqual(below["loss_thresholds"], [0.0025, 0.005, 0.01])
        # At or above one loss, the p99's (t, 2t, 4t).
        above = analyze.loss_thresholds(0.02, [100])
        self.assertEqual(above["rule"], "p99, count floor")
        self.assertFalse(above["floored"])
        self.assertEqual(above["loss_thresholds"], [0.02, 0.04, 0.08])

    def test_oracle_loss_has_no_count_floor(self):
        # Spine 3 reads a ground-truth loss of 1e-3 in every report, an
        # eighth of the samples: the p99 is 1e-3, well under one loss in
        # 100 arrivals, and the thresholds follow it.
        oracle = run("graded_switch_queue", "ep64", "2m", "healthy", 101, "a2a_oracle_calibration")
        result = self.calibrate([(oracle, self.reports(0.0, hot_loss=1e-3), None)])
        sq = result["switch_queue"]["ep64-2m"]
        self.assertEqual(sq["loss_thresholds"], [0.001, 0.002, 0.004])
        self.assertFalse(sq["loss_calibration"]["floored"])

    def test_p99_above_the_floor_and_the_fallback_headline(self):
        mix = run("graded_per_receiver_mix", "ep64", "2m", "healthy", 101, "a2a_calibration")
        # 40 sends per spine per interval; host 0 loses one on spine 0 in
        # every interval: 1/40 in 64 of 640 two-base-RTT samples, above the
        # p99 and the floor; at 40 base RTTs host 0's spine 0 reads 20/800.
        lost = {(0, i, 0) for i in range(40)}
        result = self.calibrate([(mix, self.reports(0.03), self.losses(40, lost, hosts=(0, 8)))])
        calibration = result["per_receiver"]["ep64-2m"]["loss_calibration"]
        self.assertAlmostEqual(calibration["24648"]["p99"], 1 / 40)
        self.assertFalse(calibration["24648"]["floored"])
        self.assertEqual(result["per_receiver"]["ep64-2m"]["loss_thresholds"]["24648"], [0.025, 0.05, 0.1])
        self.assertEqual(calibration["24648"]["rule"], "p99, count floor")
        self.assertAlmostEqual(calibration["492960"]["p99"], 1 / 40)
        # 800 sends per 40-base-RTT interval on spine 3: 0.8 expected losses.
        self.assertAlmostEqual(result["headline_rule"]["expected_losses_at_1e3"], 0.8)
        self.assertEqual(result["headline"], "ep64-2m-loss1e2_up")
        # A headline set by hand, with its reason, survives recalibration.
        kept = {"headline": "ep64-256k-loss1e2_up", "headline_reason": "gate"}
        result = self.calibrate([(mix, self.reports(0.03), self.losses(40, lost, hosts=(0, 8)))], existing=kept)
        self.assertEqual(result["headline"], "ep64-256k-loss1e2_up")
        self.assertAlmostEqual(result["headline_rule"]["expected_losses_at_1e3"], 0.8)

    def test_interval_sums(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_calibration_run(root, "r", [], [(self.INTERVAL * k, 3, 3, 10, k % 2) for k in range(1, 11)])
            sums = analyze.interval_losses(root / "r", 5 * self.INTERVAL)
        self.assertEqual(dict(sums), {(3, 0, 3): [50, 3], (3, 1, 3): [50, 2]})


if __name__ == "__main__":
    unittest.main()
