"""Phase 9: the a2a_* run lists and profiles, the sender loss trace, the
all-to-all cost pairing and the oracle-headroom gate."""

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

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


# Every stage, a2a_sweep with its compute gaps taken as measured.
@mock.patch.object(profiles, "A2A_SWEEP_COMPUTE_MEASURED", True)
class Stages(unittest.TestCase):
    def counts(self, calibration, tuning):
        return {s: len(profiles.a2a_stage_runs(s, calibration, tuning)) for s in profiles.A2A_STAGES}

    def test_counts(self):
        self.assertEqual(
            self.counts(None, None),
            {"a2a_oracle_calibration": 12, "a2a_gate": 96, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 207,
             "a2a_gate_reps": 192, "a2a_candidates": 384, "a2a_receiver": 288, "a2a_rs3": 24,
             "a2a_tail": 288, "a2a_ackloss": 72, "a2a_candidates_ack": 288, "a2a_tail_ack": 480,
             "a2a_sweep_probe": 5, "a2a_sweep": 432},
        )
        self.assertEqual(
            self.counts(CALIBRATION, TUNING),
            {"a2a_oracle_calibration": 12, "a2a_gate": 192, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 591,
             "a2a_gate_reps": 192, "a2a_candidates": 576, "a2a_receiver": 288, "a2a_rs3": 24,
             "a2a_tail": 288, "a2a_ackloss": 72, "a2a_candidates_ack": 288, "a2a_tail_ack": 480,
             "a2a_sweep_probe": 5, "a2a_sweep": 432},
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
        self.assertEqual(profiles.a2a_fault("loss3e3_up"),
                         [{"model": "gray", "error_rate": 3e-3, **onset, "leaf": 0, "direction": "up"}])
        self.assertEqual(profiles.a2a_fault("loss3e2_up"),
                         [{"model": "gray", "error_rate": 3e-2, **onset, "leaf": 0, "direction": "up"}])
        self.assertEqual(profiles.a2a_fault("rate16g_up"),
                         [{"model": "gray", "rate": "16Gbps", **onset, "leaf": 0, "direction": "up"}])
        with self.assertRaises(ValueError):
            profiles.a2a_fault("rate16g_down")

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


class Pinned(unittest.TestCase):
    """The stages already run keep their runs and profiles: with the
    committed calibration and tuning files, each stage's run count and the
    SHA-256 of its (run, profile) list as sorted-key JSON."""

    STAGES = {
        "a2a_oracle_calibration": (12, "31f5990e685ae5f60638c1eaf52ad5f69c6596c63a7c5f5eb768b49f77cb16c5"),
        "a2a_gate": (192, "3c357e69fa16e34a9fc55a62f963139e96a7a23b1d1f959956e616fd1612cbe0"),
        "a2a_calibration": (24, "675d8c23fc6a6f8a67b757db8562622c600845540cc78f967986001515754628"),
        "a2a_tuning": (192, "925db500b66a10101bdbdb42648082fab13142c00ab46c2c98745f09e167cfc5"),
        "a2a_tuning_mix": (90, "74f7d49039a9af6db751907c32dd1b22857a09e2faff3a0402b31fda93e950e8"),
        "a2a_comparison": (483, "c474e142572f80deec35a34d684bd888f10b8f37ccbd15749f60b762e18759a4"),
        "a2a_gate_reps": (192, "24aa26836d4b8b9d84ee1f891fa3b5491fc66a2e24ec5a40d8b9796b148a6e7c"),
        "a2a_candidates": (576, "7a61e5928a9a6e4783158628bc9da3a0dd14c0a05b4a02f564af65b945a77c8a"),
        "a2a_receiver": (288, "8766cfad51e73e4ec67c4f56e1a9847bbda29b66360d33b75d81ea16f50a01d5"),
        "a2a_rs3": (24, "08327ca646ad41a5c726d432c8914523f8842bea24b80996bf7d3a37c890bc7d"),
        "a2a_tail": (288, "1423a97492ff2063138f9d8d7e6940612d0ea70a3f6f13b786175c38e13dee26"),
        "a2a_ackloss": (72, "4b07e07ca7ddbda6e89ff7582467dc78775041c61b1376e91249366cfba0072b"),
        "a2a_candidates_ack": (288, "b56bc643a6790b42f5784d2bd4d1626bcceee7b21dc515488776fea7e128c8cb"),
    }

    def test_runs_and_profiles_unchanged(self):
        here = Path(profiles.__file__).parent
        calibration = json.loads((here / "a2a_calibration.json").read_text())
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        for stage, pinned in self.STAGES.items():
            runs = profiles.a2a_stage_runs(stage, calibration, tuning)
            text = json.dumps([[r, profiles.a2a_stage_profile(r, calibration)] for r in runs], sort_keys=True)
            self.assertEqual((len(runs), hashlib.sha256(text.encode()).hexdigest()), pinned, stage)


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

    def test_record_filter_selects_the_hold_arms(self):
        # The workflow's record_filter, passed to shards.py as --filter, on
        # the run keys <stage>-<group>-<size>-<condition>-<arm>-s<seed>.
        tuning = json.loads((Path(profiles.__file__).parent / "a2a_tuning.json").read_text())
        calibration = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_candidates", calibration, tuning)
        hold = set(profiles.A2A_REPS_HOLD_ARMS)
        expression = "(" + "|".join(sorted(hold)) + ")-s[0-9]+$"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a2a_candidates").mkdir()
            (root / "a2a_candidates" / "runs.json").write_text(json.dumps(runs))
            subprocess.run(
                [sys.executable, str(Path(profiles.__file__).parent / "shards.py"), str(root),
                 "--filter", expression, "--runs-per-shard", "3", "--out", str(root / "shards.json")],
                check=True,
            )
            keys = [k for shard in json.loads((root / "shards.json").read_text()) for k in shard["keys"].split()]
        self.assertEqual(sorted(keys), sorted(r["key"] for r in runs if r["arm"] in hold))
        # The 28 gate cells and 4 healthy pairs by 4 arms by 3 seeds.
        self.assertEqual(len(keys), (28 + 4) * 4 * 3)


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


class Tail(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())
        self.runs = profiles.a2a_stage_runs("a2a_tail", self.calibration, self.tuning)

    def test_runs(self):
        # The 16 loss cells and the 4 blackhole cells of the gate, whatever
        # the tuning file admits, and 4 healthy pairs, by 4 arms by 3 seeds.
        self.assertEqual(len(self.runs), (16 + 4 + 4) * 4 * 3)
        self.assertEqual({r["arm"] for r in self.runs},
                         {"uniform_tail", "reps_tail", "reps_hold_auto_tail", "reps_hold_auto_tail4"})
        self.assertEqual({r["seed"] for r in self.runs}, {301, 302, 303})
        faulted = {(r["group"], r["size"], r["condition"]) for r in self.runs if r["condition"] != "healthy"}
        loss = {c for c in faulted if c[2].startswith("loss")}
        self.assertEqual(loss, {(g, s, c) for g, s in profiles.A2A_CELLS
                                for c in ("loss1e2_up", "loss1e3_up", "loss1e2_down", "loss1e3_down")})
        self.assertEqual(self.runs, profiles.a2a_stage_runs("a2a_tail", self.calibration, None))
        self.assertEqual({c for c in faulted if c[2] == "blackhole_up"},
                         {(g, s, "blackhole_up") for g, s in profiles.A2A_CELLS})
        self.assertEqual(len(faulted), 20)
        self.assertEqual({(r["group"], r["size"]) for r in self.runs if r["condition"] == "healthy"},
                         set(profiles.A2A_CELLS))
        self.assertEqual(len({r["key"] for r in self.runs}), len(self.runs))

    def test_settings(self):
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                  for r in self.runs if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep16", "2m", "loss1e2_up", 301)}
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep16", "2m", "loss1e2_up", 301)}
        tail = {"path_seq": True, "tail_recovery": True}
        self.assertEqual(by_arm["uniform_tail"], {"mode": "spray_uniform", **tail})
        self.assertEqual(by_arm["reps_tail"], {"mode": "spray_uniform", "selector": "reps_spine", **tail})
        self.assertEqual(by_arm["reps_hold_auto_tail"], {**candidates["reps_hold_auto"], **tail})
        self.assertEqual(by_arm["reps_hold_auto_tail"]["sentinel_mode"], "duplicate")
        self.assertNotIn("hold_rule", by_arm["reps_hold_auto_tail"])
        self.assertNotIn("tail_recovery_delay_ns", by_arm["reps_hold_auto_tail"])
        self.assertNotIn("tail_loss_rtts", by_arm["reps_hold_auto_tail"])
        self.assertEqual(by_arm["reps_hold_auto_tail4"],
                         {**candidates["reps_hold_auto"], **tail, "tail_loss_rtts": 4})

    def test_candidates_cover_the_tail_arms(self):
        onset = analyze.DEGRADE_AT_NS
        settings = profiles.a2a_tail_settings(self.tuning)
        interval = settings["reps_hold_auto_tail"]["loss_interval_ns"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, setting in settings.items():
                for condition, mean in (("healthy", 10), ("loss1e2_up", 30)):
                    r = run(arm, "ep16", "2m", condition, 301, "a2a_tail", setting)
                    runs.append(r)
                    d = root / r["key"]
                    (d / "ns3").mkdir(parents=True)
                    timing = {"complete": True, "iteration_completion_ns": {"1": mean, "2": mean},
                              "iteration_start_ns": {"1": 0, "2": onset}}
                    (d / "summary.json").write_text(json.dumps({"all_to_all": timing}))
                    (d / "ns3" / "rto_events.csv").write_text(
                        "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n5,0,1,3,0,1\n")
                    if arm in ("reps_hold_auto_tail", "reps_hold_auto_tail4"):
                        end = (onset // interval + 1) * interval
                        (d / "ns3" / "spine_losses.csv").write_text(
                            "time_ns,rank,spine,sent,lost,grade\n"
                            f"{end},0,3,0,0,3\n{end},0,1,100,0,3\n")
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_candidates(analyze.a2a_rows([listing], root, "a2a_tail"), root)
        self.assertEqual(set(cell["arms"]), set(settings))
        for arm, entry in cell["arms"].items():
            self.assertEqual(entry["cost_ns"], [20.0])
            self.assertEqual(entry["rto_per_iteration"], [[1, 0]])
            self.assertEqual(entry["setting"], settings[arm])
        for arm in ("reps_hold_auto_tail", "reps_hold_auto_tail4"):
            self.assertEqual(cell["arms"][arm]["reaction_ns"],
                             [(onset // interval + 1) * interval - onset])
        # No sender loss trace under the uniform draw or reps_oracle.
        self.assertEqual(cell["arms"]["uniform_tail"]["reaction_ns"], [None])
        self.assertEqual(cell["arms"]["reps_tail"]["reaction_ns"], [None])


class AckLoss(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())
        self.runs = profiles.a2a_stage_runs("a2a_ackloss", self.calibration, self.tuning)

    def test_runs(self):
        # blackhole_up on the 4 groups and sizes and the 4 loss conditions on
        # EP64 at 256 KiB, and 4 healthy pairs, by 2 arms by 3 seeds.
        self.assertEqual(len(self.runs), (8 + 4) * 2 * 3)
        self.assertEqual({r["arm"] for r in self.runs}, {"reps_hold_auto_ack", "reps_spine_ack"})
        self.assertEqual({r["seed"] for r in self.runs}, {301, 302, 303})
        faulted = {(r["group"], r["size"], r["condition"]) for r in self.runs if r["condition"] != "healthy"}
        self.assertEqual(faulted, {(g, s, "blackhole_up") for g, s in profiles.A2A_CELLS}
                         | {("ep64", "256k", c) for c in ("loss1e2_up", "loss1e3_up", "loss1e2_down", "loss1e3_down")})
        self.assertEqual({(r["group"], r["size"]) for r in self.runs if r["condition"] == "healthy"},
                         set(profiles.A2A_CELLS))
        self.assertEqual(len({r["key"] for r in self.runs}), len(self.runs))
        self.assertIn("a2a_ackloss-ep64-256k-loss1e2_up-reps_spine_ack-s301", {r["key"] for r in self.runs})

    def test_settings(self):
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                  for r in self.runs if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep64", "256k", "loss1e2_up", 301)}
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep64", "256k", "loss1e2_up", 301)}
        self.assertEqual(by_arm["reps_hold_auto_ack"], {**candidates["reps_hold_auto"], "ack_loss_rtts": 2})
        self.assertEqual(by_arm["reps_spine_ack"], {"mode": "spray_uniform", "selector": "reps_spine", "ack_loss_rtts": 2})
        for values in by_arm.values():
            self.assertNotIn("path_seq", values)
            self.assertNotIn("tail_recovery", values)



class CandidatesAck(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())
        self.runs = profiles.a2a_stage_runs("a2a_candidates_ack", self.calibration, self.tuning)

    def test_runs(self):
        # The 28 gate cells and 4 healthy pairs by 3 arms by 3 seeds.
        self.assertEqual(len(self.runs), (28 + 4) * 3 * 3)
        self.assertEqual({r["arm"] for r in self.runs},
                         {"reps_hold_auto_ack", "reps_flow_hold_ack", "mrc_hold_auto_ack"})
        self.assertEqual({r["seed"] for r in self.runs}, {301, 302, 303})
        candidates = profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
        faulted = {(r["group"], r["size"], r["condition"]) for r in self.runs if r["condition"] != "healthy"}
        self.assertEqual(faulted, set(profiles.a2a_gate_cells()))
        self.assertEqual(faulted, {(r["group"], r["size"], r["condition"]) for r in candidates
                                   if r["condition"] != "healthy"})
        self.assertEqual({(r["group"], r["size"]) for r in self.runs if r["condition"] == "healthy"},
                         set(profiles.A2A_CELLS))
        for arm in ("reps_hold_auto_ack", "reps_flow_hold_ack", "mrc_hold_auto_ack"):
            self.assertEqual(len([r for r in self.runs if r["arm"] == arm and r["condition"] == "healthy"]), 4 * 3)
        self.assertEqual(len({r["key"] for r in self.runs}), len(self.runs))
        self.assertIn("a2a_candidates_ack-ep16-2m-structural-mrc_hold_auto_ack-s303", {r["key"] for r in self.runs})

    def test_settings(self):
        cell = ("ep64", "256k", "loss1e2_up", 301)
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                  for r in self.runs if (r["group"], r["size"], r["condition"], r["seed"]) == cell}
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == cell}
        for arm, base in (("reps_hold_auto_ack", "reps_hold_auto"), ("reps_flow_hold_ack", "reps_flow_hold"),
                          ("mrc_hold_auto_ack", "mrc_hold_auto")):
            self.assertEqual(by_arm[arm], {**candidates[base], "ack_loss_rtts": 2})

    def test_shared_with_ackloss(self):
        # reps_hold_auto_ack's run and profile on a cell both stages run
        # differ from a2a_ackloss's only in the stage.
        cell = ("ep16", "2m", "blackhole_up", 302)
        mine = [r for r in self.runs if r["arm"] == "reps_hold_auto_ack"
                and (r["group"], r["size"], r["condition"], r["seed"]) == cell]
        theirs = [r for r in profiles.a2a_stage_runs("a2a_ackloss", self.calibration, self.tuning)
                  if r["arm"] == "reps_hold_auto_ack" and (r["group"], r["size"], r["condition"], r["seed"]) == cell]
        self.assertEqual(len(mine), 1)
        self.assertEqual(len(theirs), 1)
        self.assertEqual(mine[0]["setting"], theirs[0]["setting"])
        self.assertEqual(mine[0]["key"], theirs[0]["key"].replace("a2a_ackloss-", "a2a_candidates_ack-", 1))
        strip = lambda r: {k: v for k, v in r.items() if k not in ("key", "stage")}
        self.assertEqual(strip(mine[0]), strip(theirs[0]))
        profile = profiles.a2a_stage_profile(mine[0], self.calibration)
        other = profiles.a2a_stage_profile(theirs[0], self.calibration)
        self.assertEqual(profile["name"], mine[0]["key"])
        self.assertEqual({**profile, "name": None}, {**other, "name": None})

class TailAck(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())
        self.runs = profiles.a2a_stage_runs("a2a_tail_ack", self.calibration, self.tuning)

    def test_runs(self):
        # The 28 gate cells and 4 healthy pairs by 5 arms by 3 seeds.
        self.assertEqual(len(self.runs), (28 + 4) * 5 * 3)
        self.assertEqual({r["arm"] for r in self.runs},
                         {"reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack",
                          "reps_tail", "uniform_tail"})
        self.assertEqual({r["seed"] for r in self.runs}, {301, 302, 303})
        faulted = {(r["group"], r["size"], r["condition"]) for r in self.runs if r["condition"] != "healthy"}
        self.assertEqual(faulted, set(profiles.a2a_gate_cells()))
        self.assertEqual({(r["group"], r["size"]) for r in self.runs if r["condition"] == "healthy"},
                         set(profiles.A2A_CELLS))
        self.assertEqual(len({r["key"] for r in self.runs}), len(self.runs))
        self.assertEqual(self.runs, profiles.a2a_stage_runs("a2a_tail_ack", self.calibration, self.tuning))
        self.assertIn("a2a_tail_ack-ep64-256k-loss1e2_up-reps_hold_leaf_tail_ack-s301", {r["key"] for r in self.runs})

    def test_settings(self):
        cell = ("ep64", "256k", "loss1e2_up", 301)
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                  for r in self.runs if (r["group"], r["size"], r["condition"], r["seed"]) == cell}
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == cell}
        transport = {"path_seq": True, "tail_recovery": True, "tail_loss_rtts": 4}
        hold = {**transport, "ack_loss_rtts": 2}
        self.assertEqual(by_arm["reps_hold_auto_tail_ack"], {**candidates["reps_hold_auto"], **hold})
        self.assertEqual(by_arm["reps_hold_leaf_tail_ack"],
                         {**candidates["reps_hold_auto"], **hold, "hold_scope": "leaf"})
        self.assertEqual(by_arm["reps_flow_hold_tail_ack"], {**candidates["reps_flow_hold"], **hold})
        self.assertNotIn("hold_scope", by_arm["reps_flow_hold_tail_ack"])
        self.assertEqual(by_arm["reps_tail"], {"mode": "spray_uniform", "selector": "reps_spine", **transport})
        self.assertEqual(by_arm["uniform_tail"], {"mode": "spray_uniform", **transport})
        # The arms the stage does not run take the same transport.
        settings = profiles.a2a_tail_ack_settings(self.tuning, tuple(profiles.A2A_TAIL_ACK_ARMS))
        others = {}
        for arm in ("reps_oracle_tail", "mrc_hold_auto_tail_ack"):
            r = {**self.runs[0], "arm": arm, "setting": settings[arm], "group": "ep64", "size": "256k"}
            others[arm] = profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
        self.assertEqual(others["reps_oracle_tail"], {"mode": "spray_uniform", "selector": "reps_oracle",
                                                      "grade_cost_source": "switch_queue", **transport})
        self.assertEqual(others["mrc_hold_auto_tail_ack"], {**candidates["mrc_hold_auto"], **hold})
        # Every arm's transport keys are the same, so the baselines match
        # a2a_tail's but for tail_loss_rtts.
        tail = profiles.a2a_tail_settings(self.tuning)
        for arm in ("reps_tail", "uniform_tail"):
            self.assertEqual(settings[arm], {**tail[arm], "tail_loss_rtts": 4})
        for setting in settings.values():
            self.assertEqual({k: setting[k] for k in transport}, transport)


class Sweep(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_probe(self):
        runs = profiles.a2a_stage_runs("a2a_sweep_probe", self.calibration, self.tuning)
        self.assertEqual([(r["group"], r["size"], r["iterations"], r["compute_ns"]) for r in runs],
                         [("ep8", "256k", 40, 29_000), ("ep32", "256k", 40, 127_000), ("ep64", "64k", 40, 65_000),
                          ("ep64", "1m", 40, 1_036_000), ("ep32", "4m", 12, 2_039_000)])
        self.assertEqual({(r["arm"], r["condition"], r["seed"]) for r in runs}, {("uniform_tail", "healthy", 301)})
        self.assertEqual(runs[0]["key"], "a2a_sweep_probe-ep8-256k-healthy-uniform_tail-s301")
        documents = {(r["group"], r["size"]): profiles.a2a_stage_profile(r, self.calibration) for r in runs}
        self.assertEqual(documents[("ep8", "256k")]["workload"]["group"], {"leaves": [0]})
        self.assertEqual(documents[("ep32", "4m")]["workload"],
                         {"kind": "all_to_all", "group": {"leaves": [0, 1, 2, 3]}, "message_bytes": 4 * 1024 * 1024,
                          "iterations": 12, "compute_ns": 2_039_000})
        self.assertEqual(documents[("ep32", "4m")]["steps"], 12)
        self.assertEqual(documents[("ep64", "64k")]["network"]["load_balancing"],
                         {"mode": "spray_uniform", "path_seq": True, "tail_recovery": True, "tail_loss_rtts": 4})
        self.assertEqual({r["k"] for r in runs if r["group"] == "ep32"}, {31})

    def test_refused_until_measured(self):
        with self.assertRaises(ValueError):
            profiles.a2a_stage_runs("a2a_sweep", self.calibration, self.tuning)

    @mock.patch.object(profiles, "A2A_SWEEP_COMPUTE_MEASURED", True)
    def test_runs(self):
        runs = profiles.a2a_stage_runs("a2a_sweep", self.calibration, self.tuning)
        arms = {"reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack",
                "reps_tail", "reps_oracle_tail", "uniform_tail"}
        # 14 cells, the anchor and the handover cell, and 8 healthy pairs,
        # by 6 arms by 3 seeds.
        self.assertEqual(len(runs), (14 + 1 + 1 + 8) * 6 * 3)
        self.assertEqual({r["arm"] for r in runs}, arms)
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual(len({r["key"] for r in runs}), len(runs))
        faulted = {(r["group"], r["size"], r["condition"]) for r in runs if r["condition"] != "healthy"}
        self.assertEqual(faulted, {
            ("ep8", "256k", "loss1e2_up"), ("ep16", "256k", "loss1e2_up"), ("ep32", "256k", "loss1e2_up"),
            ("ep64", "256k", "loss1e2_up"), ("ep64", "64k", "loss1e2_up"), ("ep64", "1m", "loss1e2_up"),
            ("ep64", "2m", "loss1e2_up"), ("ep64", "256k", "loss1e3_up"), ("ep64", "256k", "loss3e3_up"),
            ("ep64", "256k", "loss3e2_up"), ("ep64", "64k", "loss1e3_up"), ("ep64", "64k", "loss3e2_up"),
            ("ep64", "2m", "loss1e3_up"), ("ep64", "2m", "loss3e2_up"),
            ("ep32", "4m", "loss3e2_up"), ("ep64", "256k", "rate16g_up")})
        self.assertEqual({(r["group"], r["size"]) for r in runs if r["condition"] == "healthy"},
                         {(g, s) for g, s, _ in faulted})
        timing = {(r["group"], r["size"]): (r["iterations"], r["compute_ns"]) for r in runs}
        self.assertEqual(timing, {
            ("ep8", "256k"): (40, 29_000), ("ep16", "256k"): (40, 62_000), ("ep32", "256k"): (40, 127_000),
            ("ep64", "256k"): (40, 259_000), ("ep64", "64k"): (40, 65_000), ("ep64", "1m"): (40, 1_036_000),
            ("ep64", "2m"): (12, 2_070_000), ("ep32", "4m"): (12, 2_039_000)})
        settings = profiles.a2a_tail_ack_settings(self.tuning, tuple(arms))
        for r in runs:
            self.assertEqual(r["setting"], settings[r["arm"]])
        # A run of a cell the gate also has is the a2a_tail_ack run of it,
        # but for the stage.
        theirs = {r["key"].split("-", 1)[1]: r for r in profiles.a2a_stage_runs("a2a_tail_ack", self.calibration,
                                                                             self.tuning)}
        mine = next(r for r in runs if r["key"] == "a2a_sweep-ep64-2m-loss1e2_up-reps_hold_leaf_tail_ack-s302")
        self.assertEqual({**mine, "key": None, "stage": None},
                         {**theirs[mine["key"].split("-", 1)[1]], "key": None, "stage": None})
        handover = next(r for r in runs if r["condition"] == "rate16g_up")
        document = profiles.a2a_stage_profile(handover, self.calibration)
        self.assertEqual(document["network"]["link_failures"], profiles.a2a_fault("rate16g_up"))


class SweepProbe(unittest.TestCase):
    def test_compute_gap(self):
        runs = [r for r in profiles.a2a_stage_runs("a2a_sweep_probe", None, None) if r["group"] == "ep32"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # EP32 at 256 KiB complete with a median of 388.4 us, the burst
            # of EP64 at 256 KiB in A2A_COMPUTE_NS; EP32 at 4 MiB incomplete.
            for r, complete in zip(runs, (True, False)):
                (root / r["key"]).mkdir()
                timing = {"complete": complete, "iterations_completed": 40 if complete else 7,
                          "mean_ns": 390_000, "p50_ns": 388_400, "p99_ns": 400_000, "max_ns": 401_000}
                (root / r["key"] / "summary.json").write_text(json.dumps({"all_to_all": timing}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs + [run("uniform", "ep64", "2m", "healthy", 301)]))
            rows = analyze.a2a_sweep_probe([listing], root, "a2a_sweep_probe")
        self.assertEqual([(r["size"], r["median_iteration_ns"], r["compute_ns"]) for r in rows],
                         [("256k", 388_400, profiles.A2A_COMPUTE_NS[("ep64", "256k")]), ("4m", None, None)])
        self.assertEqual((rows[0]["probe_compute_ns"], rows[0]["iterations_completed"], rows[0]["iterations"]),
                         (127_000, 40, 40))
        markdown = analyze.sweep_probe_markdown(rows)
        self.assertIn("| ep32 | 256k | 40 of 40 | 388.4 | 259000 |", markdown)
        self.assertIn("| ep32 | 4m | 7 of 12 | - | - |", markdown)


class Rs3(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())
        self.runs = profiles.a2a_stage_runs("a2a_rs3", self.calibration, self.tuning)

    def test_runs(self):
        # 1 cell by 4 arms by 3 seeds, faulted and healthy.
        self.assertEqual(len(self.runs), 1 * 4 * 3 * 2)
        self.assertEqual({r["arm"] for r in self.runs}, set(profiles.A2A_RS3_ARMS))
        self.assertEqual({(r["group"], r["size"]) for r in self.runs}, {("ep16", "2m")})
        self.assertEqual({r["condition"] for r in self.runs}, {"healthy", "flap1e2_up"})
        self.assertEqual({r["seed"] for r in self.runs}, {301, 302, 303})
        self.assertEqual(profiles.A2A_RS3_ITERATIONS, {("ep16", "2m"): 610})
        self.assertEqual(profiles.A2A_RS3_REPEAT, {("ep16", "2m"): 7})
        for r in self.runs:
            cell = (r["group"], r["size"])
            self.assertEqual(r["iterations"], profiles.A2A_RS3_ITERATIONS[cell])
            # The healthy run covers the onset and three cycles.
            period = profiles.A2A_RS3_PERIOD_NS[cell]
            self.assertGreaterEqual(r["iterations"] * period, profiles.DEGRADE_AT_NS + 3 * 250_000_000)
            self.assertLess((r["iterations"] - 1) * period, profiles.DEGRADE_AT_NS + 3 * 250_000_000)
            self.assertEqual("flap" in r, r["condition"] == "flap1e2_up")

    def test_profiles(self):
        by_arm = {}
        for r in self.runs:
            document = profiles.a2a_stage_profile(r, self.calibration)
            self.assertEqual(document["steps"], r["iterations"])
            self.assertEqual(document["workload"]["iterations"], r["iterations"])
            failures = document["network"]["link_failures"]
            if r["condition"] == "healthy":
                self.assertEqual(failures, [])
                continue
            self.assertEqual(failures, [{"model": "gray", "error_rate": 0.01, "start_ns": profiles.DEGRADE_AT_NS,
                                         "spine": 3, "leaf": 0, "direction": "up", "duration_ns": 50_000_000,
                                         "repeat": 7, "period_ns": 250_000_000}])
            by_arm[r["arm"]] = document["network"]["load_balancing"]
        candidates = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                      for r in profiles.a2a_stage_runs("a2a_candidates", self.calibration, self.tuning)
                      if (r["group"], r["size"], r["condition"], r["seed"]) == ("ep16", "2m", "healthy", 301)}
        self.assertEqual(by_arm["reps_hold_auto"], candidates["reps_hold_auto"])
        trickle = by_arm["reps_hold_auto_trickle"]
        self.assertEqual(trickle, {
            "mode": "spray_uniform", "selector": "reps_hold", "held_exploration": "sentinel",
            "loss_thresholds": candidates["reps_hold_auto"]["loss_thresholds"],
            "loss_interval_ns": candidates["reps_hold_auto"]["loss_interval_ns"], "hold_scope": "auto",
            "hold_rule": "llr", "loss_p0": 0.0, "loss_p1": 0.001, "hold_alpha": 0.001, "hold_beta": 0.001,
            "hold_release": "trickle", "hold_trickle": 0.01, "sentinel_mode": "duplicate", "path_seq": False,
        })
        self.assertEqual(by_arm["reps"], {"mode": "ev_hash", "selector": "reps"})
        self.assertEqual(by_arm["uniform"]["mode"], "spray_uniform")

    def test_pairs_by_iteration_count(self):
        rows = [
            {**run("reps", "ep16", "2m", "healthy", 301, "a2a_candidates"), "iterations": 40, "iterations_ns": [1]},
            {**run("reps", "ep16", "2m", "healthy", 301, "a2a_rs3"), "iterations": 813, "iterations_ns": [2]},
            {**run("reps", "ep16", "2m", "flap1e2_up", 301, "a2a_rs3"), "iterations": 813, "iterations_ns": [5]},
        ]
        (row,) = analyze.a2a_costs(rows)
        self.assertEqual((row["healthy_key"], row["cost_ns"]), ("a2a_rs3-ep16-2m-healthy-reps-s301", 3))


class Cycles(unittest.TestCase):
    FLAP = {"error_rate": 0.01, "start_ns": 1000, "on_ns": 100, "period_ns": 500, "repeat": 2}

    def test_periods(self):
        self.assertEqual(analyze.flap_periods(self.FLAP), [
            ("on", 0, 1000, 1100), ("off", 0, 1100, 1500), ("on", 1, 1500, 1600), ("off", 1, 1600, None)])

    def write(self, root, r, starts, completions, trace=None):
        d = root / r["key"]
        (d / "ns3").mkdir(parents=True)
        timing = {"complete": True,
                  "iteration_completion_ns": {str(i + 1): c for i, c in enumerate(completions)},
                  "iteration_start_ns": {str(i + 1): s for i, s in enumerate(starts)}}
        (d / "summary.json").write_text(json.dumps({"all_to_all": timing}))
        if trace is not None:
            (d / "ns3" / "spine_losses.csv").write_text(
                "time_ns,rank,spine,sent,lost,grade\n" + "".join(",".join(map(str, t)) + "\n" for t in trace))

    def test_costs_and_readmission(self):
        setting = {"loss_interval_ns": 100}
        healthy = {**run("reps_hold_auto", "ep64", "256k", "healthy", 301, "a2a_rs3", setting), "iterations": 6}
        faulted = {**run("reps_hold_auto", "ep64", "256k", "flap1e2_up", 301, "a2a_rs3", setting),
                   "iterations": 6, "flap": self.FLAP}
        starts = [0, 1000, 1050, 1200, 1550, 1700]
        # Host 0 (leaf 0) sends 100 per interval; spine 3's share is held to
        # 0 from the first on-period and returns above 1/16 in the interval
        # [1300, 1400), and in [1800, 1900) after the second; host 9 (leaf
        # 1) sends on spine 3 alone and is not read.
        share = {i: 0 for i in range(10, 20)} | {13: 7, 14: 10, 18: 7}
        trace = []
        for i, n in share.items():
            end = (i + 1) * 100
            trace += [(end, 0, 3, n, 0, 3), (end, 0, 1, 100 - n, 0, 3), (end, 9, 3, 50, 0, 3)]
        # Exactly 1/16 is not above it.
        trace += [(1200, 1, 3, 25, 0, 3), (1200, 1, 2, 275, 0, 3)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, healthy, starts, [100] * 6)
            self.write(root, faulted, starts, [100, 300, 200, 150, 400, 120], trace)
            listing = root / "runs.json"
            listing.write_text(json.dumps([healthy, faulted]))
            (cell,) = analyze.a2a_cycles(analyze.a2a_rows([listing], root, "a2a_rs3"), root)
        self.assertEqual((cell["group"], cell["size"], cell["condition"]), ("ep64", "256k", "flap1e2_up"))
        periods = cell["arms"]["reps_hold_auto"]["periods"]
        self.assertEqual([(p["kind"], p["occurrence"]) for p in periods],
                         [("on", 0), ("off", 0), ("on", 1), ("off", 1)])
        self.assertEqual([p["iterations"] for p in periods], [[2], [1], [1], [1]])
        self.assertEqual([p["cost_ns_mean"] for p in periods], [150, 50, 300, 20])
        self.assertEqual([p["readmission_ns"] for p in periods], [None, [300], None, [300]])
        self.assertEqual(periods[1]["readmission_ns_count"], 1)
        self.assertEqual([p["detection_ns"] for p in periods], [[100], None, [100], None])
        self.assertEqual(cell["arms"]["reps_hold_auto"]["share_basis"], ["all"])
        text = analyze.cycles_markdown([cell])
        self.assertIn("### reps_hold_auto", text)
        self.assertIn("| ep64 | 256k | flap1e2_up | all | off 0 | 0.001 | 0.002 | 1 | 0.1 (1) |  | 0.3 (1) "
                      "| - (0) | - (0) |", text)
        self.assertIn("| on 0 | 0.001 | 0.001 | 2 | 0.1 (1) | 0.1 (1) |  |  |  |", text)

    def test_remote_and_all_bases(self):
        flap = {"error_rate": 0.01, "start_ns": 1_000_000, "on_ns": 1_000_000, "period_ns": 5_000_000, "repeat": 1}
        interval = 250_000
        setting = {"loss_interval_ns": interval}
        healthy = {**run("reps_hold_auto", "ep16", "2m", "healthy", 301, "a2a_rs3", setting), "iterations": 2}
        faulted = {**run("reps_hold_auto", "ep16", "2m", "flap1e2_up", 301, "a2a_rs3", setting),
                   "iterations": 2, "flap": flap}
        # Host 0 (leaf 0) sends 100 per interval toward leaf 1, spine 3
        # taking share[i] of them, and 100 toward leaf 0 on spine 1; host 9
        # (leaf 1) sends 100 toward leaf 0 on spine 3.
        share = [12, 12, 12, 12, 12, 5, 0, 0, 0, 7, 10, 12, 13, 13, 13, 13]
        rows = []
        for i, n in enumerate(share):
            end = (i + 1) * interval
            rows += [(end, 0, 1, 3, n), (end, 0, 1, 1, 100 - n), (end, 0, 0, 1, 100), (end, 9, 0, 3, 100)]
        found = {}
        for basis, header, form in (
                ("remote", "time_ns,rank,destination_leaf,spine,sent,lost,grade",
                 lambda r: f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},0,3"),
                ("all", "time_ns,rank,spine,sent,lost,grade",
                 lambda r: f"{r[0]},{r[1]},{r[3]},{r[4]},0,3")):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write(root, healthy, [0, 3_000_000], [100, 100])
                self.write(root, faulted, [0, 3_000_000], [100, 100])
                (root / faulted["key"] / "ns3" / "spine_losses.csv").write_text(
                    header + "\n" + "".join(form(r) + "\n" for r in rows))
                listing = root / "runs.json"
                listing.write_text(json.dumps([healthy, faulted]))
                (cell,) = analyze.a2a_cycles(analyze.a2a_rows([listing], root, "a2a_rs3"), root)
            entry = cell["arms"]["reps_hold_auto"]
            self.assertEqual(entry["share_basis"], [basis])
            on, off = entry["periods"]
            found[basis] = (on["detection_ns"], off["readmission_ns"], off["readmission_1ms_ns"],
                            off["readmission_90pct_1ms_ns"])
            self.assertIsNone(on["readmission_ns"])
            self.assertIsNone(off["detection_ns_mean"])
        # Remote: 5 / 100 in [1.25, 1.5) ms; 7 / 100 in [2.25, 2.5) ms; over
        # 1 ms, 29 / 400 by 3 ms and 48 / 400 (above 0.9 / 8) by 3.5 ms.
        self.assertEqual(found["remote"], ([500_000], [500_000], [1_000_000], [1_500_000]))
        # All: the sends toward leaf 0 halve every share.
        self.assertEqual(found["all"], ([250_000], [1_250_000], [1_750_000], [None]))

    def test_no_trace_and_incomplete(self):
        healthy = {**run("uniform", "ep16", "2m", "healthy", 302, "a2a_rs3"), "iterations": 2}
        faulted = {**run("uniform", "ep16", "2m", "flap1e2_up", 302, "a2a_rs3"), "iterations": 2, "flap": self.FLAP}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write(root, healthy, [0, 1000], [100, 100])
            self.write(root, faulted, [0, 1000], [100])
            summary = root / faulted["key"] / "summary.json"
            data = json.loads(summary.read_text())
            data["all_to_all"]["complete"] = False
            summary.write_text(json.dumps(data))
            listing = root / "runs.json"
            listing.write_text(json.dumps([healthy, faulted]))
            (cell,) = analyze.a2a_cycles(analyze.a2a_rows([listing], root), root)
        periods = cell["arms"]["uniform"]["periods"]
        self.assertEqual([p["cost_ns"] for p in periods], [[float("inf")]] * 4)
        self.assertEqual(periods[1]["readmission_ns"], [None])


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
            self.assertEqual(result, {"reaction_ns": (first + 3) * interval - onset, "share_basis": "all"})
            # Down: without destination_leaf, the hosts off leaf 1 (host 0 alone here).
            self.assertEqual(analyze.reaction(root, "loss1e2_down", interval)["share_basis"], "all")
            self.assertIsNone(analyze.reaction(root / "none", "loss1e2_up", interval)["reaction_ns"])

    def test_destination_leaf_rows(self):
        end = (analyze.DEGRADE_AT_NS // 1000 + 2) * 1000
        rows = [(end, 0, 1, 3, 1, 0, 3), (end, 0, 1, 0, 99, 0, 3), (end, 0, 2, 3, 50, 0, 3), (end, 0, 2, 0, 50, 0, 3)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.trace(root, "time_ns,rank,destination_leaf,spine,sent,lost,grade", rows)
            result = analyze.reaction(root, "loss1e2_down", 1000)
        self.assertEqual(result, {"reaction_ns": end - analyze.DEGRADE_AT_NS, "share_basis": "remote"})

    def test_remote_rows_per_condition(self):
        # (rank, destination_leaf): rank 0 is on leaf 0, rank 9 on leaf 1.
        pairs = [(0, 0), (0, 1), (0, 2), (9, 0), (9, 1), (17, 1)]

        def kept(condition, leaf_column):
            keep, basis = analyze.faulted_share_rows(condition, leaf_column)
            return [pair for pair in pairs
                    if keep({"rank": str(pair[0]), "destination_leaf": str(pair[1])})], basis

        self.assertEqual(kept("loss1e2_up", True), ([(0, 1), (0, 2)], "remote"))
        self.assertEqual(kept("loss1e2_down", True), ([(0, 1), (17, 1)], "remote"))
        self.assertEqual(kept("structural", True), ([(0, 1), (0, 2), (9, 0), (17, 1)], "remote"))
        self.assertEqual(kept("loss1e2_up", False), ([(0, 0), (0, 1), (0, 2)], "all"))
        self.assertEqual(kept("loss1e2_down", False), ([(0, 0), (0, 1), (0, 2), (17, 1)], "all"))
        self.assertEqual(kept("structural", False), (pairs, "all"))

    def test_window_share(self):
        # Interval 1, window 3: the share over the intervals ending in (e - 3, e].
        totals = {i: 100 for i in range(8)} | {8: 0}
        faulted = {0: 30, 1: 0, 2: 0, 3: 0, 4: 0, 5: 7, 6: 6, 7: 7, 8: 0}

        def at(start, end, level):
            return analyze.first_window_above(totals, faulted, 1, start, end, level, window_ns=3)
        # The window at 3 reaches back to 1 only; interval 0 is out of it.
        self.assertIsNone(at(3, 5, 1 / 16))
        # 5..7 holds 20 / 300, above 1/16; 4..6 holds 13 / 300.
        self.assertEqual(at(3, None, 1 / 16), 5)
        self.assertIsNone(at(3, 7, 1 / 16))
        # The window takes intervals before start: at 2, 0..2 holds 30 / 300.
        self.assertEqual(at(2, None, 0.09), 1)
        # Per interval, 7 / 100 in interval 5 is above 1/16.
        self.assertEqual(analyze.first_above(totals, faulted, 1, 3, None), 3)

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
