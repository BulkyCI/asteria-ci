"""Schedule arithmetic and run lists of the transient-fault stages."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import profiles  # noqa: E402
import summarize  # noqa: E402
import transient  # noqa: E402

TUNING = {
    "graded_per_connection": {"gamma": 0.5},
    "graded_per_receiver": {"gamma": 0.25},
    "graded_per_receiver_mix": {"gamma": 1.0, "sender_pool_weight": 0.75},
    "graded_per_receiver_shared": {"gamma": 0.5},
}

REDIRECT = {
    "half_rate": {"flag": 0.15, "omega": 0.7, "validity_base_rtts": 3.0, "trim_scoring": "mark"},
    "gray": {"flag": 0.15, "omega": 0.3, "validity_base_rtts": 1.0, "trim_scoring": "ignore"},
}

# T (us): last occurrence's end and required run (ms), then per K the
# message size (MiB) and the line-rate run (ms).
TABLE = {
    100: (9.0, 10.1, {8: (61, 10.23), 32: (16, 10.74), 64: (8, 10.74)}),
    200: (9.8, 10.9, {8: (65, 10.91), 32: (17, 11.41), 64: (9, 12.08)}),
    400: (11.4, 12.5, {8: (75, 12.58), 32: (19, 12.75), 64: (10, 13.42)}),
    800: (14.6, 15.7, {8: (94, 15.77), 32: (24, 16.11), 64: (12, 16.11)}),
    1600: (21.0, 22.1, {8: (132, 22.15), 32: (33, 22.15), 64: (17, 22.82)}),
}


class Schedule(unittest.TestCase):
    def test_occurrences_at_400_us(self):
        windows = transient.occurrence_windows(400)
        self.assertEqual(len(windows), 8)
        self.assertEqual(windows[0], (1_232_400, 1_632_400))
        self.assertEqual(windows[1], (2_632_400, 3_032_400))
        self.assertEqual(windows[7], (1_232_400 + 7 * 1_400_000, 1_232_400 + 7 * 1_400_000 + 400_000))

    def test_last_end_and_required_run(self):
        for t_us, (last_end_ms, required_ms, _) in TABLE.items():
            self.assertAlmostEqual(transient.occurrence_windows(t_us)[-1][1] / 1e6, last_end_ms, delta=0.05)
            self.assertEqual(transient.required_run_ns(t_us), round(required_ms * 1e6))

    def test_message_sizes_and_line_rate_runs(self):
        for t_us, (_, _, per_k) in TABLE.items():
            for k, (mib, run_ms) in per_k.items():
                with self.subTest(t_us=t_us, k=k):
                    self.assertEqual(transient.message_bytes(t_us, k), mib * transient.MIB)
                    self.assertAlmostEqual(transient.line_rate_run_ns(t_us, k) / 1e6, run_ms, delta=0.005)

    def test_profile_failure_reads_back_as_the_schedule(self):
        for kind, geometry in (("half_rate", "spinewide"), ("blackhole", "downlink0"), ("loss", "spinewide")):
            run = {"key": "x", "arm": "uniform", "setting": None, "k": 32, "kind": kind,
                   "geometry": geometry, "t_us": 200, "seed": 301}
            document = transient.profile(run, None)
            (failure,) = document["network"]["link_failures"]
            self.assertEqual(summarize.occurrences(failure), transient.occurrence_windows(200))
            self.assertEqual(failure["spine"], 3)
            self.assertEqual(failure.get("leaf"), 0 if geometry == "downlink0" else None)
            self.assertEqual(document["workload"]["message_bytes"], 17 * transient.MIB)
            self.assertEqual(document["workload"]["receiver_leaves"], [0, 1])
            self.assertEqual(document["workload"]["receiver_count"], 8)


class Runs(unittest.TestCase):
    def test_counts_and_keys(self):
        expected = {"t_calibration": 27, "t_tuning": 36, "t_comparison": 663}
        for stage, count in expected.items():
            listed = transient.runs(stage, TUNING, REDIRECT)
            keys = [r["key"] for r in listed]
            self.assertEqual(len(keys), count, stage)
            self.assertEqual(len(set(keys)), count, stage)
            self.assertTrue(all(k.split("-")[0] == stage for k in keys))
        self.assertEqual(len(set(transient.CELLS)), 18)

    def test_comparison_pairs_and_arms(self):
        listed = transient.runs("t_comparison", TUNING)
        faulted = [r for r in listed if r["kind"] != "healthy"]
        healthy = {(r["arm"], r["k"], r["t_us"], r["seed"]) for r in listed if r["kind"] == "healthy"}
        self.assertEqual(len(faulted), 18 * 8 * 3)
        for run in faulted:
            self.assertIn((run["arm"], run["k"], run["t_us"], run["seed"]), healthy)
        self.assertEqual(
            next(r for r in listed if r["arm"] == "graded_switch_queue")["setting"], {"gamma": 0.25}
        )
        self.assertEqual(
            listed[0]["key"].count("-"), 6, "stage-k-kind-geometry-t-arm-seed"
        )

    def test_graded_balancing(self):
        for arm, weight in (("graded_per_receiver", None), ("graded_per_receiver_shared", 1.0),
                            ("graded_per_receiver_mix", 0.75)):
            values = transient.balancing(arm, 32, None, transient.tuned_setting(arm, TUNING))
            self.assertEqual(values["edge_window_penalty"], 0)
            self.assertIs(values["edge_freeze"], False)
            self.assertEqual(values.get("sender_pool_weight"), weight)
        self.assertNotIn("edge_freeze", transient.balancing("reps", 32, None, None))

    def test_every_graded_profile_smooths_the_estimator(self):
        for stage in ("t_calibration", "t_tuning", "t_comparison"):
            for run in transient.runs(stage, TUNING, REDIRECT):
                values = transient.profile(run, None)["network"]["load_balancing"]
                if values["mode"] != "spray_policy":
                    self.assertNotIn("estimator_interval_samples", values)
                    continue
                with self.subTest(key=run["key"]):
                    self.assertEqual(values["estimator_interval_samples"], 0)
                    self.assertEqual(values["estimator_gain"], 0.25)

    def test_graded_profiles_carry_the_calibrated_thresholds(self):
        calibration = {
            scope: {"32": {"congestion_thresholds": [0.25, 0.5, 1.0], "trim_thresholds": [0.05, 0.1, 0.2]}}
            for scope in ("per_connection", "per_receiver")
        }
        calibration["switch_queue"] = {"32": {"congestion_thresholds": [0.2, 0.4, 0.8]}}
        for run in transient.runs("t_comparison", TUNING, REDIRECT):
            if run["k"] != 32 or not run["arm"].startswith("graded"):
                continue
            values = transient.profile(run, calibration)["network"]["load_balancing"]
            with self.subTest(key=run["key"]):
                if run["arm"] == "graded_switch_queue":
                    self.assertEqual(values["congestion_thresholds"], [0.2, 0.4, 0.8])
                    self.assertNotIn("trim_thresholds", values)
                else:
                    self.assertEqual(values["congestion_thresholds"], [0.25, 0.5, 1.0])
                    self.assertEqual(values["trim_thresholds"], [0.05, 0.1, 0.2])

    def test_redirect_arm(self):
        listed = [r for r in transient.runs("t_comparison", TUNING, REDIRECT) if r["arm"] == "redirect_per_connection"]
        faulted = {(r["k"], r["kind"], r["geometry"], r["t_us"]) for r in listed if r["kind"] != "healthy"}
        self.assertEqual(faulted, {
            (k, kind, "spinewide", t)
            for kind in ("half_rate", "blackhole")
            for k, t in ((8, 400), (32, 400), (64, 400), (32, 100), (32, 200), (32, 800), (32, 1600))
        })
        self.assertEqual(len(listed), 42 + 21)
        for run in listed:
            self.assertEqual(run["setting"], REDIRECT["half_rate"])
            values = transient.profile(run, None)["network"]["load_balancing"]
            self.assertEqual(values, {"mode": "spray_uniform", "selector": "ack_redirect",
                                      "scope": "per_connection", **REDIRECT["half_rate"]})
        self.assertFalse(any(r["arm"] == "redirect_per_connection" for r in transient.runs("t_tuning", None, REDIRECT)))
        self.assertFalse(any(r["arm"] == "redirect_per_connection" for r in transient.runs("t_comparison", TUNING)))

    def test_tuning_labels(self):
        keys = [r["key"] for r in transient.runs("t_tuning", None)]
        self.assertIn("t_tuning-k32-half_rate-spinewide-t400-graded_per_receiver_mix-gm050-sw025-s201", keys)
        self.assertIn("t_tuning-k32-healthy-none-t400-graded_per_connection-gm100-s201", keys)
        self.assertEqual(profiles.setting_name({"estimator_gain": 1 / 16}), "g16")


if __name__ == "__main__":
    unittest.main()
