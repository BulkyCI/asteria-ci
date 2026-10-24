"""Phase 9: the a2a_* run lists and profiles, the sender loss trace, the
all-to-all cost pairing and the oracle-headroom gate."""

import hashlib
import json
import re
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


def measured_4k():
    """Every 4096-byte compute gap taken as measured."""
    return mock.patch.dict(profiles.A2A_PAYLOAD_COMPUTE_MEASURED,
                           {profiles.A2A_PAYLOAD_4K: tuple(profiles.A2A_PAYLOAD_COMPUTE_NS[profiles.A2A_PAYLOAD_4K])})


# Per P, the probe's 2 pairs at seed 301 and the calibration's at seeds
# 101-103.
SPINES_COUNTS = {**{f"a2a_spines{spines}_probe_4k": 2 for spines in (8, 32, 128, 256)},
                 **{f"a2a_spines{spines}_calibration_4k": 6 for spines in (8, 32, 128, 256)},
                 # Per P, the wide-leaf validity run of each pair (P above 8),
                 # the calibration's 2 pairs at 3 seeds, and the comparison's
                 # 2 pairs by 7 conditions by 8 arms by 3 seeds, by 4 (3
                 # conditions and healthy) by 2 arms by 3 seeds, and by 3 (2
                 # conditions and healthy) by 1 arm by 3 seeds.
                 **{f"a2a_wide{wide}_validity_4k": 2 for wide in (32, 128, 256)},
                 **{f"a2a_wide{wide}_calibration_4k": 6 for wide in (8, 32, 128, 256)},
                 **{f"a2a_wide{wide}_4k": 402 for wide in (8, 32, 128, 256)},
                 # The more seeds: 3 seeds of 5 arms on the 4 multi-fault cells
                 # and 2 healthy pairs, and of 3 arms on 256 KiB loss1e2_down
                 # and loss1e3_down.
                 **{f"a2a_wide{wide}_seeds_4k": 108 for wide in (8, 32, 128, 256)},
                 # The sentinel spread: 12 seeds of 2 arms on 256 KiB
                 # loss1e2_down, the sparse draw and their healthy pair.
                 **{f"a2a_wide{wide}_spread_4k": 72 for wide in (32, 256)},
                 # The receiver hold widened: 6 seeds of rx with 8 slots,
                 # spread and widened on 3 cells and their healthy pair, and
                 # of the two spread arms on multi_loss1e3 and healthy.
                 "a2a_wide256_widen_4k": 48,
                 # The receiver widening at its first loss: 3 seeds of that
                 # arm on 2 cells and healthy.
                 "a2a_wide256_rauto_4k": 9,
                 # The sentinel spread at 128 spines and 2 MiB: 3 seeds of 2
                 # arms on loss1e2_down, loss1e3_down, the two multi-fault
                 # draws and healthy.
                 "a2a_wide128_spread2m_4k": 30,
                 # The overdue repair at 256 spines: 6 seeds of the widen
                 # stage's 3 arms on multi_loss1e3 and healthy.
                 "a2a_wide256_od_4k": 36, "a2a_wide256_od2_4k": 54, "a2a_wide256_rauto_od_4k": 24,
                 "a2a_wide32_rack_4k": 12, "a2a_wide32_rack2_4k": 24, "a2a_wide256_rack_4k": 78, "a2a_wide32_themis_4k": 36,
                 "a2a_wide32_themis2_4k": 18, "a2a_wide32_themis3_4k": 18, "a2a_wide32_themis4_4k": 27,
                 "a2a_wide256_themis_4k": 36,
                 "a2a_wide32_fair_4k": 45,
                 "a2a_wide64_4k": 402, "a2a_wide64_calibration_4k": 6, "a2a_wide64_seeds_4k": 108,
                 "a2a_wide64_validity_4k": 2,
                 **{f"a2a_wide{wide}_matrix_4k": 144 for wide in (32, 64, 128, 256)},
                 "a2a_wide256_down16_4k": 48,
                 "a2a_wide256_uid_4k": 4, "a2a_wide256_mrc_4k": 54}


def measured_spines():
    """Every path-count stage's pair taken as measured, at its 8-spine
    4096-byte gap."""
    return mock.patch.dict(profiles.A2A_SPINES_COMPUTE_MEASURED,
                           {spines: profiles.A2A_SPINES_PAIRS for spines in profiles.A2A_SPINES})


def spines_gaps():
    """The path-count stages' gaps, each P's at its 8-spine 4096-byte gap."""
    return mock.patch.dict(profiles.A2A_SPINES_COMPUTE_NS,
                           {spines: {pair: profiles.A2A_MEASURED_4K_COMPUTE_NS[pair]
                                     for pair in profiles.A2A_SPINES_PAIRS}
                            for spines in profiles.A2A_SPINES})


def calibrated_4k(calibration):
    """calibration with an empty per_receiver entry at 4096 B for every
    (group, size) off one leaf that a 4096-byte stage runs and calibration
    lacks, as those stages require."""
    entries = {profiles.a2a_cell_name(group, size, profiles.A2A_PAYLOAD_4K,
                                      wide=profiles.A2A_WIDE_CALIBRATION_STAGES.get(stage)): {}
               for stage, pairs in profiles.A2A_CALIBRATION_STAGES_4K.items() for group, size in pairs}
    calibration = calibration or {}
    return {**calibration, "per_receiver": {**entries, **calibration.get("per_receiver", {})}}


# Every stage, those at 4096 B with their compute gaps taken as measured and
# every cell calibrated at that payload.
@measured_4k()
@measured_spines()
@spines_gaps()
class Stages(unittest.TestCase):
    def runs(self, stage, calibration, tuning):
        if stage in profiles.A2A_STAGE_FIXTURE:
            calibration = calibrated_4k(calibration)
        return profiles.a2a_stage_runs(stage, calibration, tuning)

    def counts(self, calibration, tuning):
        return {s: len(self.runs(s, calibration, tuning)) for s in profiles.A2A_STAGES}

    def test_counts(self):
        self.assertEqual(
            self.counts(None, None),
            {"a2a_oracle_calibration": 12, "a2a_gate": 96, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 207,
             "a2a_gate_reps": 192, "a2a_candidates": 384, "a2a_receiver": 288, "a2a_rs3": 24,
             "a2a_tail": 288, "a2a_ackloss": 72, "a2a_candidates_ack": 288, "a2a_tail_ack": 480,
             "a2a_probe_4k": 9, "a2a_calibration_4k": 24,
             "a2a_tail_ack_4k": 576, "a2a_sweep_4k": 288,
             "a2a_downlink_4k": 120, "a2a_decode_probe_4k": 1, "a2a_decode_calibration_4k": 3,
             "a2a_decode_4k": 108, "a2a_reaction_4k": 36, "ar_parity_probe_4k": 1,
             "ar_parity_calibration_4k": 3, "ar_parity_4k": 36, "a2a_ackroute_4k": 441,
             "a2a_noack_4k": 486, "a2a_hostlink_4k": 225, "a2a_ackroute_fwd_4k": 126,
             "a2a_downlink_spray_4k": 252, "a2a_twoend_4k": 441, "a2a_hostlink_s_4k": 135,
             "a2a_twoend_base_4k": 126, "a2a_twoend_retract_4k": 189, "a2a_overdue_4k": 378,
             "a2a_rset_sweep_4k": 63, "a2a_twoend_rset_4k": 189,
             **SPINES_COUNTS},
        )
        self.assertEqual(
            self.counts(CALIBRATION, TUNING),
            {"a2a_oracle_calibration": 12, "a2a_gate": 192, "a2a_calibration": 24, "a2a_tuning": 192, "a2a_tuning_mix": 90, "a2a_comparison": 591,
             "a2a_gate_reps": 192, "a2a_candidates": 576, "a2a_receiver": 288, "a2a_rs3": 24,
             "a2a_tail": 288, "a2a_ackloss": 72, "a2a_candidates_ack": 288, "a2a_tail_ack": 480,
             "a2a_probe_4k": 9, "a2a_calibration_4k": 24,
             "a2a_tail_ack_4k": 576, "a2a_sweep_4k": 288,
             "a2a_downlink_4k": 120, "a2a_decode_probe_4k": 1, "a2a_decode_calibration_4k": 3,
             "a2a_decode_4k": 108, "a2a_reaction_4k": 36, "ar_parity_probe_4k": 1,
             "ar_parity_calibration_4k": 3, "ar_parity_4k": 36, "a2a_ackroute_4k": 441,
             "a2a_noack_4k": 486, "a2a_hostlink_4k": 225, "a2a_ackroute_fwd_4k": 126,
             "a2a_downlink_spray_4k": 252, "a2a_twoend_4k": 441, "a2a_hostlink_s_4k": 135,
             "a2a_twoend_base_4k": 126, "a2a_twoend_retract_4k": 189, "a2a_overdue_4k": 378,
             "a2a_rset_sweep_4k": 63, "a2a_twoend_rset_4k": 189,
             **SPINES_COUNTS},
        )
        admitted = {**TUNING, "admitted": ["ep64-2m-loss1e2_up", "ep16-256k-structural"]}
        # 2 cells by 6 arms by 3 seeds, and 2 healthy pairs by 6 arms by 3
        # seeds; plus the soft oracle reading, 4 loss cells and a healthy
        # pair by 3 seeds.
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_comparison", None, admitted)), 87)

    def test_keys_unique_and_prefixed(self):
        for stage in profiles.A2A_STAGES:
            runs = self.runs(stage, CALIBRATION, TUNING)
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
        down = {**onset, "leaf": 1, "direction": "down"}
        self.assertEqual(profiles.a2a_fault("blackhole_down"), [{"model": "silent", **down}])
        self.assertEqual(profiles.a2a_fault("halfrate_down"), [{"model": "gray", "rate": "200Gbps", **down}])
        with self.assertRaises(ValueError):
            profiles.a2a_fault("delay_up")

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
            for run_ in self.runs(stage, CALIBRATION, TUNING):
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
        "a2a_tail_ack": (480, "b4b584454fab1186f027e048ee0ec894358c7328a7bf4cba63602631f185c591"),
    }

    def test_runs_and_profiles_unchanged(self):
        here = Path(profiles.__file__).parent
        calibration = json.loads((here / "a2a_calibration.json").read_text())
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        for stage, pinned in self.STAGES.items():
            runs = profiles.a2a_stage_runs(stage, calibration, tuning)
            text = json.dumps([[r, profiles.a2a_stage_profile(r, calibration)] for r in runs], sort_keys=True)
            self.assertEqual((len(runs), hashlib.sha256(text.encode()).hexdigest()), pinned, stage)


class Identity(unittest.TestCase):
    def test_per_key(self):
        def write(root, key, starts, rto, extra=None):
            (root / key).mkdir(parents=True)
            transport = {"rto_fired:control": {"count": rto, "bytes": 0}, **(extra or {})}
            timing = {"iteration_start_ns": {"1": starts[0], "2": starts[1]},
                      "iteration_completion_ns": {"1": 10, "2": 10}}
            (root / key / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))

        with tempfile.TemporaryDirectory() as directory:
            a, b = Path(directory) / "a", Path(directory) / "b"
            write(a, "same", (0, 20), 3)
            write(b, "same", (0, 20), 3)
            write(a, "late", (0, 20), 3)
            write(b, "late", (0, 21), 4, {"receiver_tail_missing:control": {"count": 1, "bytes": 0}})
            write(a, "only_a", (0, 20), 3)
            rows = {r["key"]: r for r in analyze.identity(a, b)}
            self.assertEqual(set(rows), {"same", "late"})
            self.assertEqual(rows["same"]["status"], "identical")
            self.assertEqual(rows["late"]["differences"],
                             ["iteration_start_ns", "receiver_tail_missing:control None -> 1", "rto_fired:control 3 -> 4"])
            listing = Path(directory) / "runs.json"
            listing.write_text(json.dumps([{"key": "same"}, {"key": "only_a"}]))
            self.assertEqual([(r["key"], r["status"]) for r in analyze.identity(a, b, listing)],
                             [("only_a", "missing"), ("same", "identical")])


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
        self.assertEqual(result["damaged_lines"], 0)

    def test_lines_a_storage_fault_filled_with_nul_bytes_are_counted_not_read(self):
        # The fault's runs are longer than the csv module's field limit.
        damaged = "20" + "\0" * 200_000 + "0,1,3,10,2,3\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            path.write_text("time_ns,rank,spine,sent,lost,grade\n100,0,3,10,1,3\n" + damaged + "300,0,3,10,4,3\n")
            result = summarize.spine_losses(path, 3, 50)
        self.assertEqual(result["spines"], {"3": {"sent": 20, "lost": 5}})
        self.assertEqual(result["damaged_lines"], 1)
        self.assertEqual(result["cumulative_lost_reached_ns"], {"1": 100, "5": 300, "20": None})

    def test_a_row_cut_short_at_the_end_is_left_out(self):
        # A simulator stopped by its time limit leaves its last row cut
        # short, here inside its lost count.
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            path.write_text("time_ns,rank,spine,sent,lost,grade\n100,0,3,10,1,3\n300,0,3,10,")
            result = summarize.spine_losses(path, 3, 50)
        self.assertEqual((result["spines"], result["damaged_lines"]), ({"3": {"sent": 10, "lost": 1}}, 0))


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

    def test_payloads_pair_apart(self):
        rows = []
        for payload, healthy, faulted in ((None, 100, 150), (4096, 200, 260)):
            extra = {} if payload is None else {"fixture": {**profiles.A2A_FIXTURE_4K, "payload_bytes": payload}}
            stage = "a2a_tail_ack" if payload is None else "a2a_tail_ack_4k"
            rows.append({**run("uniform_tail", "ep64", "2m", "healthy", 301, stage), **extra,
                         "iterations_ns": [healthy, healthy]})
            rows.append({**run("uniform_tail", "ep64", "2m", "loss1e2_up", 301, stage), **extra,
                         "iterations_ns": [faulted, faulted]})
        self.assertEqual(sorted(r["cost_ns"] for r in analyze.a2a_costs(rows)), [50, 60])
        cells = analyze.a2a_cost_cells(rows, uniform="uniform_tail")
        self.assertEqual([((c.get("fixture") or {}).get("payload_bytes"), c["arms"]["uniform_tail"]["cost_ns"])
                          for c in cells], [(None, [50]), (4096, [60])])

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
        # The workflow's record_filter, passed to profiles.py write as
        # --filter, on the run keys <stage>-<group>-<size>-<condition>-<arm>-s<seed>;
        # shards.py shards every run written.
        here = Path(profiles.__file__).parent
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        calibration = json.loads((here / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_candidates", calibration, tuning)
        hold = set(profiles.A2A_REPS_HOLD_ARMS)
        expression = "(" + "|".join(sorted(hold)) + ")-s[0-9]+$"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(
                [sys.executable, str(here / "profiles.py"), "write", "--out", str(root / "a2a_candidates"),
                 "--stage", "a2a_candidates", f"--filter={expression}",
                 "--a2a-calibration", str(here / "a2a_calibration.json"), "--a2a-tuning", str(here / "a2a_tuning.json")],
                check=True, stdout=subprocess.DEVNULL,
            )
            subprocess.run(
                [sys.executable, str(here / "shards.py"), str(root),
                 "--runs-per-shard", "3", "--out", str(root / "shards.json")],
                check=True,
            )
            keys = [k for shard in json.loads((root / "shards.json").read_text()) for k in shard["keys"].split()]
        self.assertEqual(sorted(keys), sorted(r["key"] for r in runs if r["arm"] in hold))
        # The 28 gate cells and 4 healthy pairs by 4 arms by 3 seeds.
        self.assertEqual(len(keys), (28 + 4) * 4 * 3)

    def test_shards_beyond_those_run_at_once_are_refused(self):
        # On the cluster a shard that cannot start with the others outwaits
        # its runner; --at-once refuses such a plan.
        here = Path(profiles.__file__).parent
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "stage").mkdir()
            (root / "stage" / "runs.json").write_text(json.dumps([{"key": f"stage-r{i}", "k": 1} for i in range(9)]))

            def plan(*extra):
                return subprocess.run(
                    [sys.executable, str(here / "shards.py"), str(root), "--runs-per-shard", "3", *extra,
                     "--out", str(root / "shards.json")],
                    capture_output=True, text=True,
                )

            refused = plan("--at-once", "2")
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("3 shards, but 2 run at once", refused.stderr)
            self.assertEqual(plan("--at-once", "3").returncode, 0)
            self.assertEqual(len(json.loads((root / "shards.json").read_text())), 3)
            self.assertEqual(plan().returncode, 0)


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


class Payload4k(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_probe(self):
        runs = profiles.a2a_stage_runs("a2a_probe_4k", self.calibration, self.tuning)
        self.assertEqual([(r["group"], r["size"], r["iterations"], r["compute_ns"]) for r in runs],
                         [("ep64", "2m", 12, 2_070_000), ("ep64", "256k", 40, 259_000), ("ep16", "2m", 40, 493_000),
                          ("ep16", "256k", 40, 62_000), ("ep8", "256k", 40, 29_000), ("ep32", "256k", 40, 127_000),
                          ("ep64", "64k", 40, 65_000), ("ep64", "1m", 40, 1_036_000), ("ep32", "4m", 12, 2_039_000)])
        self.assertEqual({(r["arm"], r["condition"], r["seed"]) for r in runs}, {("uniform_tail", "healthy", 301)})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * 9)
        self.assertEqual(runs[4]["key"], "a2a_probe_4k-ep8-256k-healthy-uniform_tail-s301")
        documents = {(r["group"], r["size"]): profiles.a2a_stage_profile(r, self.calibration) for r in runs}
        self.assertEqual({d["network"]["packet_payload_bytes"] for d in documents.values()}, {4096})
        self.assertEqual(documents[("ep8", "256k")]["workload"]["group"], {"leaves": [0]})
        self.assertEqual(documents[("ep32", "4m")]["workload"],
                         {"kind": "all_to_all", "group": {"leaves": [0, 1, 2, 3]}, "message_bytes": 4 * 1024 * 1024,
                          "iterations": 12, "compute_ns": 2_039_000, "dataset_splits": 1})
        self.assertEqual(documents[("ep32", "4m")]["steps"], 12)
        self.assertEqual(documents[("ep64", "64k")]["network"]["load_balancing"],
                         {"mode": "spray_uniform", "path_seq": True, "tail_recovery": True, "tail_loss_rtts": 4,
                          "path_state": "connection"})
        self.assertEqual({r["k"] for r in runs if r["group"] == "ep32"}, {31})
        # The stages at the default payload write none.
        for r in profiles.a2a_stage_runs("a2a_tail_ack", self.calibration, self.tuning)[:32]:
            self.assertNotIn("fixture", r)
            self.assertNotIn("packet_payload_bytes", profiles.a2a_stage_profile(r, self.calibration)["network"])

    def test_refused_until_measured(self):
        unmeasured = {profiles.A2A_PAYLOAD_4K: (profiles.A2A_RING,)}
        with mock.patch.dict(profiles.A2A_PAYLOAD_COMPUTE_MEASURED, unmeasured):
            for stage in ("a2a_calibration_4k", "a2a_tail_ack_4k", "a2a_sweep_4k"):
                with self.assertRaisesRegex(ValueError, "provisional"):
                    profiles.a2a_stage_runs(stage, calibrated_4k(self.calibration), self.tuning)

    @measured_4k()
    def test_refused_until_calibrated(self):
        # The committed file without its 4096-byte cells.
        default_only = {**self.calibration, "per_receiver": {
            cell: entry for cell, entry in self.calibration["per_receiver"].items()
            if not cell.endswith(f"-p{profiles.A2A_PAYLOAD_4K}")}}
        with self.assertRaisesRegex(ValueError, r"\['ep64-2m-p4096', 'ep64-256k-p4096', 'ep16-2m-p4096', "
                                                r"'ep16-256k-p4096'\]"):
            profiles.a2a_stage_runs("a2a_tail_ack_4k", default_only, self.tuning)
        with self.assertRaisesRegex(ValueError, r"\['ep16-256k-p4096', 'ep32-256k-p4096', .*'ep32-4m-p4096'\]"):
            profiles.a2a_stage_runs("a2a_sweep_4k", default_only, self.tuning)
        lacking = calibrated_4k(self.calibration)
        del lacking["per_receiver"]["ep64-1m-p4096"]
        with self.assertRaisesRegex(ValueError, r"\['ep64-1m-p4096'\]"):
            profiles.a2a_stage_runs("a2a_sweep_4k", lacking, self.tuning)
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_tail_ack_4k", lacking, self.tuning)), 576)

    @measured_4k()
    def test_calibration_runs(self):
        runs = profiles.a2a_stage_runs("a2a_calibration_4k", self.calibration, self.tuning)
        # a2a_calibration's per_receiver pass on the 8 pairs off one leaf, by
        # 3 seeds.
        self.assertEqual(len(runs), 8 * 3)
        self.assertEqual({(r["group"], r["size"]) for r in runs}, set(profiles.A2A_CALIBRATION_4K_CELLS))
        self.assertEqual(len(profiles.A2A_CALIBRATION_4K_CELLS), 8)
        self.assertEqual({r["seed"] for r in runs}, {101, 102, 103})
        gate = next(r for r in profiles.a2a_stage_runs("a2a_calibration", self.calibration, self.tuning)
                    if r["arm"] == "graded_per_receiver_mix" and (r["group"], r["size"], r["seed"]) == ("ep64", "256k", 101))
        mine = next(r for r in runs if (r["group"], r["size"], r["seed"]) == ("ep64", "256k", 101))
        self.assertEqual(mine["key"], "a2a_calibration_4k-ep64-256k-healthy-graded_per_receiver_mix-s101")
        self.assertEqual({**mine, "key": None, "stage": None, "fixture": None, "compute_ns": None},
                         {**gate, "key": None, "stage": None, "fixture": None, "compute_ns": None})
        self.assertEqual(mine["compute_ns"], profiles.A2A_PAYLOAD_COMPUTE_NS[4096][("ep64", "256k")])
        # Both run at the simulator's thresholds, whatever the calibration.
        profile = profiles.a2a_stage_profile(mine, calibrated_4k(self.calibration))
        self.assertEqual(profile["network"]["load_balancing"],
                         profiles.a2a_stage_profile(gate, self.calibration)["network"]["load_balancing"])
        self.assertEqual(profile["network"]["packet_payload_bytes"], 4096)

    @measured_4k()
    def test_tail_ack(self):
        calibration = calibrated_4k(self.calibration)
        calibration["per_receiver"]["ep64-256k-p4096"] = {"loss_thresholds": {"24648": [0.0075, 0.015, 0.03]}}
        runs = profiles.a2a_stage_runs("a2a_tail_ack_4k", calibration, self.tuning)
        # a2a_tail_ack's 5 arms and reps_oracle_tail on the 28 gate cells
        # and 4 healthy pairs, by 3 seeds.
        self.assertEqual(len(runs), (28 + 4) * 6 * 3)
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_ARMS_4K))
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs if r["condition"] != "healthy"},
                         set(profiles.a2a_gate_cells()))
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        theirs = {r["key"]: r for r in profiles.a2a_stage_runs("a2a_tail_ack", self.calibration, self.tuning)}
        self.assertFalse({r["key"] for r in runs} & set(theirs))
        for r in runs:
            other = theirs.get(r["key"].replace("a2a_tail_ack_4k-", "a2a_tail_ack-", 1))
            if other is not None:
                self.assertEqual({**r, "key": None, "stage": None, "fixture": None, "compute_ns": None},
                                 {**other, "key": None, "stage": None, "fixture": None, "compute_ns": None})
                self.assertEqual(r["compute_ns"], profiles.A2A_PAYLOAD_COMPUTE_NS[4096][(r["group"], r["size"])])
        self.assertEqual(sum(r["arm"] == "reps_oracle_tail" for r in runs), (28 + 4) * 3)
        hold = next(r for r in runs if r["key"] == "a2a_tail_ack_4k-ep64-256k-loss1e2_up-reps_hold_auto_tail_ack-s301")
        balancing = profiles.a2a_stage_profile(hold, calibration)["network"]["load_balancing"]
        self.assertEqual(balancing["loss_thresholds"], [0.0075, 0.015, 0.03])
        # reps_hold_pair_tail_ack keeps its setting, though no stage runs it.
        self.assertNotIn("reps_hold_pair_tail_ack", profiles.A2A_ARMS_4K)
        pair = {**hold, "arm": "reps_hold_pair_tail_ack",
                "setting": profiles.a2a_tail_ack_settings(self.tuning, ("reps_hold_pair_tail_ack",))[
                    "reps_hold_pair_tail_ack"]}
        self.assertEqual(profiles.a2a_stage_profile(pair, calibration)["network"]["load_balancing"],
                         {**balancing, "hold_scope": "pair"})
        default = profiles.a2a_stage_profile(theirs[hold["key"].replace("_4k", "", 1)], calibration)
        self.assertEqual(default["network"]["load_balancing"]["loss_thresholds"],
                         self.calibration["per_receiver"]["ep64-256k"]["loss_thresholds"]["24648"])

    @measured_4k()
    def test_sweep(self):
        runs = profiles.a2a_stage_runs("a2a_sweep_4k", calibrated_4k(self.calibration), self.tuning)
        # 14 cells, the anchor and the handover cell, and 8 healthy pairs,
        # but the 5 cells and 3 healthy pairs a2a_tail_ack_4k runs, by 6
        # arms by 3 seeds.
        self.assertEqual(len(runs), (14 + 1 + 1 + 8 - 5 - 3) * 6 * 3)
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_ARMS_4K))
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        self.assertEqual(len({r["key"] for r in runs}), len(runs))
        faulted = {(r["group"], r["size"], r["condition"]) for r in runs if r["condition"] != "healthy"}
        self.assertEqual(faulted, {
            ("ep8", "256k", "loss1e2_up"), ("ep32", "256k", "loss1e2_up"),
            ("ep64", "64k", "loss1e2_up"), ("ep64", "1m", "loss1e2_up"),
            ("ep64", "256k", "loss3e3_up"),
            ("ep64", "256k", "loss3e2_up"), ("ep64", "64k", "loss1e3_up"), ("ep64", "64k", "loss3e2_up"),
            ("ep64", "2m", "loss3e2_up"),
            ("ep32", "4m", "loss3e2_up"), ("ep64", "256k", "rate16g_up")})
        self.assertEqual({(r["group"], r["size"]) for r in runs if r["condition"] == "healthy"},
                         {("ep8", "256k"), ("ep32", "256k"), ("ep64", "64k"), ("ep64", "1m"), ("ep32", "4m")})
        timing = {(r["group"], r["size"]): (r["iterations"], r["compute_ns"]) for r in runs}
        self.assertEqual(timing, {pair: (profiles.A2A_RUN_ITERATIONS[pair], profiles.A2A_PAYLOAD_COMPUTE_NS[4096][pair])
                                  for pair in timing})
        self.assertEqual(timing[("ep32", "4m")], (12, 1_787_000))
        settings = profiles.a2a_tail_ack_settings(self.tuning, profiles.A2A_ARMS_4K)
        for r in runs:
            self.assertEqual(r["setting"], settings[r["arm"]])
        # The runs the stage leaves to a2a_tail_ack_4k are a2a_tail_ack_4k's
        # runs, but for the stage.
        source, shared = profiles.a2a_shared_4k("a2a_sweep_4k")
        self.assertEqual(source, "a2a_tail_ack_4k")
        self.assertEqual(set(shared), {
            ("ep16", "256k", "loss1e2_up"), ("ep64", "256k", "loss1e2_up"), ("ep64", "2m", "loss1e2_up"),
            ("ep64", "256k", "loss1e3_up"), ("ep64", "2m", "loss1e3_up"),
            ("ep16", "256k", "healthy"), ("ep64", "256k", "healthy"), ("ep64", "2m", "healthy")})
        theirs = {r["key"].split("-", 1)[1]: r
                  for r in profiles.a2a_stage_runs("a2a_tail_ack_4k", calibrated_4k(self.calibration), self.tuning)}
        with mock.patch.dict(profiles.A2A_SHARED_4K, clear=True):
            every = profiles.a2a_stage_runs("a2a_sweep_4k", calibrated_4k(self.calibration), self.tuning)
        left = [r for r in every if r not in runs]
        self.assertEqual(len(left), 8 * 6 * 3)
        for mine in left:
            self.assertIn((mine["group"], mine["size"], mine["condition"]), shared)
            self.assertEqual({**mine, "key": None, "stage": None},
                             {**theirs[mine["key"].split("-", 1)[1]], "key": None, "stage": None})
        handover = next(r for r in runs if r["condition"] == "rate16g_up")
        document = profiles.a2a_stage_profile(handover, self.calibration)
        self.assertEqual(document["network"]["link_failures"], profiles.a2a_fault("rate16g_up"))
        # EP8 has no per_receiver entry: its hold arms read the placeholder.
        for r in runs:
            balancing = profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
            if r["group"] == "ep8" and "loss_thresholds" in balancing:
                self.assertEqual(balancing["loss_thresholds"], profiles.A2A_LOSS_THRESHOLDS_PLACEHOLDER)


class Fixture(unittest.TestCase):
    @measured_4k()
    @measured_spines()
    @spines_gaps()
    def test_every_settled_stage_takes_it(self):
        here = Path(profiles.__file__).parent
        calibration = calibrated_4k(json.loads((here / "a2a_calibration.json").read_text()))
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        faults = set()
        for stage in profiles.A2A_STAGE_FIXTURE:
            fixture = profiles.A2A_STAGE_FIXTURE[stage]
            for r in profiles.a2a_stage_runs(stage, calibration, tuning):
                network = (document := profiles.a2a_stage_profile(r, calibration))["network"]
                uniform = network["load_balancing"]["mode"] == "spray_uniform"
                all_to_all = document["workload"]["kind"] == "all_to_all"
                self.assertEqual(r["fixture"], {key: value for key, value in fixture.items()
                                                if (key != "path_state" or uniform)
                                                and (key != "dataset_splits" or all_to_all)})
                self.assertEqual(network["packet_payload_bytes"], 4096)
                self.assertEqual(network["congestion_control"],
                                 {"mode": "nscc", "window_state": "connection", "rtt_sample": "karn"})
                self.assertEqual(network["load_balancing"].get("path_state"), "connection" if uniform else None)
                self.assertEqual(document["workload"].get("dataset_splits"), 1 if all_to_all else None)
                for failure in network["link_failures"]:
                    gray_loss = failure["model"] == "gray" and "error_rate" in failure
                    self.assertEqual(failure.get("loss_unit"), "byte" if gray_loss else None)
                    faults.add((r["condition"], gray_loss))
        # Loss up and down takes the byte unit; blackhole, half rate,
        # rate16g and the flap keep their models.
        self.assertTrue({("loss1e3_up", True), ("loss1e2_down", True), ("loss3e2_up", True),
                         ("blackhole_up", False), ("halfrate_up", False), ("structural", False),
                         ("rate16g_up", False), ("blackhole_down", False)} <= faults)

    def test_earlier_stages_take_none(self):
        for stage in ("a2a_gate", "a2a_tail_ack"):
            for r in profiles.a2a_stage_runs(stage, CALIBRATION, TUNING)[:40]:
                document = profiles.a2a_stage_profile(r, CALIBRATION)
                self.assertNotIn("fixture", r)
                self.assertNotIn("packet_payload_bytes", document["network"])
                self.assertEqual(document["network"]["congestion_control"], {"mode": "nscc"})
                self.assertNotIn("path_state", document["network"]["load_balancing"])
                self.assertNotIn("dataset_splits", document["workload"])


class Downlink(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = calibrated_4k(json.loads((here / "a2a_calibration.json").read_text()))

    @measured_4k()
    def test_runs_and_settings(self):
        runs = profiles.a2a_stage_runs("a2a_downlink_4k", self.calibration, self.tuning)
        # 6 cells and 2 healthy pairs by 5 arms by 3 seeds.
        self.assertEqual(len(runs), (6 + 2) * 5 * 3)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         {(g, "256k", c) for g in ("ep64", "ep16")
                          for c in ("blackhole_down", "halfrate_down", "healthy")}
                         | {("ep64", "256k", c) for c in ("loss1e3_down", "loss1e2_down")})
        self.assertEqual({r["arm"] for r in runs}, {"reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_rx",
                                                    "reps_tail", "mrc_hold_auto_tail_ack", "reps_oracle_tail"})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        cell = ("ep64", "256k", "blackhole_down", 301)
        by_arm = {r["arm"]: profiles.a2a_stage_profile(r, self.calibration)["network"]
                  for r in runs if (r["group"], r["size"], r["condition"], r["seed"]) == cell}
        self.assertEqual(by_arm["reps_hold_auto_tail_ack_rx"]["load_balancing"],
                         {**by_arm["reps_hold_auto_tail_ack"]["load_balancing"], "receiver_hold": True,
                          "receiver_hold_clean": 1024, "receiver_tail_missing": True})
        self.assertEqual(by_arm["reps_tail"]["link_failures"], profiles.a2a_fault("blackhole_down"))
        # The gray downlink loses whole frames.
        lossy = next(r for r in runs if r["key"] == "a2a_downlink_4k-ep64-256k-loss1e2_down-reps_tail-s301")
        self.assertEqual(profiles.a2a_stage_profile(lossy, self.calibration)["network"]["link_failures"],
                         [{**profiles.a2a_fault("loss1e2_down")[0], "loss_unit": "byte"}])

    def test_refused_per_pair(self):
        def measured_but(pair):
            pairs = tuple(p for p in profiles.A2A_PAYLOAD_COMPUTE_NS[4096] if p != pair)
            return mock.patch.dict(profiles.A2A_PAYLOAD_COMPUTE_MEASURED, {4096: pairs})

        with measured_but(("ep16", "256k")):
            with self.assertRaisesRegex(ValueError, r"\['ep16-256k'\] are provisional"):
                profiles.a2a_stage_runs("a2a_downlink_4k", self.calibration, self.tuning)
        # A stage that does not run the pair is not held back by it.
        with measured_but(("ep16", "2m")):
            with self.assertRaisesRegex(ValueError, r"\['ep16-2m'\] are provisional"):
                profiles.a2a_stage_runs("a2a_tail_ack_4k", self.calibration, self.tuning)
            self.assertEqual(len(profiles.a2a_stage_runs("a2a_downlink_4k", self.calibration, self.tuning)), 120)
            self.assertEqual(len(profiles.a2a_stage_runs("a2a_sweep_4k", self.calibration, self.tuning)), 288)

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {"reps_hold_auto_tail_ack": [300, 310, 320], "reps_hold_auto_tail_ack_rx": [100, 110, 120],
                 "reps_tail": [400, 420, 440]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, values in costs.items():
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("blackhole_down", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, "a2a_downlink_4k"),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        transport = {}
                        if arm.endswith("_rx") and condition != "healthy":
                            transport = {name: {"count": count, "bytes": 0} for name, count in (
                                ("receiver_tail_missing:control", 10), ("receiver_tail_missing_spine3:control", 9),
                                ("receiver_tail_missing_spine5:control", 1), ("receiver_tail_missing_late:control", 1),
                                ("receiver_tail_missing_late_spine5:control", 1))}
                            # Host 9 on leaf 1 holds spine 3 for leaf 0 twice; host 10
                            # holds spine 5 once, a false hold.
                            (d / "ns3" / "receiver_holds.csv").write_text(
                                "time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n"
                                + "".join(f"{onset + 1000 * i},9,0,3,10,{lost},{hold}\n" for i, (lost, hold) in
                                          enumerate([(0, "none"), (2, "leaf"), (1, "leaf"), (0, "none"), (1, "leaf")]))
                                + f"{onset + 1000},10,0,5,10,1,leaf\n")
                        if arm != "reps_tail" and condition != "healthy":
                            (d / "ns3" / "spine_losses.csv").write_text(
                                "time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                                f"{onset + 1000},0,1,3,10,0,3\n{onset + 2000},0,1,3,10,4,0\n"
                                f"{onset + 3000},0,1,3,0,0,0\n{onset + 3000},0,1,2,10,0,0\n")
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_downlink(analyze.a2a_rows([listing], root, "a2a_downlink_4k"), root)
        self.assertEqual(cell["fixture"], profiles.A2A_FIXTURE_4K)
        self.assertEqual(cell["receiver_versus_sender"], "reps_hold_auto_tail_ack_rx beats reps_hold_auto_tail_ack")
        rx, sender = cell["arms"]["reps_hold_auto_tail_ack_rx"], cell["arms"]["reps_hold_auto_tail_ack"]
        self.assertEqual(rx["versus_reference_cost"], "reps_hold_auto_tail_ack_rx beats reps_tail")
        self.assertEqual(rx["receiver_tail_missing_by_spine"], [{3: 9, 5: 1}] * 3)
        self.assertEqual(rx["receiver_tail_missing_late_by_spine"], [{5: 1}] * 3)
        self.assertEqual(rx["receiver_holds"], [{"holds": {3: 2, 5: 1}, "lost": {3: 4, 5: 1}}] * 3)
        self.assertEqual(rx["false_receiver_holds"], {"holds": [1, 1, 1], "lost": [1, 1, 1]})
        self.assertEqual(rx["receiver_reaction_ns"], [1000] * 3)
        self.assertEqual(sender["sender_holds"], [{"holds": {2: 1, 3: 1}, "lost": {2: 0, 3: 4}}] * 3)
        self.assertEqual(sender["false_sender_holds"], {"holds": [1, 1, 1], "lost": [0, 0, 0]})
        self.assertEqual(sender["receiver_holds"], [None] * 3)
        self.assertEqual(sender["false_receiver_holds"], {"holds": [None] * 3, "lost": [None] * 3})
        self.assertEqual(cell["arms"]["reps_tail"]["receiver_tail_missing_by_spine"], [{}] * 3)
        markdown = analyze.downlink_markdown([cell])
        self.assertIn("| reps_hold_auto_tail_ack_rx | 0.1 (3) | reps_hold_auto_tail_ack_rx beats reps_tail ", markdown)
        self.assertIn("| 3:27, 5:3 | 5:3 | 3:6, 5:3 | 3:12, 5:3 | 2:3, 3:3 | 2:0, 3:12 | 3, 3 | 3, 0 |", markdown)
        self.assertIn("ep64 256k blackhole_down: reps_hold_auto_tail_ack_rx against reps_hold_auto_tail_ack: "
                      "reps_hold_auto_tail_ack_rx beats reps_hold_auto_tail_ack", markdown)


class AckRoute(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_runs_and_settings(self):
        # The stage's pairs are measured and calibrated in the committed files.
        runs = profiles.a2a_stage_runs("a2a_ackroute_4k", self.calibration, self.tuning)
        conditions = ("blackhole_up", "blackhole_down", "loss1e2_up", "loss1e2_down", "loss1e3_up", "loss1e3_down")
        # 18 cells and 3 healthy pairs by 7 arms by 3 seeds.
        self.assertEqual(len(runs), (18 + 3) * 7 * 3)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         {(g, s, c) for g, s in (("ep64", "256k"), ("ep64", "decode8"), ("ep64", "2m"))
                          for c in conditions + ("healthy",)})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail_ack", "reps_hold_auto_tail4", "reps_tail", "reps_oracle_tail",
                          "reps_hold_auto_tail4_route_oracle", "reps_oracle_tail_route_oracle",
                          "reps_hold_auto_tail_ack_data_spine"])
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        for r in runs:
            self.assertEqual(r["setting"]["ack_route"],
                             "data_spine" if r["arm"].endswith("_data_spine") else "spray")
            self.assertEqual(r["setting"].get("ack_route_oracle", False), r["arm"].endswith("_route_oracle"))

        def balancing(stage, arm, size="256k", condition="loss1e2_up", calibration=None):
            key = f"{stage}-ep64-{size}-{condition}-{arm}-s301"
            (r,) = [r for r in profiles.a2a_stage_runs(stage, calibration or self.calibration, self.tuning)
                    if r["key"] == key]
            return profiles.a2a_stage_profile(r, self.calibration)["network"]

        stage = "a2a_ackroute_4k"
        # Each arm is its earlier stage's arm with the answers sprayed.
        for arm, earlier in (("reps_hold_auto_tail_ack", "a2a_tail_ack_4k"), ("reps_tail", "a2a_tail_ack_4k"),
                             ("reps_oracle_tail", "a2a_tail_ack_4k")):
            self.assertEqual(balancing(stage, arm)["load_balancing"],
                             {**balancing(earlier, arm)["load_balancing"], "ack_route": "spray"})
        hold = balancing(stage, "reps_hold_auto_tail_ack")["load_balancing"]
        tail4 = balancing(stage, "reps_hold_auto_tail4")["load_balancing"]
        self.assertEqual(tail4, {k: v for k, v in hold.items() if k != "ack_loss_rtts"})
        self.assertEqual(tail4["tail_loss_rtts"], 4)
        self.assertEqual(tail4["loss_thresholds"],
                         self.calibration["per_receiver"]["ep64-256k-p4096"]["loss_thresholds"][str(
                             tail4["loss_interval_ns"])])
        for arm, of in profiles.A2A_ACKROUTE_ORACLE_ARMS.items():
            self.assertEqual(balancing(stage, arm)["load_balancing"],
                             {**balancing(stage, of)["load_balancing"], "ack_route_oracle": True})
        # The control: a2a_tail_ack_4k's arm with today's routing written out,
        # on each cell's own calibration, EP64 at 2 MiB among them.
        for size in ("256k", "2m"):
            self.assertEqual(balancing(stage, "reps_hold_auto_tail_ack_data_spine", size)["load_balancing"],
                             {**balancing("a2a_tail_ack_4k", "reps_hold_auto_tail_ack", size)["load_balancing"],
                              "ack_route": "data_spine"})
        oracle = balancing(stage, "reps_oracle_tail_route_oracle")["load_balancing"]
        self.assertEqual((oracle["selector"], oracle["grade_cost_source"]), ("reps_oracle", "switch_queue"))
        # The decode cell's downlink black hole, as a2a_downlink_4k's down cells.
        decode = balancing(stage, "reps_tail", "decode8", "blackhole_down")
        self.assertEqual(decode["link_failures"], profiles.a2a_fault("blackhole_down"))
        self.assertEqual(decode["link_failures"][0]["leaf"], 1)
        self.assertEqual(balancing(stage, "reps_tail", "decode8", "loss1e3_down")["link_failures"],
                         [{**profiles.a2a_fault("loss1e3_down")[0], "loss_unit": "byte"}])

    def test_correct_hold(self):
        def correct(route, condition, sender, destination, spine=3):
            return analyze.correct_hold(route, condition, sender, destination, spine)

        for condition in ("blackhole_up", "loss1e2_up", "loss1e3_up"):
            # Data from leaf 0 crosses the uplink; under data_spine the answers
            # leaf 0 sends back cross it too.
            self.assertTrue(correct("data_spine", condition, 0, 5))
            self.assertTrue(correct("data_spine", condition, 0, None))
            self.assertTrue(correct("data_spine", condition, 5, 0))
            self.assertFalse(correct("data_spine", condition, 5, 1))
            self.assertFalse(correct("data_spine", condition, 5, None))
            self.assertTrue(correct("spray", condition, 0, 5))
            self.assertTrue(correct("spray", condition, 0, None))
            self.assertFalse(correct("spray", condition, 5, 0))
            self.assertFalse(correct("spray", condition, 1, 2))
        for condition in ("blackhole_down", "loss1e2_down", "loss1e3_down"):
            # Data toward leaf 1 crosses the downlink; under data_spine the
            # answers sent back to leaf 1 cross it too.
            self.assertTrue(correct("data_spine", condition, 4, 1))
            self.assertTrue(correct("data_spine", condition, 1, 4))
            self.assertTrue(correct("data_spine", condition, 1, None))
            self.assertFalse(correct("data_spine", condition, 0, 4))
            self.assertFalse(correct("data_spine", condition, 0, None))
            self.assertTrue(correct("spray", condition, 4, 1))
            self.assertFalse(correct("spray", condition, 1, 4))
            self.assertFalse(correct("spray", condition, 1, None))
        for route in profiles.A2A_ACKROUTE["ack_route"], "data_spine":
            self.assertFalse(correct(route, "blackhole_up", 0, 5, 2))
            self.assertFalse(correct(route, "healthy", 0, 5))
        # Toward the sender's own leaf nothing crosses a spine.
        for route in analyze.A2A_ACK_ROUTES:
            for condition in ("blackhole_up", "loss1e2_up"):
                self.assertFalse(correct(route, condition, 0, 0))
            for condition in ("blackhole_down", "loss1e2_down"):
                self.assertFalse(correct(route, condition, 1, 1))
        # Under the per-connection routes, as under spray, only the data's
        # path counts.
        for route in ("reverse_set", "repath"):
            for condition in ("blackhole_up", "loss1e2_up", "blackhole_down", "loss1e2_down"):
                for sender, destination in ((0, 4), (4, 0), (4, 1), (1, 4), (0, None), (1, None)):
                    self.assertEqual(correct(route, condition, sender, destination),
                                     correct("spray", condition, sender, destination), (route, condition))
        with self.assertRaisesRegex(ValueError, "ack_route"):
            correct("echo", "blackhole_up", 0, 5)
        with self.assertRaisesRegex(ValueError, "faulted direction"):
            correct("spray", "structural", 0, 5)

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {"reps_hold_auto_tail4": [300, 310, 320], "reps_hold_auto_tail4_route_oracle": [100, 110, 120],
                 "reps_tail": [400, 420, 440], "reps_hold_auto_tail4_cover_reverse_set": [200, 210, 220]}
        # Holds of spine 3: host 0 on leaf 0 toward leaf 5, host 9 on leaf 1
        # toward leaf 0, and host 9 of host scope; host 0 of spine 2.
        holds = ("time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                 f"{onset + 1000},0,5,3,10,0,3\n{onset + 2000},0,5,3,10,4,0\n{onset + 3000},0,5,3,0,0,0\n"
                 f"{onset + 2000},9,0,3,10,2,0\n{onset + 3000},9,0,3,10,0,3\n{onset + 4000},9,0,3,10,1,0\n"
                 f"{onset + 2000},0,5,2,10,1,0\n")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, values in costs.items():
                setting = {"ack_route": "reverse_set" if arm.endswith("reverse_set") else "spray",
                           **({"ack_route_oracle": True} if "oracle" in arm else {})}
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("blackhole_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, "a2a_ackroute_4k", setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        transport = {}
                        if condition != "healthy":
                            transport = {name: {"count": count, "bytes": 0} for name, count in (
                                ("answer_drop_up:control", 7), ("answer_drop_up_spine3:control", 7),
                                ("answer_drop_down:control", 0))}
                        if arm != "reps_tail":
                            (d / "ns3" / "spine_losses.csv").write_text(
                                holds if condition != "healthy" else holds.splitlines()[0] + "\n"
                                f"{onset + 1000},9,0,4,10,1,0\n")
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            rows = analyze.a2a_rows([listing], root, "a2a_ackroute_4k")
            (cell,) = analyze.a2a_ackroute(rows, root)
            (data_spine,) = analyze.a2a_ackroute(rows, root, "data_spine")
        hold, oracle = cell["arms"]["reps_hold_auto_tail4"], cell["arms"]["reps_hold_auto_tail4_route_oracle"]
        self.assertEqual(oracle["versus_reference_cost"], "reps_hold_auto_tail4_route_oracle beats reps_tail")
        self.assertEqual(cell["versus"], {
            "reps_hold_auto_tail4_route_oracle against reps_hold_auto_tail4":
                "reps_hold_auto_tail4_route_oracle beats reps_hold_auto_tail4"})
        self.assertEqual(hold["ack_route"], ["spray"] * 3)
        # Under spray only host 0's hold of spine 3 is correct; host 9's two
        # toward leaf 0 and host 0's of spine 2 are false.
        self.assertEqual(hold["sender_hold_reading"],
                         [{"correct_holds": 1, "false_holds": 3, "correct_held_rows": 2, "false_held_rows": 3}] * 3)
        # A run under reverse_set is read under its own routing, which, as
        # spray, counts the data's path alone.
        reverse_set = cell["arms"]["reps_hold_auto_tail4_cover_reverse_set"]
        self.assertEqual(reverse_set["ack_route"], ["reverse_set"] * 3)
        self.assertEqual(reverse_set["sender_hold_reading"], hold["sender_hold_reading"])
        # Under data_spine host 9's toward leaf 0 are correct too.
        self.assertEqual(data_spine["arms"]["reps_hold_auto_tail4"]["sender_hold_reading"],
                         [{"correct_holds": 3, "false_holds": 1, "correct_held_rows": 4, "false_held_rows": 1}] * 3)
        self.assertEqual(hold["healthy_sender_hold_reading"],
                         [{"correct_holds": 0, "false_holds": 1, "correct_held_rows": 0, "false_held_rows": 1}] * 3)
        self.assertEqual(hold["answer_drop_up_by_spine"], [{3: 7}] * 3)
        self.assertEqual(hold["answer_drop_down_by_spine"], [{}] * 3)
        self.assertEqual(cell["arms"]["reps_tail"]["sender_hold_reading"], [None] * 3)
        markdown = analyze.ackroute_markdown([cell])
        self.assertIn("| ep64 | 256k | blackhole_up | reps_hold_auto_tail4 | spray | 0.3 (3) "
                      "| reps_hold_auto_tail4 beats reps_tail ", markdown)
        self.assertIn("| 3, 6 | 9, 9 | 3, 3 | 3:21 | none | - |", markdown)
        self.assertIn("| reps_tail | spray | 0.4 (3) | - | - | -, - | -, - | -, - | 3:21 | none | - |", markdown)
        self.assertIn("- ep64 256k blackhole_up: reps_hold_auto_tail4_route_oracle against reps_hold_auto_tail4: "
                      "reps_hold_auto_tail4_route_oracle beats reps_hold_auto_tail4", markdown)

    def test_host_and_pair_scope_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            # Host scope: host 3 on leaf 0 and host 12 on leaf 1 hold spine 3
            # toward every leaf.
            path.write_text("time_ns,rank,spine,sent,lost,grade\n1,3,3,10,1,0\n1,12,3,10,1,0\n")
            self.assertEqual(analyze.sender_hold_reading(path, "loss1e2_up", "spray"),
                             {"correct_holds": 1, "false_holds": 1, "correct_held_rows": 1, "false_held_rows": 1})
            # Pair scope: host 12 toward host 3 (leaf 0), host 12 toward host 20.
            path.write_text("time_ns,rank,destination,spine,sent,lost,grade\n1,12,3,3,10,1,0\n1,12,20,3,10,1,0\n")
            self.assertEqual(analyze.sender_hold_reading(path, "loss1e2_up", "data_spine"),
                             {"correct_holds": 1, "false_holds": 1, "correct_held_rows": 1, "false_held_rows": 1})
            self.assertIsNone(analyze.sender_hold_reading(Path(directory) / "none.csv", "loss1e2_up", "spray"))

    def test_the_manifest_names_the_failed_links(self):
        # A single fault reads as its condition does, from its manifest.
        for condition in ("blackhole_up", "loss1e2_up", "loss1e3_down", "blackhole_down", "hostloss1e2_up",
                          "healthy"):
            links = tuple((f["leaf"], f["spine"], f["direction"]) for f in profiles.a2a_fault(condition)
                          if "spine" in f)
            for route in analyze.A2A_ACK_ROUTES:
                for sender in range(8):
                    for destination in (*range(8), None):
                        for spine in (2, 3):
                            with self.subTest(condition=condition, route=route, sender=sender,
                                              destination=destination, spine=spine):
                                self.assertEqual(
                                    analyze.correct_hold(route, condition, sender, destination, spine, links),
                                    analyze.correct_hold(route, condition, sender, destination, spine))
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)
            (run / "ns3").mkdir()
            # Leaf 2's uplink to spine 5, both directions of leaf 4's link to
            # spine 1 (a failure of it in both), and a host's link; 16 hosts
            # per leaf.
            (run / "manifest.json").write_text(json.dumps({
                "ranks": 128, "physical_topology": {"hosts_per_leaf": 16},
                "link_failures": [{"model": "gray", "leaf": 2, "spine": 5, "direction": "up"},
                                  {"model": "silent", "leaf": 4, "spine": 1, "direction": "both"},
                                  {"model": "gray", "host": 9, "spine": None, "direction": "up"}]}))
            self.assertEqual(analyze.run_faults(run, "multi")[0], ((2, 5, "up"), (4, 1, "both")))
            path = run / "ns3" / "spine_losses.csv"
            path.write_text("time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                            # Host 32 on leaf 2 toward leaf 3 over spine 5: the data crosses leaf 2's uplink.
                            "1,32,3,5,10,1,0\n"
                            # Host 48 on leaf 3 toward leaf 4 over spine 1: spine 1's downlink to leaf 4.
                            "1,48,4,1,10,1,0\n"
                            # Host 64 on leaf 4 toward leaf 0 over spine 1: leaf 4's uplink.
                            "1,64,0,1,10,1,0\n"
                            # Host 0 on leaf 0 toward leaf 1 over spine 3, and host 32 toward leaf 3
                            # over spine 1: no failed link.
                            "1,0,1,3,10,1,0\n1,32,3,1,10,1,0\n")
            self.assertEqual(analyze.sender_hold_reading(path, "multi", "spray"),
                             {"correct_holds": 3, "false_holds": 2, "correct_held_rows": 3, "false_held_rows": 2})
            self.assertTrue(analyze.correct_receiver_hold("multi", 4, 3, 1, ((4, 1, "both"),)))
            self.assertFalse(analyze.correct_receiver_hold("multi", 3, 0, 1, ((4, 1, "both"),)))

    def test_same_leaf_holds_are_false(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            # Host 3 on leaf 0 holds spine 3 toward leaf 0, and toward leaf 5.
            path.write_text("time_ns,rank,destination_leaf,spine,sent,lost,grade\n1,3,0,3,10,1,0\n1,3,5,3,10,1,0\n")
            for route in ("data_spine", "spray"):
                self.assertEqual(analyze.sender_hold_reading(path, "blackhole_up", route),
                                 {"correct_holds": 1, "false_holds": 1, "correct_held_rows": 1, "false_held_rows": 1})
            # Pair scope: host 3 toward host 5, on its own leaf.
            path.write_text("time_ns,rank,destination,spine,sent,lost,grade\n1,3,5,3,10,1,0\n")
            self.assertEqual(analyze.sender_hold_reading(path, "blackhole_up", "data_spine"),
                             {"correct_holds": 0, "false_holds": 1, "correct_held_rows": 0, "false_held_rows": 1})


class AckRouteForward(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_runs_and_settings(self):
        stage = "a2a_ackroute_fwd_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 18 cells and 3 healthy pairs by 2 arms by 3 seeds.
        self.assertEqual(len(runs), (18 + 3) * 2 * 3)
        ackroute = profiles.a2a_stage_runs("a2a_ackroute_4k", self.calibration, self.tuning)
        self.assertEqual({(r["group"], r["size"], r["condition"], r["seed"]) for r in runs},
                         {(r["group"], r["size"], r["condition"], r["seed"]) for r in ackroute})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_oracle_tail_fwd", "reps_oracle_tail_fwd_route_oracle"])
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        # Every profile is its a2a_ackroute_4k arm's but for its name and
        # oracle_hops forward.
        theirs = {r["key"]: r for r in ackroute}
        for r in runs:
            of = profiles.A2A_ACKROUTE_FWD_ARMS[r["arm"]]
            other = theirs[r["key"].replace(stage, "a2a_ackroute_4k").replace(f"-{r['arm']}-", f"-{of}-")]
            self.assertEqual(r["setting"], {**other["setting"], "oracle_hops": "forward"})
            mine = profiles.a2a_stage_profile(r, self.calibration)
            expected = profiles.a2a_stage_profile(other, self.calibration)
            balancing = mine["network"]["load_balancing"]
            self.assertEqual(balancing, {**expected["network"]["load_balancing"], "oracle_hops": "forward"})
            self.assertEqual((balancing["selector"], balancing["ack_route"]), ("reps_oracle", "spray"))
            self.assertEqual(balancing.get("ack_route_oracle", False), r["arm"].endswith("_route_oracle"))
            for document in (mine, expected):
                document.pop("name")
                document["network"]["load_balancing"].pop("oracle_hops", None)
            self.assertEqual(mine, expected)

    def test_analyzer_with_root(self):
        # The forward arms in one out root, a2a_ackroute_4k's in another;
        # each run's answers lost per spine say which root it was read from.
        costs = {("a2a_ackroute_fwd_4k", "reps_oracle_tail_fwd_route_oracle"): [100, 110, 120],
                 ("a2a_ackroute_fwd_4k", "reps_oracle_tail_fwd"): [200, 210, 220],
                 ("a2a_ackroute_4k", "reps_oracle_tail_route_oracle"): [300, 310, 320],
                 ("a2a_ackroute_4k", "reps_hold_auto_tail_ack_data_spine"): [400, 420, 440],
                 ("a2a_ackroute_4k", "reps_tail"): [500, 520, 540]}
        with tempfile.TemporaryDirectory() as directory:
            roots = {"a2a_ackroute_fwd_4k": Path(directory) / "fwd", "a2a_ackroute_4k": Path(directory) / "b0"}
            runs = {stage: [] for stage in roots}
            for (stage, arm), values in costs.items():
                setting = {"ack_route": "data_spine" if arm.endswith("_data_spine") else "spray"}
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("loss1e2_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, stage, setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs[stage].append(r)
                        d = roots[stage] / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        drops = 7 if stage == "a2a_ackroute_fwd_4k" else 5
                        transport = {} if condition == "healthy" else {
                            "answer_drop_up_spine3:control": {"count": drops, "bytes": 0}}
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            for stage, root in roots.items():
                (root / "runs.json").write_text(json.dumps(runs[stage]))
            fwd = roots["a2a_ackroute_fwd_4k"]
            rows = analyze.a2a_rows([fwd / "runs.json"], fwd, "a2a_ackroute_fwd_4k")
            (alone,) = analyze.a2a_ackroute(rows, fwd)
            (cell,) = analyze.a2a_ackroute(rows + analyze.a2a_root_rows(roots["a2a_ackroute_4k"], "a2a_ackroute_4k"),
                                           fwd)
            markdown = Path(directory) / "holds.md"
            subprocess.run(
                [sys.executable, str(Path(analyze.__file__)), "a2a_ackroute", "--runs", str(fwd / "runs.json"),
                 "--out-root", str(fwd), "--stage", "a2a_ackroute_fwd_4k", "--with-root",
                 str(roots["a2a_ackroute_4k"]), "--markdown", str(markdown)],
                check=True, capture_output=True,
            )
            markdown = markdown.read_text()
        fwd_oracle = "reps_oracle_tail_fwd_route_oracle"
        self.assertEqual(alone["versus"], {
            f"{fwd_oracle} against reps_oracle_tail_fwd": f"{fwd_oracle} beats reps_oracle_tail_fwd"})
        self.assertEqual(sorted(cell["arms"]), sorted(arm for _, arm in costs))
        self.assertEqual(cell["versus"], {
            f"{fwd_oracle} against reps_oracle_tail_route_oracle": f"{fwd_oracle} beats reps_oracle_tail_route_oracle",
            f"{fwd_oracle} against reps_oracle_tail_fwd": f"{fwd_oracle} beats reps_oracle_tail_fwd",
            f"{fwd_oracle} against reps_hold_auto_tail_ack_data_spine":
                f"{fwd_oracle} beats reps_hold_auto_tail_ack_data_spine"})
        self.assertEqual(cell["arms"][fwd_oracle]["cost_ns"], [100, 110, 120])
        self.assertEqual(cell["arms"]["reps_hold_auto_tail_ack_data_spine"]["cost_ns"], [400, 420, 440])
        self.assertEqual(cell["arms"][fwd_oracle]["stages"], ["a2a_ackroute_fwd_4k"])
        self.assertEqual(cell["arms"]["reps_oracle_tail_route_oracle"]["stages"], ["a2a_ackroute_4k"])
        self.assertEqual(cell["arms"][fwd_oracle]["answer_drop_up_by_spine"], [{3: 7}] * 3)
        self.assertEqual(cell["arms"]["reps_oracle_tail_route_oracle"]["answer_drop_up_by_spine"], [{3: 5}] * 3)
        self.assertEqual(cell["arms"][fwd_oracle]["versus_reference_cost"], f"{fwd_oracle} beats reps_tail")
        self.assertIn(f"- ep64 256k loss1e2_up: {fwd_oracle} against reps_hold_auto_tail_ack_data_spine: "
                      f"{fwd_oracle} beats reps_hold_auto_tail_ack_data_spine", markdown)
        self.assertIn(f"| ep64 | 256k | loss1e2_up | {fwd_oracle} | spray | 0.1 (3) | {fwd_oracle} beats reps_tail ",
                      markdown)


class DownlinkSpray(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def balancing(self, stage, arm, group="ep64", size="256k", condition="blackhole_down"):
        key = f"{stage}-{group}-{size}-{condition}-{arm}-s301"
        (r,) = [r for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning) if r["key"] == key]
        return profiles.a2a_stage_profile(r, self.calibration)["network"]

    def test_runs_and_settings(self):
        stage = "a2a_downlink_spray_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 10 faulted cells and 4 healthy pairs by 6 arms by 3 seeds.
        self.assertEqual(len(runs), (10 + 4) * 6 * 3)
        down = {(g, "256k", c) for g in ("ep64", "ep16") for c in ("blackhole_down", "halfrate_down")}
        down |= {("ep64", "256k", c) for c in ("loss1e3_down", "loss1e2_down")}
        down |= {("ep64", "decode8", c) for c in ("loss1e3_down", "blackhole_down")}
        down |= {("ep64", "2m", c) for c in ("loss1e3_down", "loss1e2_down")}
        healthy = {(g, s, "healthy") for g, s in (("ep64", "256k"), ("ep16", "256k"), ("ep64", "decode8"),
                                                  ("ep64", "2m"))}
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs}, down | healthy)
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail4", "reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_rx_closed",
                          "reps_hold_auto_tail4_rx_closed", "reps_tail", "reps_oracle_tail_fwd"])
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        for r in runs:
            self.assertEqual(r["setting"]["ack_route"], "spray")
        # Each arm is its earlier stage's arm with the answers sprayed.
        for arm, earlier in (("reps_hold_auto_tail_ack", "a2a_downlink_4k"), ("reps_tail", "a2a_downlink_4k"),
                             ("reps_hold_auto_tail4", "a2a_ackroute_4k")):
            spray = {} if earlier == "a2a_ackroute_4k" else {"ack_route": "spray"}
            condition = "loss1e2_down" if earlier == "a2a_ackroute_4k" else "blackhole_down"
            self.assertEqual(self.balancing(stage, arm, condition=condition)["load_balancing"],
                             {**self.balancing(earlier, arm, condition=condition)["load_balancing"], **spray})
        receiver = self.balancing("a2a_downlink_4k", "reps_hold_auto_tail_ack_rx")["load_balancing"]
        closed = self.balancing(stage, "reps_hold_auto_tail_ack_rx_closed")["load_balancing"]
        self.assertEqual(closed, {**receiver, "ack_route": "spray", "stray_path_seq": "closed"})
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_rx_closed")["load_balancing"],
                         {k: v for k, v in closed.items() if k != "ack_loss_rtts"})
        self.assertEqual(self.balancing(stage, "reps_oracle_tail_fwd", condition="loss1e2_down")["load_balancing"],
                         self.balancing("a2a_ackroute_fwd_4k", "reps_oracle_tail_fwd",
                                        condition="loss1e2_down")["load_balancing"])
        # The decode cell's and EP64 2 MiB's downlink faults, on their own
        # calibration.
        decode = self.balancing(stage, "reps_hold_auto_tail_ack", "ep64", "decode8", "blackhole_down")
        self.assertEqual(decode["link_failures"], profiles.a2a_fault("blackhole_down"))
        two = self.balancing(stage, "reps_hold_auto_tail_ack_rx_closed", "ep64", "2m", "loss1e3_down")
        self.assertEqual(two["link_failures"], [{**profiles.a2a_fault("loss1e3_down")[0], "loss_unit": "byte"}])
        self.assertEqual(two["load_balancing"]["loss_thresholds"],
                         self.calibration["per_receiver"]["ep64-2m-p4096"]["loss_thresholds"][str(
                             two["load_balancing"]["loss_interval_ns"])])

    def test_correct_receiver_hold(self):
        for condition in ("blackhole_down", "loss1e3_down", "halfrate_down"):
            # The data toward leaf 1 crosses the downlink.
            self.assertTrue(analyze.correct_receiver_hold(condition, 1, 0, 3))
            self.assertTrue(analyze.correct_receiver_hold(condition, 1, 5, 3))
            self.assertFalse(analyze.correct_receiver_hold(condition, 1, 1, 3))
            self.assertFalse(analyze.correct_receiver_hold(condition, 0, 1, 3))
            self.assertFalse(analyze.correct_receiver_hold(condition, 1, 0, 2))
        # Up: the data from leaf 0 crosses the uplink.
        self.assertTrue(analyze.correct_receiver_hold("loss1e2_up", 4, 0, 3))
        self.assertFalse(analyze.correct_receiver_hold("loss1e2_up", 0, 4, 3))
        self.assertFalse(analyze.correct_receiver_hold("healthy", 1, 0, 3))
        self.assertFalse(analyze.correct_receiver_hold("hostloss1e2_up", 1, 0, 3))
        with self.assertRaisesRegex(ValueError, "faulted direction"):
            analyze.correct_receiver_hold("structural", 1, 0, 3)

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {"reps_hold_auto_tail_ack_rx_closed": [100, 110, 120], "reps_hold_auto_tail_ack": [200, 210, 220],
                 "reps_hold_auto_tail4": [300, 310, 320], "reps_hold_auto_tail4_rx_closed": [150, 160, 170],
                 "reps_tail": [500, 520, 540]}
        # Sender holds of spine 3: host 0 on leaf 0 toward leaf 1 (correct)
        # and host 9 on leaf 1 toward leaf 0 (false under spray); host 0's of
        # spine 2 toward leaf 5 (false).
        senders = ("time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                   f"{onset + 1000},0,1,3,10,1,3\n{onset + 2000},0,1,3,10,4,0\n"
                   f"{onset + 2000},9,0,3,10,2,0\n{onset + 2000},0,5,2,10,1,0\n")
        # Receiver holds: host 9 on leaf 1 of spine 3 for leaf 0 (correct) and
        # of spine 5 (false); host 2 on leaf 0 of spine 3 for leaf 1 (false).
        receivers = ("time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n"
                     f"{onset + 500},9,0,3,10,2,none\n{onset + 1500},9,0,3,10,3,leaf\n"
                     f"{onset + 1500},9,0,5,10,1,leaf\n{onset + 1500},2,1,3,10,1,host\n")
        healthy_receivers = "time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n" f"{onset},9,0,4,10,1,leaf\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, values in costs.items():
                setting = {"ack_route": "spray", **({"stray_path_seq": "closed"} if "rx" in arm else {})}
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("blackhole_down", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, "a2a_downlink_spray_4k", setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        transport = {}
                        if arm != "reps_tail":
                            (d / "ns3" / "spine_losses.csv").write_text(
                                senders if condition != "healthy" else senders.splitlines()[0] + "\n")
                        if "rx" in arm:
                            (d / "ns3" / "receiver_holds.csv").write_text(
                                receivers if condition != "healthy" else healthy_receivers)
                            if condition != "healthy":
                                transport = {name: {"count": count, "bytes": 0} for name, count in (
                                    ("receiver_tail_missing:control", 7),
                                    ("receiver_tail_missing_spine3:control", 7),
                                    ("receiver_tail_missing_late_spine3:control", 1))}
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            rows = analyze.a2a_rows([listing], root, "a2a_downlink_spray_4k")
            (cell,) = analyze.a2a_downlink_spray(rows, root)
            markdown_path = root / "holds.md"
            subprocess.run(
                [sys.executable, str(Path(analyze.__file__)), "a2a_downlink_spray", "--runs", str(listing),
                 "--out-root", str(root), "--markdown", str(markdown_path)],
                check=True, capture_output=True,
            )
            markdown = markdown_path.read_text()
        rx = cell["arms"]["reps_hold_auto_tail_ack_rx_closed"]
        self.assertEqual(rx["sender_hold_ns"], [2000] * 3)
        self.assertEqual(rx["sender_lost_before"], [5] * 3)
        self.assertEqual(rx["receiver_hold_ns"], [1500] * 3)
        self.assertEqual(rx["receiver_lost_before"], [5] * 3)
        self.assertEqual(rx["receiver_tail_missing_by_spine"], [{3: 7}] * 3)
        self.assertEqual(rx["receiver_tail_missing_late_by_spine"], [{3: 1}] * 3)
        self.assertEqual(rx["ack_route"], ["spray"] * 3)
        self.assertEqual(rx["sender_hold_reading"],
                         [{"correct_holds": 1, "false_holds": 2, "correct_held_rows": 1, "false_held_rows": 2}] * 3)
        self.assertEqual(rx["receiver_hold_reading"],
                         [{"correct_holds": 1, "false_holds": 2, "correct_held_rows": 1, "false_held_rows": 2}] * 3)
        self.assertEqual(rx["healthy_receiver_hold_reading"],
                         [{"correct_holds": 0, "false_holds": 1, "correct_held_rows": 0, "false_held_rows": 1}] * 3)
        tail = cell["arms"]["reps_tail"]
        self.assertEqual((tail["sender_hold_ns"], tail["receiver_hold_reading"]), ([None] * 3, [None] * 3))
        self.assertEqual(cell["arms"]["reps_hold_auto_tail_ack"]["receiver_hold_ns"], [None] * 3)
        self.assertEqual(cell["versus"], {
            "reps_hold_auto_tail_ack_rx_closed against reps_hold_auto_tail_ack":
                "reps_hold_auto_tail_ack_rx_closed beats reps_hold_auto_tail_ack",
            "reps_hold_auto_tail4_rx_closed against reps_hold_auto_tail4":
                "reps_hold_auto_tail4_rx_closed beats reps_hold_auto_tail4",
            "reps_hold_auto_tail4_rx_closed against reps_hold_auto_tail_ack_rx_closed":
                "reps_hold_auto_tail_ack_rx_closed beats reps_hold_auto_tail4_rx_closed",
            "reps_hold_auto_tail_ack against reps_hold_auto_tail4":
                "reps_hold_auto_tail_ack beats reps_hold_auto_tail4"})
        self.assertIn("| ep64 | 256k | blackhole_down | reps_hold_auto_tail_ack_rx_closed | 0.1 (3) "
                      "| reps_hold_auto_tail_ack_rx_closed beats reps_tail | 2.0, 2.0, 2.0 | 5, 5, 5 "
                      "| 1.5, 1.5, 1.5 | 5, 5, 5 | 3:21 | 3:3 | 3, 3 | 6, 6 | 3, 3 | 6, 6 | 0, 0; 3, 3 | - |",
                      markdown)
        self.assertIn("| reps_tail | 0.5 (3) | - | -, -, - | -, -, - | -, -, - | -, -, - | none | none | -, - | -, - "
                      "| -, - | -, - | -, -; -, - | - |", markdown)
        self.assertIn("- ep64 256k blackhole_down: reps_hold_auto_tail_ack against reps_hold_auto_tail4: "
                      "reps_hold_auto_tail_ack beats reps_hold_auto_tail4", markdown)


class TwoEnd(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def balancing(self, stage, arm, group="ep64", size="256k", condition="blackhole_up"):
        key = f"{stage}-{group}-{size}-{condition}-{arm}-s301"
        (r,) = [r for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning) if r["key"] == key]
        return profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]

    def test_runs_and_settings(self):
        stage = "a2a_twoend_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 18 faulted cells and 3 healthy pairs by 7 arms by 3 seeds.
        self.assertEqual(len(runs), (18 + 3) * 7 * 3)
        ackroute = profiles.a2a_stage_runs("a2a_ackroute_4k", self.calibration, self.tuning)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         {(r["group"], r["size"], r["condition"]) for r in ackroute})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail4", "reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_answer_seq",
                          "reps_hold_auto_tail4_cover_route_oracle", "reps_hold_auto_tail4_cover_dup",
                          "reps_hold_auto_tail_ack_data_spine", "reps_tail"])
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        # a2a_ackroute_4k's arms as there.
        for arm in ("reps_hold_auto_tail4", "reps_hold_auto_tail_ack_data_spine", "reps_tail"):
            self.assertEqual(self.balancing(stage, arm), self.balancing("a2a_ackroute_4k", arm))
        reference = self.balancing("a2a_ackroute_4k", "reps_hold_auto_tail4")
        cover = self.balancing(stage, "reps_hold_auto_tail4_cover")
        self.assertEqual(cover, {**reference, "coverage_first": True, "copy_expiry": "deadline"})
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_cover_answer_seq"),
                         {**cover, "answer_path_seq": True})
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_cover_route_oracle"),
                         {**cover, "ack_route_oracle": True})
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_cover_dup"),
                         {**cover, "duplicate_report": True})
        self.assertEqual(reference["ack_route"], "spray")
        # The decode cell and EP64 2 MiB on their own calibration.
        two = self.balancing(stage, "reps_hold_auto_tail4_cover_answer_seq", "ep64", "2m", "loss1e3_down")
        self.assertEqual(two["loss_thresholds"],
                         self.calibration["per_receiver"]["ep64-2m-p4096"]["loss_thresholds"][str(
                             two["loss_interval_ns"])])
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_cover", "ep64", "decode8", "loss1e2_up"),
                         {**self.balancing("a2a_ackroute_4k", "reps_hold_auto_tail4", "ep64", "decode8",
                                           "loss1e2_up"), "coverage_first": True, "copy_expiry": "deadline"})

    def test_correct_answer_hold(self):
        for condition in ("blackhole_up", "loss1e2_up"):
            # Up: the answers from leaf 0 over spine 3 cross its uplink.
            self.assertTrue(analyze.correct_answer_hold(condition, 0, 4, 3))
            self.assertFalse(analyze.correct_answer_hold(condition, 4, 0, 3))
            self.assertFalse(analyze.correct_answer_hold(condition, 0, 0, 3))
            self.assertFalse(analyze.correct_answer_hold(condition, 0, 4, 2))
        # Down: the answers toward leaf 1 over spine 3 cross the downlink.
        self.assertTrue(analyze.correct_answer_hold("loss1e3_down", 5, 1, 3))
        self.assertFalse(analyze.correct_answer_hold("loss1e3_down", 1, 5, 3))
        self.assertFalse(analyze.correct_answer_hold("healthy", 0, 4, 3))

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {"reps_hold_auto_tail4": [300, 310, 320], "reps_hold_auto_tail4_cover": [200, 210, 220],
                 "reps_hold_auto_tail4_cover_answer_seq": [100, 110, 120],
                 "reps_hold_auto_tail4_cover_route_oracle": [100, 112, 118],
                 "reps_hold_auto_tail_ack_data_spine": [150, 160, 170], "reps_tail": [500, 520, 540]}
        # Sender holds of spine 3: host 0 on leaf 0 toward leaf 1 (correct
        # under spray) and host 9 on leaf 1 toward leaf 0 (false).
        senders = ("time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                   f"{onset + 2000},0,1,3,10,4,0\n{onset + 2000},9,0,3,10,2,0\n")
        # Answer holds: host 9 (leaf 1) holds route 3 of leaf 0's answers
        # (correct) and releases it, and route 5 (false); hosts 1 and 2 on
        # leaf 0 hold spine 3 of their answers to leaf 1 (correct), host 1
        # lets it go; host 9 holds spine 3 toward leaf 0 (false).
        answers = ("time_ns,side,rank,leaf,spine,held\n"
                   f"{onset + 1000},sender,9,0,3,1\n{onset + 1100},receiver,1,1,3,1\n"
                   f"{onset + 1200},receiver,2,1,3,1\n{onset + 1300},sender,9,0,5,1\n"
                   f"{onset + 1400},receiver,9,0,3,1\n{onset + 5000},sender,9,0,3,0\n"
                   f"{onset + 6000},receiver,1,1,3,0\n")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, values in costs.items():
                setting = {"ack_route": "data_spine" if arm.endswith("data_spine") else "spray"}
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("blackhole_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, "a2a_twoend_4k", setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        transport = {}
                        if arm != "reps_tail":
                            (d / "ns3" / "spine_losses.csv").write_text(
                                senders if condition != "healthy" else senders.splitlines()[0] + "\n")
                        if "cover" in arm:
                            transport["answer_loss_covered:control"] = {"count": 40, "bytes": 0}
                        if arm.endswith("answer_seq"):
                            (d / "ns3" / "answer_holds.csv").write_text(
                                answers if condition != "healthy" else answers.splitlines()[0] + "\n")
                            for name, count in (("answer_route_lost", 6), ("answer_route_lost_spine3", 5),
                                                ("answer_route_lost_spine5", 1), ("answer_probe", 2),
                                                ("answer_route_hold", 2), ("answer_route_release", 1)):
                                transport[f"{name}:control"] = {"count": count if condition != "healthy" else 0,
                                                                "bytes": 0}
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            rows = analyze.a2a_rows([listing], root, "a2a_twoend_4k")
            (cell,) = analyze.a2a_twoend(rows, root)
            markdown_path = root / "answers.md"
            subprocess.run(
                [sys.executable, str(Path(analyze.__file__)), "a2a_twoend", "--runs", str(listing),
                 "--out-root", str(root), "--markdown", str(markdown_path)],
                check=True, capture_output=True,
            )
            markdown = markdown_path.read_text()
        sequenced = cell["arms"]["reps_hold_auto_tail4_cover_answer_seq"]
        self.assertEqual(sequenced["answer_loss_covered"], [40] * 3)
        self.assertEqual(sequenced["answer_route_lost"], [6] * 3)
        self.assertEqual(sequenced["answer_route_lost_by_spine"], [{3: 5, 5: 1}] * 3)
        self.assertEqual(sequenced["answer_probe"], [2] * 3)
        self.assertEqual(sequenced["sender_hold_reading"],
                         [{"correct_holds": 1, "false_holds": 1, "correct_held_rows": 1, "false_held_rows": 1}] * 3)
        (reading,) = set(json.dumps(r, sort_keys=True) for r in sequenced["answer_hold_reading"])
        self.assertEqual(json.loads(reading), {
            "sender": {"correct_holds": 1, "false_holds": 1, "releases": 1, "held_at_end": 1},
            "receiver": {"correct_holds": 2, "false_holds": 1, "releases": 1, "held_at_end": 2},
            "receiver_holds": [
                {"sender_leaf": 0, "spine": 3, "correct": False, "receivers": 1, "first_ns": 1400, "holds": 1,
                 "releases": 0, "held_at_end": 1},
                {"sender_leaf": 1, "spine": 3, "correct": True, "receivers": 2, "first_ns": 1100, "holds": 2,
                 "releases": 1, "held_at_end": 1}]})
        self.assertEqual(sequenced["healthy_answer_hold_reading"],
                         [{"sender": {"correct_holds": 0, "false_holds": 0, "releases": 0, "held_at_end": 0},
                           "receiver": {"correct_holds": 0, "false_holds": 0, "releases": 0, "held_at_end": 0},
                           "receiver_holds": []}] * 3)
        cover = cell["arms"]["reps_hold_auto_tail4_cover"]
        self.assertEqual((cover["answer_loss_covered"], cover["answer_route_lost"], cover["answer_hold_reading"]),
                         ([40] * 3, [None] * 3, [None] * 3))
        self.assertEqual(cell["arms"]["reps_hold_auto_tail4"]["answer_loss_covered"], [None] * 3)
        self.assertEqual(cell["versus"], {
            "reps_hold_auto_tail4_cover against reps_hold_auto_tail4":
                "reps_hold_auto_tail4_cover beats reps_hold_auto_tail4",
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover":
                "reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail4_cover",
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover_route_oracle":
                "within seed noise",
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail_ack_data_spine":
                "reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail_ack_data_spine"})
        self.assertIn("| ep64 | 256k | blackhole_up | reps_hold_auto_tail4_cover_answer_seq | 0.1 (3) "
                      "| reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail4 | - | 3, 3 | 3, 3 | 0, 0 "
                      "| 120 | 18 | 3:15, 5:3 | 3, 3, 3, 3 | 6, 3, 3, 6 | 6 | -, - | - | -, - | - |", markdown)
        self.assertIn("| reps_tail | 0.5 (3) | reps_hold_auto_tail4 beats reps_tail | - | -, - | -, - | -, - | - | - "
                      "| - | -, -, -, - | -, -, -, - | - | -, - | - | -, - | - |", markdown)
        self.assertIn("- ep64 256k blackhole_up reps_hold_auto_tail4_cover_answer_seq s301: receiver answer holds: "
                      "leaf 0 spine 3 (false): 1 receivers from 1.4 us, 1 holds, 0 released, 1 at end; "
                      "leaf 1 spine 3 (correct): 2 receivers from 1.1 us, 2 holds, 1 released, 1 at end", markdown)
        self.assertIn("- ep64 256k blackhole_up: reps_hold_auto_tail4_cover_answer_seq against "
                      "reps_hold_auto_tail4_cover: reps_hold_auto_tail4_cover_answer_seq beats "
                      "reps_hold_auto_tail4_cover", markdown)


class TwoEndBase(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def balancing(self, stage, arm, condition="blackhole_up"):
        key = f"{stage}-ep64-256k-{condition}-{arm}-s301"
        (r,) = [r for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning) if r["key"] == key]
        return profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]

    def test_runs_and_settings(self):
        stage = "a2a_twoend_base_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 18 faulted cells and 3 healthy pairs by 2 arms by 3 seeds.
        self.assertEqual(len(runs), (18 + 3) * 2 * 3)
        twoend = profiles.a2a_stage_runs("a2a_twoend_4k", self.calibration, self.tuning)
        self.assertEqual({(r["group"], r["size"], r["condition"], r["seed"]) for r in runs},
                         {(r["group"], r["size"], r["condition"], r["seed"]) for r in twoend})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover_repath"])
        cover = self.balancing("a2a_twoend_4k", "reps_hold_auto_tail4_cover", "loss1e2_down")
        for arm, route in (("reps_hold_auto_tail4_cover_reverse_set", "reverse_set"),
                           ("reps_hold_auto_tail4_cover_repath", "repath")):
            self.assertEqual(self.balancing(stage, arm, "loss1e2_down"), {**cover, "ack_route": route})

    def test_retract_runs_and_settings(self):
        stage = "a2a_twoend_retract_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 18 faulted cells and 3 healthy pairs by 3 arms by 3 seeds.
        self.assertEqual(len(runs), (18 + 3) * 3 * 3)
        twoend = profiles.a2a_stage_runs("a2a_twoend_4k", self.calibration, self.tuning)
        self.assertEqual({(r["group"], r["size"], r["condition"], r["seed"]) for r in runs},
                         {(r["group"], r["size"], r["condition"], r["seed"]) for r in twoend})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_answer_seq",
                          "reps_hold_auto_tail4_cover_retract"])
        # The coverage-first arms are a2a_twoend_4k's, run by run.
        by_key = {r["key"].split("-", 1)[1]: r for r in twoend}
        for run in runs:
            if run["arm"] != "reps_hold_auto_tail4_cover_retract":
                other = by_key[run["key"].split("-", 1)[1]]
                self.assertEqual({**profiles.a2a_stage_profile(run, self.calibration), "name": None},
                                 {**profiles.a2a_stage_profile(other, self.calibration), "name": None})
        cover = self.balancing("a2a_twoend_4k", "reps_hold_auto_tail4_cover", "loss1e2_down")
        self.assertEqual(self.balancing(stage, "reps_hold_auto_tail4_cover_retract", "loss1e2_down"),
                         {**cover, "duplicate_report": "retract"})

    def test_read_with_twoend(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {("a2a_twoend_4k", "reps_hold_auto_tail4_cover"): [300, 310, 320],
                 ("a2a_twoend_4k", "reps_hold_auto_tail4_cover_answer_seq"): [100, 110, 120],
                 ("a2a_twoend_base_4k", "reps_hold_auto_tail4_cover_reverse_set"): [200, 205, 215],
                 ("a2a_twoend_base_4k", "reps_hold_auto_tail4_cover_repath"): [290, 330, 310]}
        with tempfile.TemporaryDirectory() as directory:
            roots = {"a2a_twoend_4k": Path(directory) / "twoend", "a2a_twoend_base_4k": Path(directory) / "base"}
            runs = {stage: [] for stage in roots}
            for (stage, arm), values in costs.items():
                setting = {"ack_route": arm.rpartition("_cover_")[2] if "_cover_" in arm else "spray",
                           "coverage_first": True}
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("blackhole_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, stage, setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs[stage].append(r)
                        d = roots[stage] / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        transport = {}
                        if arm.endswith("reverse_set"):
                            transport = {"answer_set_probe:control": {"count": 30, "bytes": 0},
                                         "answer_set_replace:control": {"count": 4, "bytes": 0}}
                        if arm.endswith("repath"):
                            transport = {"answer_repath:control": {"count": 7, "bytes": 0}}
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            for stage, root in roots.items():
                (root / "runs.json").write_text(json.dumps(runs[stage]))
            markdown_path = Path(directory) / "base.md"
            subprocess.run(
                [sys.executable, str(Path(analyze.__file__)), "a2a_twoend", "--runs",
                 str(roots["a2a_twoend_base_4k"] / "runs.json"), "--out-root", str(roots["a2a_twoend_base_4k"]),
                 "--stage", "a2a_twoend_base_4k", "--with-root", str(roots["a2a_twoend_4k"]),
                 "--markdown", str(markdown_path)],
                check=True, capture_output=True,
            )
            markdown = markdown_path.read_text()
            rows = (analyze.a2a_rows([roots["a2a_twoend_base_4k"] / "runs.json"], roots["a2a_twoend_base_4k"],
                                     "a2a_twoend_base_4k")
                    + analyze.a2a_root_rows(roots["a2a_twoend_4k"], "a2a_twoend_4k"))
            (cell,) = analyze.a2a_twoend(rows, roots["a2a_twoend_base_4k"])
        self.assertEqual(cell["arms"]["reps_hold_auto_tail4_cover_reverse_set"]["answer_set_replace"], [4] * 3)
        self.assertEqual(cell["arms"]["reps_hold_auto_tail4_cover_repath"]["answer_repath"], [7] * 3)
        self.assertEqual(cell["versus"], {
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover":
                "reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail4_cover",
            "reps_hold_auto_tail4_cover_reverse_set against reps_hold_auto_tail4_cover":
                "reps_hold_auto_tail4_cover_reverse_set beats reps_hold_auto_tail4_cover",
            "reps_hold_auto_tail4_cover_repath against reps_hold_auto_tail4_cover": "within seed noise",
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover_reverse_set":
                "reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail4_cover_reverse_set",
            "reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover_repath":
                "reps_hold_auto_tail4_cover_answer_seq beats reps_hold_auto_tail4_cover_repath"})
        self.assertIn("| reps_hold_auto_tail4_cover_reverse_set | 0.2 (3) ", markdown)
        self.assertIn("| - | 90, 12 | - | -, - | - |", markdown)
        self.assertIn("| - | -, - | 21 | -, - | - |", markdown)


class NoAck(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_runs(self):
        runs = profiles.a2a_stage_runs("a2a_noack_4k", self.calibration, self.tuning)
        faulted = set()
        for stage in profiles.A2A_NOACK_SOURCES:
            faulted |= set(profiles.a2a_faulted_4k(stage)[0])
        # 28 gate cells, 11 more of the sweep and 5 of the decode cell, and
        # 10 healthy pairs, by 3 arms by 3 seeds.
        self.assertEqual(len(faulted), 44)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs if r["condition"] != "healthy"},
                         faulted)
        self.assertEqual(len({(r["group"], r["size"]) for r in runs if r["condition"] == "healthy"}), 10)
        self.assertEqual(len(runs), (44 + 10) * 3 * 3)
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail4", "reps_hold_leaf_tail4", "reps_flow_hold_tail4"])
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual((analyze.A2A_NOACK_ARMS, analyze.A2A_NOACK_SOURCES),
                         (profiles.A2A_NOACK_ARMS, profiles.A2A_NOACK_SOURCES))

    def test_profiles_are_the_ack_arms_less_ack_loss_rtts(self):
        ack = {}
        for stage in profiles.A2A_NOACK_SOURCES:
            for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning):
                ack.setdefault((r["arm"], r["group"], r["size"], r["condition"], r["seed"]), r)
        for r in profiles.a2a_stage_runs("a2a_noack_4k", self.calibration, self.tuning):
            theirs = ack[(profiles.A2A_NOACK_ARMS[r["arm"]], r["group"], r["size"], r["condition"], r["seed"])]
            for key in ("compute_ns", "iterations", "fixture", "message_bytes"):
                self.assertEqual(r[key], theirs[key])
            mine = profiles.a2a_stage_profile(r, self.calibration)
            expected = profiles.a2a_stage_profile(theirs, self.calibration)
            self.assertEqual(expected["network"]["load_balancing"].pop("ack_loss_rtts"), 2)
            expected["name"] = mine["name"]
            self.assertEqual(json.dumps(mine), json.dumps(expected), r["key"])

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        with tempfile.TemporaryDirectory() as directory:
            root, theirs = Path(directory) / "noack", Path(directory) / "ack"

            def write(out, arm, stage, costs):
                runs = []
                for seed, cost in zip((301, 302, 303), costs):
                    for condition, extra in (("healthy", 0), ("loss1e2_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, stage), "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = out / r["key"]
                        d.mkdir(parents=True)
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": {}}))
                return runs

            runs = write(root, "reps_hold_auto_tail4", "a2a_noack_4k", [500, 520, 540])
            runs += write(root, "reps_hold_leaf_tail4", "a2a_noack_4k", [100, 110, 120])
            (root / "runs.json").write_text(json.dumps(runs))
            ack = write(theirs, "reps_hold_auto_tail_ack", "a2a_tail_ack_4k", [300, 310, 320])
            ack += write(theirs, "reps_hold_leaf_tail_ack", "a2a_tail_ack_4k", [300, 310, 320])
            (theirs / "runs.json").write_text(json.dumps(ack))
            rows = analyze.a2a_rows([root / "runs.json"], root, "a2a_noack_4k")
            (cell,) = analyze.a2a_noack(rows, root, analyze.a2a_ack_rows([theirs]))
            (alone,) = analyze.a2a_noack(rows, root)
        auto, leaf = cell["arms"]["reps_hold_auto_tail4"], cell["arms"]["reps_hold_leaf_tail4"]
        self.assertEqual((auto["ack_arm"], auto["ack_stages"], auto["ack_cost_ns"]),
                         ("reps_hold_auto_tail_ack", ["a2a_tail_ack_4k"], [300, 310, 320]))
        self.assertEqual(auto["minus_ack_cost_ns"], 210)
        self.assertEqual(auto["versus_ack_cost"], "reps_hold_auto_tail_ack beats reps_hold_auto_tail4")
        self.assertEqual(leaf["versus_ack_cost"], "reps_hold_leaf_tail4 beats reps_hold_leaf_tail_ack")
        self.assertEqual((alone["arms"]["reps_hold_auto_tail4"]["ack_cost_ns"],
                          alone["arms"]["reps_hold_auto_tail4"]["versus_ack_cost"]), (None, None))
        markdown = analyze.noack_markdown([cell])
        self.assertIn("| ep64 | 256k | loss1e2_up | reps_hold_auto_tail4 | 0.5 (3) | reps_hold_auto_tail_ack | 0.3 "
                      "| 0.2 | reps_hold_auto_tail_ack beats reps_hold_auto_tail4 |", markdown)


class HostLink(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_faults(self):
        onset = profiles.DEGRADE_AT_NS
        self.assertEqual(profiles.a2a_fault("hostloss1e2_up"),
                         [{"model": "gray", "error_rate": 1e-2, "start_ns": onset, "host": 9, "direction": "up"}])
        self.assertEqual(profiles.a2a_fault("hostloss1e3_down"),
                         [{"model": "gray", "error_rate": 1e-3, "start_ns": onset, "host": 9, "direction": "down"}])
        self.assertEqual(profiles.A2A_FAULT_HOST // 8, 1)
        self.assertEqual((analyze.A2A_FAULT_HOST, analyze.A2A_HOST_PREFIX),
                         (profiles.A2A_FAULT_HOST, profiles.A2A_HOST_PREFIX))
        for condition in ("hostloss5e2_up", "hostloss1e2_both", "hostblackhole_up"):
            with self.assertRaisesRegex(ValueError, "unknown condition"):
                profiles.a2a_fault(condition)

    def test_runs_and_settings(self):
        runs = profiles.a2a_stage_runs("a2a_hostlink_4k", self.calibration, self.tuning)
        conditions = ("hostloss1e2_up", "hostloss1e3_up", "hostloss1e2_down", "hostloss1e3_down")
        # 12 cells and 3 healthy pairs by 5 arms by 3 seeds.
        self.assertEqual(len(runs), (12 + 3) * 5 * 3)
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         {(g, s, c) for g, s in (("ep64", "256k"), ("ep64", "2m"), ("ep64", "decode8"))
                          for c in conditions + ("healthy",)})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack",
                          "reps_tail", "uniform_tail"])
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))

        def network(stage, arm, size, condition):
            key = f"{stage}-ep64-{size}-{condition}-{arm}-s301"
            (r,) = [r for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning) if r["key"] == key]
            return profiles.a2a_stage_profile(r, self.calibration)["network"]

        # Each arm as a2a_tail_ack_4k and a2a_decode_4k run it, the answers
        # on their data's spines; the loss per byte.
        for arm in profiles.A2A_HOSTLINK_ARMS:
            hostlink = network("a2a_hostlink_4k", arm, "256k", "hostloss1e2_up")
            self.assertEqual(hostlink["load_balancing"],
                             network("a2a_tail_ack_4k", arm, "256k", "loss1e2_up")["load_balancing"])
            self.assertNotIn("ack_route", hostlink["load_balancing"])
            self.assertEqual(network("a2a_hostlink_4k", arm, "decode8", "healthy"),
                             network("a2a_decode_4k", arm, "decode8", "healthy"))
        self.assertEqual(network("a2a_hostlink_4k", "reps_tail", "decode8", "hostloss1e3_down")["link_failures"],
                         [{**profiles.a2a_fault("hostloss1e3_down")[0], "loss_unit": "byte"}])

    def test_summary_of_a_host_link_run(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            path.write_text("time_ns,rank,destination_leaf,spine,sent,lost,grade\n10,0,1,3,10,2,0\n20,0,1,5,10,3,0\n")
            result = summarize.spine_losses(path, None, 5)
        self.assertEqual(result["faulted_spine"], None)
        self.assertEqual(result["spines"], {"3": {"sent": 10, "lost": 2}, "5": {"sent": 10, "lost": 3}})
        self.assertEqual(set(result["cumulative_lost_reached_ns"].values()), {None})

    def test_every_hold_is_false_and_no_reaction(self):
        for route in ("data_spine", "spray"):
            for condition in ("hostloss1e2_up", "hostloss1e3_down"):
                self.assertFalse(analyze.correct_hold(route, condition, 0, 0, 3))
                self.assertFalse(analyze.correct_hold(route, condition, 1, 1, 3))
        with tempfile.TemporaryDirectory() as directory:
            d = Path(directory)
            (d / "ns3").mkdir()
            (d / "ns3" / "spine_losses.csv").write_text(
                "time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                f"{analyze.DEGRADE_AT_NS + 24648},0,1,3,10,0,3\n")
            (d / "ns3" / "receiver_holds.csv").write_text("time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n")
            self.assertEqual(analyze.reaction(d, "hostloss1e2_up", 24648), {"reaction_ns": None, "share_basis": None})
            self.assertEqual(analyze.receiver_reaction(d, "hostloss1e2_down"),
                             {"receiver_reaction_ns": None, "receiver_basis": None})

    def test_host_link_reading(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "spine_losses.csv"
            # Host 0 holds spine 3 toward leaf 1 twice and spine 5 toward
            # leaf 2 once; host 12 loses on spine 2 toward leaf 1 and holds
            # nothing.
            path.write_text("time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                            "10,0,1,3,10,2,0\n20,0,1,3,10,0,3\n30,0,1,3,10,1,0\n40,0,1,3,10,1,0\n"
                            "10,0,2,5,10,1,0\n10,12,1,2,10,4,3\n")
            reading = analyze.host_link_reading(path)
            self.assertEqual(reading["faulted_leaf"], 1)
            self.assertEqual(reading["holding_series"], [
                {"rank": 0, "destination_leaf": 1, "spine": 3, "holds": 2, "held_rows": 3},
                {"rank": 0, "destination_leaf": 2, "spine": 5, "holds": 1, "held_rows": 1}])
            self.assertEqual(reading["toward_faulted_leaf"], {"series": 1, "holds": 2, "held_rows": 3})
            self.assertEqual(reading["toward_other_leaves"], {"series": 1, "holds": 1, "held_rows": 1})
            self.assertEqual(reading["toward_every_leaf"], {"series": 0, "holds": 0, "held_rows": 0})
            self.assertEqual(reading["lost_toward_faulted_leaf_by_spine"], {2: 4, 3: 4})
            self.assertEqual(reading["lost_by_spine"], {2: 4, 3: 4, 5: 1})
            # Host scope: every hold is toward every leaf.
            path.write_text("time_ns,rank,spine,sent,lost,grade\n10,0,3,10,2,0\n")
            self.assertEqual(analyze.host_link_reading(path)["toward_every_leaf"],
                             {"series": 1, "holds": 1, "held_rows": 1})
            self.assertIsNone(analyze.host_link_reading(Path(directory) / "none.csv"))

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {"reps_hold_auto_tail_ack": [300, 310, 320], "reps_tail": [400, 420, 440]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, values in costs.items():
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("hostloss1e2_down", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, "a2a_hostlink_4k"),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        if arm != "reps_tail":
                            (d / "ns3" / "spine_losses.csv").write_text(
                                "time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                                + (f"{onset + 1000},0,1,3,10,2,0\n{onset + 1000},0,1,4,10,3,3\n"
                                   f"{onset + 1000},0,2,6,10,1,0\n" if condition != "healthy" else ""))
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": {}}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_hostlink(analyze.a2a_rows([listing], root, "a2a_hostlink_4k"), root)
        hold = cell["arms"]["reps_hold_auto_tail_ack"]
        self.assertEqual(hold["versus_reference_cost"], "reps_hold_auto_tail_ack beats reps_tail")
        self.assertEqual(hold["reaction_ns"], [None] * 3)
        self.assertEqual(hold["sender_hold_reading"],
                         [{"correct_holds": 0, "false_holds": 2, "correct_held_rows": 0, "false_held_rows": 2}] * 3)
        self.assertEqual([h["toward_faulted_leaf"] for h in hold["host_link"]],
                         [{"series": 1, "holds": 1, "held_rows": 1}] * 3)
        self.assertEqual([h["lost_toward_faulted_leaf_by_spine"] for h in hold["host_link"]], [{3: 2, 4: 3}] * 3)
        self.assertEqual(cell["arms"]["reps_tail"]["host_link"], [None] * 3)
        markdown = analyze.hostlink_markdown([cell])
        self.assertIn("| ep64 | 256k | hostloss1e2_down | reps_hold_auto_tail_ack | 0.3 (3) "
                      "| reps_hold_auto_tail_ack beats reps_tail | 6, 6 | 3, 3 (3) | 3, 3 (3) | 0, 0 (0) | 0, 0 "
                      "| 3:6, 4:9 | 3:6, 4:9, 6:3 | - | - |", markdown)
        self.assertIn("| reps_tail | 0.4 (3) | - | -, - | - | - | - | -, - | - | - | - | - |", markdown)
        self.assertIn("|---" * 15 + "|\n", markdown)
        self.assertEqual(cell["versus"], {})

    def test_cover_runs_and_settings(self):
        stage = "a2a_hostlink_s_4k"
        runs = profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        # 12 cells and 3 healthy pairs by 3 arms by 3 seeds.
        self.assertEqual(len(runs), (12 + 3) * 3 * 3)
        hostlink = profiles.a2a_stage_runs("a2a_hostlink_4k", self.calibration, self.tuning)
        self.assertEqual({(r["group"], r["size"], r["condition"], r["seed"]) for r in runs},
                         {(r["group"], r["size"], r["condition"], r["seed"]) for r in hostlink})
        self.assertEqual(list(dict.fromkeys(r["arm"] for r in runs)),
                         ["reps_hold_auto_tail_ack_cover", "reps_hold_leaf_tail_ack_cover",
                          "reps_flow_hold_tail_ack_cover"])
        self.assertEqual([r["fixture"] for r in runs], [profiles.A2A_FIXTURE_4K] * len(runs))
        # Each arm its a2a_hostlink_4k arm's run but for the stage, the arm
        # and the two keys.
        theirs = {r["key"]: r for r in hostlink}
        for r in runs:
            of = profiles.A2A_HOSTLINK_COVER_ARMS[r["arm"]]
            key = r["key"].replace(stage, "a2a_hostlink_4k", 1).replace(f"-{r['arm']}-", f"-{of}-")
            mine = profiles.a2a_stage_profile(r, self.calibration)
            other = profiles.a2a_stage_profile(theirs[key], self.calibration)
            self.assertEqual(mine["network"]["load_balancing"],
                             {**other["network"]["load_balancing"], "coverage_first": True,
                              "copy_expiry": "deadline"})
            self.assertNotIn("ack_route", mine["network"]["load_balancing"])
            mine["network"]["load_balancing"] = other["network"]["load_balancing"]
            self.assertEqual(mine["network"], other["network"])
            self.assertEqual((r["compute_ns"], r["iterations"]), (theirs[key]["compute_ns"], theirs[key]["iterations"]))

    def test_cover_analyzer_with_roots(self):
        onset = analyze.DEGRADE_AT_NS
        costs = {("a2a_hostlink_4k", "reps_hold_auto_tail_ack"): [300, 310, 320],
                 ("a2a_hostlink_4k", "reps_tail"): [400, 420, 440],
                 ("a2a_hostlink_s_4k", "reps_hold_auto_tail_ack_cover"): [100, 110, 120]}
        with tempfile.TemporaryDirectory() as directory:
            roots = {"a2a_hostlink_4k": Path(directory) / "hostlink", "a2a_hostlink_s_4k": Path(directory) / "s"}
            runs = {stage: [] for stage in roots}
            for (stage, arm), values in costs.items():
                setting = {"coverage_first": True} if arm.endswith("_cover") else None
                for seed, cost in zip((301, 302, 303), values):
                    for condition, extra in (("healthy", 0), ("hostloss1e2_up", cost)):
                        r = {**run(arm, "ep64", "256k", condition, seed, stage, setting),
                             "fixture": profiles.A2A_FIXTURE_4K}
                        runs[stage].append(r)
                        d = roots[stage] / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        (d / "ns3" / "spine_losses.csv").write_text(
                            "time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
                            + (f"{onset + 1000},0,1,3,10,2,0\n" if condition != "healthy" else ""))
                        transport = ({"answer_loss_covered:control": {"count": 5, "bytes": 0}}
                                     if setting and condition != "healthy" else {})
                        timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + extra, "2": 1000 + extra},
                                  "iteration_start_ns": {"1": 0, "2": onset}}
                        (d / "summary.json").write_text(json.dumps({"all_to_all": timing, "transport": transport}))
            for stage, root in roots.items():
                (root / "runs.json").write_text(json.dumps(runs[stage]))
            markdown_path = Path(directory) / "s.md"
            subprocess.run(
                [sys.executable, str(Path(analyze.__file__)), "a2a_hostlink", "--runs",
                 str(roots["a2a_hostlink_s_4k"] / "runs.json"), "--out-root", str(roots["a2a_hostlink_s_4k"]),
                 "--stage", "a2a_hostlink_s_4k", "--with-root", str(roots["a2a_hostlink_4k"]),
                 "--markdown", str(markdown_path)],
                check=True, capture_output=True,
            )
            markdown = markdown_path.read_text()
            rows = (analyze.a2a_rows([roots["a2a_hostlink_s_4k"] / "runs.json"], roots["a2a_hostlink_s_4k"],
                                     "a2a_hostlink_s_4k")
                    + analyze.a2a_root_rows(roots["a2a_hostlink_4k"], "a2a_hostlink_4k"))
            (cell,) = analyze.a2a_hostlink(rows, roots["a2a_hostlink_s_4k"])
        cover = cell["arms"]["reps_hold_auto_tail_ack_cover"]
        self.assertEqual(cover["answer_loss_covered"], [5] * 3)
        self.assertEqual(cover["sender_hold_reading"],
                         [{"correct_holds": 0, "false_holds": 1, "correct_held_rows": 0, "false_held_rows": 1}] * 3)
        self.assertEqual(cell["arms"]["reps_hold_auto_tail_ack"]["answer_loss_covered"], [None] * 3)
        self.assertEqual(cell["versus"], {"reps_hold_auto_tail_ack_cover against reps_hold_auto_tail_ack":
                                          "reps_hold_auto_tail_ack_cover beats reps_hold_auto_tail_ack"})
        self.assertIn("| ep64 | 256k | hostloss1e2_up | reps_hold_auto_tail_ack_cover | 0.1 (3) "
                      "| reps_hold_auto_tail_ack_cover beats reps_tail | 3, 3 | 3, 3 (3) | 0, 0 (0) | 0, 0 (0) "
                      "| 0, 0 | 3:6 | 3:6 | 15 | - |", markdown)
        self.assertIn("- ep64 256k hostloss1e2_up: reps_hold_auto_tail_ack_cover against reps_hold_auto_tail_ack: "
                      "reps_hold_auto_tail_ack_cover beats reps_hold_auto_tail_ack", markdown)


class Spines(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())

    def test_fixtures(self):
        self.assertEqual({spines: fixture["spine_link_rate"] for spines, fixture in profiles.A2A_SPINES_FIXTURE.items()},
                         {8: "400Gbps", 32: "100Gbps", 128: "25Gbps", 256: "12.5Gbps"})
        for spines, fixture in profiles.A2A_SPINES_FIXTURE.items():
            self.assertEqual(fixture, {**profiles.A2A_FIXTURE_4K, "spine_count": spines,
                                       "spine_link_rate": fixture["spine_link_rate"], "pair_bandwidth": "spines",
                                       "port_rate_thresholds": True,
                                       "base_rtt_ns": profiles.A2A_SPINES_BASE_RTT_NS[spines]})
        # The 8-spine fabric's base RTT is the one every earlier stage takes.
        self.assertEqual(profiles.A2A_SPINES_BASE_RTT_NS[8], profiles.BASE_RTT_NS)

    def test_probe_runs_at_each_spine_count(self):
        for spines in profiles.A2A_SPINES:
            stage = f"a2a_spines{spines}_probe_4k"
            runs = profiles.a2a_stage_runs(stage, None, self.tuning)
            self.assertEqual([(r["group"], r["size"], r["condition"], r["arm"], r["seed"]) for r in runs],
                             [("ep64", "256k", "healthy", "uniform_tail", 301),
                              ("ep64", "2m", "healthy", "uniform_tail", 301)])
            self.assertEqual([r["compute_ns"] for r in runs],
                             [profiles.A2A_PROBE_4K_COMPUTE_NS[("ep64", size)] for size in ("256k", "2m")])
            document = profiles.a2a_stage_profile(runs[1], None)
            network = document["network"]
            rtt = profiles.A2A_SPINES_BASE_RTT_NS[spines]
            self.assertEqual((network["spine_count"], network["spine_link_rate"], network["pair_bandwidth"]),
                             (spines, profiles.A2A_SPINES_FIXTURE[spines]["spine_link_rate"], "spines"))
            self.assertEqual(network["switch"], {"profile": "ue", "port_rate_thresholds": True})
            self.assertEqual(network["transport_recovery"]["retransmission_timeout_ns"], 10 * rtt)
            self.assertEqual(network["link_rate"], "400Gbps")
            self.assertNotIn("base_rtt_ns", network)

    def test_calibration_reads_the_measured_gaps(self):
        stage = "a2a_spines256_calibration_4k"
        with self.assertRaisesRegex(ValueError, "provisional: enter the probe's values at 256 spines"):
            profiles.a2a_stage_runs(stage, None, self.tuning)
        with measured_spines(), spines_gaps():
            runs = profiles.a2a_stage_runs(stage, None, self.tuning)
            self.assertEqual(len(runs), 6)
            self.assertEqual({r["seed"] for r in runs}, {101, 102, 103})
            self.assertEqual({r["arm"] for r in runs}, {"graded_per_receiver_mix"})
            balancing = profiles.a2a_stage_profile(runs[0], None)["network"]["load_balancing"]
        # The loss and pool decay intervals of two base RTTs at 256 spines.
        self.assertEqual(balancing["loss_interval_ns"], 2 * 17_404)
        self.assertEqual(balancing["pool_decay_interval_ns"], 2 * 17_404)
        self.assertEqual(balancing["held_sentinel_interval_ns"], profiles.A2A_SENTINEL_INTERVAL_NS)
        self.assertNotIn("path_state", runs[0]["fixture"])
        self.assertEqual(profiles.a2a_cell_name("ep64", "2m", 4096, 256), "ep64-2m-p4096-s256")

    def test_base_rtt_keys_take_whole_base_rtts(self):
        document = profiles.profile("x", 64, "healthy", {"mode": "spray_uniform", "loss_interval_ns": 100})
        with self.assertRaisesRegex(ValueError, "no whole number of base RTTs"):
            profiles.a2a_settle(document, profiles.A2A_SPINES_FIXTURE[32])


class RsetSweep(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_runs(self):
        # 7 settings on the 6 blackhole cells and their 3 healthy pairs, at
        # seed 301, each a2a_twoend_base_4k's reverse set but for its keys.
        runs = profiles.a2a_stage_runs("a2a_rset_sweep_4k", self.calibration, self.tuning)
        self.assertEqual(len(runs), 7 * 9)
        self.assertEqual({r["seed"] for r in runs}, {301})
        self.assertEqual({r["condition"] for r in runs}, {"blackhole_up", "blackhole_down", "healthy"})
        self.assertEqual({(r["group"], r["size"]) for r in runs},
                         {("ep64", "256k"), ("ep64", "decode8"), ("ep64", "2m")})
        base = {r["key"].split("-", 1)[1]: profiles.a2a_stage_profile(r, self.calibration)
                for r in profiles.a2a_stage_runs("a2a_twoend_base_4k", self.calibration, self.tuning)
                if r["arm"] == "reps_hold_auto_tail4_cover_reverse_set"}
        keys = ("ack_route_set_size", "ack_route_set_probe_rtts", "ack_route_set_reply")
        settings = set()
        for r in runs:
            document = profiles.a2a_stage_profile(r, self.calibration)
            balancing = document["network"]["load_balancing"]
            pair = base[r["key"].split("-", 1)[1].replace(r["arm"], "reps_hold_auto_tail4_cover_reverse_set")]
            self.assertEqual({**document, "name": pair["name"],
                              "network": {**document["network"], "load_balancing": {
                                  k: v for k, v in balancing.items() if k not in keys}}}, pair, r["key"])
            settings.add(tuple(balancing.get(k) for k in keys))
        self.assertEqual(settings, {(None, None, None)} | {
            (None if size == 2 else size, None if interval == 1 else interval, "answer")
            for size in (2, 4) for interval in (1, 4, 16)})


class TwoendRset(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = json.loads((here / "a2a_calibration.json").read_text())

    def test_runs(self):
        # 3 arms on a2a_twoend_4k's 18 cells and their 3 healthy pairs, seeds
        # 301-303: a2a_twoend_4k's answer_seq arm as it is, and
        # a2a_twoend_base_4k's reverse set with a sweep setting's keys, none
        # with the overdue repair.
        runs = profiles.a2a_stage_runs("a2a_twoend_rset_4k", self.calibration, self.tuning)
        self.assertEqual(len(runs), 3 * 21 * 3)
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_TWOEND_RSET_ARMS))
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual({(r["group"], r["size"], r["condition"]) for r in runs},
                         set(profiles.a2a_read_cells_4k("a2a_twoend_4k")))
        twoend = {r["key"].split("-", 1)[1]: profiles.a2a_stage_profile(r, self.calibration)
                  for r in profiles.a2a_stage_runs("a2a_twoend_4k", self.calibration, self.tuning)
                  if r["arm"] == "reps_hold_auto_tail4_cover_answer_seq"}
        base = {r["key"].split("-", 1)[1]: profiles.a2a_stage_profile(r, self.calibration)
                for r in profiles.a2a_stage_runs("a2a_twoend_base_4k", self.calibration, self.tuning)
                if r["arm"] == "reps_hold_auto_tail4_cover_reverse_set"}
        keys = ("ack_route_set_size", "ack_route_set_probe_rtts", "ack_route_set_reply")
        settings = {}
        for r in runs:
            document = profiles.a2a_stage_profile(r, self.calibration)
            balancing = document["network"]["load_balancing"]
            self.assertNotIn("overdue_repair", balancing, r["key"])
            key = r["key"].split("-", 1)[1]
            if r["arm"] == "reps_hold_auto_tail4_cover_answer_seq":
                self.assertEqual({**document, "name": twoend[key]["name"]}, twoend[key], r["key"])
                continue
            pair = base[key.replace(r["arm"], "reps_hold_auto_tail4_cover_reverse_set")]
            self.assertEqual({**document, "name": pair["name"],
                              "network": {**document["network"], "load_balancing": {
                                  k: v for k, v in balancing.items() if k not in keys}}}, pair, r["key"])
            settings.setdefault(r["arm"], set()).add(tuple(balancing.get(k) for k in keys))
        self.assertEqual(settings, {"reps_hold_auto_tail4_cover_reverse_set_2x4_answer": {(None, 4, "answer")},
                                    "reps_hold_auto_tail4_cover_reverse_set_2x16_answer": {(None, 16, "answer")}})


class Wide(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        committed = json.loads((here / "a2a_calibration.json").read_text())
        # Each wide-leaf cell takes the 4096-byte cell's thresholds.
        self.calibration = {**committed, "per_receiver": {
            **committed["per_receiver"],
            **{f"{group}-{size}-p4096-w{wide}": committed["per_receiver"][f"{group}-{size}-p4096"]
               for group, size in profiles.A2A_WIDE_PAIRS for wide in profiles.A2A_WIDE}}}

    def test_fixtures(self):
        self.assertEqual(profiles.A2A_WIDE_FIXTURE[8], {**profiles.A2A_FIXTURE_4K, "wide": 8})
        for wide in (32, 128, 256):
            self.assertEqual(profiles.A2A_WIDE_FIXTURE[wide],
                             {**profiles.A2A_FIXTURE_4K, "wide": wide, "buffer_size_mb": 4 * wide, "job_hosts": 8,
                              "background": {"pattern": "leaf_shift", "rate": "350Gbps"},
                              "sparse_hold_trace": True})
        self.assertNotIn("a2a_wide8_validity_4k", profiles.A2A_STAGES)

    @measured_4k()
    def test_eight_is_the_settled_fixture(self):
        # At P = 8 a run's profile is the settled fixture's run of the same
        # arm, cell and seed in a2a_twoend_4k or a2a_ackroute_fwd_4k, but
        # for its name.
        def profiles_of(stage):
            return {r["key"].split("-", 1)[1]: profiles.a2a_stage_profile(r, self.calibration)
                    for r in profiles.a2a_stage_runs(stage, self.calibration, self.tuning)}

        wide = profiles_of("a2a_wide8_4k")
        compared = 0
        for stage in ("a2a_twoend_4k", "a2a_ackroute_fwd_4k", "a2a_twoend_rset_4k"):
            for key, document in profiles_of(stage).items():
                if stage == "a2a_twoend_rset_4k":
                    # The reverse set at the comparison's setting alone.
                    if f"-{profiles.A2A_WIDE_REVERSE_SET}-" not in key:
                        continue
                    key = key.replace(profiles.A2A_WIDE_REVERSE_SET, "reps_hold_auto_tail4_cover_reverse_set")
                if key in wide:
                    self.assertEqual({**wide[key], "name": document["name"]}, document, key)
                    compared += 1
        # reps_hold_auto_tail4_cover and reps_tail of a2a_twoend_4k and
        # reps_oracle_tail_fwd of a2a_ackroute_fwd_4k, on 4 conditions and
        # the healthy pair of 2 sizes at 3 seeds; and
        # reps_hold_auto_tail4_cover_answer_seq of a2a_twoend_4k and
        # a2a_twoend_rset_4k's arm of A2A_WIDE_REVERSE_SET on blackhole up and
        # the healthy pair.
        self.assertEqual(compared, 3 * 5 * 2 * 3 + 2 * 2 * 2 * 3)

    @measured_4k()
    def test_members(self):
        runs = profiles.a2a_stage_runs("a2a_wide32_4k", self.calibration, self.tuning)
        self.assertEqual(len(runs), 402)
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_WIDE_ARMS))
        conditions = set(profiles.A2A_WIDE_CONDITIONS) | {"healthy"}
        for arm in profiles.A2A_WIDE_ARMS:
            self.assertEqual({r["condition"] for r in runs if r["arm"] == arm},
                             {"blackhole_up", "multi_loss1e3", "multi_loss1e2", "healthy"}
                             if arm in ("reps_hold_auto_tail4_cover_answer_seq",
                                        "reps_hold_auto_tail4_cover_reverse_set")
                             else {"multi_loss1e3", "multi_loss1e2", "healthy"}
                             if arm == "reps_hold_auto_tail4_cover_reverse_set_rx_slots8" else conditions, arm)
        self.assertEqual({(r["size"], r["compute_ns"], r["iterations"]) for r in runs},
                         {("256k", 236_000, 40), ("2m", 1_816_000, 12)})
        for r in runs:
            document = profiles.a2a_stage_profile(r, self.calibration)
            network, workload = document["network"], document["workload"]
            self.assertEqual((network["spine_count"], network["hosts_per_leaf"], document["parallelism"]["dp"]),
                             (32, 32, 256))
            self.assertEqual(network["fabric"]["buffer_size_mb"], 128)
            self.assertEqual(network["link_rate"], "400Gbps")
            self.assertNotIn("spine_link_rate", network)
            self.assertEqual(network["transport_recovery"]["retransmission_timeout_ns"], 10 * profiles.BASE_RTT_NS)
            self.assertEqual(workload["group"], {"leaves": list(range(8)), "hosts": 8})
            self.assertEqual(workload["background"], {"pattern": "leaf_shift", "rate": "350Gbps"})
            hold = network["load_balancing"].get("selector") == "reps_hold"
            self.assertEqual(network.get("sparse_hold_trace"), True if hold else None)
            self.assertNotIn("port_counter_interval_ns", network)
            self.assertEqual(network["load_balancing"]["ack_route"],
                             "reverse_set" if r["arm"] in ("reps_hold_auto_tail4_cover_reverse_set",
                                                           "reps_hold_auto_tail4_cover_reverse_set_rx",
                                                           "reps_hold_auto_tail4_cover_reverse_set_rx_slots8")
                             else "spray")
            if r["arm"] == "reps_hold_auto_tail4_cover" and r["size"] == "2m":
                self.assertEqual(network["load_balancing"]["loss_thresholds"],
                                 self.calibration["per_receiver"]["ep64-2m-p4096-w32"]["loss_thresholds"]["24648"])

    @measured_4k()
    def test_validity(self):
        for stage, wide in profiles.A2A_WIDE_VALIDITY_STAGES.items():
            runs = profiles.a2a_stage_runs(stage, None, self.tuning)
            self.assertEqual([(r["size"], r["arm"], r["condition"], r["seed"], r["iterations"], r["compute_ns"])
                              for r in runs],
                             [("256k", "uniform_tail", "healthy", 301, 2, 236_000),
                              ("2m", "uniform_tail", "healthy", 301, 2, 1_816_000)])
            network = profiles.a2a_stage_profile(runs[1], None)["network"]
            self.assertEqual((network["spine_count"], network["port_counter_interval_ns"]), (wide, 50_000))
            self.assertNotIn("sparse_hold_trace", network)

    @measured_4k()
    def test_arms(self):
        settings = profiles.a2a_wide_settings(self.tuning)
        cover = profiles.a2a_twoend_settings(self.tuning)["reps_hold_auto_tail4_cover"]
        self.assertEqual(settings["reps_hold_auto_tail4_cover"], cover)
        self.assertEqual(settings["reps_hold_auto_tail4_cover_rx"],
                         {**cover, "path_seq": True, "receiver_hold": True, "receiver_hold_clean": 1024,
                          "receiver_tail_missing": True, "stray_path_seq": "closed"})
        self.assertEqual(settings["reps_oracle_tail_fwd"],
                         profiles.a2a_ackroute_fwd_settings(self.tuning)["reps_oracle_tail_fwd"])
        self.assertEqual(settings["reps_hold_auto_tail4_cover_answer_seq"],
                         profiles.a2a_twoend_settings(self.tuning)["reps_hold_auto_tail4_cover_answer_seq"])
        self.assertEqual(settings["reps_hold_auto_tail4_cover_reverse_set"],
                         {**cover, "ack_route": "reverse_set", "ack_route_set_probe_rtts": 4,
                          "ack_route_set_reply": "answer"})
        # The two-ended arm and the reverse set with the receiver's hold:
        # each its two arms' keys together.
        self.assertEqual(settings["reps_hold_auto_tail4_cover_rx_answer_seq"],
                         {**settings["reps_hold_auto_tail4_cover_rx"], "answer_path_seq": True})
        self.assertEqual(settings["reps_hold_auto_tail4_cover_rx_answer_seq"],
                         {**settings["reps_hold_auto_tail4_cover_answer_seq"], **profiles.A2A_WIDE_RECEIVER})
        self.assertEqual(settings["reps_hold_auto_tail4_cover_reverse_set_rx"],
                         {**settings["reps_hold_auto_tail4_cover_reverse_set"], **profiles.A2A_WIDE_RECEIVER})
        # The receiver arms with their report a list of 8 spines.
        for arm in ("reps_hold_auto_tail4_cover_rx", "reps_hold_auto_tail4_cover_reverse_set_rx"):
            self.assertEqual(settings[f"{arm}_slots8"], {**settings[arm], "receiver_hold_slots": 8})
        self.assertEqual({arm: setting["ack_route"] for arm, setting in settings.items()},
                         {**{arm: "spray" for arm in profiles.A2A_WIDE_ARMS},
                          "reps_hold_auto_tail4_cover_reverse_set": "reverse_set",
                          "reps_hold_auto_tail4_cover_reverse_set_rx": "reverse_set",
                          "reps_hold_auto_tail4_cover_reverse_set_rx_slots8": "reverse_set"})
        # The comparison reads its own P's calibration cells.
        lacking = {**self.calibration, "per_receiver": {
            k: v for k, v in self.calibration["per_receiver"].items() if not k.endswith("-w128")}}
        with self.assertRaisesRegex(ValueError, r"\['ep64-256k-p4096-w128', 'ep64-2m-p4096-w128'\]"):
            profiles.a2a_stage_runs("a2a_wide128_4k", lacking, self.tuning)

    def test_committed_calibration(self):
        # The committed calibration holds the 8-, 32- and 128-spine wide-leaf
        # cells, so the comparisons at 8, 32 and 128 plan on it alone, and the
        # 256-spine 256 KiB cell, so the comparison at 256 plans its 256 KiB
        # runs under a record filter; its 2 MiB cell is still missing. At 8
        # spines the pass is the settled fixture's, whose cells it repeats but
        # for the runs read.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        for stage in ("a2a_wide8_4k", "a2a_wide32_4k", "a2a_wide128_4k"):
            self.assertEqual(len(profiles.a2a_stage_runs(stage, committed, self.tuning)), 402)
        for group, size in profiles.A2A_WIDE_PAIRS:
            wide, settled = (committed["per_receiver"][f"{group}-{size}-p4096{suffix}"] for suffix in ("-w8", ""))
            self.assertEqual({k: v for k, v in wide.items() if k not in ("runs", "loss_runs")},
                             {k: v for k, v in settled.items() if k not in ("runs", "loss_runs")})
        self.assertEqual(len(profiles.a2a_stage_runs("a2a_wide256_4k", committed, self.tuning,
                                                     "^a2a_wide256_4k-ep64-256k-")), 201)
        with self.assertRaisesRegex(ValueError, re.escape("no calibrated loss thresholds on ['ep64-2m-p4096-w256']")):
            profiles.a2a_stage_runs("a2a_wide256_4k", committed, self.tuning)

    def test_record_filter_needs_the_pairs_it_selects(self):
        # Under a record filter a stage needs the calibrated loss thresholds
        # of the pairs it selects only: the 256-spine comparison's 256 KiB
        # arms and cells, given the -w256 256 KiB cell alone, plan; a 2 MiB
        # run among them refuses, naming that pair.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        calibration = {**committed, "per_receiver": {
            **{k: v for k, v in committed["per_receiver"].items() if not k.endswith("-w256")},
            "ep64-256k-p4096-w256": self.calibration["per_receiver"]["ep64-256k-p4096-w256"]}}
        arms = "reps_hold_auto_tail4_cover(_rx|_reverse_set|_reverse_set_rx)?-s30[123]$"
        two = "reps_hold_auto_tail4_cover(_rx)?-s30[123]$"
        record_filter = (f"^a2a_wide256_4k-ep64-256k-(multi_loss1e3|multi_loss1e2|healthy)-{arms}"
                         f"|^a2a_wide256_4k-ep64-256k-(loss1e2_down|loss1e3_down)-{two}")
        runs = profiles.a2a_stage_runs("a2a_wide256_4k", calibration, self.tuning, record_filter)
        self.assertEqual(len(runs), 48)
        every = profiles.a2a_stage_runs("a2a_wide256_4k", self.calibration, self.tuning)
        self.assertEqual(runs, [r for r in every if re.search(record_filter, r["key"])])
        for run in runs:
            profiles.a2a_stage_profile(run, calibration)
        refusal = re.escape("no calibrated loss thresholds on ['ep64-2m-p4096-w256']")
        with self.assertRaisesRegex(ValueError, refusal):
            profiles.a2a_stage_runs("a2a_wide256_4k", calibration, self.tuning,
                                    record_filter + "|-2m-healthy-reps_hold_auto_tail4_cover-s301$")
        with self.assertRaisesRegex(ValueError, refusal):
            profiles.a2a_stage_runs("a2a_wide256_4k", calibration, self.tuning)

    @measured_4k()
    def test_overdue_repair(self):
        # The stage's one overdue repair, on every arm: none until it is
        # chosen, none under off, reprobe with the limit under a limit.
        with mock.patch.object(profiles, "A2A_WIDE_OVERDUE_REPAIR", None):
            with self.assertRaisesRegex(ValueError, "a2a_wide32_4k's overdue repair is not chosen"):
                profiles.a2a_stage_runs("a2a_wide32_4k", self.calibration, self.tuning)
        # Off, as committed.
        self.assertEqual(profiles.A2A_WIDE_OVERDUE_REPAIR, "off")
        off = profiles.a2a_wide_settings(self.tuning)
        self.assertFalse(any("overdue_repair" in setting for setting in off.values()))
        with mock.patch.object(profiles, "A2A_WIDE_OVERDUE_REPAIR", 3):
            settings = profiles.a2a_wide_settings(self.tuning)
            for arm, setting in settings.items():
                self.assertEqual(setting, {**off[arm], "overdue_repair": "reprobe", "overdue_repair_limit": 3}, arm)
            runs = profiles.a2a_stage_runs("a2a_wide32_4k", self.calibration, self.tuning)
            self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_WIDE_ARMS))
            for r in runs:
                balancing = profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                self.assertEqual((balancing.get("overdue_repair"), balancing.get("overdue_repair_limit")),
                                 ("reprobe", 3), r["key"])
                self.assertNotIn("held_loss_rtts", balancing)

    @measured_4k()
    def test_reverse_set(self):
        # The reverse set's setting the stage names, one of the sweep's: none
        # until it is chosen, its keys on the reverse set alone once it is.
        with mock.patch.object(profiles, "A2A_WIDE_REVERSE_SET", None):
            with self.assertRaisesRegex(ValueError, "a2a_wide32_4k's reverse set is not chosen"):
                profiles.a2a_stage_runs("a2a_wide32_4k", self.calibration, self.tuning)
        # 2x4_answer, as committed: a set of 2 spines (the default, unwritten)
        # probed once per 4 base RTTs, the replies queued as the answers are.
        self.assertEqual(profiles.A2A_WIDE_REVERSE_SET, "reps_hold_auto_tail4_cover_reverse_set_2x4_answer")
        with mock.patch.object(profiles, "A2A_WIDE_REVERSE_SET", "reps_hold_auto_tail4_cover_reverse_set"):
            base = profiles.a2a_wide_settings(self.tuning)
        keys = ("ack_route_set_size", "ack_route_set_probe_rtts", "ack_route_set_reply")
        reverse_set = ("reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover_reverse_set_rx",
                       "reps_hold_auto_tail4_cover_reverse_set_rx_slots8")
        for chosen, expected in (("reps_hold_auto_tail4_cover_reverse_set_2x4_answer", (None, 4, "answer")),
                                 ("reps_hold_auto_tail4_cover_reverse_set_4x4_answer", (4, 4, "answer"))):
            with mock.patch.object(profiles, "A2A_WIDE_REVERSE_SET", chosen):
                settings = profiles.a2a_wide_settings(self.tuning)
                for arm, setting in settings.items():
                    self.assertEqual(setting, {**base[arm], **(profiles.A2A_RSET_SWEEP_ARMS[chosen]
                                                               if arm in reverse_set else {})})
                runs = profiles.a2a_stage_runs("a2a_wide32_4k", self.calibration, self.tuning)
                self.assertEqual(len(runs), 402)
                for r in runs:
                    balancing = profiles.a2a_stage_profile(r, self.calibration)["network"]["load_balancing"]
                    self.assertEqual(tuple(balancing.get(k) for k in keys),
                                     expected if r["arm"] in reverse_set else (None, None, None), r["key"])

    def test_seeds(self):
        # Seeds 304-306 of 5 arms on the 4 multi-fault cells and of S, rx and
        # rx with 8 slots on 256 KiB loss1e2_down and loss1e3_down, with
        # healthy pairs: each
        # run's profile is the comparison's of its arm and cell but for its
        # name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        for wide in (8, 32):
            runs = profiles.a2a_stage_runs(f"a2a_wide{wide}_seeds_4k", committed, self.tuning)
            self.assertEqual(len(runs), 108)
            self.assertEqual({r["seed"] for r in runs}, {304, 305, 306})
            cells = {arm: {(r["size"], r["condition"]) for r in runs if r["arm"] == arm}
                     for arm in profiles.A2A_WIDE_SEEDS_ARMS}
            multi = {(size, condition) for size in ("256k", "2m")
                     for condition in ("multi_loss1e3", "multi_loss1e2", "healthy")}
            down = {("256k", "loss1e2_down"), ("256k", "loss1e3_down")}
            self.assertEqual(cells, {"reps_hold_auto_tail4_cover": multi | down,
                                     "reps_hold_auto_tail4_cover_rx": multi | down,
                                     "reps_hold_auto_tail4_cover_rx_slots8": multi | down,
                                     "reps_hold_auto_tail4_cover_reverse_set": multi,
                                     "reps_hold_auto_tail4_cover_reverse_set_rx": multi})
            comparison = {(r["arm"], r["size"], r["condition"]): profiles.a2a_stage_profile(r, committed)
                          for r in profiles.a2a_stage_runs(f"a2a_wide{wide}_4k", committed, self.tuning)
                          if r["seed"] == 301}
            for r in runs:
                document = profiles.a2a_stage_profile(r, committed)
                pair = comparison[(r["arm"], r["size"], r["condition"])]
                self.assertEqual({**document, "name": pair["name"]}, pair, r["key"])

    def test_multi_fault(self):
        self.assertEqual(profiles.a2a_fault("multi_loss1e2"),
                         [{"model": "gray", "error_rate": 0.01, "start_ns": profiles.DEGRADE_AT_NS,
                           "draw": {"fraction": 0.02, "seed": 1}}])
        self.assertEqual(profiles.a2a_fault("sparse_loss1e2"),
                         [{"model": "gray", "error_rate": 0.01, "start_ns": profiles.DEGRADE_AT_NS,
                           "draw": {"fraction": 0.001, "seed": 1}}])
        for condition in ("multi_blackhole", "multi", "multiloss1e3", "sparse", "sparse_blackhole"):
            with self.assertRaisesRegex(ValueError, "unknown condition"):
                profiles.a2a_fault(condition)
        self.assertTrue(analyze.multi_condition("multi_loss1e3"))
        self.assertTrue(analyze.multi_condition("sparse_loss1e2"))
        self.assertFalse(analyze.multi_condition("loss1e3_up"))

    def test_widen(self):
        # Seeds 307-312 of rx with 8 slots, spread and widened at 2, on 256
        # KiB loss1e2_down, the sparse draw and multi_loss1e3, and of the two
        # spread arms on multi_loss1e3, each with its healthy pair: each
        # run's profile is a2a_wide256_spread_4k's of its arm (the widened
        # one's that of rx with 8 slots and spread, with receiver_hold_widen
        # 2) and cell, but for its name; multi_loss1e3's, that of
        # loss1e2_down but for its faults.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_widen_4k", committed, self.tuning)
        self.assertEqual(len(runs), 48)
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        widened = "reps_hold_auto_tail4_cover_rx_slots8_spread64_widen2"
        cells = {arm: {r["condition"] for r in runs if r["arm"] == arm} for arm in profiles.A2A_WIDE_WIDEN_ARMS}
        self.assertEqual(cells, {"reps_hold_auto_tail4_cover_spread64": {"multi_loss1e3", "healthy"},
                                 "reps_hold_auto_tail4_cover_rx_slots8_spread64": {"multi_loss1e3", "healthy"},
                                 widened: {"loss1e2_down", "sparse_loss1e2", "multi_loss1e3", "healthy"}})
        spread = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                  for r in profiles.a2a_stage_runs("a2a_wide256_spread_4k", committed, self.tuning)
                  if r["seed"] >= 307}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            base = "reps_hold_auto_tail4_cover_rx_slots8_spread64" if r["arm"] == widened else r["arm"]
            pair = spread[(base, "loss1e2_down" if r["condition"] == "multi_loss1e3" else r["condition"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"],
                         **({"receiver_hold_widen": 2} if r["arm"] == widened else {})}
            expected = {**pair, "name": document["name"], "network": {**pair["network"], "load_balancing": balancing}}
            if r["condition"] == "multi_loss1e3":
                unit = pair["network"]["link_failures"][0]["loss_unit"]
                expected["network"]["link_failures"] = [{**failure, "loss_unit": unit}
                                                        for failure in profiles.a2a_fault("multi_loss1e3")]
            self.assertEqual(document, expected, r["key"])

    def test_rauto(self):
        # Seeds 307-309 of the widened receiver arm with
        # receiver_hold_auto_leaves 1 on 256 KiB sparse_loss1e2 and
        # multi_loss1e3, each with its healthy pair: each run's profile is
        # the widened arm's of a2a_wide256_widen_4k on healthy, with the
        # cell's faults and receiver_hold_auto_leaves 1, but for its name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_rauto_4k", committed, self.tuning)
        self.assertEqual(len(runs), 9)
        self.assertEqual({r["seed"] for r in runs}, {307, 308, 309})
        widened = "reps_hold_auto_tail4_cover_rx_slots8_spread64_widen2"
        rauto = f"{widened}_rauto1"
        self.assertEqual({r["arm"] for r in runs}, {rauto})
        self.assertEqual({r["condition"] for r in runs}, {"sparse_loss1e2", "multi_loss1e3", "healthy"})
        healthy = {(r["arm"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                   for r in profiles.a2a_stage_runs("a2a_wide256_widen_4k", committed, self.tuning)
                   if r["condition"] == "healthy"}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            pair = healthy[(widened if r["arm"] == rauto else r["arm"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"],
                         **({"receiver_hold_auto_leaves": 1} if r["arm"] == rauto else {})}
            expected = {**pair, "name": document["name"], "network": {**pair["network"], "load_balancing": balancing}}
            if r["condition"] != "healthy":
                expected["network"]["link_failures"] = [{**failure, "loss_unit": "byte"}
                                                        for failure in profiles.a2a_fault(r["condition"])]
            self.assertEqual(document, expected, r["key"])

    def test_od(self):
        # Seeds 307-312 of a2a_wide256_widen_4k's three arms with the
        # overdue repair on 256 KiB multi_loss1e3, each with its healthy
        # pair: each run's profile is its arm's of a2a_wide256_widen_4k on
        # the same cell and seed with overdue_repair reprobe and
        # overdue_repair_limit 3, but for its name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_od_4k", committed, self.tuning)
        self.assertEqual(len(runs), 36)
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        self.assertEqual({r["arm"] for r in runs}, {f"{arm}_od" for arm in profiles.A2A_WIDE_WIDEN_ARMS})
        self.assertEqual({r["condition"] for r in runs}, {"multi_loss1e3", "healthy"})
        widen = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for r in profiles.a2a_stage_runs("a2a_wide256_widen_4k", committed, self.tuning)}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            pair = widen[(r["arm"].removesuffix("_od"), r["condition"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"], "overdue_repair": "reprobe", "overdue_repair_limit": 3}
            expected = {**pair, "name": document["name"], "network": {**pair["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, r["key"])

    def test_od2(self):
        # a2a_wide256_od_4k's arms on 256 KiB loss1e2_down and
        # sparse_loss1e2, seeds 307-312: each faulted run's profile is its
        # base arm's of a2a_wide256_spread_4k or a2a_wide256_widen_4k on the
        # same cell and seed with overdue_repair reprobe and limit 3, and
        # each healthy run's is a2a_wide256_od_4k's, but for its name; the
        # stage builds at a2a_wide256_od_4k's commit.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_od2_4k", committed, self.tuning)
        self.assertEqual(len(runs), 54)
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_WIDE_OD_ARMS))
        self.assertEqual({r["condition"] for r in runs}, {"loss1e2_down", "sparse_loss1e2", "healthy"})
        pairs = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for stage in ("a2a_wide256_spread_4k", "a2a_wide256_widen_4k")
                 for r in profiles.a2a_stage_runs(stage, committed, self.tuning)}
        od = {(r["arm"], r["seed"]): profiles.a2a_stage_profile(r, committed)
              for r in profiles.a2a_stage_runs("a2a_wide256_od_4k", committed, self.tuning) if r["condition"] == "healthy"}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            if r["condition"] == "healthy":
                self.assertEqual(document, {**od[(r["arm"], r["seed"])], "name": document["name"]}, r["key"])
                continue
            pair = pairs[(r["arm"].removesuffix("_od"), r["condition"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"], "overdue_repair": "reprobe", "overdue_repair_limit": 3}
            expected = {**pair, "name": document["name"], "network": {**pair["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, r["key"])
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_od2_4k"],
                         profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_od_4k"])

    def test_rauto_od(self):
        # The receiver arm that holds a spine host-wide at its first loss,
        # with the overdue repair, on a2a_wide256_od_4k's and
        # a2a_wide256_od2_4k's cells, seeds 307-312, each with its healthy
        # pair: each run's profile is the widened receiver arm's _od run of
        # those stages on the same cell and seed with
        # receiver_hold_auto_leaves 1, but for its name, at their commit.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_rauto_od_4k", committed, self.tuning)
        self.assertEqual(len(runs), 24)
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        self.assertEqual({r["arm"] for r in runs}, {profiles.A2A_WIDE_RAUTO_OD_ARM})
        self.assertEqual({r["condition"] for r in runs},
                         {"multi_loss1e3", "loss1e2_down", "sparse_loss1e2", "healthy"})
        widened = f"{profiles.A2A_WIDE_WIDEN_ARM}_od"
        pairs = {(r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for stage in ("a2a_wide256_od_4k", "a2a_wide256_od2_4k")
                 for r in profiles.a2a_stage_runs(stage, committed, self.tuning) if r["arm"] == widened}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            pair = pairs[(r["condition"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"], "receiver_hold_auto_leaves": 1}
            expected = {**pair, "name": document["name"], "network": {**pair["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, r["key"])
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_rauto_od_4k"],
                         profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_od_4k"])

    def test_rack_pilot(self):
        # The sender-only spread arm with the overdue repair at 32 spines on
        # 256 KiB multi_loss1e3, seeds 307-309, each with its healthy pair,
        # and the same with rack_reorder_ns 3081: the _rack run's profile is
        # its _od twin's with that key, but for its name, at the default
        # commit.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide32_rack_4k", committed, self.tuning)
        self.assertEqual(len(runs), 12)
        self.assertEqual({r["seed"] for r in runs}, {307, 308, 309})
        self.assertEqual({r["condition"] for r in runs}, {"multi_loss1e3", "healthy"})
        od, rack = profiles.A2A_WIDE_RACK_PILOT_ARMS
        self.assertEqual(od, "reps_hold_auto_tail4_cover_spread64_od")
        documents = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed) for r in runs}
        for (arm, condition, seed), document in documents.items():
            if arm != rack:
                continue
            twin = documents[(od, condition, seed)]
            balancing = {**twin["network"]["load_balancing"], "rack_reorder_ns": 3081}
            expected = {**twin, "name": document["name"], "network": {**twin["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, (arm, condition, seed))
            self.assertEqual(document["network"]["spine_count"], 32)
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide32_rack_4k"],
                         "e5b9d1011345dfd0a38d0eaaf4de79f7f1c25ba3")

    def test_rack2_pilot(self):
        # a2a_wide32_rack_4k's S_spread_od and three arms of the rule, on its
        # cells and seeds, each with its healthy pair: each _rack arm's
        # profile is the _od twin's with its keys, but for its name, at the
        # default commit.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide32_rack2_4k", committed, self.tuning)
        self.assertEqual(len(runs), 24)
        self.assertEqual({r["seed"] for r in runs}, {307, 308, 309})
        self.assertEqual({r["condition"] for r in runs}, {"multi_loss1e3", "healthy"})
        od = profiles.A2A_WIDE_RACK2_ARMS[0]
        keys = {f"{od}_rackrtt": {"rack_reorder_ns": 12324},
                f"{od}_rackdefer": {"rack_reorder_ns": 3081, "rack_defer_loss": True},
                f"{od}_rackrttdefer": {"rack_reorder_ns": 12324, "rack_defer_loss": True}}
        self.assertEqual(set(profiles.A2A_WIDE_RACK2_ARMS), {od, *keys})
        pilot = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for r in profiles.a2a_stage_runs("a2a_wide32_rack_4k", committed, self.tuning)}
        documents = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed) for r in runs}
        for (arm, condition, seed), document in documents.items():
            twin = documents[(od, condition, seed)]
            if arm == od:
                self.assertEqual(document, {**pilot[(od, condition, seed)], "name": document["name"]})
                continue
            balancing = {**twin["network"]["load_balancing"], **keys[arm]}
            expected = {**twin, "name": document["name"], "network": {**twin["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, (arm, condition, seed))
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide32_rack2_4k"],
                         "6268ca702026b7af170dfea97c4c2464500eb1f3")

    def test_rack_wide256(self):
        # The second pilot's _rackdefer rule at 256 spines on the sender-only
        # spread arm, the widened receiver arm and the first-loss receiver
        # arm, on three cells and healthy, seeds 307-312, and the sender-only
        # arm's healthy runs without the rule: each _rackdefer run's profile
        # is its _od arm's run of a2a_wide256_od_4k, a2a_wide256_od2_4k or
        # a2a_wide256_rauto_od_4k with the rule's keys, but for its name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_rack_4k", committed, self.tuning)
        self.assertEqual(len(runs), 78)
        od, *racks = profiles.A2A_WIDE_RACK_ARMS
        self.assertEqual({r["condition"] for r in runs if r["arm"] == od}, {"healthy"})
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        twins = {}
        for stage in ("a2a_wide256_od_4k", "a2a_wide256_od2_4k", "a2a_wide256_rauto_od_4k"):
            for r in profiles.a2a_stage_runs(stage, committed, self.tuning):
                twins[(r["arm"], r["condition"], r["seed"])] = profiles.a2a_stage_profile(r, committed)
        keys = {"rack_reorder_ns": 3081, "rack_defer_loss": True}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            base = r["arm"].removesuffix("_rackdefer")
            twin = twins[(base, r["condition"], r["seed"])]
            balancing = {**twin["network"]["load_balancing"], **(keys if r["arm"] in racks else {})}
            expected = {**twin, "name": document["name"], "network": {**twin["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, r["key"])
        self.assertEqual(set(racks), {"reps_hold_auto_tail4_cover_spread64_od_rackdefer",
                                      "reps_hold_auto_tail4_cover_rx_slots8_spread64_widen2_od_rackdefer",
                                      "reps_hold_auto_tail4_cover_rx_slots8_spread64_widen2_rauto1_od_rackdefer"})
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_rack_4k"],
                         "6268ca702026b7af170dfea97c4c2464500eb1f3")

    def test_mrc(self):
        # a2a_wide256_rack_4k's three _rackdefer arms under selector mrc_hold
        # on loss1e2_down, sparse_loss1e2 and healthy, seeds 307-312: each run's
        # profile is its REPS arm's run of a2a_wide256_rack_4k but for its name
        # and the selector, at a2a_wide256_rack_4k's commit.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_mrc_4k", committed, self.tuning)
        self.assertEqual(len(runs), 54)
        self.assertEqual({r["condition"] for r in runs}, {"loss1e2_down", "sparse_loss1e2", "healthy"})
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        twins = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for r in profiles.a2a_stage_runs("a2a_wide256_rack_4k", committed, self.tuning)}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            twin = twins[(profiles.A2A_WIDE_MRC_ARMS[r["arm"]], r["condition"], r["seed"])]
            balancing = {**twin["network"]["load_balancing"], "selector": "mrc_hold"}
            expected = {**twin, "name": document["name"], "network": {**twin["network"], "load_balancing": balancing}}
            self.assertEqual(document, expected, r["key"])
        self.assertEqual(set(profiles.A2A_WIDE_MRC_ARMS),
                         {"mrc_hold_auto_tail4_cover_spread64_od_rackdefer",
                          "mrc_hold_auto_tail4_cover_rx_slots8_spread64_widen2_od_rackdefer",
                          "mrc_hold_auto_tail4_cover_rx_slots8_spread64_widen2_rauto1_od_rackdefer"})
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_mrc_4k"],
                         profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_rack_4k"])

    def test_themis256(self):
        # a2a_wide32_themis4_4k's _psn and _t2fl arms at 256 spines on
        # a2a_wide256_mrc_4k's cells and seeds: each run's profile is its
        # sender-only MRC twin's but for its name, its load balancing, which is
        # the 32-spine arm's, and no loss hold trace; it builds at
        # incast/asteria.ref.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_themis_4k", committed, self.tuning)
        self.assertEqual(len(runs), 36)
        self.assertEqual({r["arm"] for r in runs}, {"themis_psn_od_rackdefer", "themis_t2fl_od_rackdefer"})
        self.assertEqual({r["condition"] for r in runs}, {"loss1e2_down", "sparse_loss1e2", "healthy"})
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        mrc = {(r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
               for r in profiles.a2a_stage_runs("a2a_wide256_mrc_4k", committed, self.tuning)
               if r["arm"] == "mrc_hold_auto_tail4_cover_spread64_od_rackdefer"}
        small = {r["arm"]: profiles.a2a_stage_profile(r, committed)["network"]["load_balancing"]
                 for r in profiles.a2a_stage_runs("a2a_wide32_themis4_4k", committed, self.tuning)}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            twin = mrc[(r["condition"], r["seed"])]
            network = {k: v for k, v in twin["network"].items() if k != "sparse_hold_trace"}
            expected = {**twin, "name": document["name"], "network": {**network, "load_balancing": small[r["arm"]]}}
            self.assertEqual(document, expected, r["key"])
            self.assertEqual(document["network"]["spine_count"], 256)
        self.assertNotIn("a2a_wide256_themis_4k", profiles.A2A_STAGE_ASTERIA_REF)

    def test_matrix(self):
        # The same arms, cells and seeds at every P. A baseline's profile is
        # its sender-only MRC twin's but for its name and its load balancing,
        # which is its selector with the twin's transport keys and its own
        # keys, no loss hold and no hold trace; the MRC arms at 256 spines are
        # a2a_wide256_mrc_4k's but for their names. Every stage builds at the
        # commit adding mrc_spine and ue_aware_spine.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        mrc = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
               for r in profiles.a2a_stage_runs("a2a_wide256_mrc_4k", committed, self.tuning)}
        sender = profiles.A2A_WIDE_MRC_ARMS and next(iter(profiles.A2A_WIDE_MRC_ARMS))
        selectors = {"uniform_od_rackdefer": None, "reps_spine_od_rackdefer": "reps_spine",
                     "mrc_spine_od_rackdefer": "mrc_spine", "ue_aware_spine_od_rackdefer": "ue_aware_spine",
                     "reps_oracle_fwd_od_rackdefer": "reps_oracle"}
        own = {"reps_oracle_fwd_od_rackdefer": {"oracle_hops": "forward", "grade_cost_source": "switch_queue"}}
        for stage, wide in profiles.A2A_MATRIX_STAGES.items():
            if stage == "a2a_wide64_matrix_4k" and "ep64-256k-p4096-w64" not in committed["per_receiver"]:
                continue
            runs = profiles.a2a_stage_runs(stage, committed, self.tuning)
            self.assertEqual(len(runs), 144, stage)
            self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_MATRIX_ARMS))
            self.assertEqual({r["condition"] for r in runs}, {"loss1e2_down", "sparse_loss1e2", "healthy"})
            self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
            documents = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                         for r in runs}
            for (arm, condition, seed), document in documents.items():
                self.assertEqual(document["network"]["spine_count"], wide)
                twin = documents[(sender, condition, seed)]
                if arm in selectors:
                    balancing = document["network"]["load_balancing"]
                    held = twin["network"]["load_balancing"]
                    transport = {key: held[key] for key in (*profiles.A2A_THEMIS_TRANSPORT_KEYS, "path_state")}
                    expected = {"mode": "spray_uniform", **transport, **own.get(arm, {})}
                    if selectors[arm]:
                        expected["selector"] = selectors[arm]
                    self.assertEqual(balancing, expected, arm)
                    network = {k: v for k, v in twin["network"].items() if k != "sparse_hold_trace"}
                    self.assertEqual(document, {**twin, "name": document["name"],
                                                "network": {**network, "load_balancing": balancing}}, arm)
                elif wide == 256:
                    self.assertEqual(document, {**mrc[(arm, condition, seed)], "name": document["name"]}, arm)
            self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF[stage], "d68572320c38133238019796d6c675d0022532d0")

    def test_downlinks(self):
        # Four of the 256-spine matrix's arms on 16 downlinks at 1e-2, each
        # run's profile the matrix twin's but for its name and its faults, so
        # the matrix's healthy runs are this cell's healthy pairs.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        matrix = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                  for r in profiles.a2a_stage_runs("a2a_wide256_matrix_4k", committed, self.tuning)}
        runs = profiles.a2a_stage_runs("a2a_wide256_down16_4k", committed, self.tuning)
        self.assertEqual(len(runs), 48)
        self.assertEqual({r["condition"] for r in runs}, {"down16_loss1e2", "healthy"})
        self.assertEqual({r["arm"] for r in runs}, {"mrc_hold_auto_tail4_cover_spread64_od_rackdefer",
                                                    "mrc_hold_auto_tail4_cover_rx_slots8_spread64_widen2_od_rackdefer",
                                                    "mrc_spine_od_rackdefer", "reps_spine_od_rackdefer"})
        self.assertEqual({r["seed"] for r in runs}, set(range(307, 313)))
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            twin = matrix[(r["arm"], "loss1e2_down" if r["condition"] != "healthy" else "healthy", r["seed"])]
            faults = document["network"]["link_failures"]
            if r["condition"] == "healthy":
                self.assertEqual(faults, [])
            else:
                self.assertEqual(len({(f["leaf"], f["spine"]) for f in faults}), 16)
                self.assertEqual({f["leaf"] for f in faults}, set(range(8)))
                self.assertTrue(all(f["direction"] == "down" and f["error_rate"] == 1e-2 and f["spine"] < 32
                                    and f["start_ns"] == profiles.DEGRADE_AT_NS for f in faults))
            self.assertEqual(document, {**twin, "name": document["name"],
                                        "network": {**twin["network"], "link_failures": faults}}, r["key"])
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_down16_4k"],
                         profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_matrix_4k"])

    def test_fair(self):
        # One spraying rule for every arm: the Themis arms are
        # a2a_wide32_themis4_4k's, and each loss-hold arm's profile is its
        # REPS twin's but for its name and its load balancing, which is the
        # twin's under selector themis_hold without REPS's own keys, with the
        # Themis keys of _psn or _t2fl; it builds at the commit adding
        # themis_hold.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide32_fair_4k", committed, self.tuning)
        self.assertEqual(len(runs), 45)
        self.assertEqual({r["condition"] for r in runs}, {"multi_loss1e3", "loss1e2_down", "healthy"})
        self.assertEqual({r["seed"] for r in runs}, {307, 308, 309})
        documents = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed) for r in runs}
        themis4 = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                   for r in profiles.a2a_stage_runs("a2a_wide32_themis4_4k", committed, self.tuning)}
        reps = {(r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                for r in profiles.a2a_stage_runs("a2a_wide32_themis_4k", committed, self.tuning)
                if r["arm"] == profiles.A2A_WIDE_RACK_ARMS[1]}
        receiver = {key: value for key, value in
                    profiles.a2a_wide_rack_settings(self.tuning)[profiles.A2A_WIDE_RACK_ARMS[2]].items()
                    if key not in profiles.a2a_wide_rack_settings(self.tuning)[profiles.A2A_WIDE_RACK_ARMS[1]]}
        self.assertIn("receiver_hold", receiver)
        t2fl = {"themis_ooo_threshold": 2, "themis_stash_min_ns": 6162, "themis_leaf_avoidance": True}
        added = {"themis_hold_auto_tail4_cover_spread64_od_rackdefer": {"themis_ooo_threshold": 0},
                 "themis_hold_auto_tail4_cover_rx_slots8_spread64_widen2_od_rackdefer":
                     {"themis_ooo_threshold": 0, **receiver},
                 "themis_hold_auto_tail4_cover_spread64_t2fl_od_rackdefer": t2fl}
        for (arm, condition, seed), document in documents.items():
            if arm in ("themis_psn_od_rackdefer", "themis_t2fl_od_rackdefer"):
                expected = {**themis4[(arm, condition, seed)], "name": document["name"]}
                self.assertEqual(document, expected, arm)
                continue
            twin = reps[(condition, seed)]
            balancing = {key: value for key, value in twin["network"]["load_balancing"].items()
                         if key not in ("buffer_size", "freezing_timeout_ns")}
            expected = {**twin, "name": document["name"],
                        "network": {**twin["network"], "load_balancing": {**balancing, "selector": "themis_hold",
                                                                          **added[arm]}}}
            self.assertEqual(document, expected, arm)
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide32_fair_4k"],
                         "db37884ce4cf1b6999a95e40eb434988236cdd30")

    def test_themis_pilot(self):
        # The sender-only _rackdefer arm and three Themis-style arms at 32
        # spines: each Themis arm's load balancing is selector themis with the
        # _rackdefer arm's transport keys and its own threshold, no loss hold
        # key, and no sparse hold trace (no loss trace is written).
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide32_themis_4k", committed, self.tuning)
        self.assertEqual(len(runs), 36)
        self.assertEqual({r["condition"] for r in runs}, {"multi_loss1e3", "loss1e2_down", "healthy"})
        documents = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed) for r in runs}
        rack = documents[(profiles.A2A_WIDE_RACK_ARMS[1], "healthy", 307)]["network"]["load_balancing"]
        thresholds = {"themis_psn_od_rackdefer": 0, "themis_t5_od_rackdefer": 5, "themis_paper_od_rackdefer": None}
        for (arm, condition, seed), document in documents.items():
            if arm not in thresholds:
                continue
            balancing = document["network"]["load_balancing"]
            expected = {"mode": "spray_uniform", "selector": "themis", "path_state": "connection",
                        **{key: rack[key] for key in profiles.A2A_THEMIS_TRANSPORT_KEYS}}
            if thresholds[arm] is not None:
                expected["themis_ooo_threshold"] = thresholds[arm]
            self.assertEqual(balancing, expected, (arm, condition, seed))
            self.assertNotIn("sparse_hold_trace", document["network"])
            self.assertEqual(document["network"]["spine_count"], 32)
        self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF["a2a_wide32_themis_4k"],
                         "db1e2d6bddf9df39709b289baf552d5113376eab")

    def test_themis2(self):
        # The _psn arm and floored threshold arms at 32 spines per stage: each
        # run's profile is its a2a_wide32_themis_4k _psn twin's, with the
        # floored arm's keys, and the same seeds and cells. The read stages
        # stay at the commits they built; a2a_wide32_themis4_4k builds at
        # incast/asteria.ref.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        pilot = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for r in profiles.a2a_stage_runs("a2a_wide32_themis_4k", committed, self.tuning)}
        t2f = {"themis_ooo_threshold": 2, "themis_stash_min_ns": 6162}
        floored = {"a2a_wide32_themis2_4k": {"themis_t5f_od_rackdefer": {"themis_ooo_threshold": 5,
                                                                         "themis_stash_min_ns": 24648}},
                   "a2a_wide32_themis3_4k": {"themis_t2f_od_rackdefer": t2f},
                   "a2a_wide32_themis4_4k": {"themis_t2f_od_rackdefer": t2f,
                                             "themis_t2fl_od_rackdefer": {**t2f, "themis_leaf_avoidance": True}}}
        refs = {"a2a_wide32_themis2_4k": "973dcc9f26ac0139e8e1bfed9bb6a423e443fe43",
                "a2a_wide32_themis3_4k": "f13dafaecc651b15af7f297c40df142299adb6d7"}
        for stage, arms in floored.items():
            runs = profiles.a2a_stage_runs(stage, committed, self.tuning)
            self.assertEqual(len(runs), 9 * (1 + len(arms)))
            self.assertEqual({r["arm"] for r in runs}, {"themis_psn_od_rackdefer", *arms})
            for run in runs:
                document = profiles.a2a_stage_profile(run, committed)
                twin = pilot[("themis_psn_od_rackdefer", run["condition"], run["seed"])]
                expected = {**twin["network"]["load_balancing"], **arms.get(run["arm"], {})}
                self.assertEqual(document["network"]["load_balancing"], expected, (stage, run["arm"]))
                self.assertEqual({k: v for k, v in document.items() if k not in ("name", "network")},
                                 {k: v for k, v in twin.items() if k not in ("name", "network")}, (stage, run["arm"]))
            self.assertEqual(profiles.A2A_STAGE_ASTERIA_REF.get(stage), refs.get(stage), stage)

    def test_uid(self):
        # The healthy runs of seeds 309 and 311 of the receiver arm and its
        # _od arm: each run's profile is the same arm's run of
        # a2a_wide256_widen_4k or a2a_wide256_od_4k, but for its name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide256_uid_4k", committed, self.tuning)
        self.assertEqual(sorted((r["arm"], r["condition"], r["seed"]) for r in runs),
                         sorted((arm, "healthy", seed) for arm in ("reps_hold_auto_tail4_cover_rx_slots8_spread64",
                                                                   "reps_hold_auto_tail4_cover_rx_slots8_spread64_od")
                                for seed in (309, 311)))
        pairs = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                 for stage in ("a2a_wide256_widen_4k", "a2a_wide256_od_4k")
                 for r in profiles.a2a_stage_runs(stage, committed, self.tuning)}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            pair = pairs[(r["arm"], r["condition"], r["seed"])]
            self.assertEqual(document, {**pair, "name": document["name"]}, r["key"])
        self.assertNotIn("a2a_wide256_uid_4k", profiles.A2A_STAGE_ASTERIA_REF)

    def test_stage_asteria_ref(self):
        # profiles.py write leaves a pinned stage's commit in its profile
        # directory, for the workflow to build; an unpinned stage leaves
        # none and builds at incast/asteria.ref.
        here = Path(profiles.__file__).parent
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for stage in ("a2a_wide256_od_4k", "a2a_wide256_widen_4k"):
                subprocess.run(
                    [sys.executable, str(here / "profiles.py"), "write", "--out", str(root / stage), "--stage", stage,
                     "--filter=-s307$", "--a2a-calibration", str(here / "a2a_calibration.json"),
                     "--a2a-tuning", str(here / "a2a_tuning.json")],
                    check=True, stdout=subprocess.DEVNULL,
                )
            self.assertEqual((root / "a2a_wide256_od_4k" / "asteria.ref").read_text(),
                             profiles.A2A_STAGE_ASTERIA_REF["a2a_wide256_od_4k"] + "\n")
            self.assertFalse((root / "a2a_wide256_widen_4k" / "asteria.ref").exists())
        self.assertTrue(set(profiles.A2A_STAGE_ASTERIA_REF) <= set(profiles.A2A_STAGES))
        self.assertTrue(all(len(ref) == 40 for ref in profiles.A2A_STAGE_ASTERIA_REF.values()))

    def test_spread_2m(self):
        # Seeds 301-303 of the two spread arms at 128 spines on 2 MiB
        # loss1e2_down, loss1e3_down and the two multi-fault draws, each with its healthy
        # pair: each run's profile is a2a_wide128_4k's of its base arm and
        # cell with held_sentinel_spread 64, but for its name.
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        runs = profiles.a2a_stage_runs("a2a_wide128_spread2m_4k", committed, self.tuning)
        self.assertEqual(len(runs), 30)
        self.assertEqual({r["seed"] for r in runs}, {301, 302, 303})
        self.assertEqual({(r["size"], r["condition"]) for r in runs},
                         {("2m", c) for c in ("loss1e2_down", "loss1e3_down", "multi_loss1e2", "multi_loss1e3",
                                              "healthy")})
        comparison = {(r["arm"], r["condition"], r["seed"]): profiles.a2a_stage_profile(r, committed)
                      for r in profiles.a2a_stage_runs("a2a_wide128_4k", committed, self.tuning)
                      if r["size"] == "2m"}
        for r in runs:
            document = profiles.a2a_stage_profile(r, committed)
            pair = comparison[(r["arm"].removesuffix("_spread64"), r["condition"], r["seed"])]
            balancing = {**pair["network"]["load_balancing"], "held_sentinel_spread": 64}
            self.assertEqual(document, {**pair, "name": document["name"],
                                        "network": {**pair["network"], "load_balancing": balancing}}, r["key"])

    def test_spread(self):
        # Seeds 301-312 of S and rx with 8 slots, each with a sentinel
        # spread of 64, on 256 KiB loss1e2_down and the sparse draw with
        # their healthy pair: each run's profile is the comparison's of its
        # base arm and cell, or of loss1e2_down's but for its faults on the
        # sparse draw, with held_sentinel_spread 64, but for its name (read
        # at 32 spines).
        committed = json.loads((Path(profiles.__file__).parent / "a2a_calibration.json").read_text())
        for wide in (32, 256):
            runs = profiles.a2a_stage_runs(f"a2a_wide{wide}_spread_4k", committed, self.tuning)
            self.assertEqual(len(runs), 72)
            self.assertEqual({r["seed"] for r in runs}, set(range(301, 313)))
            self.assertEqual({r["arm"] for r in runs}, {"reps_hold_auto_tail4_cover_spread64",
                                                        "reps_hold_auto_tail4_cover_rx_slots8_spread64"})
            self.assertEqual({(r["size"], r["condition"]) for r in runs},
                             {("256k", "loss1e2_down"), ("256k", "sparse_loss1e2"), ("256k", "healthy")})
            # a2a_wide256_4k plans only with its 2 MiB pair calibrated.
            if wide == 256:
                continue
            comparison = {(r["arm"], r["condition"]): profiles.a2a_stage_profile(r, committed)
                          for r in profiles.a2a_stage_runs(f"a2a_wide{wide}_4k", committed, self.tuning)
                          if r["seed"] == 301 and r["size"] == "256k"}
            for r in runs:
                document = profiles.a2a_stage_profile(r, committed)
                base = r["arm"].removesuffix("_spread64")
                pair = comparison[(base, "loss1e2_down" if r["condition"] == "sparse_loss1e2" else r["condition"])]
                balancing = {**pair["network"]["load_balancing"], "held_sentinel_spread": 64}
                expected = {**pair, "name": document["name"],
                            "network": {**pair["network"], "load_balancing": balancing}}
                if r["condition"] == "sparse_loss1e2":
                    unit = pair["network"]["link_failures"][0]["loss_unit"]
                    expected["network"]["link_failures"] = [{**failure, "loss_unit": unit}
                                                            for failure in profiles.a2a_fault("sparse_loss1e2")]
                self.assertEqual(document, expected, r["key"])

    def test_reactions_read_the_runs_fabric(self):
        # 32 hosts per leaf and 32 spines: host 20 is on leaf 0, host 40 on
        # leaf 1, and only a share under 1/64 reads as the reaction, 0.03 not.
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            (run_dir / "ns3").mkdir()
            (run_dir / "manifest.json").write_text(json.dumps(
                {"ranks": 256, "physical_topology": {"hosts_per_leaf": 32, "spine_count": 32}, "link_failures": []}))
            interval = 2 * profiles.BASE_RTT_NS
            onset = profiles.DEGRADE_AT_NS
            first = onset // interval + 1
            rows = [f"{(first + 1) * interval},0,1,3,100,0,3", f"{(first + 1) * interval},40,0,3,100,0,3",
                    f"{(first + 2) * interval},0,1,3,3,0,3", f"{(first + 2) * interval},20,1,4,97,0,3",
                    f"{(first + 3) * interval},0,1,3,1,0,3", f"{(first + 3) * interval},20,1,4,99,0,3"]
            (run_dir / "ns3" / "spine_losses.csv").write_text(
                "time_ns,rank,destination_leaf,spine,sent,lost,grade\n" + "\n".join(rows) + "\n")
            self.assertEqual(analyze.run_fabric(run_dir), (32, 32))
            reaction = analyze.reaction(run_dir, "loss1e3_up", interval)
            self.assertEqual(reaction, {"reaction_ns": (first + 3) * interval - onset, "share_basis": "remote"})
            self.assertEqual(analyze.reaction(run_dir, "multi_loss1e3", interval),
                             {"reaction_ns": None, "share_basis": None})
        self.assertEqual(analyze.run_fabric(Path(directory)), (8, 8))


class Overdue(unittest.TestCase):
    def test_arms_are_the_key_off_arms_with_it(self):
        here = Path(profiles.__file__).parent
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        calibration = json.loads((here / "a2a_calibration.json").read_text())
        settings = profiles.a2a_overdue_settings(tuning)
        off = {**profiles.a2a_twoend_settings(tuning), **profiles.a2a_twoend_base_settings(tuning),
               **profiles.a2a_twoend_retract_settings(tuning)}
        for arm, of in profiles.A2A_OVERDUE_ARMS.items():
            self.assertEqual(settings[arm], {**off[of], **profiles.A2A_OVERDUE})
        self.assertEqual(analyze.A2A_OVERDUE_ARMS, profiles.A2A_OVERDUE_ARMS)
        runs = profiles.a2a_stage_runs("a2a_overdue_4k", calibration, tuning)
        twoend = profiles.a2a_stage_runs("a2a_twoend_4k", calibration, tuning)
        self.assertEqual({r["key"].split("-", 1)[1].rsplit("-", 2)[0] for r in runs},
                         {r["key"].split("-", 1)[1].rsplit("-", 2)[0] for r in twoend})
        # Each run's profile is its key-off arm's run's, with the key.
        twoend_profiles = {r["key"]: profiles.a2a_stage_profile(r, calibration) for r in twoend}
        for r in runs:
            of = profiles.A2A_OVERDUE_ARMS[r["arm"]]
            key = r["key"].replace("a2a_overdue_4k", "a2a_twoend_4k").replace(r["arm"], of)
            if key not in twoend_profiles:
                continue
            document = profiles.a2a_stage_profile(r, calibration)
            balancing = document["network"]["load_balancing"]
            self.assertEqual((balancing.pop("overdue_repair"), balancing.pop("overdue_repair_limit")),
                             ("reprobe", 3))
            self.assertEqual({**document, "name": twoend_profiles[key]["name"]}, twoend_profiles[key])


class CopyDeadline(unittest.TestCase):
    @measured_spines()
    @spines_gaps()
    def test_plumbing(self):
        here = Path(profiles.__file__).parent
        tuning = json.loads((here / "a2a_tuning.json").read_text())
        calibration = json.loads((here / "a2a_calibration.json").read_text())
        settings = profiles.a2a_tail_ack_settings(tuning, profiles.A2A_ARMS_4K)
        deadline = profiles.a2a_copy_deadline(settings)
        for arm in profiles.A2A_ARMS_4K:
            hold = arm in profiles.A2A_HOLD_ARMS_4K
            self.assertEqual(settings[arm].get("sentinel_mode") == "duplicate", hold)
            self.assertEqual(deadline[arm], {**settings[arm], "copy_expiry": "deadline"} if hold else settings[arm])
        (r,) = [r for r in profiles.a2a_stage_runs("a2a_tail_ack_4k", calibration, tuning)
                if r["key"] == "a2a_tail_ack_4k-ep64-256k-loss1e2_up-reps_hold_auto_tail_ack-s301"]
        r = {**r, "setting": deadline["reps_hold_auto_tail_ack"]}
        balancing = profiles.a2a_stage_profile(r, calibration)["network"]["load_balancing"]
        self.assertEqual((balancing["sentinel_mode"], balancing["copy_expiry"]), ("duplicate", "deadline"))
        # Only the coverage-first arms select it.
        cover = {"a2a_twoend_4k": set(profiles.A2A_TWOEND_COVER_ARMS),
                 "a2a_hostlink_s_4k": set(profiles.A2A_HOSTLINK_COVER_ARMS),
                 "a2a_twoend_base_4k": set(profiles.A2A_TWOEND_BASE_ARMS),
                 "a2a_twoend_retract_4k": set(profiles.A2A_TWOEND_RETRACT_ARMS),
                 **{stage: {"reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_rx",
                            "reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_reverse_set",
                            "reps_hold_auto_tail4_cover_rx_answer_seq", "reps_hold_auto_tail4_cover_reverse_set_rx",
                            "reps_hold_auto_tail4_cover_rx_slots8", "reps_hold_auto_tail4_cover_reverse_set_rx_slots8"}
                    for stage in profiles.A2A_WIDE_STAGES},
                 "a2a_overdue_4k": set(profiles.A2A_OVERDUE_ARMS) - {"reps_hold_auto_tail4_od"},
                 "a2a_rset_sweep_4k": set(profiles.A2A_RSET_SWEEP_ARMS),
                 "a2a_twoend_rset_4k": set(profiles.A2A_TWOEND_RSET_ARMS),
                 **{stage: set(profiles.A2A_WIDE_SEEDS_ARMS) for stage in profiles.A2A_WIDE_SEEDS_STAGES},
                 **{stage: set(profiles.A2A_WIDE_SPREAD_ARMS) for stage in profiles.A2A_WIDE_SPREAD_STAGES},
                 "a2a_wide256_widen_4k": set(profiles.A2A_WIDE_WIDEN_ARMS),
                 "a2a_wide256_rauto_4k": set(profiles.A2A_WIDE_RAUTO_ARMS),
                 "a2a_wide128_spread2m_4k": set(profiles.A2A_WIDE_SPREAD_ARMS),
                 "a2a_wide256_od_4k": set(profiles.A2A_WIDE_OD_ARMS),
                 "a2a_wide256_od2_4k": set(profiles.A2A_WIDE_OD_ARMS),
                 "a2a_wide256_rauto_od_4k": {profiles.A2A_WIDE_RAUTO_OD_ARM},
                 "a2a_wide32_rack_4k": set(profiles.A2A_WIDE_RACK_PILOT_ARMS),
                 "a2a_wide32_rack2_4k": set(profiles.A2A_WIDE_RACK2_ARMS),
                 "a2a_wide256_rack_4k": set(profiles.A2A_WIDE_RACK_ARMS),
                 "a2a_wide32_themis_4k": {profiles.A2A_WIDE_RACK_ARMS[1]},
                 "a2a_wide256_uid_4k": set(profiles.A2A_WIDE_UID_ARMS),
                 "a2a_wide256_mrc_4k": set(profiles.A2A_WIDE_MRC_ARMS),
                 "a2a_wide32_fair_4k": set(profiles.A2A_WIDE_PSN_HOLD_ARMS),
                 **{stage: set(profiles.A2A_WIDE_MRC_ARMS) for stage in profiles.A2A_MATRIX_STAGES},
                 "a2a_wide256_down16_4k": set(profiles.A2A_DOWNLINKS_ARMS[:2])}
        for stage in profiles.A2A_STAGES:
            runs = profiles.a2a_stage_runs(stage, calibrated_4k(calibration), tuning)
            self.assertEqual({r["arm"] for r in runs if "copy_expiry" in (r["setting"] or {})},
                             cover.get(stage, set()), stage)


class Decode(unittest.TestCase):
    def setUp(self):
        here = Path(profiles.__file__).parent
        self.tuning = json.loads((here / "a2a_tuning.json").read_text())
        self.calibration = calibrated_4k(json.loads((here / "a2a_calibration.json").read_text()))

    def test_probe(self):
        (probe,) = profiles.a2a_stage_runs("a2a_decode_probe_4k", None, None)
        self.assertEqual(probe["key"], "a2a_decode_probe_4k-ep64-decode8-healthy-uniform_tail-s301")
        self.assertEqual((probe["message_bytes"], probe["combine_bytes"], probe["expert_compute_ns"],
                          probe["iterations"], probe["compute_ns"], probe["fixture"]["payload_bytes"]),
                         (59_264, 114_688, 129_000, 58, 244_000, 4096))
        workload = profiles.a2a_stage_profile(probe, None)["workload"]
        self.assertEqual(workload, {"kind": "all_to_all", "group": "all", "message_bytes": 59_264, "iterations": 58,
                                    "compute_ns": 244_000, "combine_bytes": 114_688, "expert_compute_ns": 129_000,
                                    "dataset_splits": 1})
        # The training cells' runs carry neither key.
        for r in profiles.a2a_stage_runs("a2a_probe_4k", None, None):
            self.assertNotIn("combine_bytes", profiles.a2a_stage_profile(r, None)["workload"])

    def test_refused_until_its_own_probe(self):
        pairs = tuple(profiles.A2A_PROBE_4K_COMPUTE_NS)
        with mock.patch.dict(profiles.A2A_PAYLOAD_COMPUTE_MEASURED, {4096: pairs}):
            for stage in ("a2a_decode_calibration_4k", "a2a_decode_4k"):
                with self.assertRaisesRegex(ValueError, r"\['ep64-decode8'\] are provisional"):
                    profiles.a2a_stage_runs(stage, self.calibration, self.tuning)
        with measured_4k():
            lacking = json.loads(json.dumps(self.calibration))
            del lacking["per_receiver"]["ep64-decode8-p4096"]
            with self.assertRaisesRegex(ValueError, r"\['ep64-decode8-p4096'\]"):
                profiles.a2a_stage_runs("a2a_decode_4k", lacking, self.tuning)

    @measured_4k()
    def test_calibration_and_stage(self):
        calibration = profiles.a2a_stage_runs("a2a_decode_calibration_4k", self.calibration, self.tuning)
        self.assertEqual([(r["arm"], r["size"], r["condition"], r["seed"]) for r in calibration],
                         [("graded_per_receiver_mix", "decode8", "healthy", seed) for seed in (101, 102, 103)])
        self.assertEqual({r["setting"]["sender_pool_weight"] for r in calibration}, {0.75})
        runs = profiles.a2a_stage_runs("a2a_decode_4k", self.calibration, self.tuning)
        # 5 conditions and the healthy pair by 6 arms by 3 seeds.
        self.assertEqual(len(runs), (5 + 1) * 6 * 3)
        self.assertEqual({r["condition"] for r in runs},
                         {"loss1e3_up", "loss1e2_up", "loss1e3_down", "loss1e2_down", "blackhole_up", "healthy"})
        self.assertEqual({r["arm"] for r in runs}, set(profiles.A2A_ARMS_4K))
        self.assertEqual({(r["combine_bytes"], r["expert_compute_ns"], r["iterations"]) for r in runs},
                         {(114_688, 129_000, 58)})

    @measured_4k()
    def test_expected_evidence(self):
        # Per pair per layer, one dispatch flow of 15 packets and one combine
        # flow of 28: 43 packets, 5.375 on each spine.
        runs = [r for r in profiles.a2a_stage_runs("a2a_decode_4k", self.calibration, self.tuning)
                if r["arm"] == "uniform_tail" and r["seed"] == 301 and r["condition"] in ("healthy", "loss1e2_up")]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for r in runs:
                (root / r["key"]).mkdir()
                completion = 500 if r["condition"] == "healthy" else 600
                timing = {"complete": True, "iteration_completion_ns": {"1": completion}}
                (root / r["key"] / "summary.json").write_text(json.dumps({"all_to_all": timing}))
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_sweep(analyze.a2a_rows([listing], root, "a2a_decode_4k"), root)
        self.assertEqual((cell["flows_per_pair"], cell["n_s_pair"], cell["n_s_flow"]), (2, 5.375, 2.6875))
        self.assertAlmostEqual(cell["m_pair"], 0.05375)
        self.assertAlmostEqual(cell["host_evidence"], 3.01)


class Reaction4k(unittest.TestCase):
    @measured_4k()
    def test_runs(self):
        here = Path(profiles.__file__).parent
        calibration = calibrated_4k(json.loads((here / "a2a_calibration.json").read_text()))
        runs = profiles.a2a_stage_runs("a2a_reaction_4k", calibration, None)
        # 2 cells and their healthy pairs by 3 arms by 3 seeds.
        self.assertEqual(len(runs), (2 + 2) * 3 * 3)
        self.assertEqual({(r["size"], r["condition"]) for r in runs},
                         {(s, c) for s in ("256k", "decode8") for c in ("loss1e3_down", "healthy")})
        self.assertEqual({r["arm"] for r in runs},
                         {"reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_rx", "reps_tail"})
        self.assertEqual({r.get("combine_bytes") for r in runs if r["size"] == "decode8"}, {114_688})

    def test_analyzer(self):
        onset = analyze.DEGRADE_AT_NS
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runs = []
            for arm, hold in (("reps_hold_auto_tail_ack", 3000), ("reps_hold_auto_tail_ack_rx", 2000)):
                for seed in (301, 302, 303):
                    for condition in ("healthy", "loss1e3_down"):
                        r = run(arm, "ep64", "256k", condition, seed, "a2a_reaction_4k")
                        runs.append(r)
                        d = root / r["key"]
                        (d / "ns3").mkdir(parents=True)
                        completion = 500 if condition == "healthy" else 600
                        (d / "summary.json").write_text(json.dumps(
                            {"all_to_all": {"complete": True, "iteration_completion_ns": {"1": completion}}}))
                        if condition == "healthy":
                            continue
                        # Leaf 0's host 0 toward leaf 1 counts a loss each interval
                        # and holds at hold; host 9's own leaf is not read.
                        (d / "ns3" / "spine_losses.csv").write_text(
                            "time_ns,rank,destination_leaf,spine,sent,lost,grade\n" + "".join(
                                f"{onset + t},0,1,3,10,1,{0 if t >= hold else 3}\n" for t in (1000, 2000, 3000))
                            + f"{onset + 1000},9,1,3,10,5,0\n")
                        if arm.endswith("_rx"):
                            (d / "ns3" / "receiver_holds.csv").write_text(
                                "time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n"
                                f"{onset + 1000},9,0,3,10,2,leaf\n{onset + 1000},0,1,3,10,7,leaf\n")
            listing = root / "runs.json"
            listing.write_text(json.dumps(runs))
            (cell,) = analyze.a2a_reaction(analyze.a2a_rows([listing], root, "a2a_reaction_4k"), root)
        rx, sender = cell["arms"]["reps_hold_auto_tail_ack_rx"], cell["arms"]["reps_hold_auto_tail_ack"]
        self.assertEqual((rx["sender_hold_ns"], rx["sender_lost_before"]), ([2000] * 3, [2] * 3))
        self.assertEqual((rx["receiver_hold_ns"], rx["receiver_lost_before"]), ([1000] * 3, [2] * 3))
        self.assertEqual((rx["lead_ns"], rx["lead_iterations"]), ([1000] * 3, [2.0] * 3))
        self.assertEqual((sender["receiver_hold_ns"], sender["lead_ns"]), ([None] * 3, [None] * 3))
        self.assertEqual(sender["sender_hold_ns_mean"], 3000)
        self.assertEqual((cell["receiver_lead_ns"], cell["receiver_lead_iterations"]), ([2000] * 3, [4.0] * 3))
        markdown = analyze.reaction_markdown([cell])
        self.assertIn("| reps_hold_auto_tail_ack_rx | 0.1 | 2.0, 2.0, 2.0 | 2, 2, 2 | 1.0, 1.0, 1.0 | 2, 2, 2 "
                      "| 1.0, 1.0, 1.0 | 2.00, 2.00, 2.00 |", markdown)
        self.assertIn("receiver ahead of reps_hold_auto_tail_ack's sender by 2.0, 2.0, 2.0 us, 4.00, 4.00, 4.00 "
                      "iterations", markdown)


class Ring(unittest.TestCase):
    def test_stages(self):
        here = Path(profiles.__file__).parent
        calibration = calibrated_4k(json.loads((here / "a2a_calibration.json").read_text()))
        # The ring's gap needs no probe: its stages run at once.
        (probe,) = profiles.a2a_stage_runs("ar_parity_probe_4k", None, None)
        self.assertEqual(probe["key"], "ar_parity_probe_4k-dp64-ar25m-healthy-uniform_tail-s301")
        self.assertEqual(len(profiles.a2a_stage_runs("ar_parity_calibration_4k", None, None)), 3)
        runs = profiles.a2a_stage_runs("ar_parity_4k", calibration, None)
        # The loss cell and its healthy pair by 6 arms by 3 seeds.
        self.assertEqual(len(runs), 2 * 6 * 3)
        self.assertEqual({(r["condition"], r["iterations"], r["compute_ns"], r["k"]) for r in runs},
                         {("loss1e2_up", 12, 0, 63), ("healthy", 12, 0, 63)})
        document = profiles.a2a_stage_profile(next(r for r in runs if r["condition"] == "loss1e2_up"), calibration)
        self.assertEqual(document["workload"], {"kind": "sequential_dp_all_reduce"})
        self.assertEqual((document["dp_all_reduce_bytes"], document["steps"], document["compute_duration_us"]),
                         (26_214_400, 12, 0))
        self.assertEqual(document["network"]["packet_payload_bytes"], 4096)
        self.assertEqual(document["network"]["link_failures"],
                         [{**profiles.a2a_fault("loss1e2_up")[0], "loss_unit": "byte"}])
        # The ring keeps its workload's chunking.
        self.assertNotIn("dataset_splits", runs[0]["fixture"])
        with self.assertRaisesRegex(ValueError, r"\['dp64-ar25m-p4096'\]"):
            profiles.a2a_stage_runs("ar_parity_4k", None, None)


class ComputeGaps(unittest.TestCase):
    def test_compute_gap(self):
        runs = [r for r in profiles.a2a_stage_runs("a2a_probe_4k", None, None) if r["group"] == "ep32"]
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
            rows = analyze.a2a_compute_gaps([listing], root, "a2a_probe_4k")
        self.assertEqual([(r["size"], r["fixture"], r["median_iteration_ns"], r["compute_ns"]) for r in rows],
                         [("256k", profiles.A2A_FIXTURE_4K, 388_400, profiles.A2A_COMPUTE_NS[("ep64", "256k")]),
                          ("4m", profiles.A2A_FIXTURE_4K, None, None)])
        self.assertEqual((rows[0]["probe_compute_ns"], rows[0]["iterations_completed"], rows[0]["iterations"]),
                         (127_000, 40, 40))
        markdown = analyze.compute_gaps_markdown(rows)
        self.assertIn("| ep32 | 256k | 40 of 40 | 388.4 | 259000 |", markdown)
        self.assertIn("| ep32 | 4m | 7 of 12 | - | - |", markdown)

    def test_decode_period(self):
        # The rest of the 711 us layer period after t_h, rounded up.
        (probe,) = profiles.a2a_stage_runs("a2a_decode_probe_4k", None, None)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / probe["key"]).mkdir()
            timing = {"complete": True, "iterations_completed": 58, "mean_ns": 467_500, "p50_ns": 467_300,
                      "p99_ns": 480_000, "max_ns": 481_000}
            (root / probe["key"] / "summary.json").write_text(json.dumps({"all_to_all": timing}))
            listing = root / "runs.json"
            listing.write_text(json.dumps([probe]))
            (row,) = analyze.a2a_compute_gaps([listing], root, "a2a_decode_probe_4k")
        self.assertEqual((row["median_iteration_ns"], row["compute_ns"]), (467_300, 244_000))


class SweepCells(unittest.TestCase):
    COSTS = {
        ("ep64", "256k", "loss1e2_up"): {
            "uniform_tail": [400, 420, 440], "reps_tail": [300, 310, 320], "reps_oracle_tail": [100, 110, 120],
            "reps_hold_auto_tail_ack": [150, 160, 170], "reps_hold_leaf_tail_ack": [200, 210, 220],
            "reps_flow_hold_tail_ack": [250, 260, 300]},
        ("ep64", "256k", "rate16g_up"): {"uniform_tail": [40, 50, 60]},
        ("ep8", "256k", "loss1e2_up"): {"uniform_tail": [-1, 0, 1], "reps_hold_auto_tail_ack": [5, 6, 7]},
    }
    # EP8's runs leave the payload at the generator's default.
    SHAPE = {"ep64": 63, "ep8": 7}

    def cells(self):
        with tempfile.TemporaryDirectory() as directory:
            # The sweep's cells that a2a_tail_ack_4k runs are in its out root.
            roots = {stage: Path(directory) / stage for stage in ("a2a_sweep_4k", "a2a_tail_ack_4k")}
            runs = {stage: [] for stage in roots}
            _, shared = profiles.a2a_shared_4k("a2a_sweep_4k")

            def write(arm, group, size, condition, seed, cost, stage=None):
                stage = stage or ("a2a_tail_ack_4k" if (group, size, condition) in shared else "a2a_sweep_4k")
                root = roots[stage]
                r = {**run(arm, group, size, condition, seed, stage), "k": self.SHAPE[group],
                     "message_bytes": 256 * 1024, "iterations": 2,
                     **({} if group == "ep8" else {"fixture": profiles.A2A_FIXTURE_4K})}
                runs[stage].append(r)
                (root / r["key"] / "ns3").mkdir(parents=True)
                timing = {"complete": True, "iteration_completion_ns": {"1": 1000 + cost, "2": 1000 + cost}}
                (root / r["key"] / "summary.json").write_text(json.dumps({"all_to_all": timing}))
                if condition == "healthy" or group == "ep8" or arm not in analyze.A2A_SWEEP_HOLDS + (
                        analyze.A2A_SWEEP_POOLED,):
                    return
                # Counted: leaf 0's host 0 toward leaf 1 on spine 3, 8 losses
                # (16 without a destination leaf, every destination); not
                # counted: another spine, another leaf's host, leaf 0 itself.
                if arm == "reps_flow_hold_tail_ack":
                    trace = "time_ns,rank,spine,sent,lost,grade\n5,0,3,99,16,3\n5,9,3,99,3,3\n5,0,2,99,4,3\n"
                else:
                    trace = ("time_ns,rank,destination_leaf,spine,sent,lost,grade\n5,0,1,3,99,8,0\n"
                             "5,1,0,3,99,5,3\n5,9,2,3,99,7,0\n5,2,3,2,99,4,3\n")
                (root / r["key"] / "ns3" / "spine_losses.csv").write_text(trace)

            for (group, size, condition), arms in self.COSTS.items():
                for arm, costs in arms.items():
                    for seed, cost in zip((301, 302, 303), costs):
                        write(arm, group, size, condition, seed, cost)
                        if not any(r["key"].endswith(f"-{group}-{size}-healthy-{arm}-s{seed}")
                                   for listed in runs.values() for r in listed):
                            write(arm, group, size, "healthy", seed, 0)
            # A cell of a2a_tail_ack_4k the sweep does not read.
            write("uniform_tail", "ep64", "256k", "blackhole_up", 301, 900, "a2a_tail_ack_4k")
            for stage, root in roots.items():
                (root / "runs.json").write_text(json.dumps(runs[stage]))
            sweep = roots["a2a_sweep_4k"]
            with self.assertRaisesRegex(ValueError, "a2a_tail_ack_4k runs every cell it reads"):
                analyze.a2a_shared_rows("a2a_tail_ack_4k", roots["a2a_tail_ack_4k"])
            rows = analyze.a2a_rows([sweep / "runs.json"], sweep, "a2a_sweep_4k")
            return {(c["group"], c["size"], c["condition"]): c
                    for c in analyze.a2a_sweep(rows, sweep, roots["a2a_tail_ack_4k"])}

    def test_cells(self):
        cells = self.cells()
        self.assertEqual(set(cells), set(self.COSTS))
        cell = cells[("ep64", "256k", "loss1e2_up")]
        self.assertEqual({stage for entry in cell["arms"].values() for stage in entry["stages"]}, {"a2a_tail_ack_4k"})
        # One flow of 256 KiB per pair: 64 packets of 4096 B, 8 per spine.
        self.assertEqual((cell["payload_bytes"], cell["flows_per_pair"], cell["n_s_flow"], cell["n_s_pair"], cell["p"]),
                         (4096, 1, 8.0, 8.0, 0.01))
        self.assertAlmostEqual(cell["m_flow"], 0.08)
        self.assertAlmostEqual(cell["m_pair"], 0.08)
        self.assertAlmostEqual(cell["host_evidence"], 4.48)
        arms = cell["arms"]
        self.assertEqual((arms["reps_flow_hold_tail_ack"]["cost_ns_mean"], arms["reps_flow_hold_tail_ack"]["cost_ns_range"]),
                         (270, 50))

        def recovered(arm):
            return sum(1 - c / u for c, u in zip(self.COSTS[("ep64", "256k", "loss1e2_up")][arm], [400, 420, 440])) / 3

        self.assertAlmostEqual(arms["reps_hold_leaf_tail_ack"]["recovered_mean"], 0.5)
        self.assertEqual(cell["oracle_versus_reps"], "reps_oracle_tail beats reps_tail")
        for arm in ("reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack"):
            self.assertAlmostEqual(cell["ceiling"][arm], recovered(arm) / recovered("reps_oracle_tail"))
        leaf, flow = (cell["gaps"][arm] for arm in analyze.A2A_SWEEP_HOLDS)
        self.assertAlmostEqual(leaf["recovered_gap"], recovered("reps_hold_auto_tail_ack") - 0.5)
        self.assertEqual((leaf["cost_gap_ns"], leaf["verdict"]),
                         (50, "reps_hold_auto_tail_ack beats reps_hold_leaf_tail_ack"))
        self.assertEqual((flow["cost_gap_ns"], flow["verdict"]),
                         (110, "reps_hold_auto_tail_ack beats reps_flow_hold_tail_ack"))
        # 8 losses per run over 8 hosts and 2 iterations; 16 without a
        # destination leaf; no trace under the baselines.
        self.assertEqual(arms["reps_hold_auto_tail_ack"]["spine_losses_per_host_iteration"], 0.5)
        self.assertEqual(arms["reps_flow_hold_tail_ack"]["spine_losses_per_host_iteration"], 1.0)
        self.assertIsNone(arms["uniform_tail"]["spine_losses_per_host_iteration"])
        self.assertIsNone(cell["control"])
        handover = cells[("ep64", "256k", "rate16g_up")]
        # Against a2a_tail_ack_4k's healthy runs.
        self.assertEqual(handover["arms"]["uniform_tail"]["cost_ns_mean"], 50)
        self.assertEqual((handover["n_s_pair"], handover["p"], handover["m_flow"], handover["m_pair"],
                          handover["host_evidence"]), (8.0, None, None, None, None))
        self.assertEqual(handover["ceiling"], dict.fromkeys(handover["ceiling"]))
        self.assertIsNone(handover["oracle_versus_reps"])
        control = cells[("ep8", "256k", "loss1e2_up")]
        # On the defaults, 1000 B and 4 flows: 66 packets per 64 KiB flow.
        self.assertEqual(
            (control["payload_bytes"], control["flows_per_pair"], control["n_s_flow"], control["n_s_pair"]),
            (1000, 4, 8.25, 33.0))
        self.assertEqual(control["host_evidence"], 0)
        self.assertEqual(control["control"], {
            "within_range_of_zero": {"reps_hold_auto_tail_ack": False, "uniform_tail": True},
            "seed_rule": ["uniform_tail beats reps_hold_auto_tail_ack"]})
        self.assertIsNone(control["arms"]["uniform_tail"]["recovered_mean"])
        markdown = analyze.sweep_markdown(list(cells.values()))
        self.assertIn("| ep64 | 256k | loss1e2_up | 4096 | 8 / 8 | 0.01 | 0.08 / 0.08 | 4.48 "
                      "| 0.50 / 0.50 / 1.00 | reps_oracle_tail beats reps_tail |", markdown)
        self.assertIn("| ep64 | 256k | loss1e2_up | reps_flow_hold_tail_ack | 0.3 | 0.1 | 0.358 |", markdown)
        self.assertIn("Control ep8 256k loss1e2_up: mean cost within its seed range of zero: "
                      "reps_hold_auto_tail_ack False, uniform_tail True; seed-rule verdicts other than noise: "
                      "uniform_tail beats reps_hold_auto_tail_ack", markdown)


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
            self.assertEqual(analyze.rto_counts(root),
                             {"rto_events": None, "rto_per_iteration": None, "rto_run": None})
            times = (50, 100, 999, 1000, 4000, 6000, 7000)
            (root / "ns3" / "rto_events.csv").write_text(
                "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n"
                + "".join(f"{t},0,1,3,0,4096\n" for t in times))
            # Within one RTO of the first: one timeout of the queue pair.
            self.assertEqual(analyze.rto_counts(root),
                             {"rto_events": 7, "rto_per_iteration": [3, 2, 2], "rto_run": 1})

    def test_rto_run(self):
        rto = analyze.A2A_RTO_NS
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ns3").mkdir()
            (root / "summary.json").write_text(json.dumps({"all_to_all": {}}))
            # Host 0 toward host 9: a timeout every RTO and a bit, with a
            # further send's timeout inside each, 9 counted, then a gap of
            # two RTOs and a new run of 2. Host 0 toward host 17 and host 1
            # toward host 9, interleaved: their own runs of 3 and 1.
            rows = [(1000 + k * (rto + 100), 0, 9) for k in range(9)]
            rows += [(1000 + k * (rto + 100) + 500, 0, 9) for k in range(9)]
            rows += [(rows[8][0] + 2 * rto + k * rto, 0, 9) for k in range(2)]
            rows += [(2000 + k * rto, 0, 17) for k in range(3)] + [(2000 + rto, 1, 9)]
            (root / "ns3" / "rto_events.csv").write_text(
                "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n"
                + "".join(f"{t},{rank},{dst},3,0,4096\n" for t, rank, dst in sorted(rows)))
            counts = analyze.rto_counts(root)
            self.assertEqual((counts["rto_events"], counts["rto_run"]), (24, 9))
            (root / "ns3" / "rto_events.csv").write_text(
                "time_ns,rank,dst_rank,spine,sentinel,bytes_outstanding\n")
            self.assertEqual(analyze.rto_counts(root)["rto_run"], 0)

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
            self.assertEqual((entry["rto_run"], entry["rto_run_longest"], entry["rto_run_over_limit"]), ([1], 1, []))
            self.assertEqual(entry["receiver_reaction_ns"], [None])
            self.assertIn("RTOs mean", analyze.candidates_markdown([cell]))
            self.assertIn("| 1.0 (1) | 1 (none) |", analyze.candidates_markdown([cell]))
            entry.update(rto_run=[8], rto_run_longest=8, rto_run_over_limit=[301])
            self.assertIn("| 8 (s301) |", analyze.candidates_markdown([cell]))


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
            keep, basis = analyze.faulted_share_rows(condition, ("destination_leaf",) if leaf_column else ())
            return [pair for pair in pairs
                    if keep({"rank": str(pair[0]), "destination_leaf": str(pair[1])})], basis

        self.assertEqual(kept("loss1e2_up", True), ([(0, 1), (0, 2)], "remote"))
        self.assertEqual(kept("loss1e2_down", True), ([(0, 1), (17, 1)], "remote"))
        self.assertEqual(kept("structural", True), ([(0, 1), (0, 2), (9, 0), (17, 1)], "remote"))
        self.assertEqual(kept("loss1e2_up", False), ([(0, 0), (0, 1), (0, 2)], "all"))
        self.assertEqual(kept("loss1e2_down", False), ([(0, 0), (0, 1), (0, 2), (17, 1)], "all"))
        self.assertEqual(kept("structural", False), (pairs, "all"))
        # Under hold_scope pair the destination host's node id gives the leaf:
        # the same rows, a destination host on each destination leaf.
        for condition in ("loss1e2_up", "loss1e2_down", "structural"):
            keep, basis = analyze.faulted_share_rows(condition, ("time_ns", "rank", "destination", "spine"))
            self.assertEqual(([pair for pair in pairs
                               if keep({"rank": str(pair[0]), "destination": str(8 * pair[1] + 5)})], basis),
                             kept(condition, True))

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

    def test_4k_cells_merge_into_the_committed_file(self):
        here = Path(profiles.__file__).parent
        committed = json.loads((here / "a2a_calibration.json").read_text())
        runs = [({**run("graded_per_receiver_mix", group, size, "healthy", 101, "a2a_calibration_4k"),
                  "fixture": profiles.A2A_FIXTURE_4K}, self.reports(0.03, 0.02), self.losses(50))
                for group, size in profiles.A2A_CALIBRATION_4K_CELLS]
        result = self.calibrate(runs, existing=json.loads(json.dumps(committed)))
        self.assertEqual({k: v for k, v in result.items() if k != "per_receiver"},
                         {k: v for k, v in committed.items() if k != "per_receiver"})
        cells = [f"{group}-{size}-p4096" for group, size in profiles.A2A_CALIBRATION_4K_CELLS]
        self.assertEqual(result["per_receiver"], {**committed["per_receiver"],
                                                  **{c: result["per_receiver"][c] for c in cells}})
        self.assertIn("ep64-256k-p4096", result["per_receiver"])
        self.assertNotIn("ep8-256k-p4096", result["per_receiver"])
        self.assertEqual(result["per_receiver"]["ep64-1m-p4096"]["loss_thresholds"]["24648"], [0.005, 0.01, 0.02])
        with measured_4k():
            sweep = profiles.a2a_stage_runs("a2a_sweep_4k", result, None)
            self.assertEqual(len(profiles.a2a_stage_runs("a2a_tail_ack_4k", result, None)), 576)
        hold = next(r for r in sweep if (r["group"], r["size"], r["arm"]) == ("ep64", "1m", "reps_hold_auto_tail_ack"))
        balancing = profiles.a2a_stage_profile(hold, result)["network"]["load_balancing"]
        self.assertEqual(balancing["loss_thresholds"], [0.005, 0.01, 0.02])

    def test_spine_count_cells(self):
        # A path-count fixture's cells by spine count, their loss intervals
        # in its base RTTs.
        runs = [({**run("graded_per_receiver_mix", "ep64", "2m", "healthy", 101, stage),
                  "fixture": profiles.A2A_SPINES_FIXTURE[spines]}, self.reports(0.03, 0.02), self.losses(50))
                for stage, spines in profiles.A2A_SPINES_CALIBRATION_STAGES.items()]
        result = self.calibrate(runs)
        for spines, rtt in profiles.A2A_SPINES_BASE_RTT_NS.items():
            cell = result["per_receiver"][f"ep64-2m-p4096-s{spines}"]
            self.assertEqual(sorted(cell["loss_thresholds"], key=int), [str(n * rtt) for n in (2, 10, 40)])

    def test_wide_cells(self):
        # A wide-leaf fixture's cells by P, at the 8-spine fabric's loss
        # intervals.
        runs = [({**run("graded_per_receiver_mix", "ep64", "256k", "healthy", 101, stage),
                  "fixture": profiles.A2A_WIDE_FIXTURE[wide]}, self.reports(0.03, 0.02), self.losses(50))
                for stage, wide in profiles.A2A_WIDE_CALIBRATION_STAGES.items()]
        result = self.calibrate(runs)
        for wide in profiles.A2A_WIDE:
            cell = result["per_receiver"][f"ep64-256k-p4096-w{wide}"]
            self.assertEqual(sorted(cell["loss_thresholds"], key=int), ["24648", "123240", "492960"])

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


class RecordFilter(unittest.TestCase):
    """profiles.py write --filter, as the workflow passes record_filter: an
    empty one writes what the write without it does, to the byte, and one
    that selects a run writes that run alone, as the whole stage has it."""

    STAGES = ("comparison", "t_comparison", "a2a_probe", "a2a_candidates", "a2a_wide32_4k")

    def write(self, out: Path, stage: str, *extra: str) -> dict[str, bytes]:
        here = Path(profiles.__file__).parent
        command = [sys.executable, str(here / "profiles.py"), "write", "--out", str(out), "--stage", stage]
        for flag, name in (("--calibration", "calibration.json"), ("--tuning", "tuning.json"),
                           ("--transient-calibration", "transient_calibration.json"),
                           ("--transient-tuning", "transient_tuning.json"),
                           ("--a2a-calibration", "a2a_calibration.json"), ("--a2a-tuning", "a2a_tuning.json")):
            if (here / name).is_file():
                command += [flag, str(here / name)]
        subprocess.run(command + list(extra), check=True, stdout=subprocess.DEVNULL)
        return {path.name: path.read_bytes() for path in out.iterdir()}

    def test_each_family(self):
        for stage in self.STAGES:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                every = self.write(root / "every", stage)
                self.assertEqual(self.write(root / "empty", stage, "--filter="), every)
                listed = json.loads(every["runs.json"])
                self.assertGreater(len(listed), 1)
                run = listed[1]
                one = self.write(root / "one", stage, f"--filter=^{re.escape(run['key'])}$")
                self.assertEqual(set(one), {"runs.json", run["profile"]})
                self.assertEqual(one[run["profile"]], every[run["profile"]])
                self.assertEqual(json.loads(one["runs.json"]), [run])


if __name__ == "__main__":
    unittest.main()
