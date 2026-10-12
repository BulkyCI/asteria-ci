#!/usr/bin/env python3
"""Profiles and run lists of the incast load-balancing study.

The fabric is eight leaves of eight hosts under eight spines at 400 Gbps,
1 us host links and 2 us switch links (base RTT 12,324 ns), the UEC switch
preset, NSCC, packet trimming and selective repair. Every host on leaf 0
receives K messages of 32 MiB at once from the other leaves (workload
"incast"). From 100 base RTTs on, spine 3 is healthy, at half rate, at 1
percent loss, or silent.

Arms are named load-balancing settings. The grade arms read thresholds and a
gain from a calibration file once one exists; before that they run at the
simulator defaults, which is what the calibration runs themselves use.

    profiles.py write --out DIR [--calibration FILE] [--tuning FILE] --stage S

writes DIR/<run key>.json for every run of the stage and DIR/runs.json, the
list of {key, profile, seed, stage, k, condition, arm}. Stages named t_*
are the transient-fault stages of transient.py, which read their own
calibration and tuning files (--transient-calibration, --transient-tuning)
and the redirect_per_connection setting of --tuning. The a2a_probe stage
runs the all-to-all workload on the same fabric (see a2a_runs). The a2a_*
stages after it are Phase 9's (see a2a_stage_runs); they read
--a2a-calibration and --a2a-tuning (incast/a2a_calibration.json and
incast/a2a_tuning.json) when given. Standard library only.
"""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Any

BASE_RTT_NS = 12_324
DEGRADE_AT_NS = 100 * BASE_RTT_NS
MESSAGE_BYTES = 32 * 1024 * 1024
DEGRADED_SPINE = 3
FAN_INS = (8, 32, 64)
CONDITIONS = ("healthy", "half_rate", "gray", "silent")
SEEDS = {"calibration": (101, 102, 103), "tuning": (201,), "comparison": (301, 302, 303)}
# Candidate estimator gains for the gain calibration, DCTCP's 1/16 among them.
GAINS = (1 / 64, 1 / 32, 1 / 16, 1 / 8, 1 / 4)
TUNING_FAN_IN = 32
# The short stage, exploratory and outside the pre-registered comparison:
# flows a sixteenth as long, degraded ten base RTTs in, so a connection
# lives about a hundred base RTTs and its own arrivals are few.
SHORT_MESSAGE_BYTES = 2 * 1024 * 1024
SHORT_DEGRADE_AT_NS = 10 * BASE_RTT_NS
TUNING_CONDITIONS = ("half_rate", "gray")

REDIRECT_FLAGS = (0.05, 0.10, 0.15)
REDIRECT_OMEGAS = (0.3, 0.5, 0.7)
REDIRECT_VALIDITIES = (1.0, 2.0, 3.0)
REDIRECT_TRIM_SCORING = ("mark", "ignore")

# The grade arms and the scope each is calibrated at.
GRADE_SCOPES = {
    "graded_per_receiver": "per_receiver",
    "graded_per_connection": "per_connection",
}
# The same arms with the edge bit's window penalty left on: every
# acknowledgement whose report has the edge bit cuts the NSCC window. The
# study's graded arms turn it off, so every arm runs the same NSCC and only
# the spray weights differ; the edge stage measures what it costs.
EDGE_ARMS = {
    "graded_per_receiver_edge": "graded_per_receiver",
    "graded_per_connection_edge": "graded_per_connection",
}
SPRAY_POLICY_EDGE_PENALTY = 64
FIXED_ARMS = (
    "graded_per_receiver",
    "graded_per_connection",
    "graded_switch_queue",
    "reps",
    "mrc",
    "ue_aware",
    "uniform",
)


def failures(condition: str, degrade_at_ns: int = DEGRADE_AT_NS) -> list[dict[str, Any]]:
    start = {"start_ns": degrade_at_ns, "spine": DEGRADED_SPINE}
    if condition == "healthy":
        return []
    if condition == "half_rate":
        return [{"model": "gray", "rate": "200Gbps", **start}]
    if condition == "gray":
        return [{"model": "gray", "error_rate": 0.01, **start}]
    if condition == "silent":
        return [{"model": "silent", **start}]
    raise ValueError(f"unknown condition {condition}")


def profile(
    name: str,
    fan_in: int,
    condition: str,
    balancing: dict[str, Any],
    message_bytes: int = MESSAGE_BYTES,
    degrade_at_ns: int = DEGRADE_AT_NS,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "name": name,
        "parallelism": {"tp": 1, "pp": 1, "dp": 64},
        "steps": 1,
        "compute_duration_us": 0,
        "tp_all_reduce_bytes": 0,
        "pp_bytes": 0,
        "seed": 1,
        "selection_policy": {"p_low": 0.0, "p_high": 0.0, "domain": "admission"},
        "workload": {
            "kind": "incast",
            "receiver_leaf": 0,
            "fan_in": fan_in,
            "message_bytes": message_bytes,
        },
        "network": {
            "topology": "clos",
            "hosts_per_leaf": 8,
            "spine_count": 8,
            "link_rate": "400Gbps",
            "host_link_delay_ns": 1_000,
            "switch_link_delay_ns": 2_000,
            "switch": {"profile": "ue"},
            "fabric": {"buffer_size_mb": 32, "pfc_enabled": False, "headroom_factor": 0},
            "packet_trimming": {"mode": "ftd"},
            "transport_recovery": {
                "retransmission_timeout_ns": 10 * BASE_RTT_NS,
                "max_retransmission_retries": 1024,
                "selective_repair": True,
            },
            "congestion_control": {"mode": "nscc"},
            "load_balancing": balancing,
            "link_failures": failures(condition, degrade_at_ns),
        },
        "microburst_enabled": False,
        "microburst_bytes": MESSAGE_BYTES,
        "microburst_flow_count": 7,
        "microburst_destination_rank": 4,
        "microburst_offset_spacing_ns": 0,
    }


def _graded(calibration: dict[str, Any] | None, scope_key: str, fan_in: int) -> dict[str, Any]:
    """Thresholds and gain calibrated at one scope and fan-in, or nothing
    before then. The calibration file maps scope, then K, to its values."""
    entry = (calibration or {}).get(scope_key, {}).get(str(fan_in))
    if entry is None:
        return {}
    values = {"congestion_thresholds": entry["congestion_thresholds"]}
    if "estimator_gain" in entry:
        values["estimator_gain"] = entry["estimator_gain"]
    return values


def setting_name(setting: dict[str, Any]) -> str:
    if set(setting) == {"estimator_gain"}:
        return f"g{round(1 / setting['estimator_gain'])}"
    if "gamma" in setting and set(setting) <= {
        "gamma", "sender_pool_weight", "loss_interval_ns", "held_sentinel_interval_ns", "pool_decay_interval_ns",
        "loss_hold_clean_sentinels",
    }:
        # gm025 for gamma 0.25, sw050 for a sender pool weight of 0.5, li10
        # for a loss interval of ten base RTTs, hs81 for a held-spine
        # sentinel every 81 base RTTs (1 ms), pd2 for a pool decay every two,
        # lh1024 for a loss hold released after 1024 clean sentinels.
        parts = [f"gm{round(setting['gamma'] * 100):03d}"]
        if "sender_pool_weight" in setting:
            parts.append(f"sw{round(setting['sender_pool_weight'] * 100):03d}")
        if "loss_interval_ns" in setting:
            parts.append(f"li{round(setting['loss_interval_ns'] / BASE_RTT_NS)}")
        if "held_sentinel_interval_ns" in setting:
            parts.append(f"hs{round(setting['held_sentinel_interval_ns'] / BASE_RTT_NS)}")
        if "pool_decay_interval_ns" in setting:
            parts.append(f"pd{round(setting['pool_decay_interval_ns'] / BASE_RTT_NS)}")
        if "loss_hold_clean_sentinels" in setting:
            parts.append(f"lh{setting['loss_hold_clean_sentinels']}")
        return "-".join(parts)
    parts = [f"f{round(setting['flag'] * 100):02d}", f"w{round(setting['omega'] * 10)}"]
    if "validity_base_rtts" in setting:
        parts.append(f"v{round(setting['validity_base_rtts'])}")
    if "trim_scoring" in setting:
        parts.append(setting["trim_scoring"])
    return "-".join(parts)


def redirect_settings(scope: str) -> list[dict[str, Any]]:
    if scope == "per_connection":
        return [
            {"flag": f, "omega": w, "validity_base_rtts": v, "trim_scoring": t}
            for f, w, v, t in itertools.product(
                REDIRECT_FLAGS, REDIRECT_OMEGAS, REDIRECT_VALIDITIES, REDIRECT_TRIM_SCORING
            )
        ]
    return [{"flag": f, "omega": w} for f, w in itertools.product(REDIRECT_FLAGS, REDIRECT_OMEGAS)]


def balancing(
    arm: str,
    fan_in: int,
    calibration: dict[str, Any] | None,
    setting: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if arm in EDGE_ARMS:
        return {
            **balancing(EDGE_ARMS[arm], fan_in, calibration, setting),
            "edge_window_penalty": SPRAY_POLICY_EDGE_PENALTY,
        }
    if arm in GRADE_SCOPES:
        # A setting here is the gain calibration's candidate gain.
        return {
            "mode": "spray_policy",
            "grade_scope": GRADE_SCOPES[arm],
            "edge_window_penalty": 0,
            **_graded(calibration, GRADE_SCOPES[arm], fan_in),
            **(setting or {}),
        }
    if arm == "graded_switch_queue":
        values = {
            "mode": "spray_policy",
            "grade_scope": "per_receiver",
            "grade_cost_source": "switch_queue",
            "edge_window_penalty": 0,
        }
        thresholds = _graded(calibration, "switch_queue", fan_in).get("congestion_thresholds")
        if thresholds is not None:
            values["congestion_thresholds"] = thresholds
        return values
    if arm in ("redirect_per_connection", "redirect_per_receiver"):
        scope = "per_connection" if arm == "redirect_per_connection" else "per_receiver"
        values = {"mode": "spray_uniform", "selector": "ack_redirect", "scope": scope, **(setting or {})}
        if scope == "per_receiver":
            gain = _graded(calibration, "per_receiver", fan_in).get("estimator_gain")
            if gain is not None:
                values["estimator_gain"] = gain
        return values
    if arm == "reps":
        return {"mode": "ev_hash", "selector": "reps"}
    if arm == "mrc":
        return {"mode": "ev_hash", "selector": "mrc"}
    if arm == "ue_aware":
        return {"mode": "ev_hash", "selector": "ue_aware"}
    if arm == "uniform":
        return {"mode": "spray_uniform"}
    raise ValueError(f"unknown arm {arm}")


def runs(stage: str, calibration: dict[str, Any] | None, tuning: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Every run of a stage as (key, arm, setting, K, condition, seed)."""
    out: list[dict[str, Any]] = []

    def add(arm: str, fan_in: int, condition: str, seed: int, setting: dict[str, Any] | None = None) -> None:
        label = arm if setting is None else f"{arm}-{setting_name(setting)}"
        out.append(
            {
                "key": f"{stage}-k{fan_in}-{condition}-{label}-s{seed}",
                "stage": stage,
                "arm": arm,
                "setting": setting,
                "k": fan_in,
                "condition": condition,
                "seed": seed,
            }
        )

    if stage == "binding":
        for fan_in, condition in itertools.product(FAN_INS, ("half_rate", "gray")):
            add("uniform", fan_in, condition, SEEDS["comparison"][0])
    elif stage == "calibration":
        for fan_in, seed in itertools.product(FAN_INS, SEEDS["calibration"]):
            for arm in ("graded_per_receiver", "graded_per_connection", "graded_switch_queue"):
                add(arm, fan_in, "healthy", seed)
    elif stage == "gain":
        if calibration is None:
            raise ValueError("the gain stage needs the threshold calibration")
        for fan_in, seed, gain in itertools.product(FAN_INS, SEEDS["calibration"], GAINS):
            for arm in GRADE_SCOPES:
                add(arm, fan_in, "healthy", seed, {"estimator_gain": gain})
    elif stage == "tuning":
        for condition, seed in itertools.product(TUNING_CONDITIONS, SEEDS["tuning"]):
            for arm, scope in (("redirect_per_connection", "per_connection"), ("redirect_per_receiver", "per_receiver")):
                for setting in redirect_settings(scope):
                    add(arm, TUNING_FAN_IN, condition, seed, setting)
    elif stage == "comparison":
        # The redirect arms join once the tuning selection exists; the fixed
        # arms can run before it.
        for fan_in, condition, seed in itertools.product(FAN_INS, CONDITIONS, SEEDS["comparison"]):
            for arm in FIXED_ARMS:
                add(arm, fan_in, condition, seed)
            for arm in ("redirect_per_connection", "redirect_per_receiver"):
                if tuning is None or arm not in tuning:
                    continue
                chosen = tuning[arm].get(condition, tuning[arm]["half_rate"])
                add(arm, fan_in, condition, seed, chosen)
    elif stage == "edge":
        for condition, seed in itertools.product(CONDITIONS, SEEDS["comparison"]):
            for arm in EDGE_ARMS:
                add(arm, 32, condition, seed)
    elif stage == "short":
        for fan_in, condition, seed in itertools.product((32, 64), ("healthy", "half_rate", "silent"), SEEDS["comparison"]):
            for arm in ("graded_per_receiver", "graded_per_connection", "uniform"):
                add(arm, fan_in, condition, seed)
    elif stage == "kill":
        # The decisive subset of the comparison stage, runnable on its own.
        for condition, seed in itertools.product(("half_rate", "gray"), SEEDS["comparison"]):
            for arm in ("graded_per_receiver", "graded_per_connection", "uniform"):
                add(arm, 32, condition, seed)
    else:
        raise ValueError(f"unknown stage {stage}")
    return out


# The all-to-all probe: every host sends A2A_MESSAGE_BYTES to every other
# host, A2A_PROBE_ITERATIONS times in a chain, with A2A_COMPUTES_NS of compute
# between iterations (0, and a first guess at the healthy iteration time so
# the fabric is not saturated). From 100 base RTTs to the end, the link
# between leaf 0 and spine 3 is healthy, silent, at half rate or at 1
# percent loss. K is the fan-in at each receiver, the 63 other hosts.
A2A_STAGE = "a2a_probe"
A2A_MESSAGE_BYTES = 512 * 1024
A2A_PROBE_ITERATIONS = 40
A2A_COMPUTES_NS = (0, 200_000)
A2A_CONDITIONS = ("healthy", "blackhole", "half_rate", "loss")
A2A_ARMS = ("uniform", "graded_switch_queue")
A2A_FAN_IN = 63
A2A_SEED = 301
A2A_LOSS_RATE = 0.01
A2A_LEAF = 0


def a2a_failures(condition: str) -> list[dict[str, Any]]:
    """The link from spine 3 to leaf 0 (the t_ stages' downlink0 form),
    degraded from 100 base RTTs to the end of the run."""
    if condition == "healthy":
        return []
    model = {
        "blackhole": {"model": "silent"},
        "half_rate": {"model": "gray", "rate": "200Gbps"},
        "loss": {"model": "gray", "error_rate": A2A_LOSS_RATE},
    }[condition]
    return [{**model, "start_ns": DEGRADE_AT_NS, "spine": DEGRADED_SPINE, "leaf": A2A_LEAF}]


def a2a_runs(transient_tuning: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The probe's runs; graded_switch_queue takes the transient study's
    tuned gamma once that selection exists."""
    import transient

    out = []
    for compute_ns, condition, arm in itertools.product(A2A_COMPUTES_NS, A2A_CONDITIONS, A2A_ARMS):
        setting = None if arm == "uniform" else transient.tuned_setting(arm, transient_tuning)
        out.append(
            {
                "key": f"{A2A_STAGE}-c{compute_ns // 1000}-{condition}-{arm}-s{A2A_SEED}",
                "stage": A2A_STAGE,
                "arm": arm,
                "setting": setting,
                "k": A2A_FAN_IN,
                "condition": condition,
                "compute_ns": compute_ns,
                "seed": A2A_SEED,
            }
        )
    return out


def a2a_profile(run: dict[str, Any], transient_calibration: dict[str, Any] | None) -> dict[str, Any]:
    """The t_ stages' fabric, transport and arm settings (the switch-queue
    thresholds of their K = 64 calibration) under the all-to-all workload."""
    import transient

    document = profile(
        run["key"], 64, "healthy",
        transient.balancing(run["arm"], 64, transient_calibration, run["setting"]),
    )
    document["steps"] = A2A_PROBE_ITERATIONS
    document["workload"] = {
        "kind": "all_to_all",
        "group": "all",
        "message_bytes": A2A_MESSAGE_BYTES,
        "iterations": A2A_PROBE_ITERATIONS,
        "compute_ns": run["compute_ns"],
    }
    document["network"]["link_failures"] = a2a_failures(run["condition"])
    return document


# Phase 9: the expert-parallel all-to-all on asymmetric and degraded
# fabrics (receiver-driven-spraying docs/astra-sim-engineering-plan.md,
# "Phase 9 specification (2026-10-11)"). Every rank of a group (EP64, every
# host; EP16, the 16 hosts of leaves 0 and 1) runs A2A_ITERATIONS all-to-alls
# of 2 MiB or 256 KiB per pair in a chain, with A2A_COMPUTE_NS of compute
# between them. Faults sit on spine 3 from 100 base RTTs to the end of the
# run, each in one direction of one link, except the structural asymmetry:
# every link of spine 3 at 200 Gbps from t = 0, both directions, never
# restored. The healthy pair of every cell, (f) included, is the run with no
# fault.
#
# Stages:
#
#   a2a_oracle_calibration  the oracle's calibration pass (Gate 2's first
#                           step): healthy graded_switch_queue runs, seeds
#                           101-103, per group and size
#   a2a_gate                the oracle-headroom gate: uniform and the oracle
#                           on the 28 cells and their healthy pairs, seeds
#                           301-303; the oracle joins a group and size once
#                           a2a_calibration.json holds its switch_queue entry
#   a2a_calibration         healthy runs of graded_per_connection and of
#                           graded_per_receiver_mix (w 0.75, loss trace on at
#                           a two-base-RTT interval), seeds 101-103, per
#                           group and size: spine_reports.csv and
#                           spine_losses.csv for the graded passes
#   a2a_tuning              the 96 settings on the headline cell (group,
#                           size and condition, EP64 at 256 KiB loss1e2_up
#                           by default), seed 201, each with its healthy
#                           pair
#   a2a_tuning_mix          a2a_tuning's graded_per_receiver_mix settings
#                           alone, each with its healthy pair, on the same
#                           cell and seed (its keys are a2a_tuning's with
#                           the stage renamed): the mix arm re-tuned once it
#                           propagates the pool hold (A2A_POOL_HOLD_ARMS)
#   a2a_comparison          reps and mrc, and every graded arm a2a_tuning.json
#                           selects, on the admitted cells and their healthy
#                           pairs, seeds 301-303 (uniform and the oracle are
#                           the gate's runs); plus a reading, not a compared
#                           arm: graded_switch_queue_soft (the oracle with
#                           softer loss thresholds, see A2A_SOFT_ORACLE) on
#                           the four EP64 2 MiB loss cells and their healthy
#                           pair, seeds 301-303, whatever the gate admitted
#   a2a_gate_reps           reps_oracle (REPS over the spines the switches'
#                           state shows no fault on) and reps on the 28 cells
#                           and their healthy pairs, seeds 301-303
#
# a2a_calibration.json (read when present) maps a scope (switch_queue,
# per_connection, per_receiver: the pooled pass) and then a group and size
# ("ep64-2m") to congestion_thresholds, trim_thresholds (not for
# switch_queue) and loss_thresholds, the last either one list or a map from
# a loss interval in ns to a list; its "headline" names the tuning cell as
# <group>-<size>-<condition> ("ep64-256k-loss1e2_up"), and
# "headline_reason" says why when it departs from the P17 rule. a2a_tuning.json (read when present) maps each tuned arm
# to its setting (gamma, sender_pool_weight, loss_interval_ns,
# held_sentinel_interval_ns, pool_decay_interval_ns,
# loss_hold_clean_sentinels) and its
# "admitted" lists the cells the gate admitted ("ep64-2m-loss1e2_up"). The
# calibration stages themselves run at the simulator defaults, except for
# held exploration, pool decay and the loss hold (see
# A2A_SENTINEL_INTERVAL_NS and A2A_LOSS_HOLD_CLEAN_SENTINELS), which every
# stage sets.
#
# A run key is <stage>-<group>-<size>-<condition>-<arm>[-<setting>]-s<seed>.

MIB = 1024 * 1024
A2A_GROUPS: dict[str, Any] = {"ep64": "all", "ep16": {"leaves": [0, 1]}}
A2A_GROUP_SIZES = {"ep64": 64, "ep16": 16}
A2A_SIZES = {"2m": 2 * MIB, "256k": 256 * 1024}
A2A_CELLS = tuple(itertools.product(A2A_GROUPS, A2A_SIZES))
# Set from the probe (a2a_probe, run 38170259475 shard s006, healthy, EP64 at
# 512 KiB, 200 us of compute): uniform and the oracle alike took 775.8 us per
# iteration at the median, 40 of 40 iterations, against a 660 us line-rate
# burst (63 x 512 KiB at 50 GB/s), so the measured burst is 1.176 times the
# line-rate one. Each cell's burst t_h is taken as 1.176 times its line-rate
# burst V / 50 GB/s (V the bytes a host sends per iteration), and the compute
# gap is two thirds of it, so the hosts are busy 60 percent of the period
# (t_h / (t_h + compute) = 0.6), rounded to a whole microsecond.
A2A_COMPUTE_NS = {
    ("ep64", "2m"): 2_070_000,  # burst 1.176 x 2.642 ms = 3.107 ms
    ("ep64", "256k"): 259_000,  # 1.176 x 330.3 us = 388.4 us
    ("ep16", "2m"): 493_000,  # 1.176 x 629.1 us = 739.9 us
    ("ep16", "256k"): 62_000,  # 1.176 x 78.6 us = 92.5 us
}
# Iterations per group and size. The probe ran two runs per hosted runner at
# 51 min each for EP64 at 512 KiB over 40 iterations (about 85 GB of data);
# EP64 at 2 MiB keeps to about 100 GB of data per run (64 hosts x 126 MiB x
# 12), the others run the specification's 40.
A2A_ITERATIONS = {
    ("ep64", "2m"): 12,
    ("ep64", "256k"): 40,
    ("ep16", "2m"): 40,
    ("ep16", "256k"): 40,
}
A2A_SPINE = DEGRADED_SPINE
A2A_REPORT_INTERVAL_NS = 2 * BASE_RTT_NS
# Loss-interval candidates: 2, 10 and 40 base RTTs.
A2A_LOSS_INTERVALS_NS = tuple(n * BASE_RTT_NS for n in (2, 10, 40))
# Placeholder until calibration: the fraction one loss reads at about 38
# sends per spine per two-base-RTT interval (the spec's loss-grade
# arithmetic, 0.026), then two and four times it.
A2A_LOSS_THRESHOLDS_PLACEHOLDER = [0.026, 0.052, 0.104]
# Conditions: (a), (b), (c1), (c2), (d), (e), (f).
A2A_FAULTS = (
    "loss1e3_up", "loss1e2_up", "loss1e3_down", "loss1e2_down",
    "blackhole_up", "halfrate_up", "structural",
)
# The tuning cell, <group>-<size>-<condition>. P17 picked (b) or (a) on
# EP64 at 2 MiB, but the oracle-headroom gate dropped every EP64 2 MiB loss
# cell (the oracle, holding a spine without exploration for a 3 ms
# line-rate burst, gives up an eighth of the leaf's uplinks, which costs
# more than the loss), so the headline is (b) on EP64 at 256 KiB (P14
# precedes P17).
A2A_HEADLINE = "ep64-256k-loss1e2_up"
A2A_SEEDS = {"calibration": (101, 102, 103), "tuning": (201,), "comparison": (301, 302, 303)}
A2A_ORACLE = "graded_switch_queue"
A2A_ORACLE_SETTING = {"gamma": 0.5}  # its Phase 8 setting, not retuned
# A reading in a2a_comparison, not a compared arm: the oracle with softer
# loss thresholds, so 1e-3 grades 2 and 1e-2 grades 0, and no exploration
# (as the oracle), on the EP64 2 MiB loss cells the gate dropped, to see
# whether a softer loss hold recovers the headroom there.
A2A_SOFT_ORACLE = "graded_switch_queue_soft"
A2A_SOFT_LOSS_THRESHOLDS = [0.0005, 0.002, 0.008]
A2A_SOFT_CELL = ("ep64", "2m")
A2A_SOFT_CONDITIONS = ("loss1e3_up", "loss1e2_up", "loss1e3_down", "loss1e2_down")
A2A_MIX_WEIGHT = 0.75
A2A_GAMMAS = (0.25, 0.5, 1.0)
A2A_POOL_WEIGHTS = (0.25, 0.5, 0.75)
# Held-spine exploration. The oracle holds a spine with no exploration
# (held_exploration "none"), since its loss hold releases from switch
# state. Every other graded arm sends one sentinel data packet per held
# spine every A2A_SENTINEL_INTERVAL_NS (held_exploration "sentinel"): a
# sentinel lost in a black hole costs its queue pair an RTO, so the
# interval trades blackhole residual against recovery detection; tuning
# sweeps it.
A2A_SENTINEL_INTERVAL_NS = 1_000_000  # 1 ms
A2A_SENTINEL_INTERVALS_NS = (A2A_REPORT_INTERVAL_NS, 10 * BASE_RTT_NS, A2A_SENTINEL_INTERVAL_NS)
# The host-wide pool of an arm with sender_pool_weight above 0 decays once
# per report interval: the pool decays per report otherwise, so with 63
# destinations its memory would be under 2 us.
A2A_POOL_DECAY_INTERVAL_NS = A2A_REPORT_INTERVAL_NS
A2A_POOL_DECAY_INTERVALS_NS = (A2A_REPORT_INTERVAL_NS, 10 * BASE_RTT_NS)
# A loss-held spine releases only after A2A_LOSS_HOLD_CLEAN_SENTINELS
# consecutive clean sentinels (loss_hold_clean_sentinels; every graded arm
# but the oracle, which has no sender loss counting). The expected release
# after n consecutive clean sentinels is ((1-p)^-n - 1)/p sentinels, so at
# 2-RTT sentinels n = 1024 releases a 1e-3 link after about 44 ms and a 1e-2
# link effectively never, while a healed link releases after about 25 ms.
# Tuning sweeps n.
A2A_LOSS_HOLD_CLEAN_SENTINELS = 1024
A2A_LOSS_HOLD_CLEAN_SENTINELS_SWEEP = (256, 1024)
A2A_TUNED_ARMS = (
    "graded_per_connection", "graded_per_receiver",
    "graded_per_receiver_mix", "graded_per_receiver_shared",
)
A2A_FIXED_ARMS = ("reps", "mrc")
A2A_SCOPES = {
    "graded_per_connection": "per_connection",
    "graded_per_receiver": "per_receiver",
    "graded_per_receiver_mix": "per_receiver",
    "graded_per_receiver_shared": "per_receiver",
    "graded_switch_queue": "switch_queue",
    "graded_switch_queue_soft": "switch_queue",
}
# Arms whose pool hold propagates (pool_hold_propagates), in every stage. A
# spine the host-wide pool holds gets no share in any of the host's draws:
# otherwise the mix arm's local part (1 - w of each draw) cannot see the
# pool's hold, so it keeps re-admitting the spine to every destination that
# has not yet seen its own loss there. graded_per_receiver_shared (w = 1)
# draws from the pool alone and does not need it.
A2A_POOL_HOLD_ARMS = ("graded_per_receiver_mix",)
# Stages that run a2a_tuning's settings, and the arms each tunes.
A2A_TUNING_STAGES = {"a2a_tuning": A2A_TUNED_ARMS, "a2a_tuning_mix": ("graded_per_receiver_mix",)}
# reps_oracle: REPS under spray_uniform, leaving out the spines the
# switches' state shows a fault on; REPS's buffer_size and
# freezing_timeout_ns at the defaults the reps arm runs at.
A2A_REPS_ORACLE = "reps_oracle"
A2A_GATE_REPS_ARMS = (A2A_REPS_ORACLE, "reps")
A2A_STAGES = ("a2a_oracle_calibration", "a2a_gate", "a2a_calibration", "a2a_tuning", "a2a_tuning_mix",
              "a2a_comparison", "a2a_gate_reps")


def a2a_cell_name(group: str, size: str) -> str:
    return f"{group}-{size}"


def a2a_headline(calibration: dict[str, Any] | None) -> tuple[str, str, str]:
    """The tuning cell (group, size, condition) from the calibration's
    "headline", <group>-<size>-<condition>, or A2A_HEADLINE."""
    headline = (calibration or {}).get("headline", A2A_HEADLINE)
    parts = headline.split("-", 2) if isinstance(headline, str) else []
    if (len(parts) != 3 or parts[0] not in A2A_GROUPS or parts[1] not in A2A_SIZES
            or parts[2] not in A2A_FAULTS):
        raise ValueError(f"headline {headline!r} is not <group>-<size>-<condition>")
    return parts[0], parts[1], parts[2]


def a2a_fault(condition: str) -> list[dict[str, Any]]:
    """The link_failures of a condition: loss and half rate in one direction
    of one link (up: leaf 0 to spine 3; down: spine 3 to leaf 1), the
    blackhole silent on leaf 0's uplink, and the structural asymmetry the
    spine-wide rate form from t = 0 with no duration."""
    if condition == "healthy":
        return []
    onset = {"start_ns": DEGRADE_AT_NS, "spine": A2A_SPINE}
    if condition == "structural":
        return [{"model": "gray", "rate": "200Gbps", "start_ns": 0, "spine": A2A_SPINE}]
    kind, direction = condition.split("_")
    link = {"leaf": 0 if direction == "up" else 1, "direction": direction}
    if kind in ("loss1e3", "loss1e2"):
        model = {"model": "gray", "error_rate": 1e-3 if kind == "loss1e3" else 1e-2}
    elif kind == "blackhole" and direction == "up":
        model = {"model": "silent"}
    elif kind == "halfrate" and direction == "up":
        model = {"model": "gray", "rate": "200Gbps"}
    else:
        raise ValueError(f"unknown condition {condition}")
    return [{**model, **onset, **link}]


def _a2a_loss_thresholds(entry: dict[str, Any], interval_ns: int) -> list[float]:
    value = entry.get("loss_thresholds")
    if isinstance(value, dict):
        value = value.get(str(interval_ns))
    return list(value) if value else list(A2A_LOSS_THRESHOLDS_PLACEHOLDER)


def a2a_balancing(arm: str, group: str, size: str, calibration: dict[str, Any] | None,
                  setting: dict[str, Any] | None) -> dict[str, Any]:
    """An arm's load balancing: the t_ stages' graded settings (edge window
    penalty 0, edge_freeze off, estimator_interval_samples 0, gain 0.25)
    plus this phase's calibrated thresholds and the loss grade."""
    import transient

    if arm in ("uniform",) + A2A_FIXED_ARMS:
        return balancing(arm, 0, None)
    if arm == A2A_REPS_ORACLE:
        return {"mode": "spray_uniform", "selector": "reps_oracle", "grade_cost_source": "switch_queue"}
    if arm == A2A_SOFT_ORACLE:
        values = a2a_balancing(A2A_ORACLE, group, size, calibration, setting)
        values["loss_thresholds"] = list(A2A_SOFT_LOSS_THRESHOLDS)
        return values
    values = transient.balancing(arm, 0, None, setting)
    entry = (calibration or {}).get(A2A_SCOPES[arm], {}).get(a2a_cell_name(group, size), {})
    for key in ("congestion_thresholds",) + (() if arm == A2A_ORACLE else ("trim_thresholds",)):
        if key in entry:
            values[key] = entry[key]
    pooled = values.get("sender_pool_weight", 0) > 0
    interval = values.get("loss_interval_ns", A2A_REPORT_INTERVAL_NS) if pooled else A2A_REPORT_INTERVAL_NS
    values["loss_thresholds"] = _a2a_loss_thresholds(entry, interval)
    if arm == A2A_ORACLE:
        values["held_exploration"] = "none"
    else:
        values["held_exploration"] = "sentinel"
        values.setdefault("held_sentinel_interval_ns", A2A_SENTINEL_INTERVAL_NS)
        values.setdefault("loss_hold_clean_sentinels", A2A_LOSS_HOLD_CLEAN_SENTINELS)
    if pooled:
        values.setdefault("pool_decay_interval_ns", A2A_POOL_DECAY_INTERVAL_NS)
    if arm in A2A_POOL_HOLD_ARMS:
        values["pool_hold_propagates"] = True
    return values


def a2a_tuning_settings(arm: str) -> list[dict[str, Any]]:
    """An arm's tuning settings: its gamma (and, pooled, w and loss
    interval) grid at the default sentinel and pool decay intervals and
    loss hold, then gamma crossed with every other sentinel and pool decay
    interval, and gamma crossed with every other loss hold, w and the loss
    interval held at the calibration pass's (0.75, the report interval)
    and the other dimensions at their defaults. Crossing all six would make
    468 settings."""
    pooled = arm in ("graded_per_receiver_mix", "graded_per_receiver_shared")
    defaults: dict[str, Any] = {"held_sentinel_interval_ns": A2A_SENTINEL_INTERVAL_NS}
    decays: tuple[int | None, ...] = (None,)
    if pooled:
        defaults["pool_decay_interval_ns"] = A2A_POOL_DECAY_INTERVAL_NS
        decays = A2A_POOL_DECAY_INTERVALS_NS
    if arm == "graded_per_receiver_mix":
        grid = [
            {"gamma": g, "sender_pool_weight": w, "loss_interval_ns": i}
            for g, w, i in itertools.product(A2A_GAMMAS, A2A_POOL_WEIGHTS, A2A_LOSS_INTERVALS_NS)
        ]
        held = {"sender_pool_weight": A2A_MIX_WEIGHT, "loss_interval_ns": A2A_REPORT_INTERVAL_NS}
    elif arm == "graded_per_receiver_shared":
        grid = [{"gamma": g, "loss_interval_ns": i} for g, i in itertools.product(A2A_GAMMAS, A2A_LOSS_INTERVALS_NS)]
        held = {"loss_interval_ns": A2A_REPORT_INTERVAL_NS}
    else:
        grid = [{"gamma": g} for g in A2A_GAMMAS]
        held = {}
    settings = [{**setting, **defaults, "loss_hold_clean_sentinels": A2A_LOSS_HOLD_CLEAN_SENTINELS}
                for setting in grid]
    for g, sentinel, decay in itertools.product(A2A_GAMMAS, A2A_SENTINEL_INTERVALS_NS, decays):
        intervals = {"held_sentinel_interval_ns": sentinel}
        if decay is not None:
            intervals["pool_decay_interval_ns"] = decay
        if intervals != defaults:
            settings.append({"gamma": g, **held, **intervals, "loss_hold_clean_sentinels": A2A_LOSS_HOLD_CLEAN_SENTINELS})
    for g, hold in itertools.product(A2A_GAMMAS, A2A_LOSS_HOLD_CLEAN_SENTINELS_SWEEP):
        if hold != A2A_LOSS_HOLD_CLEAN_SENTINELS:
            settings.append({"gamma": g, **held, **defaults, "loss_hold_clean_sentinels": hold})
    return settings


def a2a_stage_runs(stage: str, calibration: dict[str, Any] | None,
                   tuning: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Every run of a Phase 9 stage."""
    out: list[dict[str, Any]] = []

    def add(arm, group, size, condition, seed, setting=None, label_setting=False):
        label = arm if not label_setting else f"{arm}-{setting_name(setting)}"
        out.append(
            {
                "key": f"{stage}-{group}-{size}-{condition}-{label}-s{seed}",
                "stage": stage,
                "arm": arm,
                "setting": setting,
                "k": A2A_GROUP_SIZES[group] - 1,
                "group": group,
                "size": size,
                "message_bytes": A2A_SIZES[size],
                "condition": condition,
                "compute_ns": A2A_COMPUTE_NS[(group, size)],
                "iterations": A2A_ITERATIONS[(group, size)],
                "seed": seed,
            }
        )

    def calibrated(scope, group, size):
        return a2a_cell_name(group, size) in (calibration or {}).get(scope, {})

    if stage == "a2a_oracle_calibration":
        for (group, size), seed in itertools.product(A2A_CELLS, A2A_SEEDS["calibration"]):
            add(A2A_ORACLE, group, size, "healthy", seed)
    elif stage == "a2a_calibration":
        pooled = {"sender_pool_weight": A2A_MIX_WEIGHT, "loss_interval_ns": A2A_REPORT_INTERVAL_NS}
        for (group, size), seed in itertools.product(A2A_CELLS, A2A_SEEDS["calibration"]):
            add("graded_per_connection", group, size, "healthy", seed)
            add("graded_per_receiver_mix", group, size, "healthy", seed, pooled)
    elif stage == "a2a_gate":
        for (group, size), seed in itertools.product(A2A_CELLS, A2A_SEEDS["comparison"]):
            arms = [("uniform", None)]
            if calibrated("switch_queue", group, size):
                arms.append((A2A_ORACLE, A2A_ORACLE_SETTING))
            for arm, setting in arms:
                for condition in ("healthy",) + A2A_FAULTS:
                    add(arm, group, size, condition, seed, setting)
    elif stage == "a2a_gate_reps":
        for (group, size), seed, arm in itertools.product(A2A_CELLS, A2A_SEEDS["comparison"], A2A_GATE_REPS_ARMS):
            for condition in ("healthy",) + A2A_FAULTS:
                add(arm, group, size, condition, seed)
    elif stage in A2A_TUNING_STAGES:
        group, size, headline = a2a_headline(calibration)
        for seed, arm in itertools.product(A2A_SEEDS["tuning"], A2A_TUNING_STAGES[stage]):
            for setting in a2a_tuning_settings(arm):
                for condition in (headline, "healthy"):
                    add(arm, group, size, condition, seed, setting, True)
    elif stage == "a2a_comparison":
        admitted = (tuning or {}).get("admitted")
        cells = [
            (group, size, condition)
            for (group, size), condition in itertools.product(A2A_CELLS, A2A_FAULTS)
            if admitted is None or f"{a2a_cell_name(group, size)}-{condition}" in admitted
        ]
        pairs = [cell for cell in A2A_CELLS if any(c[:2] == cell for c in cells)]
        arms = [(arm, (tuning or {}).get(arm)) for arm in A2A_TUNED_ARMS if (tuning or {}).get(arm)]
        arms += [(arm, None) for arm in A2A_FIXED_ARMS]
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], arms):
            for group, size, condition in cells:
                add(arm, group, size, condition, seed, setting)
            for group, size in pairs:
                add(arm, group, size, "healthy", seed, setting)
        # The soft oracle reading (A2A_SOFT_ORACLE), not a compared arm.
        group, size = A2A_SOFT_CELL
        for seed in A2A_SEEDS["comparison"]:
            for condition in A2A_SOFT_CONDITIONS + ("healthy",):
                add(A2A_SOFT_ORACLE, group, size, condition, seed, A2A_ORACLE_SETTING)
    else:
        raise ValueError(f"unknown stage {stage}")
    return out


def a2a_stage_profile(run: dict[str, Any], calibration: dict[str, Any] | None) -> dict[str, Any]:
    """The t_ stages' fabric and transport under the all-to-all workload;
    the calibration stages run without this phase's calibration."""
    if run["stage"] in ("a2a_oracle_calibration", "a2a_calibration"):
        calibration = None
    group, size = run["group"], run["size"]
    document = profile(
        run["key"], 64, "healthy",
        a2a_balancing(run["arm"], group, size, calibration, run["setting"]),
    )
    document["steps"] = run["iterations"]
    document["workload"] = {
        "kind": "all_to_all",
        "group": A2A_GROUPS[group],
        "message_bytes": run["message_bytes"],
        "iterations": run["iterations"],
        "compute_ns": run["compute_ns"],
    }
    document["network"]["link_failures"] = a2a_fault(run["condition"])
    return document


def write_listing(out: Path, documents: list[tuple[dict[str, Any], dict[str, Any]]]) -> int:
    """Write every (run, profile) pair and runs.json; return the count."""
    listed = []
    for run, document in documents:
        path = out / f"{run['key']}.json"
        path.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
        listed.append({**run, "profile": path.name})
    (out / "runs.json").write_text(json.dumps(listed, indent=1) + "\n", encoding="utf-8")
    print(len(listed))
    return 0


def write_transient(out: Path, stage: str, phase_tuning, calibration, tuning) -> int:
    """The t_* stages; phase_tuning (--tuning) gives redirect_per_connection
    its setting in t_comparison."""
    import transient

    redirect = (phase_tuning or {}).get(transient.REDIRECT_ARM)
    return write_listing(
        out,
        [(run, transient.profile(run, calibration)) for run in transient.runs(stage, tuning, redirect)],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("write",))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--calibration", type=Path)
    parser.add_argument("--tuning", type=Path)
    parser.add_argument("--transient-calibration", type=Path)
    parser.add_argument("--transient-tuning", type=Path)
    parser.add_argument("--a2a-calibration", type=Path)
    parser.add_argument("--a2a-tuning", type=Path)
    arguments = parser.parse_args()

    def read(path: Path | None) -> dict[str, Any] | None:
        return json.loads(path.read_text()) if path else None

    calibration = read(arguments.calibration)
    tuning = read(arguments.tuning)
    arguments.out.mkdir(parents=True, exist_ok=True)
    if arguments.stage == A2A_STAGE:
        transient_calibration = read(arguments.transient_calibration)
        return write_listing(
            arguments.out,
            [(run, a2a_profile(run, transient_calibration)) for run in a2a_runs(read(arguments.transient_tuning))],
        )
    if arguments.stage in A2A_STAGES:
        a2a_calibration = read(arguments.a2a_calibration)
        return write_listing(
            arguments.out,
            [(run, a2a_stage_profile(run, a2a_calibration))
             for run in a2a_stage_runs(arguments.stage, a2a_calibration, read(arguments.a2a_tuning))],
        )
    if arguments.stage.startswith("t_"):
        return write_transient(arguments.out, arguments.stage, tuning,
                               read(arguments.transient_calibration), read(arguments.transient_tuning))
    documents = []
    for run in runs(arguments.stage, calibration, tuning):
        shape = (
            {"message_bytes": SHORT_MESSAGE_BYTES, "degrade_at_ns": SHORT_DEGRADE_AT_NS}
            if run["stage"] == "short"
            else {}
        )
        document = profile(
            run["key"], run["k"], run["condition"],
            balancing(run["arm"], run["k"], calibration, run["setting"]), **shape,
        )
        documents.append((run, document))
    return write_listing(arguments.out, documents)

if __name__ == "__main__":
    raise SystemExit(main())
