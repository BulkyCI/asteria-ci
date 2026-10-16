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
stages after it, and the ar_parity_* stages of the ring, are Phase 9's (see
a2a_stage_runs); they read
--a2a-calibration and --a2a-tuning (incast/a2a_calibration.json and
incast/a2a_tuning.json) when given. Standard library only.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
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
#   a2a_candidates          the A2A_CANDIDATE_ARMS on the 28 cells of the
#                           gate and their healthy pairs, seeds 301-303
#   a2a_receiver            the A2A_RECEIVER_ARMS on the 28 cells and their
#                           healthy pairs, seeds 301-303
#   a2a_rs3                 the A2A_RS3_ARMS under a flapping gray link
#                           (A2A_RS3_CONDITION) on the A2A_RS3_CELLS and
#                           their healthy pairs, seeds 301-303, at
#                           A2A_RS3_ITERATIONS
#   a2a_tail                the A2A_TAIL_ARMS on every loss and blackhole
#                           cell of the gate (a2a_tail_cells) and their
#                           healthy pairs, seeds 301-303
#   a2a_ackloss             the A2A_ACKLOSS_ARMS on the A2A_ACKLOSS_CELLS
#                           (blackhole_up on every group and size, the four
#                           loss conditions on EP64 at 256 KiB) and their
#                           healthy pairs, seeds 301-303
#   a2a_candidates_ack      the A2A_CANDIDATES_ACK_ARMS on the 28 cells of
#                           the gate and their healthy pairs, seeds 301-303
#   a2a_tail_ack            the A2A_TAIL_ACK_STAGE_ARMS on the 28 cells of
#                           the gate and their healthy pairs, seeds 301-303
#
# and, on the settled fixture (A2A_FIXTURE_4K), each refused but the probe
# until A2A_PAYLOAD_COMPUTE_NS holds the probe's gaps for its pairs,
# and the faulted stages also until a2a_calibration.json has a per_receiver
# entry at that payload for every (group, size) they run off one leaf:
#
#   a2a_probe_4k            one healthy uniform_tail run, seed 301, per
#                           (group, size) of A2A_PROBE_4K_COMPUTE_NS, at its
#                           provisional compute gap
#   a2a_calibration_4k      a2a_calibration's per_receiver pass on the
#                           A2A_CALIBRATION_4K_CELLS
#   a2a_tail_ack_4k         the A2A_ARMS_4K on the 28 cells of the gate and
#                           their healthy pairs, seeds 301-303
#   a2a_sweep_4k            the A2A_ARMS_4K on the A2A_SWEEP_CELLS, the
#                           A2A_SWEEP_ANCHOR and the A2A_SWEEP_HANDOVER cell
#                           and their healthy pairs, seeds 301-303, but
#                           those of a2a_tail_ack_4k (A2A_SHARED_4K)
#   a2a_downlink_4k         the A2A_DOWNLINK_ARMS on the A2A_DOWNLINK_CELLS
#                           and their healthy pairs, seeds 301-303
#   a2a_decode_probe_4k     one healthy uniform_tail run, seed 301, of the
#                           decode cell (A2A_DECODE), at its provisional
#                           compute gap
#   a2a_decode_calibration_4k
#                           a2a_calibration's per_receiver pass on the
#                           decode cell
#   a2a_decode_4k           the A2A_ARMS_4K on the A2A_DECODE_CELLS and their
#                           healthy pair, seeds 301-303
#   a2a_reaction_4k         the A2A_REACTION_ARMS on the A2A_REACTION_CELLS
#                           and their healthy pairs, seeds 301-303
#   ar_parity_probe_4k      one healthy uniform_tail run, seed 301, of the
#                           ring (A2A_RING)
#   ar_parity_calibration_4k
#                           a2a_calibration's per_receiver pass on the ring
#   ar_parity_4k            the A2A_ARMS_4K on the ring at loss 1e-2 up and
#                           its healthy pair, seeds 301-303
#   a2a_ackroute_4k         the A2A_ACKROUTE_ARMS, every answer on a spine
#                           its receiver draws, on the A2A_ACKROUTE_CELLS and
#                           their healthy pairs, seeds 301-303
#   a2a_noack_4k            the A2A_NOACK_ARMS on every faulted cell of the
#                           A2A_NOACK_SOURCES and their healthy pairs, seeds
#                           301-303
#   a2a_hostlink_4k         the A2A_HOSTLINK_ARMS under a lossy host access
#                           link on the A2A_HOSTLINK_CELLS and their healthy
#                           pairs, seeds 301-303
#
# a2a_calibration.json (read when present) maps a scope (switch_queue,
# per_connection, per_receiver: the pooled pass) and then a group and size
# ("ep64-2m", or "ep64-2m-p4096" on the settled fixture; a2a_cell_name) to
# congestion_thresholds, trim_thresholds (not for switch_queue) and
# loss_thresholds, the last either one list or a map from a loss interval in
# ns to a list; its "headline" names the tuning cell as
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
# A condition <kind>_<direction>: the error rate of each loss kind, and the
# rate of each lower-rate kind. The gate's are loss1e3, loss1e2 and halfrate;
# the sweep adds loss3e3, loss3e2 and rate16g, 16 Gbps, four percent of the
# link's rate.
A2A_LOSS_RATES = {"loss1e3": 1e-3, "loss3e3": 3e-3, "loss1e2": 1e-2, "loss3e2": 3e-2}
A2A_LINK_RATES = {"halfrate": "200Gbps", "rate16g": "16Gbps"}
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
# The pooled calibration pass's setting: graded_per_receiver_mix at the mix
# weight, its loss trace at the report interval.
A2A_CALIBRATION_POOLED = {"sender_pool_weight": A2A_MIX_WEIGHT, "loss_interval_ns": A2A_REPORT_INTERVAL_NS}
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
# a2a_candidates' arms. The loss-hold arms (spray_uniform, selector
# reps_hold, reps_flow_hold or mrc_hold, REPS's buffer_size and
# freezing_timeout_ns, or MRC's skip_base_rtts and probe_timeouts as the mrc
# arm runs them, at their defaults, held_exploration sentinel) take
# loss_interval_ns, loss_hold_clean_sentinels and held_sentinel_interval_ns
# from a2a_tuning.json's graded_per_receiver_mix setting and its
# loss_thresholds at that interval from the per_receiver calibration, as the
# mix arm does; hold_scope keys the hold of reps_hold and mrc_hold per host,
# or per spine per host or per destination leaf (auto), and reps_flow_hold
# holds per queue pair. Every loss-hold arm keeps the count rule and the
# clean sentinels' release and sends its sentinels as copies of data
# packets (A2A_HELD_EXPLORATION). The floor arms are a2a_tuning.json's
# setting of their base arm with congestion_grade_floor 1 and nothing else:
# mix_floor1 leaves pool_hold_propagates unset.
A2A_REPS_HOLD_ARMS = {
    "reps_hold_auto": ("reps_hold", "auto"),
    "reps_hold_host": ("reps_hold", "host"),
    "reps_flow_hold": ("reps_flow_hold", None),
    "mrc_hold_auto": ("mrc_hold", "auto"),
}
A2A_HELD_EXPLORATION = {"sentinel_mode": "duplicate", "hold_release": "sentinels"}
# a2a_receiver's arms: each a candidate arm's setting, with the receiver's
# hold (path sequences in the data headers, the receiver's hold reported in
# every acknowledgement, released after A2A_RECEIVER_HOLD_CLEAN clean
# arrivals) or without it.
A2A_RECEIVER_HOLD_CLEAN = 1024
A2A_RECEIVER_HOLD = {"path_seq": True, "receiver_hold": True,
                     "receiver_hold_clean": A2A_RECEIVER_HOLD_CLEAN}
A2A_RECEIVER_ARMS = {
    "reps_hold_auto_rx": ("reps_hold_auto", True),
    "reps_hold_auto": ("reps_hold_auto", False),
    "reps_flow_hold_rx": ("reps_flow_hold", True),
}
A2A_FLOOR_ARMS = {
    "shared_floor1": "graded_per_receiver_shared",
    "mix_floor1": "graded_per_receiver_mix",
}
A2A_CONGESTION_GRADE_FLOOR = 1
A2A_CANDIDATE_ARMS = tuple(A2A_REPS_HOLD_ARMS) + tuple(A2A_FLOOR_ARMS)
# a2a_rs3: leaf 0's uplink to spine 3 at loss 1e-2 for A2A_RS3_ON_NS, then
# healthy for A2A_RS3_OFF_NS, from the onset (100 base RTTs) on, one
# link_failures entry with duration_ns, repeat and period_ns (the backend
# restores each on-period's loss at its end and fails the link again at the
# next start). The run covers A2A_RS3_CYCLES cycles after the onset at the
# healthy period per iteration of A2A_RS3_PERIOD_NS (mean iteration
# completion of the healthy runs of the cell plus compute_ns), and the
# cycles repeat until twice that healthy run length, so a run the fault
# slows down stays under the flapping link to its end.
A2A_RS3_STAGE = "a2a_rs3"
A2A_RS3_CONDITION = "flap1e2_up"
A2A_RS3_ERROR_RATE = 1e-2
A2A_RS3_ON_NS = 50_000_000
A2A_RS3_OFF_NS = 200_000_000
A2A_RS3_CYCLE_NS = A2A_RS3_ON_NS + A2A_RS3_OFF_NS
A2A_RS3_CYCLES = 3
A2A_RS3_PERIOD_NS = {
    ("ep16", "2m"): 1_232_500,  # 739.5 us completion + 493 us compute
}
A2A_RS3_CELLS = tuple(A2A_RS3_PERIOD_NS)
A2A_RS3_ITERATIONS = {
    cell: math.ceil((DEGRADE_AT_NS + A2A_RS3_CYCLES * A2A_RS3_CYCLE_NS) / period)
    for cell, period in A2A_RS3_PERIOD_NS.items()
}
A2A_RS3_REPEAT = {
    cell: math.ceil((2 * A2A_RS3_ITERATIONS[cell] * period - DEGRADE_AT_NS) / A2A_RS3_CYCLE_NS)
    for cell, period in A2A_RS3_PERIOD_NS.items()
}
# reps_hold_auto's loss hold under the llr rule and the trickle release:
# reps_hold_auto's loss interval, without the clean sentinels and the
# sentinel interval the trickle release does not read.
A2A_TRICKLE_ARMS = {"reps_hold_auto_trickle": "reps_hold_auto"}
A2A_TRICKLE_HOLD = {
    "hold_rule": "llr", "loss_p0": 0.0, "loss_p1": 0.001, "hold_alpha": 0.001, "hold_beta": 0.001,
    "hold_release": "trickle", "hold_trickle": 0.01, "sentinel_mode": "duplicate", "path_seq": False,
}
A2A_RS3_ARMS = ("reps_hold_auto", "reps_hold_auto_trickle", "reps", "uniform")
# a2a_tail's arms: tail recovery (A2A_TAIL: path sequences in the data
# headers, and the closing packets' per-spine counts from which the receiver
# requests each missing run, at the backend's default delay) on a base arm:
# the uniform draw; reps_spine, REPS over the spine indices under
# spray_uniform with no loss hold, no sentinels and no loss keys (the backend
# takes PATH_SELECTOR reps only under ev_hash, and the generator refuses
# tail_recovery there); and reps_hold_auto with its a2a_candidates setting,
# the count rule, the clean sentinels' release and duplicate sentinels;
# reps_hold_auto_tail4 is reps_hold_auto_tail with tail_loss_rtts 4
# (A2A_TAIL_KEYS), written as SPRAY_TAIL_LOSS_RTTS. The cells: every gate
# cell under a loss or blackhole condition, whatever a2a_tuning.json admits.
A2A_TAIL_STAGE = "a2a_tail"
A2A_TAIL = {"path_seq": True, "tail_recovery": True}
A2A_REPS_SPINE = "reps_spine"
A2A_TAIL_ARMS = {"uniform_tail": "uniform", "reps_tail": A2A_REPS_SPINE, "reps_hold_auto_tail": "reps_hold_auto",
                 "reps_hold_auto_tail4": "reps_hold_auto"}
A2A_TAIL_LOSS_RTTS = 4
# The keys an a2a_tail arm adds to A2A_TAIL.
A2A_TAIL_KEYS = {"reps_hold_auto_tail4": {"tail_loss_rtts": A2A_TAIL_LOSS_RTTS}}
A2A_TAIL_CONDITIONS = ("loss1e3_up", "loss1e2_up", "loss1e3_down", "loss1e2_down", "blackhole_up")
# a2a_ackloss's arms: ack_loss_rtts (A2A_ACKLOSS: load_balancing
# ack_loss_rtts, written as SPRAY_ACK_LOSS_RTTS) on a base arm:
# reps_hold_auto with its a2a_candidates setting, and reps_spine as a2a_tail's
# reps_tail runs it, without path_seq and tail_recovery. The cells:
# blackhole_up on every group and size, and the four loss conditions on EP64
# at 256 KiB.
A2A_ACKLOSS_STAGE = "a2a_ackloss"
A2A_ACK_LOSS_RTTS = 2
A2A_ACKLOSS = {"ack_loss_rtts": A2A_ACK_LOSS_RTTS}
A2A_ACKLOSS_ARMS = {"reps_hold_auto_ack": "reps_hold_auto", "reps_spine_ack": A2A_REPS_SPINE}
A2A_ACKLOSS_CELLS = tuple(
    [(group, size, "blackhole_up") for group, size in A2A_CELLS]
    + [("ep64", "256k", condition) for condition in ("loss1e2_up", "loss1e3_up", "loss1e2_down", "loss1e3_down")]
)
# a2a_candidates_ack's arms: A2A_ACKLOSS on the a2a_candidates setting of
# reps_hold_auto, reps_flow_hold and mrc_hold_auto (reps_hold_auto_ack is
# a2a_ackloss's arm of that name), on a2a_candidates' 28 cells.
A2A_CANDIDATES_ACK_STAGE = "a2a_candidates_ack"
A2A_CANDIDATES_ACK_ARMS = {"reps_hold_auto_ack": "reps_hold_auto", "reps_flow_hold_ack": "reps_flow_hold",
                           "mrc_hold_auto_ack": "mrc_hold_auto"}
# Every ack_loss_rtts arm and its base arm.
A2A_ACK_ARMS = {**A2A_ACKLOSS_ARMS, **A2A_CANDIDATES_ACK_ARMS}
# The arms of a2a_tail_ack and the stages after it, which all run one
# transport: A2A_TAIL with tail_loss_rtts A2A_TAIL_LOSS_RTTS, as
# reps_hold_auto_tail4 does. The loss-hold arms add A2A_ACKLOSS to their base
# arm's a2a_candidates setting: reps_hold_auto, reps_flow_hold and
# mrc_hold_auto, and reps_hold_leaf_tail_ack and reps_hold_pair_tail_ack,
# reps_hold_auto's setting with hold_scope leaf (a hold per destination leaf
# and spine, never widened to every destination) or pair (a hold per
# destination host and spine, shared by the queue pairs to that host, so it
# outlasts each message's queue pair), and reps_hold_auto_tail_ack_rx,
# reps_hold_auto_tail_ack's setting with the receiver's hold
# (A2A_RECEIVER_HOLD) also counting as missing the sends a tail repair
# request shows unanswered after the tail_loss_rtts guard
# (receiver_tail_missing), so that a black-holed spine, on which no data
# arrives to show a gap, is held by its receivers too. The baselines add the transport alone to reps_spine
# (reps_tail), reps_oracle (reps_oracle_tail) and the uniform draw
# (uniform_tail), so reps_tail and uniform_tail are a2a_tail's arms of those
# names with tail_loss_rtts set. tail_loss_rtts only delays the path
# selector's report of a loss a tail repair request declares, which the
# uniform draw and REPS do not read, and lengthens the time in which a repair
# of that send is moved off the declared send's path, so the baselines carry
# it too and every arm's repairs follow the same rule. a2a_tail_ack runs
# A2A_TAIL_ACK_STAGE_ARMS on the 28 cells of the gate and their healthy
# pairs; reps_hold_pair_tail_ack runs in no stage (A2A_HOLD_ARMS_4K).
A2A_TAIL_ACK_STAGE = "a2a_tail_ack"
A2A_TAIL_ACK_ARMS = {
    "reps_hold_auto_tail_ack": "reps_hold_auto", "reps_hold_leaf_tail_ack": "reps_hold_auto",
    "reps_hold_pair_tail_ack": "reps_hold_auto", "reps_hold_auto_tail_ack_rx": "reps_hold_auto",
    "reps_flow_hold_tail_ack": "reps_flow_hold", "mrc_hold_auto_tail_ack": "mrc_hold_auto",
    "reps_tail": A2A_REPS_SPINE, "reps_oracle_tail": A2A_REPS_ORACLE, "uniform_tail": "uniform",
}
# The keys an A2A_TAIL_ACK_ARMS arm adds to A2A_TAIL.
A2A_TAIL_ACK_TRANSPORT = {"tail_loss_rtts": A2A_TAIL_LOSS_RTTS}
A2A_TAIL_ACK_HOLD = {**A2A_TAIL_ACK_TRANSPORT, **A2A_ACKLOSS}
A2A_TAIL_ACK_KEYS = {
    "reps_hold_auto_tail_ack": A2A_TAIL_ACK_HOLD,
    "reps_hold_leaf_tail_ack": {**A2A_TAIL_ACK_HOLD, "hold_scope": "leaf"},
    "reps_hold_pair_tail_ack": {**A2A_TAIL_ACK_HOLD, "hold_scope": "pair"},
    "reps_hold_auto_tail_ack_rx": {**A2A_TAIL_ACK_HOLD, **A2A_RECEIVER_HOLD, "receiver_tail_missing": True},
    "reps_flow_hold_tail_ack": A2A_TAIL_ACK_HOLD,
    "mrc_hold_auto_tail_ack": A2A_TAIL_ACK_HOLD,
    "reps_tail": A2A_TAIL_ACK_TRANSPORT,
    "reps_oracle_tail": A2A_TAIL_ACK_TRANSPORT,
    "uniform_tail": A2A_TAIL_ACK_TRANSPORT,
}
A2A_TAIL_ACK_STAGE_ARMS = ("reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack",
                           "reps_tail", "uniform_tail")
# Every arm that adds transport keys (tail recovery, ack_loss_rtts,
# ack_route_oracle) to a base arm, and that base arm.
A2A_TRANSPORT_ARMS = {**A2A_TAIL_ARMS, **A2A_ACK_ARMS, **A2A_TAIL_ACK_ARMS,
                      "reps_hold_auto_tail4_route_oracle": "reps_hold_auto", "reps_oracle_tail_route_oracle":
                      A2A_REPS_ORACLE, "reps_hold_auto_tail_ack_data_spine": "reps_hold_auto",
                      "reps_hold_leaf_tail4": "reps_hold_auto", "reps_flow_hold_tail4": "reps_flow_hold"}
# The settled fixture. The stages named *_4k and the ring's run on it, where
# every stage above runs on the generator's defaults; a stage takes it from
# A2A_STAGE_FIXTURE, and each of its runs carries it as fixture, less the
# keys the run cannot take (a2a_fixture), which a2a_stage_profile writes:
#   payload_bytes   network.packet_payload_bytes: 4096 B, the largest RDMA
#                   path MTU
#   path_state      network.load_balancing.path_state: connection, every path
#                   selector's state (REPS's cache, a loss hold's exploration,
#                   reps_flow_hold's own hold) kept per destination host and
#                   priority across messages; spray_uniform only, so not for
#                   the spray_policy calibration arm
#   window_state    network.congestion_control.window_state: connection, one
#                   NSCC window per destination host, opened once and kept
#                   across messages
#   dataset_splits  workload.dataset_splits: 1, each pair's message sent as
#                   one flow per collective; all_to_all only, so the ring keeps
#                   its workload's 4 chunks
#   loss_unit       link_failures[].loss_unit: byte on every gray failure with
#                   an error_rate, a packet of s bytes lost with
#                   1 - (1 - r)^s and a full data frame at the condition's rate
# Loss thresholds (from the healthy sends per spine per interval) and compute
# gaps (from the healthy burst) depend on it, so a run on it reads its
# calibration from the cells a2a_cell_name names with its payload
# (<group>-<size>-p4096) and its compute gap from A2A_PAYLOAD_COMPUTE_NS.
A2A_FIXTURE_4K: dict[str, Any] = {"payload_bytes": 4096, "path_state": "connection", "window_state": "connection",
                                  "dataset_splits": 1, "loss_unit": "byte"}
A2A_PAYLOAD_4K = A2A_FIXTURE_4K["payload_bytes"]
# Every group and size a run names: the gate's (A2A_GROUPS, A2A_SIZES and
# A2A_ITERATIONS), which the earlier stages run, and those the sweep adds:
# EP8, the hosts of leaf 0, and EP32, those of leaves 0 to 3; 64 KiB, 1 MiB
# and 4 MiB per pair. EP32 at 4 MiB runs 12 iterations, as EP64 at 2 MiB
# does: 124 MiB per host per iteration, near EP64 at 2 MiB's 126 MiB.
A2A_RUN_GROUPS: dict[str, Any] = {**A2A_GROUPS, "ep8": {"leaves": [0]}, "ep32": {"leaves": [0, 1, 2, 3]}}
A2A_RUN_GROUP_SIZES = {**A2A_GROUP_SIZES, "ep8": 8, "ep32": 32, "dp64": 64}
A2A_RUN_SIZES = {**A2A_SIZES, "64k": 64 * 1024, "1m": MIB, "4m": 4 * MIB, "decode8": 59_264,
                 "ar25m": 25 * MIB}
A2A_RUN_ITERATIONS = {**A2A_ITERATIONS, ("ep8", "256k"): 40, ("ep32", "256k"): 40, ("ep64", "64k"): 40,
                      ("ep64", "1m"): 40, ("ep32", "4m"): 12, ("ep64", "decode8"): 58, ("dp64", "ar25m"): 12}
# The decode cell (A2A_DECODE, size decode8): one iteration is a decode layer
# exchange of EP64 at 8 token copies per pair, a dispatch all-to-all of 59,264
# B per pair (8 copies of 7,408 B: a 7,168-element FP8 hidden state with its
# scales and header), expert_compute_ns of expert compute, a combine
# all-to-all of combine_bytes per pair (8 copies of 14,336 B, the hidden
# state in BF16), then compute_ns, the rest of the layer, before the next
# dispatch. Both all-to-alls carry the iteration's step, so its completion
# runs from the dispatch's first start to the combine's last end. 58
# iterations: 29 MoE layers by 2 micro-batches, whose exchanges alternate on
# one stream. Each run of the size carries combine_bytes and
# expert_compute_ns, written into the workload.
A2A_DECODE = ("ep64", "decode8")
A2A_COMBINE = {"decode8": {"combine_bytes": 114_688, "expert_compute_ns": 129_000}}
# The ring (A2A_RING): workload sequential_dp_all_reduce, one chained ring
# all-reduce of dp_all_reduce_bytes (the size's bytes, 25 MiB, a gradient
# bucket) per step over the 64 hosts in rank order, 12 steps, no compute,
# which the workload requires. Its message to the next rank in each ring step
# is a fan-out-1 flow, the parity control for the all-to-all's fan-out.
A2A_RING = ("dp64", "ar25m")
# A group on one leaf (EP8) sends nothing up a spine: it has no loss trace to
# calibrate, its hold arms read A2A_LOSS_THRESHOLDS_PLACEHOLDER, and none of
# its sends crosses the faulted link.
A2A_ONE_LEAF_GROUPS = ("ep8",)
# a2a_probe_4k's provisional compute gap of every (group, size) a 4096-byte
# stage runs: A2A_COMPUTE_NS's rule, two thirds of 1.176 times the line-rate
# burst, rounded to a whole microsecond (A2A_COMPUTE_NS itself for the gate's
# pairs).
A2A_PROBE_4K_STAGE = "a2a_probe_4k"
A2A_PROBE_4K_COMPUTE_NS = {
    **A2A_COMPUTE_NS,
    ("ep8", "256k"): 29_000,  # burst 1.176 x 36.7 us = 43.2 us
    ("ep32", "256k"): 127_000,  # 1.176 x 162.5 us = 191.1 us
    ("ep64", "64k"): 65_000,  # 1.176 x 82.6 us = 97.1 us
    ("ep64", "1m"): 1_036_000,  # 1.176 x 1.321 ms = 1.554 ms
    ("ep32", "4m"): 2_039_000,  # 1.176 x 2.601 ms = 3.058 ms
}
# The compute gap per payload and (group, size): two thirds of the median
# iteration completion of the pair's probe run at that payload, rounded to a
# whole microsecond, as analyze.py a2a_compute_gaps prints it. A pair's value
# is taken as measured once the pair is listed in the payload's
# A2A_PAYLOAD_COMPUTE_MEASURED; until then a2a_stage_runs refuses every stage
# at the payload that runs the pair, but its probe.
# a2a_decode_probe_4k's provisional compute gap of the decode cell, the rest
# of a 711 us layer exchange period (the period analyze.py a2a_compute_gaps
# holds) after an estimated 467 us healthy exchange.
A2A_DECODE_PROBE_STAGE = "a2a_decode_probe_4k"
A2A_DECODE_PROBE_COMPUTE_NS = {A2A_DECODE: 244_000}
# The gaps measured at 4096 B on the settled fixture: a2a_probe_4k (run
# 38792714034) and a2a_decode_probe_4k (run 38792719213), every run complete,
# as analyze.py a2a_compute_gaps prints them.
A2A_MEASURED_4K_COMPUTE_NS = {
    ("ep64", "2m"): 1_816_000,  # median iteration 2,723.9 us
    ("ep64", "256k"): 236_000,  # 353.4 us
    ("ep16", "2m"): 439_000,  # 658.0 us
    ("ep16", "256k"): 62_000,  # 93.5 us
    ("ep8", "256k"): 28_000,  # 41.7 us
    ("ep32", "256k"): 120_000,  # 180.2 us
    ("ep64", "64k"): 66_000,  # 99.4 us
    ("ep64", "1m"): 913_000,  # 1,369.2 us
    ("ep32", "4m"): 1_787_000,  # 2,680.0 us
    A2A_DECODE: 330_000,  # 711 us period after a 381.9 us median layer exchange
}
A2A_PAYLOAD_COMPUTE_NS = {A2A_PAYLOAD_4K: {**A2A_MEASURED_4K_COMPUTE_NS, A2A_RING: 0}}
# The ring's gap is 0 by its workload's rule, so nothing measures it.
A2A_PAYLOAD_COMPUTE_MEASURED: dict[int, tuple[tuple[str, str], ...]] = {
    A2A_PAYLOAD_4K: (*A2A_MEASURED_4K_COMPUTE_NS, A2A_RING)}
# a2a_calibration_4k: a2a_calibration's per_receiver pass (the healthy
# graded_per_receiver_mix runs at A2A_CALIBRATION_POOLED, seeds 101-103, at the
# simulator's thresholds) on every (group, size) of a2a_probe_4k off one leaf,
# so that analyze.py a2a_calibrate adds their 4096-byte cells to
# a2a_calibration.json by the gate cells' rule.
A2A_CALIBRATION_4K_STAGE = "a2a_calibration_4k"
A2A_CALIBRATION_4K_CELLS = tuple(cell for cell in A2A_PROBE_4K_COMPUTE_NS if cell[0] not in A2A_ONE_LEAF_GROUPS)
# The arms of the 4096-byte stages, each with its A2A_TAIL_ACK_ARMS setting:
# the loss-hold arms, then the baselines. reps_hold_pair_tail_ack is not
# among them: under path_state connection a sender has one connection per
# destination host, so reps_flow_hold_tail_ack's hold per flow is already a
# hold per destination host and spine that outlasts each message.
A2A_HOLD_ARMS_4K = ("reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack")
A2A_ARMS_4K = A2A_HOLD_ARMS_4K + ("reps_tail", "reps_oracle_tail", "uniform_tail")
# a2a_tail_ack_4k: a2a_tail_ack's arms at 4096 B, with reps_oracle_tail, so
# that the gate's reading of reps_oracle against REPS is taken again at this
# payload, on the 28 cells of the gate and their healthy pairs.
A2A_TAIL_ACK_4K_STAGE = "a2a_tail_ack_4k"
# a2a_sweep_4k: one gray loss on leaf 0's uplink to spine 3, from the onset
# to the end of the run, swept over three axes through EP64 at 256 KiB and
# 1e-2: the group at 256 KiB and 1e-2 (EP8, whose all-to-all stays on leaf 0
# and crosses no spine, then EP16, EP32 and EP64, 0, 8, 24 and 56 peers of a
# leaf 0 host off its leaf), the size at EP64 and 1e-2 (64 KiB, 256 KiB, 1 MiB
# and 2 MiB per pair), the loss rate at EP64 and 256 KiB (1e-3, 3e-3, 1e-2
# and 3e-2), and the four corners of the size by loss plane at EP64
# (A2A_SWEEP_CELLS). Two cells besides: A2A_SWEEP_ANCHOR, EP32 at 4 MiB and
# 3e-2, where a pair expects more than three losses on the spine per
# iteration; and A2A_SWEEP_HANDOVER, EP64 at 256 KiB with the same uplink at
# 16 Gbps and no loss, a link degraded to a few percent of its rate rather
# than lossy, on EP64 at 256 KiB's healthy pairs. Every cell runs the
# A2A_ARMS_4K with healthy pairs, seeds 301-303, but those a2a_tail_ack_4k
# runs (A2A_SHARED_4K).
A2A_SWEEP_STAGE = "a2a_sweep_4k"
A2A_SWEEP_CELLS = (
    ("ep8", "256k", "loss1e2_up"), ("ep16", "256k", "loss1e2_up"), ("ep32", "256k", "loss1e2_up"),
    ("ep64", "256k", "loss1e2_up"), ("ep64", "64k", "loss1e2_up"), ("ep64", "1m", "loss1e2_up"),
    ("ep64", "2m", "loss1e2_up"), ("ep64", "256k", "loss1e3_up"), ("ep64", "256k", "loss3e3_up"),
    ("ep64", "256k", "loss3e2_up"), ("ep64", "64k", "loss1e3_up"), ("ep64", "64k", "loss3e2_up"),
    ("ep64", "2m", "loss1e3_up"), ("ep64", "2m", "loss3e2_up"),
)
A2A_SWEEP_ANCHOR = ("ep32", "4m", "loss3e2_up")
A2A_SWEEP_HANDOVER = ("ep64", "256k", "rate16g_up")
# The stage whose runs a 4096-byte stage reads on the cells and healthy
# pairs both read, rather than run them again: there the two stages' runs
# are the same runs but for the stage in the key, as both run the
# A2A_ARMS_4K on the settled fixture at the same compute gaps, iterations
# and seeds. a2a_sweep_4k takes a2a_tail_ack_4k's on EP64 at 256 KiB and 2
# MiB at loss 1e-3 and 1e-2 up and on EP16 at 256 KiB at loss 1e-2 up, and
# on their healthy pairs (a2a_shared_4k), which analyze.py a2a_sweep reads
# from a2a_tail_ack_4k's out root (--shared-root).
A2A_SHARED_4K = {A2A_SWEEP_STAGE: A2A_TAIL_ACK_4K_STAGE}
# a2a_downlink_4k: the downlink from spine 3 to leaf 1, the down cells' link,
# silent (blackhole_down) or at half rate (halfrate_down) from the onset, on
# EP64 and EP16 at 256 KiB, and gray at loss 1e-3 and 1e-2 (loss1e3_down,
# loss1e2_down) on EP64 at 256 KiB, each with its healthy pair, seeds
# 301-303. The arms: the sender's hold, the same with the receiver's
# (reps_hold_auto_tail_ack_rx), reps_tail, mrc_hold_auto_tail_ack and
# reps_oracle_tail.
A2A_DOWNLINK_STAGE = "a2a_downlink_4k"
A2A_DOWNLINK_CELLS = tuple((group, "256k", condition) for group in ("ep64", "ep16")
                           for condition in ("blackhole_down", "halfrate_down")) + tuple(
    ("ep64", "256k", condition) for condition in ("loss1e3_down", "loss1e2_down"))
A2A_DOWNLINK_ARMS = ("reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_rx", "reps_tail", "mrc_hold_auto_tail_ack",
                     "reps_oracle_tail")
# The decode cell's stages: a2a_decode_calibration_4k, a2a_calibration's
# per_receiver pass on it, and a2a_decode_4k, the A2A_ARMS_4K under a2a_tail's
# conditions (loss 1e-3 and 1e-2 up and down, blackhole up) with its healthy
# pair, seeds 301-303.
A2A_DECODE_CALIBRATION_STAGE = "a2a_decode_calibration_4k"
A2A_DECODE_STAGE = "a2a_decode_4k"
A2A_DECODE_CELLS = tuple(A2A_DECODE + (condition,) for condition in A2A_TAIL_CONDITIONS)
# a2a_reaction_4k: how soon each end holds a lossy downlink, loss 1e-3 on
# spine 3 to leaf 1, on EP64 at 256 KiB and on the decode cell, each with its
# healthy pair, seeds 301-303, under the sender's hold, the same with the
# receiver's (reps_hold_auto_tail_ack_rx) and reps_tail.
A2A_REACTION_STAGE = "a2a_reaction_4k"
A2A_REACTION_CELLS = (("ep64", "256k", "loss1e3_down"), A2A_DECODE + ("loss1e3_down",))
A2A_REACTION_ARMS = ("reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_rx", "reps_tail")
# a2a_ackroute_4k: every answer a receiver sends (acknowledgements, NACKs and
# repair requests) routed over a spine it draws uniformly per answer rather
# than over the spine of the data it answers (A2A_ACKROUTE: load_balancing
# ack_route spray, written as ACK_ROUTE), the data's identification still
# echoed for the sender, on EP64 at 256 KiB and 2 MiB and on the decode cell,
# each under blackhole, loss 1e-2 and loss 1e-3, up (leaf 0 to spine 3) and
# down (spine 3 to leaf 1), with its healthy pair, seeds 301-303. The arms,
# each with A2A_ACKROUTE on its setting: reps_hold_auto_tail_ack;
# reps_hold_auto_tail4, the same hold without ack_loss_rtts (a2a_tail's arm of
# that name, tail recovery at tail_loss_rtts 4); reps_tail; reps_oracle_tail;
# and reps_hold_auto_tail4 and reps_oracle_tail with ack_route_oracle
# (A2A_ACKROUTE_ORACLE: written as ACK_ROUTE_ORACLE on), whose receivers leave
# out of the draw, from the fault's onset, the spines over which an answer
# crosses the faulted link, as the switches' state shows it. A seventh arm,
# reps_hold_auto_tail_ack_data_spine, is reps_hold_auto_tail_ack with
# ack_route data_spine written out (A2A_ACKROUTE_DATA_SPINE), every answer
# over its data's spine as on every other stage: the stage's own control.
A2A_ACKROUTE_STAGE = "a2a_ackroute_4k"
A2A_ACKROUTE = {"ack_route": "spray"}
A2A_ACKROUTE_ORACLE = {"ack_route_oracle": True}
A2A_ACKROUTE_CELLS = tuple((group, size, condition) for group, size in (("ep64", "256k"), A2A_DECODE, ("ep64", "2m"))
                           for condition in ("blackhole_up", "blackhole_down", "loss1e2_up", "loss1e2_down",
                                             "loss1e3_up", "loss1e3_down"))
# The ack_route_oracle arms and the arm each adds it to.
A2A_ACKROUTE_ORACLE_ARMS = {"reps_hold_auto_tail4_route_oracle": "reps_hold_auto_tail4",
                            "reps_oracle_tail_route_oracle": "reps_oracle_tail"}
# The control arm and the arm it routes the answers of over the data's spines.
A2A_ACKROUTE_DATA_SPINE = {"reps_hold_auto_tail_ack_data_spine": "reps_hold_auto_tail_ack"}
A2A_ACKROUTE_ARMS = ("reps_hold_auto_tail_ack", "reps_hold_auto_tail4", "reps_tail", "reps_oracle_tail",
                     *A2A_ACKROUTE_ORACLE_ARMS, *A2A_ACKROUTE_DATA_SPINE)
# a2a_noack_4k: the loss-hold arms of the 4096-byte stages without
# ack_loss_rtts, tail recovery at tail_loss_rtts 4 as in them: each no-ack arm
# is its ack arm's setting less ack_loss_rtts (A2A_NOACK_ARMS), named by
# a2a_tail's reps_hold_auto_tail4, every answer over its data's spine. On
# every faulted cell of a2a_tail_ack_4k, a2a_sweep_4k and a2a_decode_4k
# (A2A_NOACK_SOURCES) and their healthy pairs, seeds 301-303, at the same
# compute gaps and iterations, so that each run's profile is its ack arm's
# run's in those stages but for ack_loss_rtts and the name.
A2A_NOACK_STAGE = "a2a_noack_4k"
A2A_NOACK_ARMS = {"reps_hold_auto_tail4": "reps_hold_auto_tail_ack", "reps_hold_leaf_tail4": "reps_hold_leaf_tail_ack",
                  "reps_flow_hold_tail4": "reps_flow_hold_tail_ack"}
A2A_NOACK_SOURCES = ("a2a_tail_ack_4k", "a2a_sweep_4k", "a2a_decode_4k")
# a2a_hostlink_4k: one host's access link lossy from the onset, no spine
# faulted: a gray loss at 1e-2 or 1e-3 (A2A_LOSS_RATES) on the link of host 9
# (A2A_FAULT_HOST, the second host of leaf 1, the leaf of the down cells'
# link), up (host to leaf) or down (leaf to host), the condition named
# host<kind>_<direction> (a2a_host_fault), its loss per byte as the fixture
# takes every gray loss. On EP64 at 256 KiB and on the decode cell, each
# with its healthy pair, seeds 301-303, under the 4096-byte arms but
# reps_oracle_tail, whose oracle reads the leaf-spine links alone, every
# answer routed over its data's spine (no ack_route).
A2A_HOST_PREFIX = "host"
A2A_FAULT_HOST = 9
A2A_HOSTLINK_STAGE = "a2a_hostlink_4k"
A2A_HOSTLINK_CONDITIONS = ("hostloss1e2_up", "hostloss1e3_up", "hostloss1e2_down", "hostloss1e3_down")
A2A_HOSTLINK_CELLS = tuple((group, size, condition) for group, size in (("ep64", "256k"), A2A_DECODE)
                           for condition in A2A_HOSTLINK_CONDITIONS)
A2A_HOSTLINK_ARMS = A2A_HOLD_ARMS_4K + ("reps_tail", "uniform_tail")
# The ring's stages: ar_parity_probe_4k, one healthy uniform_tail run, seed
# 301, which reads its healthy step time; ar_parity_calibration_4k,
# a2a_calibration's per_receiver pass on it; and ar_parity_4k, the
# A2A_ARMS_4K at loss 1e-2 on leaf 0's uplink to spine 3 with the healthy
# pair, seeds 301-303.
A2A_RING_PROBE_STAGE = "ar_parity_probe_4k"
A2A_RING_CALIBRATION_STAGE = "ar_parity_calibration_4k"
A2A_RING_STAGE = "ar_parity_4k"
# The 4096-byte stages: the probes and the (group, size) each runs at its
# provisional gap, the calibration passes and their pairs, and the faulted
# stages (a2a_faulted_4k).
A2A_PROBE_STAGES = {A2A_PROBE_4K_STAGE: A2A_PROBE_4K_COMPUTE_NS, A2A_DECODE_PROBE_STAGE: A2A_DECODE_PROBE_COMPUTE_NS,
                    A2A_RING_PROBE_STAGE: {A2A_RING: 0}}
A2A_CALIBRATION_STAGES_4K = {A2A_CALIBRATION_4K_STAGE: A2A_CALIBRATION_4K_CELLS,
                             A2A_DECODE_CALIBRATION_STAGE: (A2A_DECODE,), A2A_RING_CALIBRATION_STAGE: (A2A_RING,)}
A2A_STAGE_FIXTURE = {stage: A2A_FIXTURE_4K
                     for stage in (A2A_PROBE_4K_STAGE, A2A_CALIBRATION_4K_STAGE, A2A_TAIL_ACK_4K_STAGE,
                                   A2A_SWEEP_STAGE, A2A_DOWNLINK_STAGE, A2A_DECODE_PROBE_STAGE,
                                   A2A_DECODE_CALIBRATION_STAGE, A2A_DECODE_STAGE, A2A_REACTION_STAGE,
                                   A2A_RING_PROBE_STAGE, A2A_RING_CALIBRATION_STAGE, A2A_RING_STAGE,
                                   A2A_ACKROUTE_STAGE, A2A_NOACK_STAGE, A2A_HOSTLINK_STAGE)}
A2A_STAGES = ("a2a_oracle_calibration", "a2a_gate", "a2a_calibration", "a2a_tuning", "a2a_tuning_mix",
              "a2a_comparison", "a2a_gate_reps", "a2a_candidates", "a2a_receiver", A2A_RS3_STAGE,
              A2A_TAIL_STAGE, A2A_ACKLOSS_STAGE, A2A_CANDIDATES_ACK_STAGE, A2A_TAIL_ACK_STAGE,
              *A2A_STAGE_FIXTURE)


def a2a_cell_name(group: str, size: str, payload: int | None = None) -> str:
    """<group>-<size>, and -p<payload> for a run with a packet payload."""
    return f"{group}-{size}" + ("" if payload is None else f"-p{payload}")


def a2a_fixture(stage: str, arm: str, group: str, size: str) -> dict[str, Any] | None:
    """The stage's fixture (A2A_STAGE_FIXTURE) as a run of the arm on the
    group and size takes it: no path_state for a spray_policy arm, no
    dataset_splits for the ring; None for a stage on the defaults."""
    fixture = A2A_STAGE_FIXTURE.get(stage)
    if fixture is None:
        return None
    left_out = {"path_state"} if arm in A2A_SCOPES else set()
    if (group, size) == A2A_RING:
        left_out.add("dataset_splits")
    return {key: value for key, value in fixture.items() if key not in left_out}


def a2a_settle(document: dict[str, Any], fixture: dict[str, Any]) -> None:
    """Write a run's fixture (a2a_fixture) into its profile."""
    network = document["network"]
    if "payload_bytes" in fixture:
        network["packet_payload_bytes"] = fixture["payload_bytes"]
    if "path_state" in fixture:
        network["load_balancing"] = {**network["load_balancing"], "path_state": fixture["path_state"]}
    if "window_state" in fixture:
        network["congestion_control"] = {**network["congestion_control"], "window_state": fixture["window_state"]}
    if "dataset_splits" in fixture:
        document["workload"]["dataset_splits"] = fixture["dataset_splits"]
    if "loss_unit" in fixture:
        network["link_failures"] = [
            {**failure, "loss_unit": fixture["loss_unit"]} if failure["model"] == "gray" and "error_rate" in failure
            else failure
            for failure in network["link_failures"]
        ]


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
    """The link_failures of a condition: loss (A2A_LOSS_RATES), a lower rate
    (A2A_LINK_RATES) or the blackhole silent, in one direction of one link
    (up: leaf 0 to spine 3; down: spine 3 to leaf 1), the structural
    asymmetry the spine-wide rate form from t = 0 with no duration, and a
    host-link condition a2a_host_fault's."""
    if condition == "healthy":
        return []
    if condition.startswith(A2A_HOST_PREFIX):
        return a2a_host_fault(condition)
    onset = {"start_ns": DEGRADE_AT_NS, "spine": A2A_SPINE}
    if condition == "structural":
        return [{"model": "gray", "rate": "200Gbps", "start_ns": 0, "spine": A2A_SPINE}]
    kind, direction = condition.split("_")
    link = {"leaf": 0 if direction == "up" else 1, "direction": direction}
    if kind in A2A_LOSS_RATES:
        model = {"model": "gray", "error_rate": A2A_LOSS_RATES[kind]}
    elif kind == "blackhole":
        model = {"model": "silent"}
    elif kind in A2A_LINK_RATES:
        model = {"model": "gray", "rate": A2A_LINK_RATES[kind]}
    else:
        raise ValueError(f"unknown condition {condition}")
    return [{**model, **onset, **link}]


def a2a_host_fault(condition: str) -> list[dict[str, Any]]:
    """The link_failures of a host-link condition, host<kind>_<direction>: a
    gray loss at the loss kind's rate on A2A_FAULT_HOST's link to its leaf
    from the onset, up (what the host sends its leaf) or down (what the leaf
    sends the host)."""
    kind, direction = condition.split("_")
    loss = kind[len(A2A_HOST_PREFIX):]
    if loss not in A2A_LOSS_RATES or direction not in ("up", "down"):
        raise ValueError(f"unknown condition {condition}")
    return [{"model": "gray", "error_rate": A2A_LOSS_RATES[loss], "start_ns": DEGRADE_AT_NS,
             "host": A2A_FAULT_HOST, "direction": direction}]


def a2a_flap(flap: dict[str, Any]) -> list[dict[str, Any]]:
    """The link_failures of a2a_rs3's flapping link: leaf 0's uplink to
    spine 3 at its error rate for on_ns, repeat times, period_ns apart."""
    return [{"model": "gray", "error_rate": flap["error_rate"], "start_ns": flap["start_ns"],
             "spine": A2A_SPINE, "leaf": 0, "direction": "up", "duration_ns": flap["on_ns"],
             "repeat": flap["repeat"], "period_ns": flap["period_ns"]}]


def _a2a_loss_thresholds(entry: dict[str, Any], interval_ns: int) -> list[float]:
    value = entry.get("loss_thresholds")
    if isinstance(value, dict):
        value = value.get(str(interval_ns))
    return list(value) if value else list(A2A_LOSS_THRESHOLDS_PLACEHOLDER)


def a2a_balancing(arm: str, group: str, size: str, calibration: dict[str, Any] | None,
                  setting: dict[str, Any] | None, payload: int | None = None) -> dict[str, Any]:
    """An arm's load balancing: the t_ stages' graded settings (edge window
    penalty 0, edge_freeze off, estimator_interval_samples 0, gain 0.25)
    plus this phase's calibrated thresholds at the run's payload and the
    loss grade."""
    import transient

    if arm in ("uniform",) + A2A_FIXED_ARMS:
        return balancing(arm, 0, None)
    if arm == A2A_REPS_ORACLE:
        return {"mode": "spray_uniform", "selector": "reps_oracle", "grade_cost_source": "switch_queue"}
    if arm == A2A_REPS_SPINE:
        return {"mode": "spray_uniform", "selector": "reps_spine"}
    if arm in A2A_TRANSPORT_ARMS and A2A_TRANSPORT_ARMS[arm] not in A2A_REPS_HOLD_ARMS:
        # The setting is the transport keys alone.
        return {**a2a_balancing(A2A_TRANSPORT_ARMS[arm], group, size, calibration, None, payload),
                **(setting or {})}
    if (arm in A2A_REPS_HOLD_ARMS or arm in A2A_RECEIVER_ARMS or arm in A2A_TRICKLE_ARMS
            or arm in A2A_TRANSPORT_ARMS):
        # The setting holds the loss interval, the clean sentinels and the
        # sentinel interval (or the trickle release's hold test), the
        # exploration sends, hold_scope where the selector reads it, the
        # receiver's hold where the arm has it, and tail recovery or
        # ack_loss_rtts where the arm has it.
        base = (A2A_RECEIVER_ARMS[arm][0] if arm in A2A_RECEIVER_ARMS
                else A2A_TRICKLE_ARMS.get(arm, A2A_TRANSPORT_ARMS.get(arm, arm)))
        selector, _ = A2A_REPS_HOLD_ARMS[base]
        setting = dict(setting or {})
        entry = (calibration or {}).get("per_receiver", {}).get(a2a_cell_name(group, size, payload), {})
        return {
            "mode": "spray_uniform",
            "selector": selector,
            "loss_thresholds": _a2a_loss_thresholds(entry, setting["loss_interval_ns"]),
            "held_exploration": "sentinel",
            **setting,
        }
    if arm in A2A_FLOOR_ARMS:
        # The setting is the base arm's with congestion_grade_floor.
        values = a2a_balancing(A2A_FLOOR_ARMS[arm], group, size, calibration, setting, payload)
        values.pop("pool_hold_propagates", None)
        return values
    if arm == A2A_SOFT_ORACLE:
        values = a2a_balancing(A2A_ORACLE, group, size, calibration, setting, payload)
        values["loss_thresholds"] = list(A2A_SOFT_LOSS_THRESHOLDS)
        return values
    values = transient.balancing(arm, 0, None, setting)
    entry = (calibration or {}).get(A2A_SCOPES[arm], {}).get(a2a_cell_name(group, size, payload), {})
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


def a2a_candidate_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each candidate arm's setting; a floor arm is left out while
    a2a_tuning.json has no setting for its base arm."""
    mix = (tuning or {}).get("graded_per_receiver_mix") or {}
    hold = {
        "loss_interval_ns": mix.get("loss_interval_ns", A2A_REPORT_INTERVAL_NS),
        "loss_hold_clean_sentinels": mix.get("loss_hold_clean_sentinels", A2A_LOSS_HOLD_CLEAN_SENTINELS),
        "held_sentinel_interval_ns": mix.get("held_sentinel_interval_ns", A2A_SENTINEL_INTERVAL_NS),
    }
    settings: dict[str, dict[str, Any]] = {}
    for arm, (_, scope) in A2A_REPS_HOLD_ARMS.items():
        settings[arm] = {**hold, **({} if scope is None else {"hold_scope": scope}), **A2A_HELD_EXPLORATION}
    for arm, base in A2A_FLOOR_ARMS.items():
        if (tuning or {}).get(base):
            settings[arm] = {**tuning[base], "congestion_grade_floor": A2A_CONGESTION_GRADE_FLOOR}
    return settings


def a2a_receiver_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each receiver arm's setting: its base candidate arm's, with
    A2A_RECEIVER_HOLD where the arm holds at the receiver."""
    candidates = a2a_candidate_settings(tuning)
    return {arm: {**candidates[base], **(A2A_RECEIVER_HOLD if receiver else {})}
            for arm, (base, receiver) in A2A_RECEIVER_ARMS.items()}


def a2a_rs3_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any] | None]:
    """Each a2a_rs3 arm's setting: reps_hold_auto's candidate setting, the
    trickle arm's from it (A2A_TRICKLE_HOLD), none for reps and uniform."""
    hold = a2a_candidate_settings(tuning)["reps_hold_auto"]
    trickle = {key: value for key, value in hold.items()
               if key not in ("loss_hold_clean_sentinels", "held_sentinel_interval_ns", "hold_release")}
    settings: dict[str, dict[str, Any] | None] = {
        "reps_hold_auto": hold, "reps_hold_auto_trickle": {**trickle, **A2A_TRICKLE_HOLD},
        "reps": None, "uniform": None,
    }
    return {arm: settings[arm] for arm in A2A_RS3_ARMS}


def a2a_tail_settings(tuning: dict[str, Any] | None, arms: dict[str, str] = A2A_TAIL_ARMS,
                      keys: dict[str, dict[str, Any]] = A2A_TAIL_KEYS) -> dict[str, dict[str, Any]]:
    """Each arm's setting (a2a_tail's arms and A2A_TAIL_KEYS unless arms and
    keys are given): A2A_TAIL and the arm's keys, on its base arm's candidate
    setting (none for a baseline)."""
    candidates = a2a_candidate_settings(tuning)
    return {arm: {**candidates.get(base, {}), **A2A_TAIL, **keys.get(arm, {})}
            for arm, base in arms.items()}


def a2a_tail_ack_settings(tuning: dict[str, Any] | None,
                          arms: tuple[str, ...] = A2A_TAIL_ACK_STAGE_ARMS) -> dict[str, dict[str, Any]]:
    """Each arm's setting (a2a_tail_ack's arms unless arms is given), from
    A2A_TAIL_ACK_ARMS and A2A_TAIL_ACK_KEYS."""
    settings = a2a_tail_settings(tuning, A2A_TAIL_ACK_ARMS, A2A_TAIL_ACK_KEYS)
    return {arm: settings[arm] for arm in arms}


def a2a_ackroute_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_ackroute_4k arm's setting: reps_hold_auto_tail4's of a2a_tail
    and the others' of a2a_tail_ack, with A2A_ACKROUTE; an
    A2A_ACKROUTE_ORACLE_ARMS arm's base arm's with A2A_ACKROUTE_ORACLE; and
    the control arm's base arm's of a2a_tail_ack with ack_route data_spine."""
    base = {**a2a_tail_settings(tuning), **a2a_tail_ack_settings(tuning, tuple(A2A_TAIL_ACK_ARMS))}
    settings = {arm: {**base[arm], **A2A_ACKROUTE} for arm in A2A_ACKROUTE_ARMS if arm in base}
    for arm, of in A2A_ACKROUTE_ORACLE_ARMS.items():
        settings[arm] = {**settings[of], **A2A_ACKROUTE_ORACLE}
    for arm, of in A2A_ACKROUTE_DATA_SPINE.items():
        settings[arm] = {**base[of], "ack_route": "data_spine"}
    return {arm: settings[arm] for arm in A2A_ACKROUTE_ARMS}


def a2a_noack_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_noack_4k arm's setting: its ack arm's (A2A_NOACK_ARMS) less
    ack_loss_rtts."""
    ack = a2a_tail_ack_settings(tuning, tuple(A2A_NOACK_ARMS.values()))
    return {arm: {key: value for key, value in ack[of].items() if key != "ack_loss_rtts"}
            for arm, of in A2A_NOACK_ARMS.items()}


def a2a_ackloss_settings(tuning: dict[str, Any] | None,
                         arms: dict[str, str] = A2A_ACKLOSS_ARMS) -> dict[str, dict[str, Any]]:
    """Each arm's setting (a2a_ackloss's arms unless arms is given):
    A2A_ACKLOSS, on its base arm's candidate setting (none for
    reps_spine)."""
    candidates = a2a_candidate_settings(tuning)
    return {arm: {**candidates.get(base, {}), **A2A_ACKLOSS} for arm, base in arms.items()}


def a2a_tail_cells() -> list[tuple[str, str, str]]:
    """Every gate cell under a loss or blackhole condition."""
    return [cell for cell in a2a_gate_cells() if cell[2] in A2A_TAIL_CONDITIONS]


def a2a_gate_cells() -> list[tuple[str, str, str]]:
    """The 28 (group, size, condition) cells of the gate."""
    return [(group, size, condition)
            for (group, size), condition in itertools.product(A2A_CELLS, A2A_FAULTS)]


def a2a_faulted_4k(stage: str) -> tuple[tuple[tuple[str, str, str], ...], tuple[str, ...]]:
    """The faulted (group, size, condition) cells and the arms of a
    4096-byte stage other than a probe or a calibration pass."""
    if stage == A2A_NOACK_STAGE:
        cells = tuple(dict.fromkeys(cell for source in A2A_NOACK_SOURCES for cell in a2a_faulted_4k(source)[0]))
        return cells, tuple(A2A_NOACK_ARMS)
    return {
        A2A_TAIL_ACK_4K_STAGE: (tuple(a2a_gate_cells()), A2A_ARMS_4K),
        A2A_SWEEP_STAGE: (A2A_SWEEP_CELLS + (A2A_SWEEP_ANCHOR, A2A_SWEEP_HANDOVER), A2A_ARMS_4K),
        A2A_DOWNLINK_STAGE: (A2A_DOWNLINK_CELLS, A2A_DOWNLINK_ARMS),
        A2A_DECODE_STAGE: (A2A_DECODE_CELLS, A2A_ARMS_4K),
        A2A_REACTION_STAGE: (A2A_REACTION_CELLS, A2A_REACTION_ARMS),
        A2A_RING_STAGE: ((A2A_RING + ("loss1e2_up",),), A2A_ARMS_4K),
        A2A_ACKROUTE_STAGE: (A2A_ACKROUTE_CELLS, A2A_ACKROUTE_ARMS),
        A2A_HOSTLINK_STAGE: (A2A_HOSTLINK_CELLS, A2A_HOSTLINK_ARMS),
    }[stage]


def a2a_read_cells_4k(stage: str) -> tuple[tuple[str, str, str], ...]:
    """The (group, size, condition) cells a 4096-byte stage other than a
    probe or a calibration pass reads: its faulted cells, then their
    healthy pairs (condition healthy)."""
    cells, _ = a2a_faulted_4k(stage)
    return cells + tuple(dict.fromkeys((group, size, "healthy") for group, size, _ in cells))


def a2a_shared_4k(stage: str) -> tuple[str | None, tuple[tuple[str, str, str], ...]]:
    """The stage A2A_SHARED_4K names for a 4096-byte stage, and the cells of
    a2a_read_cells_4k both read, whose runs the stage takes from it rather
    than runs; (None, ()) for a stage that runs every cell it reads."""
    source = A2A_SHARED_4K.get(stage)
    if source is None:
        return None, ()
    theirs = set(a2a_read_cells_4k(source))
    return source, tuple(cell for cell in a2a_read_cells_4k(stage) if cell in theirs)


def a2a_admitted_cells(tuning: dict[str, Any] | None) -> list[tuple[str, str, str]]:
    """The (group, size, condition) cells a2a_tuning.json admits, every
    cell without it."""
    admitted = (tuning or {}).get("admitted")
    return [
        (group, size, condition)
        for group, size, condition in a2a_gate_cells()
        if admitted is None or f"{a2a_cell_name(group, size)}-{condition}" in admitted
    ]


def a2a_stage_runs(stage: str, calibration: dict[str, Any] | None,
                   tuning: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Every run of a Phase 9 stage."""
    out: list[dict[str, Any]] = []
    payload = (A2A_STAGE_FIXTURE.get(stage) or {}).get("payload_bytes")
    compute = A2A_COMPUTE_NS if payload is None else A2A_PAYLOAD_COMPUTE_NS[payload]

    def add(arm, group, size, condition, seed, setting=None, label_setting=False, iterations=None, flap=None,
            compute_ns=None):
        label = arm if not label_setting else f"{arm}-{setting_name(setting)}"
        out.append(
            {
                "key": f"{stage}-{group}-{size}-{condition}-{label}-s{seed}",
                "stage": stage,
                "arm": arm,
                "setting": setting,
                "k": A2A_RUN_GROUP_SIZES[group] - 1,
                "group": group,
                "size": size,
                "message_bytes": A2A_RUN_SIZES[size],
                "condition": condition,
                "compute_ns": compute_ns or compute[(group, size)],
                "iterations": iterations or A2A_RUN_ITERATIONS[(group, size)],
                "seed": seed,
                **({} if flap is None else {"flap": flap}),
                **({} if stage not in A2A_STAGE_FIXTURE else {"fixture": a2a_fixture(stage, arm, group, size)}),
                **A2A_COMBINE.get(size, {}),
            }
        )

    def calibrated(scope, group, size):
        return a2a_cell_name(group, size, payload) in (calibration or {}).get(scope, {})

    def require_measured(pairs):
        provisional = [a2a_cell_name(group, size) for group, size in pairs
                       if (group, size) not in A2A_PAYLOAD_COMPUTE_MEASURED[payload]]
        if provisional:
            raise ValueError(f"{stage}'s compute gaps on {provisional} are provisional: enter the probe's values "
                             f"at {payload} B in A2A_PAYLOAD_COMPUTE_NS and list the pairs in "
                             "A2A_PAYLOAD_COMPUTE_MEASURED")

    if stage == "a2a_oracle_calibration":
        for (group, size), seed in itertools.product(A2A_CELLS, A2A_SEEDS["calibration"]):
            add(A2A_ORACLE, group, size, "healthy", seed)
    elif stage == "a2a_calibration":
        for (group, size), seed in itertools.product(A2A_CELLS, A2A_SEEDS["calibration"]):
            add("graded_per_connection", group, size, "healthy", seed)
            add("graded_per_receiver_mix", group, size, "healthy", seed, A2A_CALIBRATION_POOLED)
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
    elif stage in ("a2a_candidates", "a2a_receiver"):
        settings = (a2a_candidate_settings if stage == "a2a_candidates" else a2a_receiver_settings)(tuning)
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], settings.items()):
            for group, size, condition in a2a_gate_cells():
                add(arm, group, size, condition, seed, setting)
            for group, size in A2A_CELLS:
                add(arm, group, size, "healthy", seed, setting)
    elif stage == A2A_TAIL_STAGE:
        cells = a2a_tail_cells()
        pairs = [cell for cell in A2A_CELLS if any(c[:2] == cell for c in cells)]
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], a2a_tail_settings(tuning).items()):
            for group, size, condition in cells:
                add(arm, group, size, condition, seed, setting)
            for group, size in pairs:
                add(arm, group, size, "healthy", seed, setting)
    elif stage == A2A_ACKLOSS_STAGE:
        cells = list(A2A_ACKLOSS_CELLS)
        pairs = [cell for cell in A2A_CELLS if any(c[:2] == cell for c in cells)]
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], a2a_ackloss_settings(tuning).items()):
            for group, size, condition in cells:
                add(arm, group, size, condition, seed, setting)
            for group, size in pairs:
                add(arm, group, size, "healthy", seed, setting)
    elif stage in (A2A_CANDIDATES_ACK_STAGE, A2A_TAIL_ACK_STAGE):
        settings = (a2a_ackloss_settings(tuning, A2A_CANDIDATES_ACK_ARMS) if stage == A2A_CANDIDATES_ACK_STAGE
                    else a2a_tail_ack_settings(tuning))
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], settings.items()):
            for group, size, condition in a2a_gate_cells():
                add(arm, group, size, condition, seed, setting)
            for group, size in A2A_CELLS:
                add(arm, group, size, "healthy", seed, setting)
    elif stage in A2A_PROBE_STAGES:
        setting = a2a_tail_ack_settings(tuning, ("uniform_tail",))["uniform_tail"]
        for (group, size), compute_ns in A2A_PROBE_STAGES[stage].items():
            add("uniform_tail", group, size, "healthy", A2A_SEEDS["comparison"][0], setting, compute_ns=compute_ns)
    elif stage in A2A_CALIBRATION_STAGES_4K:
        pairs = A2A_CALIBRATION_STAGES_4K[stage]
        require_measured(pairs)
        for (group, size), seed in itertools.product(pairs, A2A_SEEDS["calibration"]):
            add("graded_per_receiver_mix", group, size, "healthy", seed, A2A_CALIBRATION_POOLED)
    elif stage in A2A_STAGE_FIXTURE:
        cells, arms = a2a_faulted_4k(stage)
        pairs = tuple(dict.fromkeys((group, size) for group, size, _ in cells))
        require_measured(pairs)
        uncalibrated = [a2a_cell_name(group, size, payload) for group, size in pairs
                        if group not in A2A_ONE_LEAF_GROUPS and not calibrated("per_receiver", group, size)]
        if uncalibrated:
            raise ValueError(f"{stage}'s hold arms have no calibrated loss thresholds on {uncalibrated}: "
                             "merge the payload's calibration pass into a2a_calibration.json")
        settings = (a2a_ackroute_settings(tuning) if stage == A2A_ACKROUTE_STAGE
                    else a2a_noack_settings(tuning) if stage == A2A_NOACK_STAGE
                    else a2a_tail_ack_settings(tuning, arms))
        _, shared = a2a_shared_4k(stage)
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], settings.items()):
            for group, size, condition in a2a_read_cells_4k(stage):
                if (group, size, condition) not in shared:
                    add(arm, group, size, condition, seed, setting)
    elif stage == A2A_RS3_STAGE:
        for seed, (arm, setting) in itertools.product(A2A_SEEDS["comparison"], a2a_rs3_settings(tuning).items()):
            for group, size in A2A_RS3_CELLS:
                iterations = A2A_RS3_ITERATIONS[(group, size)]
                flap = {"error_rate": A2A_RS3_ERROR_RATE, "start_ns": DEGRADE_AT_NS, "on_ns": A2A_RS3_ON_NS,
                        "period_ns": A2A_RS3_CYCLE_NS, "repeat": A2A_RS3_REPEAT[(group, size)]}
                add(arm, group, size, A2A_RS3_CONDITION, seed, setting, iterations=iterations, flap=flap)
                add(arm, group, size, "healthy", seed, setting, iterations=iterations)
    elif stage == "a2a_comparison":
        cells = a2a_admitted_cells(tuning)
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
    """The t_ stages' fabric and transport under the all-to-all workload, or
    the ring's; the calibration stages run without this phase's
    calibration."""
    if run["stage"] in ("a2a_oracle_calibration", "a2a_calibration", *A2A_CALIBRATION_STAGES_4K):
        calibration = None
    group, size = run["group"], run["size"]
    document = profile(
        run["key"], 64, "healthy",
        a2a_balancing(run["arm"], group, size, calibration, run["setting"],
                      run.get("fixture", {}).get("payload_bytes")),
    )
    document["steps"] = run["iterations"]
    if (group, size) == A2A_RING:
        document["workload"] = {"kind": "sequential_dp_all_reduce"}
        document["dp_all_reduce_bytes"] = run["message_bytes"]
    else:
        document["workload"] = {
            "kind": "all_to_all",
            "group": A2A_RUN_GROUPS[group],
            "message_bytes": run["message_bytes"],
            "iterations": run["iterations"],
            "compute_ns": run["compute_ns"],
            **{key: run[key] for key in ("combine_bytes", "expert_compute_ns") if key in run},
        }
    document["network"]["link_failures"] = a2a_flap(run["flap"]) if "flap" in run else a2a_fault(run["condition"])
    a2a_settle(document, run.get("fixture", {}))
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
