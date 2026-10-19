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

    profiles.py write --out DIR [--calibration FILE] [--tuning FILE] --stage S [--filter REGEX]

writes DIR/<run key>.json for every run of the stage and DIR/runs.json, the
list of {key, profile, seed, stage, k, condition, arm}; with --filter (the
workflow's record_filter), for the runs whose key it matches (re.search)
only. Stages named t_*
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
import re
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
#   a2a_ackroute_fwd_4k     the A2A_ACKROUTE_FWD_ARMS, reps_oracle_tail's
#                           oracle over the data's own hops, on a2a_ackroute_4k's
#                           cells and healthy pairs, seeds 301-303
#   a2a_downlink_spray_4k   the A2A_DOWNLINK_SPRAY_ARMS, every answer on a
#                           spine its receiver draws, on the
#                           A2A_DOWNLINK_SPRAY_CELLS and their healthy pairs,
#                           seeds 301-303
#   a2a_twoend_4k           the A2A_TWOEND_ARMS, the sender's coverage first
#                           and the answers' path sequences, on
#                           a2a_ackroute_4k's cells and healthy pairs, seeds
#                           301-303
#   a2a_hostlink_s_4k       a2a_hostlink_4k's loss-hold arms with the
#                           sender's coverage first (A2A_HOSTLINK_COVER_ARMS)
#                           on its cells and healthy pairs, seeds 301-303
#   a2a_twoend_base_4k      the A2A_TWOEND_BASE_ARMS, the coverage-first arm
#                           with its answers routed per connection, on
#                           a2a_twoend_4k's cells and healthy pairs, seeds
#                           301-303
#   a2a_rset_sweep_4k       the A2A_RSET_SWEEP_ARMS, the reverse set at its set
#                           sizes, probe intervals and reply queueing, on
#                           a2a_twoend_4k's blackhole cells and healthy
#                           pairs, seed 301
#   a2a_twoend_rset_4k      the A2A_TWOEND_RSET_ARMS, the coverage-first arm
#                           with answer path sequences and the reverse set at
#                           two of its sweep settings, on a2a_twoend_4k's
#                           cells and healthy pairs, seeds 301-303
#   a2a_twoend_retract_4k   the A2A_TWOEND_RETRACT_ARMS, the coverage-first arm,
#                           that with answer path sequences, and that with a
#                           timed-out send's loss taken back at a flagged
#                           answer, on a2a_twoend_4k's cells and healthy
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
# ack_route_oracle, oracle_hops, stray_path_seq, coverage_first,
# answer_path_seq) to a base arm, and that base arm.
A2A_TRANSPORT_ARMS = {**A2A_TAIL_ARMS, **A2A_ACK_ARMS, **A2A_TAIL_ACK_ARMS,
                      "reps_hold_auto_tail4_route_oracle": "reps_hold_auto", "reps_oracle_tail_route_oracle":
                      A2A_REPS_ORACLE, "reps_hold_auto_tail_ack_data_spine": "reps_hold_auto",
                      "reps_hold_leaf_tail4": "reps_hold_auto", "reps_flow_hold_tail4": "reps_flow_hold",
                      "reps_oracle_tail_fwd": A2A_REPS_ORACLE, "reps_oracle_tail_fwd_route_oracle": A2A_REPS_ORACLE,
                      "reps_hold_auto_tail_ack_rx_closed": "reps_hold_auto",
                      "reps_hold_auto_tail4_rx_closed": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_rx": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_answer_seq": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_route_oracle": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_dup": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_retract": "reps_hold_auto",
                      "reps_hold_auto_tail_ack_cover": "reps_hold_auto",
                      "reps_hold_leaf_tail_ack_cover": "reps_hold_auto",
                      "reps_flow_hold_tail_ack_cover": "reps_flow_hold",
                      "reps_hold_auto_tail4_cover_reverse_set": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_repath": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_rx_answer_seq": "reps_hold_auto",
                      "reps_hold_auto_tail4_cover_reverse_set_rx": "reps_hold_auto"}
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
#   rtt_sample      network.congestion_control.rtt_sample: karn, NSCC's round
#                   trips by Karn's rule, none from an answer to a sequence sent
#                   more than once, each at least the pair's base RTT, so that
#                   a late answer to an earlier send never sets the window's
#                   base RTT and MaxWnd below the path's
# Loss thresholds (from the healthy sends per spine per interval) and compute
# gaps (from the healthy burst) depend on it, so a run on it reads its
# calibration from the cells a2a_cell_name names with its payload
# (<group>-<size>-p4096) and its compute gap from A2A_PAYLOAD_COMPUTE_NS.
A2A_FIXTURE_4K: dict[str, Any] = {"payload_bytes": 4096, "path_state": "connection", "window_state": "connection",
                                  "dataset_splits": 1, "loss_unit": "byte", "rtt_sample": "karn"}
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
# a2a_ackroute_fwd_4k: a2a_ackroute_4k's cells, healthy pairs and seeds,
# every answer on a spine its receiver draws (A2A_ACKROUTE), under two arms of
# reps_oracle_tail whose oracle reads the data's own hops alone
# (A2A_ORACLE_FORWARD: load_balancing oracle_hops forward, written as
# REPS_ORACLE_HOPS): a spine toward a destination leaf is left out where the
# sender leaf's uplink to it or its downlink to the destination leaf is
# faulted, and not where only the hops back are, which the answers no longer
# take with the data. Each arm is its a2a_ackroute_4k arm's setting
# (A2A_ACKROUTE_FWD_ARMS) with A2A_ORACLE_FORWARD: reps_oracle_tail_fwd, and
# reps_oracle_tail_fwd_route_oracle with A2A_ACKROUTE_ORACLE.
A2A_ACKROUTE_FWD_STAGE = "a2a_ackroute_fwd_4k"
A2A_ORACLE_FORWARD = {"oracle_hops": "forward"}
A2A_ACKROUTE_FWD_ARMS = {"reps_oracle_tail_fwd": "reps_oracle_tail",
                         "reps_oracle_tail_fwd_route_oracle": "reps_oracle_tail_route_oracle"}
# a2a_downlink_spray_4k: the downlink from spine 3 to leaf 1 faulted, every
# answer on a spine its receiver draws (A2A_ACKROUTE): a2a_downlink_4k's
# cells (EP64 and EP16 at 256 KiB silent or at half rate, EP64 at 256 KiB
# gray at loss 1e-3 and 1e-2), the decode cell at loss 1e-3 and silent, and
# EP64 at 2 MiB at loss 1e-3 and 1e-2, each with its healthy pair, seeds
# 301-303. The arms, each with A2A_ACKROUTE on its setting:
# reps_hold_auto_tail4 and reps_hold_auto_tail_ack, the sender's hold
# without and with ack_loss_rtts; the receiver's hold of
# reps_hold_auto_tail_ack_rx with the packets of a flow arriving after its
# receive queue pair was removed read against that queue pair
# (A2A_STRAY_CLOSED: load_balancing stray_path_seq closed, written as
# SPRAY_STRAY_PATH_SEQ), with ack_loss_rtts (reps_hold_auto_tail_ack_rx_closed)
# and without it (reps_hold_auto_tail4_rx_closed), since an acknowledgement
# no longer returns over its data's spine; reps_tail; and reps_oracle_tail
# with oracle_hops forward (reps_oracle_tail_fwd, a2a_ackroute_fwd_4k's arm).
A2A_DOWNLINK_SPRAY_STAGE = "a2a_downlink_spray_4k"
A2A_DOWNLINK_SPRAY_CELLS = A2A_DOWNLINK_CELLS + (A2A_DECODE + ("loss1e3_down",), A2A_DECODE + ("blackhole_down",),
                                                 ("ep64", "2m", "loss1e3_down"), ("ep64", "2m", "loss1e2_down"))
A2A_STRAY_CLOSED = {"stray_path_seq": "closed"}
# The receiver arms and the arm whose setting each takes A2A_STRAY_CLOSED on.
A2A_DOWNLINK_SPRAY_RECEIVER_ARMS = {"reps_hold_auto_tail_ack_rx_closed": "reps_hold_auto_tail_ack_rx",
                                    "reps_hold_auto_tail4_rx_closed": "reps_hold_auto_tail_ack_rx"}
A2A_DOWNLINK_SPRAY_ARMS = ("reps_hold_auto_tail4", "reps_hold_auto_tail_ack", *A2A_DOWNLINK_SPRAY_RECEIVER_ARMS,
                           "reps_tail", "reps_oracle_tail_fwd")
# a2a_twoend_4k: a2a_ackroute_4k's cells, healthy pairs and seeds, every
# answer on a spine its receiver draws (A2A_ACKROUTE) but under the control.
# The arms: reps_hold_auto_tail4, a2a_ackroute_4k's; reps_hold_auto_tail4_cover,
# the same with its sender applying the answer's cumulative and selective
# coverage before a loss rule declares a send lost, a covered send counted as
# an answer loss and no more (A2A_COVERAGE_FIRST: load_balancing
# coverage_first, written as SPRAY_COVERAGE_FIRST on), and its copies under
# copy_expiry deadline (A2A_COPY_DEADLINE); that with answer path sequences
# (A2A_ANSWER_PATH_SEQ: load_balancing answer_path_seq, written as
# SPRAY_ANSWER_PATH_SEQ on), the receivers numbering their answers per route
# spine, the senders holding a route their answers go missing on and telling
# the receivers, which leave it out of their draw but for a probe answer per
# millisecond until no sender holds it (reps_hold_auto_tail4_cover_answer_seq);
# that with A2A_ACKROUTE_ORACLE (reps_hold_auto_tail4_cover_route_oracle);
# reps_hold_auto_tail4_cover with its receivers flagging an answer to a
# sequence they held already and its senders telling a timed-out send's loss
# only at its resend's answer, none where that answer is flagged
# (A2A_DUPLICATE_REPORT: load_balancing duplicate_report, written as
# SPRAY_DUPLICATE_REPORT on): reps_hold_auto_tail4_cover_dup;
# a2a_ackroute_4k's control, reps_hold_auto_tail_ack_data_spine; and
# reps_tail.
A2A_TWOEND_STAGE = "a2a_twoend_4k"
A2A_COVERAGE_FIRST = {"coverage_first": True}
A2A_ANSWER_PATH_SEQ = {"answer_path_seq": True}
A2A_DUPLICATE_REPORT = {"duplicate_report": True}
# The coverage-first arms and the keys each adds to reps_hold_auto_tail4_cover.
A2A_TWOEND_COVER_ARMS = {"reps_hold_auto_tail4_cover": {},
                         "reps_hold_auto_tail4_cover_answer_seq": A2A_ANSWER_PATH_SEQ,
                         "reps_hold_auto_tail4_cover_route_oracle": A2A_ACKROUTE_ORACLE,
                         "reps_hold_auto_tail4_cover_dup": A2A_DUPLICATE_REPORT}
A2A_TWOEND_ARMS = ("reps_hold_auto_tail4", *A2A_TWOEND_COVER_ARMS, *A2A_ACKROUTE_DATA_SPINE, "reps_tail")
# a2a_twoend_base_4k: a2a_twoend_4k's cells, healthy pairs and seeds under
# two arms of reps_hold_auto_tail4_cover whose receivers route the answers per
# connection, a sending host and priority group, rather than draw a spine per
# answer (A2A_TWOEND_BASE_ARMS): reps_hold_auto_tail4_cover_reverse_set over a
# set of two spines each receiver probes once per base RTT while the data
# arrives, its sender returning each probe, and renews where a probe goes
# unanswered (ack_route reverse_set, written as ACK_ROUTE); and
# reps_hold_auto_tail4_cover_repath over one spine each receiver draws again
# at each duplicate data arrival but a copied send's (ack_route repath).
A2A_TWOEND_BASE_STAGE = "a2a_twoend_base_4k"
A2A_TWOEND_BASE_ARMS = {"reps_hold_auto_tail4_cover_reverse_set": {"ack_route": "reverse_set"},
                        "reps_hold_auto_tail4_cover_repath": {"ack_route": "repath"}}
# a2a_twoend_retract_4k: a2a_twoend_4k's cells, healthy pairs and seeds under
# reps_hold_auto_tail4_cover, reps_hold_auto_tail4_cover_answer_seq and,
# beside reps_hold_auto_tail4_cover_dup, reps_hold_auto_tail4_cover_retract
# (A2A_TWOEND_RETRACT_ARMS): reps_hold_auto_tail4_cover with its receivers
# flagging an answer to a sequence they held already and its senders telling
# each timed-out send's loss on its data spine at once, a sequence timed out
# again counted again, and taking each back from the host's loss hold at the
# flagged answer to the sequence, the hold it began graded again without it
# (A2A_DUPLICATE_RETRACT: load_balancing duplicate_report retract, written as
# SPRAY_DUPLICATE_REPORT retract). The stage's own coverage-first arms are
# a2a_twoend_4k's, so that the three are read on one build.
A2A_TWOEND_RETRACT_STAGE = "a2a_twoend_retract_4k"
A2A_DUPLICATE_RETRACT = {"duplicate_report": "retract"}
A2A_TWOEND_RETRACT_ARMS = {"reps_hold_auto_tail4_cover": {},
                           "reps_hold_auto_tail4_cover_answer_seq": A2A_ANSWER_PATH_SEQ,
                           "reps_hold_auto_tail4_cover_retract": A2A_DUPLICATE_RETRACT}
# a2a_overdue_4k: a2a_twoend_4k's cells, healthy pairs and seeds under six
# arms with the overdue repair (A2A_OVERDUE: load_balancing overdue_repair
# reprobe and overdue_repair_limit 3, written as SPRAY_OVERDUE_REPAIR and
# SPRAY_OVERDUE_REPAIR_LIMIT), a queue pair's latest send on a spine not held
# repaired once its answer is held_loss_rtts base RTTs overdue and again each
# time its last repair goes as long without an answer, up to 3 repairs, each
# counting no loss, the first answer settling the send under
# duplicate_report: a2a_twoend_4k's reps_hold_auto_tail4,
# reps_hold_auto_tail4_cover, reps_hold_auto_tail4_cover_answer_seq and
# reps_hold_auto_tail4_cover_dup, a2a_twoend_base_4k's
# reps_hold_auto_tail4_cover_reverse_set and a2a_twoend_retract_4k's
# reps_hold_auto_tail4_cover_retract, each named with _od
# (A2A_OVERDUE_ARMS), so that each reads beside its key-off runs of those
# stages.
A2A_OVERDUE_STAGE = "a2a_overdue_4k"
A2A_OVERDUE = {"overdue_repair": "reprobe", "overdue_repair_limit": 3}
A2A_OVERDUE_ARMS = {f"{arm}_od": arm for arm in (
    "reps_hold_auto_tail4", "reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_answer_seq",
    "reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover_dup", "reps_hold_auto_tail4_cover_retract")}
A2A_TRANSPORT_ARMS.update({arm: "reps_hold_auto" for arm in A2A_OVERDUE_ARMS})
# a2a_rset_sweep_4k: a2a_twoend_base_4k's reps_hold_auto_tail4_cover_reverse_set
# at its set of 2 spines probed once per base RTT, the replies queued with the
# data, and, each with the replies queued as answers (ack_route_set_reply
# answer, written as ACK_ROUTE_SET_REPLY), at sets of 2 and 4 spines
# (ack_route_set_size) probed once per 1, 4 and 16 base RTTs
# (ack_route_set_probe_rtts, written as ACK_ROUTE_SET_PROBE_RTTS), named
# _<size>x<interval>_answer (A2A_RSET_SWEEP_ARMS), on a2a_twoend_4k's
# blackhole cells (A2A_RSET_SWEEP_CELLS) and their healthy pairs at seed 301
# alone (A2A_RSET_SWEEP_SEEDS).
A2A_RSET_SWEEP_STAGE = "a2a_rset_sweep_4k"
A2A_RSET_SWEEP_ARMS = {"reps_hold_auto_tail4_cover_reverse_set": {},
                       **{f"reps_hold_auto_tail4_cover_reverse_set_{size}x{interval}_answer": {
                           **({} if size == 2 else {"ack_route_set_size": size}),
                           **({} if interval == 1 else {"ack_route_set_probe_rtts": interval}),
                           "ack_route_set_reply": "answer"}
                          for size in (2, 4) for interval in (1, 4, 16)}}
A2A_RSET_SWEEP_CELLS = tuple(cell for cell in A2A_ACKROUTE_CELLS if cell[2].startswith("blackhole"))
A2A_RSET_SWEEP_SEEDS = (301,)
A2A_TRANSPORT_ARMS.update({arm: "reps_hold_auto" for arm in A2A_RSET_SWEEP_ARMS})
# a2a_twoend_rset_4k: a2a_twoend_4k's cells, healthy pairs and seeds under
# a2a_twoend_4k's reps_hold_auto_tail4_cover_answer_seq and two of
# a2a_rset_sweep_4k's arms, reps_hold_auto_tail4_cover_reverse_set_2x4_answer
# and reps_hold_auto_tail4_cover_reverse_set_2x16_answer
# (A2A_TWOEND_RSET_ARMS), none with the overdue repair, so that the three are
# read on one build.
A2A_TWOEND_RSET_STAGE = "a2a_twoend_rset_4k"
A2A_TWOEND_RSET_ARMS = ("reps_hold_auto_tail4_cover_answer_seq",
                        "reps_hold_auto_tail4_cover_reverse_set_2x4_answer",
                        "reps_hold_auto_tail4_cover_reverse_set_2x16_answer")
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
# copy_expiry deadline (A2A_COPY_DEADLINE: load_balancing copy_expiry, written
# as SPRAY_COPY_EXPIRY): a loss-hold arm's copies under sentinel_mode
# duplicate declared lost only past tail_loss_rtts base RTTs since they were
# sent, whatever their data's acknowledgement, rather than at their data's
# acknowledgement past one base RTT. A stage selects it by passing its
# settings through a2a_copy_deadline, as the coverage-first arms of
# a2a_twoend_4k and a2a_hostlink_s_4k do.
A2A_COPY_DEADLINE = {"copy_expiry": "deadline"}
# a2a_hostlink_4k: one host's access link lossy from the onset, no spine
# faulted: a gray loss at 1e-2 or 1e-3 (A2A_LOSS_RATES) on the link of host 9
# (A2A_FAULT_HOST, the second host of leaf 1, the leaf of the down cells'
# link), up (host to leaf) or down (leaf to host), the condition named
# host<kind>_<direction> (a2a_host_fault), its loss per byte as the fixture
# takes every gray loss. On EP64 at 256 KiB and 2 MiB and on the decode
# cell, each with its healthy pair, seeds 301-303, under the 4096-byte arms but
# reps_oracle_tail, whose oracle reads the leaf-spine links alone, every
# answer routed over its data's spine (no ack_route).
A2A_HOST_PREFIX = "host"
A2A_FAULT_HOST = 9
A2A_HOSTLINK_STAGE = "a2a_hostlink_4k"
A2A_HOSTLINK_CONDITIONS = ("hostloss1e2_up", "hostloss1e3_up", "hostloss1e2_down", "hostloss1e3_down")
A2A_HOSTLINK_CELLS = tuple((group, size, condition) for group, size in (("ep64", "256k"), ("ep64", "2m"), A2A_DECODE)
                           for condition in A2A_HOSTLINK_CONDITIONS)
A2A_HOSTLINK_ARMS = A2A_HOLD_ARMS_4K + ("reps_tail", "uniform_tail")
# a2a_hostlink_s_4k: a2a_hostlink_4k's cells, healthy pairs and seeds under
# its loss-hold arms with coverage_first (A2A_COVERAGE_FIRST) and copy_expiry
# deadline (A2A_COPY_DEADLINE), every answer routed over its data's spine:
# each arm is its a2a_hostlink_4k arm's setting (A2A_HOSTLINK_COVER_ARMS)
# with both, so that its runs share a2a_hostlink_4k's cells.
A2A_HOSTLINK_COVER_STAGE = "a2a_hostlink_s_4k"
A2A_HOSTLINK_COVER_ARMS = {f"{arm}_cover": arm for arm in A2A_HOLD_ARMS_4K}
# The ring's stages: ar_parity_probe_4k, one healthy uniform_tail run, seed
# 301, which reads its healthy step time; ar_parity_calibration_4k,
# a2a_calibration's per_receiver pass on it; and ar_parity_4k, the
# A2A_ARMS_4K at loss 1e-2 on leaf 0's uplink to spine 3 with the healthy
# pair, seeds 301-303.
A2A_RING_PROBE_STAGE = "ar_parity_probe_4k"
A2A_RING_CALIBRATION_STAGE = "ar_parity_calibration_4k"
A2A_RING_STAGE = "ar_parity_4k"
# The path-count fixtures (A2A_SPINES_FIXTURE): the settled fixture on a
# fabric of P spines (A2A_SPINES), each leaf's 3.2 Tb/s up to the spines
# (A2A_LEAF_UPLINK_GBPS) split over them, so that the leaf-spine links run at
# 3.2 Tb/s / P: 400, 100, 25 and 12.5 Gb/s. A run on one carries, beyond
# A2A_FIXTURE_4K:
#   spine_count      network.spine_count: P
#   spine_link_rate  network.spine_link_rate: 3200 / P Gb/s
#   pair_bandwidth   network.pair_bandwidth: spines, a pair of hosts on two
#                    leaves taken at the sum of its paths over the spines, at
#                    most its host links (400 Gb/s at every P), as its sprayed
#                    packets cross every spine, so that its window's BDP is
#                    not one spine link's
#   port_rate_thresholds
#                    network.switch.port_rate_thresholds: true, each switch
#                    port's ECN and queue thresholds the ue profile's bytes
#                    scaled by its link speed over the host links' 400 Gb/s,
#                    so that a port marks, trims and drops at the queueing
#                    delay a 400 Gb/s port does; each switch's 32 MB shared
#                    buffer as at 8 spines
#   base_rtt_ns      the fabric's base RTT, the generator's config_base_rtt_ns
#                    (the backend's maxRtt and SprayBaseRttNs): the longest
#                    host-to-host round trip, its propagation and one 4096 B
#                    packet's serialization per hop, 81 ns at 400 Gb/s and
#                    2,621 ns at 12.5 Gb/s
# The retransmission timeout and the loss and pool decay intervals follow the
# base RTT as they do at 8 spines, 10 and 2 base RTTs (a2a_settle). At P = 8
# the fabric is A2A_FIXTURE_4K's.
A2A_SPINES = (8, 32, 128, 256)
A2A_LEAF_UPLINK_GBPS = 3200
A2A_SPINES_BASE_RTT_NS = {8: 12_324, 32: 12_816, 128: 14_782, 256: 17_404}
A2A_SPINES_FIXTURE = {spines: {**A2A_FIXTURE_4K, "spine_count": spines,
                               "spine_link_rate": f"{A2A_LEAF_UPLINK_GBPS / spines:g}Gbps",
                               "pair_bandwidth": "spines", "port_rate_thresholds": True,
                               "base_rtt_ns": A2A_SPINES_BASE_RTT_NS[spines]}
                      for spines in A2A_SPINES}
# The load-balancing keys a stage writes in base RTTs of the 8-spine fabric,
# as multiples of BASE_RTT_NS, which a path-count fixture writes in its own.
A2A_BASE_RTT_KEYS = ("loss_interval_ns", "pool_decay_interval_ns")
# Per P, a probe stage (a2a_spines<P>_probe_4k) of the pairs the path-count
# stages run (A2A_SPINES_PAIRS), EP64 at 256 KiB and 2 MiB, at a2a_probe_4k's
# provisional gaps; and a2a_calibration_4k's per_receiver pass on the same
# pairs (a2a_spines<P>_calibration_4k), at the gaps the probe at P measured:
# a pair's gap at P is taken as measured once it is entered in
# A2A_SPINES_COMPUTE_NS and listed in A2A_SPINES_COMPUTE_MEASURED; until then
# a2a_stage_runs refuses the calibration stage. analyze.py a2a_calibrate names
# the cells <group>-<size>-p4096-s<P> (a2a_cell_name).
A2A_SPINES_PAIRS = (("ep64", "256k"), ("ep64", "2m"))
A2A_SPINES_PROBE_STAGES = {f"a2a_spines{spines}_probe_4k": spines for spines in A2A_SPINES}
A2A_SPINES_CALIBRATION_STAGES = {f"a2a_spines{spines}_calibration_4k": spines for spines in A2A_SPINES}
A2A_SPINES_COMPUTE_NS: dict[int, dict[tuple[str, str], int]] = {spines: {} for spines in A2A_SPINES}
A2A_SPINES_COMPUTE_MEASURED: dict[int, tuple[tuple[str, str], ...]] = {spines: () for spines in A2A_SPINES}
# The wide-leaf fixtures (A2A_WIDE_FIXTURE): the settled fixture on a fabric
# of P spines and P hosts per leaf (A2A_WIDE), every link at 400 Gb/s, the
# EP64 job on the first A2A_WIDE_JOB_HOSTS hosts of each of the
# A2A_WIDE_LEAVES leaves and every other host a background sender. A run on
# one carries, beyond A2A_FIXTURE_4K:
#   wide               P: network.spine_count and network.hosts_per_leaf P,
#                      and parallelism.dp the fabric's 8 P hosts
#   buffer_size_mb     network.fabric.buffer_size_mb: 2 MB for each of a
#                      leaf's 2 P ports, as the 16-port leaf has 32 MB at
#                      P = 8; the backend gives every switch the one pool
#   job_hosts          workload.group: the first 8 hosts of every leaf, for
#                      a run whose group is every host
#   background         workload.background: leaf_shift at 350 Gb/s
#                      (A2A_WIDE_BACKGROUND), each other host sending one
#                      message of a job host's bytes to the other leaves with
#                      each all-to-all collective
#   sparse_hold_trace  network.sparse_hold_trace: true, for a run that writes
#                      the sender loss trace (a2a_traces_spine_loss), as the
#                      generator requires
# Every link is a 400 Gb/s one, so the base RTT, the retransmission timeout
# and the loss intervals are the 8-spine fabric's at every P. At P = 8 the
# fixture is A2A_FIXTURE_4K: its leaves have no other host.
A2A_WIDE = (8, 32, 128, 256)
A2A_WIDE_LEAVES = 8
A2A_WIDE_JOB_HOSTS = 8
A2A_WIDE_BACKGROUND = {"pattern": "leaf_shift", "rate": "350Gbps"}
A2A_WIDE_FIXTURE = {wide: {**A2A_FIXTURE_4K, "wide": wide,
                           **({} if wide == A2A_WIDE_JOB_HOSTS else {
                               "buffer_size_mb": 4 * wide, "job_hosts": A2A_WIDE_JOB_HOSTS,
                               "background": A2A_WIDE_BACKGROUND, "sparse_hold_trace": True})}
                    for wide in A2A_WIDE}
# Per P, on EP64 at 256 KiB and 2 MiB (A2A_WIDE_PAIRS), every stage at
# P = 8's measured compute gaps (A2A_PAYLOAD_COMPUTE_NS), which a member
# whose healthy span is within 1% of P = 8's takes:
# - a2a_wide<P>_validity_4k (P above 8): healthy uniform_tail at seed 301,
#   A2A_WIDE_VALIDITY_ITERATIONS iterations, with the port counters every
#   A2A_WIDE_PORT_COUNTER_NS and the switch queues' maxima
#   (network.port_counter_interval_ns), which analyze.py a2a_wide_validity
#   reads: the trims, the collective span against P = 8's, the uplinks'
#   load and the queues against their thresholds. It is the member's probe.
# - a2a_wide<P>_calibration_4k: a2a_calibration_4k's per_receiver pass on
#   the pairs. analyze.py a2a_calibrate names the cells
#   <group>-<size>-p4096-w<P> (a2a_cell_name).
# - a2a_wide<P>_4k: the pairs under loss 1e-3 up and down, loss 1e-2 down,
#   blackhole up and a multi-fault draw at loss 1e-3 and at 1e-2
#   (A2A_WIDE_CONDITIONS), each with its healthy pair, seeds 301-303, at
#   A2A_RUN_ITERATIONS, every answer on a spine its receiver draws
#   (A2A_ACKROUTE). The arms (A2A_WIDE_ARMS): reps_tail and uniform_tail;
#   reps_hold_auto_tail4_cover, a2a_twoend_4k's; reps_hold_auto_tail4_cover_rx,
#   that with the receiver's hold (A2A_RECEIVER_HOLD), which also counts as
#   missing the sends a tail repair request shows unanswered
#   (receiver_tail_missing) and reads a flow's late packets against its
#   removed queue pair (A2A_STRAY_CLOSED); reps_oracle_tail_fwd,
#   a2a_ackroute_fwd_4k's, whose oracle reads the data's own hops; and on
#   blackhole up and the two multi-fault draws alone, each with its healthy
#   pair (A2A_WIDE_ARM_CONDITIONS), reps_hold_auto_tail4_cover_answer_seq,
#   a2a_twoend_4k's, and reps_hold_auto_tail4_cover_reverse_set,
#   a2a_twoend_base_4k's; and, on every condition,
#   reps_hold_auto_tail4_cover_rx_answer_seq, reps_hold_auto_tail4_cover_rx
#   with answer path sequences (A2A_ANSWER_PATH_SEQ), and
#   reps_hold_auto_tail4_cover_reverse_set_rx, the reverse set with the
#   receiver's hold of reps_hold_auto_tail4_cover_rx (A2A_WIDE_RECEIVER);
#   and reps_hold_auto_tail4_cover_rx_slots8 on every condition, and
#   reps_hold_auto_tail4_cover_reverse_set_rx_slots8 on the two multi-fault
#   draws alone, each with its healthy pair, those two arms with the
#   receiver's report a list of A2A_WIDE_SLOTS spines (receiver_hold_slots)
#   in place of the bitmap over every spine.
#   Every arm takes the stage's overdue repair
#   (A2A_WIDE_OVERDUE_REPAIR): "off", none, or K, overdue_repair reprobe
#   with overdue_repair_limit K as in A2A_OVERDUE, which reps_tail,
#   uniform_tail and reps_oracle_tail_fwd read as the hold arms do, after the
#   same wait of 2 base RTTs (held_loss_rtts' default). It is off; None
#   would leave it unchosen, and a2a_stage_runs refuses the stage then.
#   reps_hold_auto_tail4_cover_reverse_set and
#   reps_hold_auto_tail4_cover_reverse_set_rx take the reverse set's setting
#   the stage names (A2A_WIDE_REVERSE_SET), one of a2a_rset_sweep_4k's arms
#   (A2A_RSET_SWEEP_ARMS), whose keys it adds. It is
#   reps_hold_auto_tail4_cover_reverse_set_2x4_answer, a set of 2 spines
#   probed once per 4 base RTTs with the replies queued as the answers are;
#   None would leave it unchosen, and a2a_stage_runs refuses the stage then.
# - a2a_wide<P>_seeds_4k: seeds 304-306 (A2A_WIDE_SEEDS_SEEDS) of
#   a2a_wide<P>_4k's reps_hold_auto_tail4_cover,
#   reps_hold_auto_tail4_cover_rx, reps_hold_auto_tail4_cover_reverse_set,
#   reps_hold_auto_tail4_cover_reverse_set_rx and
#   reps_hold_auto_tail4_cover_rx_slots8 (A2A_WIDE_SEEDS_ARMS) on the two
#   multi-fault draws of both pairs, and of all but the two reverse-set arms
#   on 256 KiB loss 1e-2 and 1e-3 down (A2A_WIDE_SEEDS_CELLS,
#   A2A_WIDE_SEEDS_ARM_CONDITIONS),
#   each with its healthy pairs, at a2a_wide<P>_4k's settings: each run's
#   profile is that stage's run of its arm and cell but for its name.
# - a2a_wide<P>_spread_4k (P of A2A_WIDE_SPREAD_WIDE): seeds 301-312
#   (A2A_WIDE_SPREAD_SEEDS) of reps_hold_auto_tail4_cover and
#   reps_hold_auto_tail4_cover_rx_slots8 with held_sentinel_spread
#   A2A_WIDE_SPREAD, their _spread<N> arms (A2A_WIDE_SPREAD_ARMS), at
#   a2a_wide<P>_4k's settings otherwise, on 256 KiB loss 1e-2 down and the
#   sparse draw at 1e-2 (A2A_WIDE_SPREAD_CELLS), each with its healthy pair.
# - a2a_wide256_widen_4k: seeds 307-312 (A2A_WIDE_WIDEN_SEEDS) of
#   reps_hold_auto_tail4_cover_rx_slots8_spread64_widen2, that arm of
#   a2a_wide<P>_spread_4k with receiver_hold_widen 2, on 256 KiB loss1e2_down,
#   sparse_loss1e2 and multi_loss1e3, and of the two _spread64 arms on
#   multi_loss1e3 alone (A2A_WIDE_WIDEN_ARM_CONDITIONS), each with its healthy
#   pair.
# A multi-fault condition, multi_<loss kind>: a gray loss at the kind's rate
# from the onset on A2A_MULTI_FRACTION of the leaf-spine link directions,
# drawn by A2A_MULTI_SEED (link_failures draw): the same directions in every
# run of the condition at one P, the same fraction at every P. A sparse
# condition, sparse_<loss kind>, is drawn the same way at
# A2A_SPARSE_FRACTION, 4 directions at 256 spines.
A2A_WIDE_PAIRS = (("ep64", "256k"), ("ep64", "2m"))
A2A_WIDE_VALIDITY_ITERATIONS = 2
A2A_WIDE_PORT_COUNTER_NS = 50_000
A2A_MULTI_PREFIX = "multi"
A2A_MULTI_FRACTION = 0.02
A2A_MULTI_SEED = 1
A2A_SPARSE_PREFIX = "sparse"
A2A_SPARSE_FRACTION = 0.001
A2A_WIDE_CONDITIONS = ("loss1e3_up", "loss1e3_down", "loss1e2_down", "blackhole_up", "multi_loss1e3",
                       "multi_loss1e2")
A2A_WIDE_CELLS = tuple((group, size, condition) for group, size in A2A_WIDE_PAIRS
                       for condition in A2A_WIDE_CONDITIONS)
A2A_WIDE_RECEIVER = {**A2A_RECEIVER_HOLD, "receiver_tail_missing": True, **A2A_STRAY_CLOSED}
A2A_WIDE_SLOTS = 8
A2A_WIDE_ARMS = ("reps_tail", "uniform_tail", "reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_rx",
                 "reps_oracle_tail_fwd", "reps_hold_auto_tail4_cover_answer_seq",
                 "reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover_rx_answer_seq",
                 "reps_hold_auto_tail4_cover_reverse_set_rx", f"reps_hold_auto_tail4_cover_rx_slots{A2A_WIDE_SLOTS}",
                 f"reps_hold_auto_tail4_cover_reverse_set_rx_slots{A2A_WIDE_SLOTS}")
A2A_WIDE_ARM_CONDITIONS = {
    **{arm: ("blackhole_up", "multi_loss1e3", "multi_loss1e2")
       for arm in ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_reverse_set")},
    f"reps_hold_auto_tail4_cover_reverse_set_rx_slots{A2A_WIDE_SLOTS}": ("multi_loss1e3", "multi_loss1e2")}
A2A_TRANSPORT_ARMS.update({f"{arm}_slots{A2A_WIDE_SLOTS}": "reps_hold_auto"
                           for arm in ("reps_hold_auto_tail4_cover_rx", "reps_hold_auto_tail4_cover_reverse_set_rx")})
A2A_WIDE_OVERDUE_REPAIR: str | int | None = "off"
A2A_WIDE_REVERSE_SET: str | None = "reps_hold_auto_tail4_cover_reverse_set_2x4_answer"
A2A_WIDE_VALIDITY_STAGES = {f"a2a_wide{wide}_validity_4k": wide for wide in A2A_WIDE if wide != A2A_WIDE_JOB_HOSTS}
A2A_WIDE_CALIBRATION_STAGES = {f"a2a_wide{wide}_calibration_4k": wide for wide in A2A_WIDE}
A2A_WIDE_STAGES = {f"a2a_wide{wide}_4k": wide for wide in A2A_WIDE}
A2A_WIDE_SEEDS_ARMS = ("reps_hold_auto_tail4_cover", "reps_hold_auto_tail4_cover_rx",
                       "reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover_reverse_set_rx",
                       f"reps_hold_auto_tail4_cover_rx_slots{A2A_WIDE_SLOTS}")
A2A_WIDE_SEEDS_CELLS = tuple((group, size, condition) for group, size in A2A_WIDE_PAIRS
                             for condition in ("multi_loss1e3", "multi_loss1e2")) + (
    ("ep64", "256k", "loss1e2_down"), ("ep64", "256k", "loss1e3_down"))
A2A_WIDE_SEEDS_ARM_CONDITIONS = {arm: ("multi_loss1e3", "multi_loss1e2")
                                 for arm in ("reps_hold_auto_tail4_cover_reverse_set",
                                             "reps_hold_auto_tail4_cover_reverse_set_rx")}
A2A_WIDE_SEEDS_SEEDS = (304, 305, 306)
A2A_WIDE_SEEDS_STAGES = {f"a2a_wide{wide}_seeds_4k": wide for wide in A2A_WIDE}
A2A_WIDE_SPREAD = 64
A2A_WIDE_SPREAD_BASES = ("reps_hold_auto_tail4_cover", f"reps_hold_auto_tail4_cover_rx_slots{A2A_WIDE_SLOTS}")
A2A_WIDE_SPREAD_ARMS = tuple(f"{arm}_spread{A2A_WIDE_SPREAD}" for arm in A2A_WIDE_SPREAD_BASES)
A2A_TRANSPORT_ARMS.update({arm: "reps_hold_auto" for arm in A2A_WIDE_SPREAD_ARMS})
A2A_WIDE_SPREAD_CELLS = (("ep64", "256k", "loss1e2_down"), ("ep64", "256k", f"{A2A_SPARSE_PREFIX}_loss1e2"))
A2A_WIDE_SPREAD_SEEDS = tuple(range(301, 313))
A2A_WIDE_SPREAD_WIDE = (32, 256)
A2A_WIDE_SPREAD_STAGES = {f"a2a_wide{wide}_spread_4k": wide for wide in A2A_WIDE_SPREAD_WIDE}
A2A_WIDE_WIDEN = 2
A2A_WIDE_WIDEN_ARM = f"{A2A_WIDE_SPREAD_ARMS[1]}_widen{A2A_WIDE_WIDEN}"
A2A_WIDE_WIDEN_ARMS = (*A2A_WIDE_SPREAD_ARMS, A2A_WIDE_WIDEN_ARM)
A2A_TRANSPORT_ARMS[A2A_WIDE_WIDEN_ARM] = "reps_hold_auto"
A2A_WIDE_WIDEN_CELLS = (*A2A_WIDE_SPREAD_CELLS, ("ep64", "256k", "multi_loss1e3"))
A2A_WIDE_WIDEN_ARM_CONDITIONS = {arm: ("multi_loss1e3",) for arm in A2A_WIDE_SPREAD_ARMS}
A2A_WIDE_WIDEN_SEEDS = tuple(range(307, 313))
A2A_WIDE_WIDEN_STAGES = {"a2a_wide256_widen_4k": 256}
# The 4096-byte stages: the probes and the (group, size) each runs at its
# provisional gap, the calibration passes and their pairs, and the faulted
# stages (a2a_faulted_4k).
A2A_PROBE_STAGES = {A2A_PROBE_4K_STAGE: A2A_PROBE_4K_COMPUTE_NS, A2A_DECODE_PROBE_STAGE: A2A_DECODE_PROBE_COMPUTE_NS,
                    A2A_RING_PROBE_STAGE: {A2A_RING: 0},
                    **{stage: {pair: A2A_PROBE_4K_COMPUTE_NS[pair] for pair in A2A_SPINES_PAIRS}
                       for stage in A2A_SPINES_PROBE_STAGES}}
A2A_CALIBRATION_STAGES_4K = {A2A_CALIBRATION_4K_STAGE: A2A_CALIBRATION_4K_CELLS,
                             A2A_DECODE_CALIBRATION_STAGE: (A2A_DECODE,), A2A_RING_CALIBRATION_STAGE: (A2A_RING,),
                             **{stage: A2A_SPINES_PAIRS for stage in A2A_SPINES_CALIBRATION_STAGES},
                             **{stage: A2A_WIDE_PAIRS for stage in A2A_WIDE_CALIBRATION_STAGES}}
A2A_STAGE_FIXTURE = {stage: A2A_FIXTURE_4K
                     for stage in (A2A_PROBE_4K_STAGE, A2A_CALIBRATION_4K_STAGE, A2A_TAIL_ACK_4K_STAGE,
                                   A2A_SWEEP_STAGE, A2A_DOWNLINK_STAGE, A2A_DECODE_PROBE_STAGE,
                                   A2A_DECODE_CALIBRATION_STAGE, A2A_DECODE_STAGE, A2A_REACTION_STAGE,
                                   A2A_RING_PROBE_STAGE, A2A_RING_CALIBRATION_STAGE, A2A_RING_STAGE,
                                   A2A_ACKROUTE_STAGE, A2A_NOACK_STAGE, A2A_HOSTLINK_STAGE,
                                   A2A_ACKROUTE_FWD_STAGE, A2A_DOWNLINK_SPRAY_STAGE, A2A_TWOEND_STAGE,
                                   A2A_HOSTLINK_COVER_STAGE, A2A_TWOEND_BASE_STAGE, A2A_TWOEND_RETRACT_STAGE,
                                   A2A_OVERDUE_STAGE, A2A_RSET_SWEEP_STAGE, A2A_TWOEND_RSET_STAGE)}
A2A_STAGE_FIXTURE.update({stage: A2A_SPINES_FIXTURE[spines]
                          for stages in (A2A_SPINES_PROBE_STAGES, A2A_SPINES_CALIBRATION_STAGES)
                          for stage, spines in stages.items()})
A2A_STAGE_FIXTURE.update({stage: A2A_WIDE_FIXTURE[wide]
                          for stages in (A2A_WIDE_CALIBRATION_STAGES, A2A_WIDE_STAGES, A2A_WIDE_SEEDS_STAGES,
                                         A2A_WIDE_SPREAD_STAGES, A2A_WIDE_WIDEN_STAGES)
                          for stage, wide in stages.items()})
A2A_STAGE_FIXTURE.update({stage: {**A2A_WIDE_FIXTURE[wide], "port_counter_interval_ns": A2A_WIDE_PORT_COUNTER_NS}
                          for stage, wide in A2A_WIDE_VALIDITY_STAGES.items()})
A2A_STAGES = ("a2a_oracle_calibration", "a2a_gate", "a2a_calibration", "a2a_tuning", "a2a_tuning_mix",
              "a2a_comparison", "a2a_gate_reps", "a2a_candidates", "a2a_receiver", A2A_RS3_STAGE,
              A2A_TAIL_STAGE, A2A_ACKLOSS_STAGE, A2A_CANDIDATES_ACK_STAGE, A2A_TAIL_ACK_STAGE,
              *A2A_STAGE_FIXTURE)


def a2a_cell_name(group: str, size: str, payload: int | None = None, spines: int | None = None,
                  wide: int | None = None) -> str:
    """<group>-<size>, -p<payload> for a run with a packet payload, -s<spines>
    for one on a path-count fixture and -w<wide> for one on a wide-leaf
    fixture."""
    return (f"{group}-{size}" + ("" if payload is None else f"-p{payload}")
            + ("" if spines is None else f"-s{spines}") + ("" if wide is None else f"-w{wide}"))


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
    if "rtt_sample" in fixture:
        network["congestion_control"] = {**network["congestion_control"], "rtt_sample": fixture["rtt_sample"]}
    if "dataset_splits" in fixture:
        document["workload"]["dataset_splits"] = fixture["dataset_splits"]
    if "loss_unit" in fixture:
        network["link_failures"] = [
            {**failure, "loss_unit": fixture["loss_unit"]} if failure["model"] == "gray" and "error_rate" in failure
            else failure
            for failure in network["link_failures"]
        ]
    for key in ("spine_count", "spine_link_rate", "pair_bandwidth"):
        if key in fixture:
            network[key] = fixture[key]
    if "port_rate_thresholds" in fixture:
        network["switch"] = {**network["switch"], "port_rate_thresholds": fixture["port_rate_thresholds"]}
    if "wide" in fixture:
        network["spine_count"] = network["hosts_per_leaf"] = fixture["wide"]
        document["parallelism"] = {**document["parallelism"], "dp": A2A_WIDE_LEAVES * fixture["wide"]}
    if "buffer_size_mb" in fixture:
        network["fabric"] = {**network["fabric"], "buffer_size_mb": fixture["buffer_size_mb"]}
    if "job_hosts" in fixture and document["workload"].get("group") == "all":
        document["workload"]["group"] = {"leaves": list(range(A2A_WIDE_LEAVES)), "hosts": fixture["job_hosts"]}
    if "background" in fixture and document["workload"].get("kind") == "all_to_all":
        document["workload"]["background"] = dict(fixture["background"])
    if "sparse_hold_trace" in fixture and a2a_traces_spine_loss(network["load_balancing"]):
        network["sparse_hold_trace"] = fixture["sparse_hold_trace"]
    if "port_counter_interval_ns" in fixture:
        network["port_counter_interval_ns"] = fixture["port_counter_interval_ns"]

    if "base_rtt_ns" in fixture:
        rtt = fixture["base_rtt_ns"]
        balancing = network["load_balancing"]
        for key in A2A_BASE_RTT_KEYS:
            if key in balancing and balancing[key] % BASE_RTT_NS:
                raise ValueError(f"{key} {balancing[key]} is no whole number of base RTTs")
        network["load_balancing"] = {**balancing, **{key: balancing[key] // BASE_RTT_NS * rtt
                                                     for key in A2A_BASE_RTT_KEYS if key in balancing}}
        network["transport_recovery"] = {**network["transport_recovery"], "retransmission_timeout_ns": 10 * rtt}


def a2a_traces_spine_loss(balancing: dict[str, Any]) -> bool:
    """Whether a run of the load balancing writes the sender loss trace, as
    the generator reads it: the loss hold of reps_hold and mrc_hold, and
    spray_policy's host-wide pool with loss thresholds outside the
    switch_queue cost source."""
    if balancing.get("mode") == "spray_uniform":
        return balancing.get("selector") in ("reps_hold", "mrc_hold")
    return (balancing.get("mode") == "spray_policy" and "loss_thresholds" in balancing
            and balancing.get("sender_pool_weight", 0) > 0 and balancing.get("grade_cost_source") != "switch_queue")


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
    if condition.startswith((A2A_MULTI_PREFIX, A2A_SPARSE_PREFIX)):
        return a2a_multi_fault(condition)
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


def a2a_multi_fault(condition: str) -> list[dict[str, Any]]:
    """The link_failures of a multi-fault condition, multi_<loss kind>, or a
    sparse one, sparse_<loss kind>: a gray loss at the kind's rate from the
    onset on A2A_MULTI_FRACTION, or A2A_SPARSE_FRACTION, of the leaf-spine
    link directions, drawn by A2A_MULTI_SEED."""
    prefix, _, loss = condition.partition("_")
    fraction = {A2A_MULTI_PREFIX: A2A_MULTI_FRACTION, A2A_SPARSE_PREFIX: A2A_SPARSE_FRACTION}.get(prefix)
    if fraction is None or loss not in A2A_LOSS_RATES:
        raise ValueError(f"unknown condition {condition}")
    return [{"model": "gray", "error_rate": A2A_LOSS_RATES[loss], "start_ns": DEGRADE_AT_NS,
             "draw": {"fraction": fraction, "seed": A2A_MULTI_SEED}}]


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
                  setting: dict[str, Any] | None, payload: int | None = None,
                  wide: int | None = None) -> dict[str, Any]:
    """An arm's load balancing: the t_ stages' graded settings (edge window
    penalty 0, edge_freeze off, estimator_interval_samples 0, gain 0.25)
    plus this phase's calibrated thresholds at the run's payload (and
    wide-leaf fixture) and the loss grade."""
    import transient

    if arm in ("uniform",) + A2A_FIXED_ARMS:
        return balancing(arm, 0, None)
    if arm == A2A_REPS_ORACLE:
        return {"mode": "spray_uniform", "selector": "reps_oracle", "grade_cost_source": "switch_queue"}
    if arm == A2A_REPS_SPINE:
        return {"mode": "spray_uniform", "selector": "reps_spine"}
    if arm in A2A_TRANSPORT_ARMS and A2A_TRANSPORT_ARMS[arm] not in A2A_REPS_HOLD_ARMS:
        # The setting is the transport keys alone.
        return {**a2a_balancing(A2A_TRANSPORT_ARMS[arm], group, size, calibration, None, payload, wide),
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
        entry = (calibration or {}).get("per_receiver", {}).get(a2a_cell_name(group, size, payload, wide=wide), {})
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


def a2a_ackroute_fwd_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_ackroute_fwd_4k arm's setting: its a2a_ackroute_4k arm's
    (A2A_ACKROUTE_FWD_ARMS) with A2A_ORACLE_FORWARD."""
    settings = a2a_ackroute_settings(tuning)
    return {arm: {**settings[of], **A2A_ORACLE_FORWARD} for arm, of in A2A_ACKROUTE_FWD_ARMS.items()}


def a2a_downlink_spray_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_downlink_spray_4k arm's setting, with A2A_ACKROUTE:
    reps_hold_auto_tail4's of a2a_tail and the others' of a2a_tail_ack; a
    receiver arm's (A2A_DOWNLINK_SPRAY_RECEIVER_ARMS) its arm's with
    A2A_STRAY_CLOSED, less ack_loss_rtts for reps_hold_auto_tail4_rx_closed;
    and reps_oracle_tail_fwd's of a2a_ackroute_fwd_4k."""
    base = {**a2a_tail_settings(tuning), **a2a_tail_ack_settings(tuning, tuple(A2A_TAIL_ACK_ARMS))}
    settings = {arm: {**base[arm], **A2A_ACKROUTE} for arm in A2A_DOWNLINK_SPRAY_ARMS if arm in base}
    for arm, of in A2A_DOWNLINK_SPRAY_RECEIVER_ARMS.items():
        setting = {**base[of], **A2A_ACKROUTE, **A2A_STRAY_CLOSED}
        if arm == "reps_hold_auto_tail4_rx_closed":
            setting.pop("ack_loss_rtts")
        settings[arm] = setting
    settings["reps_oracle_tail_fwd"] = a2a_ackroute_fwd_settings(tuning)["reps_oracle_tail_fwd"]
    return {arm: settings[arm] for arm in A2A_DOWNLINK_SPRAY_ARMS}


def a2a_twoend_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_twoend_4k arm's setting: a2a_ackroute_4k's for its arms, and
    a coverage-first arm's (A2A_TWOEND_COVER_ARMS) reps_hold_auto_tail4's with
    A2A_COVERAGE_FIRST, through a2a_copy_deadline, and its keys."""
    settings = a2a_ackroute_settings(tuning)
    cover = a2a_copy_deadline({"cover": {**settings["reps_hold_auto_tail4"], **A2A_COVERAGE_FIRST}})["cover"]
    for arm, keys in A2A_TWOEND_COVER_ARMS.items():
        settings[arm] = {**cover, **keys}
    return {arm: settings[arm] for arm in A2A_TWOEND_ARMS}


def a2a_twoend_base_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_twoend_base_4k arm's setting: a2a_twoend_4k's
    reps_hold_auto_tail4_cover with its keys (A2A_TWOEND_BASE_ARMS)."""
    cover = a2a_twoend_settings(tuning)["reps_hold_auto_tail4_cover"]
    return {arm: {**cover, **keys} for arm, keys in A2A_TWOEND_BASE_ARMS.items()}


def a2a_twoend_retract_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_twoend_retract_4k arm's setting: a2a_twoend_4k's
    reps_hold_auto_tail4_cover with its keys (A2A_TWOEND_RETRACT_ARMS)."""
    cover = a2a_twoend_settings(tuning)["reps_hold_auto_tail4_cover"]
    return {arm: {**cover, **keys} for arm, keys in A2A_TWOEND_RETRACT_ARMS.items()}


def a2a_overdue_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_overdue_4k arm's setting: its key-off arm's
    (A2A_OVERDUE_ARMS) of a2a_twoend_4k, a2a_twoend_base_4k or
    a2a_twoend_retract_4k, with A2A_OVERDUE."""
    settings = {**a2a_twoend_settings(tuning), **a2a_twoend_base_settings(tuning),
                **a2a_twoend_retract_settings(tuning)}
    return {arm: {**settings[of], **A2A_OVERDUE} for arm, of in A2A_OVERDUE_ARMS.items()}


def a2a_rset_sweep_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_rset_sweep_4k arm's setting: a2a_twoend_base_4k's
    reps_hold_auto_tail4_cover_reverse_set with its keys
    (A2A_RSET_SWEEP_ARMS)."""
    reverse_set = a2a_twoend_base_settings(tuning)["reps_hold_auto_tail4_cover_reverse_set"]
    return {arm: {**reverse_set, **keys} for arm, keys in A2A_RSET_SWEEP_ARMS.items()}


def a2a_twoend_rset_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_twoend_rset_4k arm's setting: a2a_twoend_4k's or
    a2a_rset_sweep_4k's (A2A_TWOEND_RSET_ARMS)."""
    settings = {**a2a_twoend_settings(tuning), **a2a_rset_sweep_settings(tuning)}
    return {arm: settings[arm] for arm in A2A_TWOEND_RSET_ARMS}


def a2a_hostlink_cover_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_hostlink_s_4k arm's setting: its a2a_hostlink_4k arm's
    (A2A_HOSTLINK_COVER_ARMS) with A2A_COVERAGE_FIRST, through
    a2a_copy_deadline."""
    base = a2a_tail_ack_settings(tuning, tuple(A2A_HOSTLINK_COVER_ARMS.values()))
    return a2a_copy_deadline({arm: {**base[of], **A2A_COVERAGE_FIRST} for arm, of in A2A_HOSTLINK_COVER_ARMS.items()})


def a2a_wide_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_wide<P>_4k arm's setting, every answer on a spine its
    receiver draws (A2A_ACKROUTE): reps_tail's of a2a_ackroute_4k,
    uniform_tail's of a2a_tail_ack with A2A_ACKROUTE,
    reps_hold_auto_tail4_cover's of a2a_twoend_4k and with A2A_WIDE_RECEIVER
    for reps_hold_auto_tail4_cover_rx, reps_oracle_tail_fwd's of
    a2a_ackroute_fwd_4k, reps_hold_auto_tail4_cover_answer_seq's of
    a2a_twoend_4k and reps_hold_auto_tail4_cover_reverse_set's of
    a2a_twoend_base_4k with A2A_WIDE_REVERSE_SET's keys;
    reps_hold_auto_tail4_cover_rx's with A2A_ANSWER_PATH_SEQ for
    reps_hold_auto_tail4_cover_rx_answer_seq, and
    reps_hold_auto_tail4_cover_reverse_set's with A2A_WIDE_RECEIVER for
    reps_hold_auto_tail4_cover_reverse_set_rx; those two receiver arms'
    with receiver_hold_slots A2A_WIDE_SLOTS for their _slots arms; every arm with
    overdue_repair reprobe and overdue_repair_limit A2A_WIDE_OVERDUE_REPAIR
    where that is a limit."""
    twoend = a2a_twoend_settings(tuning)
    cover = twoend["reps_hold_auto_tail4_cover"]
    settings = {
        "reps_tail": a2a_ackroute_settings(tuning)["reps_tail"],
        "uniform_tail": {**a2a_tail_ack_settings(tuning, ("uniform_tail",))["uniform_tail"], **A2A_ACKROUTE},
        "reps_hold_auto_tail4_cover": cover,
        "reps_hold_auto_tail4_cover_rx": {**cover, **A2A_WIDE_RECEIVER},
        "reps_oracle_tail_fwd": a2a_ackroute_fwd_settings(tuning)["reps_oracle_tail_fwd"],
        "reps_hold_auto_tail4_cover_answer_seq": twoend["reps_hold_auto_tail4_cover_answer_seq"],
        "reps_hold_auto_tail4_cover_reverse_set": {
            **a2a_twoend_base_settings(tuning)["reps_hold_auto_tail4_cover_reverse_set"],
            **A2A_RSET_SWEEP_ARMS.get(A2A_WIDE_REVERSE_SET, {})},
    }
    settings["reps_hold_auto_tail4_cover_rx_answer_seq"] = {**settings["reps_hold_auto_tail4_cover_rx"],
                                                           **A2A_ANSWER_PATH_SEQ}
    settings["reps_hold_auto_tail4_cover_reverse_set_rx"] = {**settings["reps_hold_auto_tail4_cover_reverse_set"],
                                                            **A2A_WIDE_RECEIVER}
    for arm in ("reps_hold_auto_tail4_cover_rx", "reps_hold_auto_tail4_cover_reverse_set_rx"):
        settings[f"{arm}_slots{A2A_WIDE_SLOTS}"] = {**settings[arm], "receiver_hold_slots": A2A_WIDE_SLOTS}
    overdue = ({} if A2A_WIDE_OVERDUE_REPAIR in (None, "off")
               else {"overdue_repair": "reprobe", "overdue_repair_limit": A2A_WIDE_OVERDUE_REPAIR})
    return {arm: {**settings[arm], **overdue} for arm in A2A_WIDE_ARMS}


def a2a_wide_spread_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_wide<P>_spread_4k arm's setting: its base arm's of
    a2a_wide_settings with held_sentinel_spread A2A_WIDE_SPREAD."""
    wide = a2a_wide_settings(tuning)
    return {f"{arm}_spread{A2A_WIDE_SPREAD}": {**wide[arm], "held_sentinel_spread": A2A_WIDE_SPREAD}
            for arm in A2A_WIDE_SPREAD_BASES}


def a2a_wide_widen_settings(tuning: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Each a2a_wide256_widen_4k arm's setting: the _spread64 arms' of
    a2a_wide_spread_settings, and A2A_WIDE_WIDEN_ARM's, the receiver one's with
    receiver_hold_widen A2A_WIDE_WIDEN."""
    spread = a2a_wide_spread_settings(tuning)
    return {**spread, A2A_WIDE_WIDEN_ARM: {**spread[A2A_WIDE_SPREAD_ARMS[1]], "receiver_hold_widen": A2A_WIDE_WIDEN}}


def a2a_copy_deadline(settings: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """settings with A2A_COPY_DEADLINE on every setting whose copies it
    expires, those under sentinel_mode duplicate; the others as given."""
    return {arm: {**setting, **A2A_COPY_DEADLINE} if setting.get("sentinel_mode") == "duplicate" else setting
            for arm, setting in settings.items()}


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
        A2A_ACKROUTE_FWD_STAGE: (A2A_ACKROUTE_CELLS, tuple(A2A_ACKROUTE_FWD_ARMS)),
        A2A_DOWNLINK_SPRAY_STAGE: (A2A_DOWNLINK_SPRAY_CELLS, A2A_DOWNLINK_SPRAY_ARMS),
        A2A_TWOEND_STAGE: (A2A_ACKROUTE_CELLS, A2A_TWOEND_ARMS),
        A2A_HOSTLINK_COVER_STAGE: (A2A_HOSTLINK_CELLS, tuple(A2A_HOSTLINK_COVER_ARMS)),
        A2A_TWOEND_BASE_STAGE: (A2A_ACKROUTE_CELLS, tuple(A2A_TWOEND_BASE_ARMS)),
        A2A_TWOEND_RETRACT_STAGE: (A2A_ACKROUTE_CELLS, tuple(A2A_TWOEND_RETRACT_ARMS)),
        A2A_OVERDUE_STAGE: (A2A_ACKROUTE_CELLS, tuple(A2A_OVERDUE_ARMS)),
        A2A_RSET_SWEEP_STAGE: (A2A_RSET_SWEEP_CELLS, tuple(A2A_RSET_SWEEP_ARMS)),
        A2A_TWOEND_RSET_STAGE: (A2A_ACKROUTE_CELLS, A2A_TWOEND_RSET_ARMS),
        **{wide_stage: (A2A_WIDE_CELLS, A2A_WIDE_ARMS) for wide_stage in A2A_WIDE_STAGES},
        **{seeds_stage: (A2A_WIDE_SEEDS_CELLS, A2A_WIDE_SEEDS_ARMS) for seeds_stage in A2A_WIDE_SEEDS_STAGES},
        **{spread_stage: (A2A_WIDE_SPREAD_CELLS, A2A_WIDE_SPREAD_ARMS) for spread_stage in A2A_WIDE_SPREAD_STAGES},
        **{widen_stage: (A2A_WIDE_WIDEN_CELLS, A2A_WIDE_WIDEN_ARMS) for widen_stage in A2A_WIDE_WIDEN_STAGES},
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
                   tuning: dict[str, Any] | None, record_filter: str = "") -> list[dict[str, Any]]:
    """Every run of a Phase 9 stage, or with record_filter the runs whose key
    it matches (re.search); a stage of hold arms then needs the calibrated
    loss thresholds of the pairs of those runs only."""
    out: list[dict[str, Any]] = []
    uncalibrated: list[tuple[str, str]] = []
    payload = (A2A_STAGE_FIXTURE.get(stage) or {}).get("payload_bytes")
    spines = (A2A_STAGE_FIXTURE.get(stage) or {}).get("spine_count")
    wide = (A2A_STAGE_FIXTURE.get(stage) or {}).get("wide")
    compute = (A2A_COMPUTE_NS if payload is None else A2A_SPINES_COMPUTE_NS[spines] if spines is not None
               else A2A_PAYLOAD_COMPUTE_NS[payload])
    measured = (A2A_SPINES_COMPUTE_MEASURED[spines] if spines is not None
                else A2A_PAYLOAD_COMPUTE_MEASURED.get(payload, ()))

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
        return a2a_cell_name(group, size, payload, wide=wide) in (calibration or {}).get(scope, {})

    def refuse_uncalibrated(pairs):
        names = [a2a_cell_name(group, size, payload, wide=wide) for group, size in pairs]
        raise ValueError(f"{stage}'s hold arms have no calibrated loss thresholds on {names}: "
                         "merge the payload's calibration pass into a2a_calibration.json")

    def require_measured(pairs):
        provisional = [a2a_cell_name(group, size) for group, size in pairs if (group, size) not in measured]
        if provisional and spines is not None:
            raise ValueError(f"{stage}'s compute gaps on {provisional} are provisional: enter the probe's values "
                             f"at {spines} spines in A2A_SPINES_COMPUTE_NS and list the pairs in "
                             "A2A_SPINES_COMPUTE_MEASURED")
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
    elif stage in A2A_WIDE_VALIDITY_STAGES:
        require_measured(A2A_WIDE_PAIRS)
        setting = a2a_tail_ack_settings(tuning, ("uniform_tail",))["uniform_tail"]
        for group, size in A2A_WIDE_PAIRS:
            add("uniform_tail", group, size, "healthy", A2A_SEEDS["comparison"][0], setting,
                iterations=A2A_WIDE_VALIDITY_ITERATIONS)
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
        uncalibrated = [(group, size) for group, size in pairs
                        if group not in A2A_ONE_LEAF_GROUPS and not calibrated("per_receiver", group, size)]
        if uncalibrated and not record_filter:
            refuse_uncalibrated(uncalibrated)
        wide_family = any(stage in stages for stages in (A2A_WIDE_STAGES, A2A_WIDE_SEEDS_STAGES,
                                                         A2A_WIDE_SPREAD_STAGES, A2A_WIDE_WIDEN_STAGES))
        if wide_family and A2A_WIDE_OVERDUE_REPAIR is None:
            raise ValueError(f"{stage}'s overdue repair is not chosen: set A2A_WIDE_OVERDUE_REPAIR to 'off' or "
                             "a reprobe limit")
        if wide_family and A2A_WIDE_REVERSE_SET not in A2A_RSET_SWEEP_ARMS:
            raise ValueError(f"{stage}'s reverse set is not chosen: set A2A_WIDE_REVERSE_SET to one of "
                             "A2A_RSET_SWEEP_ARMS")
        settings = (a2a_ackroute_settings(tuning) if stage == A2A_ACKROUTE_STAGE
                    else a2a_ackroute_fwd_settings(tuning) if stage == A2A_ACKROUTE_FWD_STAGE
                    else a2a_downlink_spray_settings(tuning) if stage == A2A_DOWNLINK_SPRAY_STAGE
                    else a2a_twoend_settings(tuning) if stage == A2A_TWOEND_STAGE
                    else a2a_hostlink_cover_settings(tuning) if stage == A2A_HOSTLINK_COVER_STAGE
                    else a2a_twoend_base_settings(tuning) if stage == A2A_TWOEND_BASE_STAGE
                    else a2a_twoend_retract_settings(tuning) if stage == A2A_TWOEND_RETRACT_STAGE
                    else a2a_noack_settings(tuning) if stage == A2A_NOACK_STAGE
                    else a2a_wide_settings(tuning) if stage in A2A_WIDE_STAGES
                    else {arm: setting for arm, setting in a2a_wide_settings(tuning).items()
                          if arm in A2A_WIDE_SEEDS_ARMS} if stage in A2A_WIDE_SEEDS_STAGES
                    else a2a_wide_spread_settings(tuning) if stage in A2A_WIDE_SPREAD_STAGES
                    else a2a_wide_widen_settings(tuning) if stage in A2A_WIDE_WIDEN_STAGES
                    else a2a_overdue_settings(tuning) if stage == A2A_OVERDUE_STAGE
                    else a2a_rset_sweep_settings(tuning) if stage == A2A_RSET_SWEEP_STAGE
                    else a2a_twoend_rset_settings(tuning) if stage == A2A_TWOEND_RSET_STAGE
                    else a2a_tail_ack_settings(tuning, arms))
        _, shared = a2a_shared_4k(stage)
        # An arm a stage runs on some of its conditions only, and their
        # healthy pairs.
        arm_conditions = (A2A_WIDE_ARM_CONDITIONS if stage in A2A_WIDE_STAGES
                          else A2A_WIDE_SEEDS_ARM_CONDITIONS if stage in A2A_WIDE_SEEDS_STAGES
                          else A2A_WIDE_WIDEN_ARM_CONDITIONS if stage in A2A_WIDE_WIDEN_STAGES else {})
        seeds = (A2A_RSET_SWEEP_SEEDS if stage == A2A_RSET_SWEEP_STAGE
                 else A2A_WIDE_SEEDS_SEEDS if stage in A2A_WIDE_SEEDS_STAGES
                 else A2A_WIDE_SPREAD_SEEDS if stage in A2A_WIDE_SPREAD_STAGES
                 else A2A_WIDE_WIDEN_SEEDS if stage in A2A_WIDE_WIDEN_STAGES else A2A_SEEDS["comparison"])
        for seed, (arm, setting) in itertools.product(seeds, settings.items()):
            for group, size, condition in a2a_read_cells_4k(stage):
                if condition != "healthy" and condition not in arm_conditions.get(arm, (condition,)):
                    continue
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
    if not record_filter:
        return out
    selected = selected_runs(out, record_filter)
    named = [pair for pair in uncalibrated if any((run["group"], run["size"]) == pair for run in selected)]
    if named:
        refuse_uncalibrated(named)
    return selected


def selected_runs(runs: list[dict[str, Any]], record_filter: str) -> list[dict[str, Any]]:
    """The runs whose key record_filter matches (re.search), in order."""
    pattern = re.compile(record_filter)
    return [run for run in runs if pattern.search(run["key"])]


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
                      run.get("fixture", {}).get("payload_bytes"), run.get("fixture", {}).get("wide")),
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


def write_transient(out: Path, stage: str, phase_tuning, calibration, tuning, record_filter: str = "") -> int:
    """The t_* stages; phase_tuning (--tuning) gives redirect_per_connection
    its setting in t_comparison."""
    import transient

    redirect = (phase_tuning or {}).get(transient.REDIRECT_ARM)
    runs_of = transient.runs(stage, tuning, redirect)
    return write_listing(
        out,
        [(run, transient.profile(run, calibration))
         for run in (selected_runs(runs_of, record_filter) if record_filter else runs_of)],
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
    parser.add_argument("--filter", default="")
    arguments = parser.parse_args()
    record_filter = arguments.filter

    def read(path: Path | None) -> dict[str, Any] | None:
        return json.loads(path.read_text()) if path else None

    calibration = read(arguments.calibration)
    tuning = read(arguments.tuning)
    arguments.out.mkdir(parents=True, exist_ok=True)
    if arguments.stage == A2A_STAGE:
        transient_calibration = read(arguments.transient_calibration)
        runs_of = a2a_runs(read(arguments.transient_tuning))
        return write_listing(
            arguments.out,
            [(run, a2a_profile(run, transient_calibration))
             for run in (selected_runs(runs_of, record_filter) if record_filter else runs_of)],
        )
    if arguments.stage in A2A_STAGES:
        a2a_calibration = read(arguments.a2a_calibration)
        return write_listing(
            arguments.out,
            [(run, a2a_stage_profile(run, a2a_calibration))
             for run in a2a_stage_runs(arguments.stage, a2a_calibration, read(arguments.a2a_tuning), record_filter)],
        )
    if arguments.stage.startswith("t_"):
        return write_transient(arguments.out, arguments.stage, tuning,
                               read(arguments.transient_calibration), read(arguments.transient_tuning), record_filter)
    documents = []
    runs_of = runs(arguments.stage, calibration, tuning)
    for run in selected_runs(runs_of, record_filter) if record_filter else runs_of:
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
