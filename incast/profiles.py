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
list of {key, profile, seed, stage, k, condition, arm}. Standard library only.
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


def failures(condition: str) -> list[dict[str, Any]]:
    start = {"start_ns": DEGRADE_AT_NS, "spine": DEGRADED_SPINE}
    if condition == "healthy":
        return []
    if condition == "half_rate":
        return [{"model": "gray", "rate": "200Gbps", **start}]
    if condition == "gray":
        return [{"model": "gray", "error_rate": 0.01, **start}]
    if condition == "silent":
        return [{"model": "silent", **start}]
    raise ValueError(f"unknown condition {condition}")


def profile(name: str, fan_in: int, condition: str, balancing: dict[str, Any]) -> dict[str, Any]:
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
            "message_bytes": MESSAGE_BYTES,
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
            "link_failures": failures(condition),
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
    elif stage == "kill":
        # The decisive subset of the comparison stage, runnable on its own.
        for condition, seed in itertools.product(("half_rate", "gray"), SEEDS["comparison"]):
            for arm in ("graded_per_receiver", "graded_per_connection", "uniform"):
                add(arm, 32, condition, seed)
    else:
        raise ValueError(f"unknown stage {stage}")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("write",))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--calibration", type=Path)
    parser.add_argument("--tuning", type=Path)
    arguments = parser.parse_args()
    calibration = json.loads(arguments.calibration.read_text()) if arguments.calibration else None
    tuning = json.loads(arguments.tuning.read_text()) if arguments.tuning else None
    arguments.out.mkdir(parents=True, exist_ok=True)
    listed = []
    for run in runs(arguments.stage, calibration, tuning):
        path = arguments.out / f"{run['key']}.json"
        document = profile(run["key"], run["k"], run["condition"], balancing(run["arm"], run["k"], calibration, run["setting"]))
        path.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
        listed.append({**run, "profile": path.name})
    (arguments.out / "runs.json").write_text(json.dumps(listed, indent=1) + "\n", encoding="utf-8")
    print(len(listed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
