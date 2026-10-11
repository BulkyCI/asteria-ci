#!/usr/bin/env python3
"""Profiles and run lists of the transient-fault stages of the incast study.

The fabric, transport and balancing arms are those of profiles.py. The
workload differs: every host on each of leaves 0 and 1 receives K messages
from hosts on the other leaves, so the offered load into each receiving
leaf is 8 x 400 Gbps, its full spine capacity. Spine 3
fails in eight time-bounded occurrences of T each, the first 100 base RTTs
in, one every T + 1 ms. A fault is half rate (200 Gbps), a blackhole
(silent) or 5 percent loss, on all of spine 3's links (geometry
"spinewide") or on its link to leaf 0 only ("downlink0").

Message size: the volume per receiver at 400 Gbps covers the onset rounded
up to 1.3 ms, the eight occurrences and their gaps (8T + 7 ms) and 1 ms
after the last, (1.3 ms + 8T + 8 ms) x 50 GB/s, split over K messages and
rounded up to a whole MiB.

Every graded arm, the calibration runs included, sets
estimator_interval_samples 0 (the estimator never replaces its average
with an interval's) and estimator_gain 0.25.

Stages:

    t_calibration  healthy runs of the three graded scopes, seeds 101-103,
                   every K, at the T = 400 us message size
    t_tuning       gamma per graded arm (and the sender pool weight of the
                   mixed arm) at K = 32, spine-wide half rate, T = 400 us,
                   seed 201, each setting with its paired healthy run
    t_comparison   the 18 faulted cells x 8 arms x seeds 301-303, and a
                   paired healthy run per arm, K, T and seed; with
                   profiles.py's tuning file, redirect_per_connection on
                   the spine-wide half-rate and blackhole cells at T = 400 us
                   and at K = 32, and its healthy pairs

A run key is <stage>-k<K>-<kind>-<geometry>-t<T in us>-<arm>[-<setting>]-s<seed>,
with kind "healthy" and geometry "none" for a run with no fault. Called
through profiles.py write --stage t_...
"""

from __future__ import annotations

import itertools
import math
from typing import Any

import profiles

MIB = 1024 * 1024
LINE_BYTES_PER_NS = 50  # 400 Gbps
ONSET_NS = profiles.DEGRADE_AT_NS  # 100 base RTTs
ONSET_ROUNDED_NS = 1_300_000
GAP_NS = 1_000_000  # from an occurrence's end to the next onset
TAIL_NS = 1_000_000  # after the last occurrence
OCCURRENCES = 8
RECEIVER_LEAVES = (0, 1)
RECEIVERS_PER_LEAF = 8
DURATIONS_US = (100, 200, 400, 800, 1600)
KINDS = ("half_rate", "blackhole", "loss")
GEOMETRIES = ("spinewide", "downlink0")
LOSS_RATE = 0.05
GAMMAS = (0.25, 0.5, 1.0)
POOL_WEIGHTS = (0.25, 0.5, 0.75)
TUNING_FAN_IN = 32
TUNING_DURATION_US = 400
ESTIMATOR = {"estimator_interval_samples": 0, "estimator_gain": 0.25}
SEEDS = {"t_calibration": (101, 102, 103), "t_tuning": (201,), "t_comparison": (301, 302, 303)}

# Graded arms: (profiles.py arm, sender pool weight). None marks the tuned
# weight; graded_switch_queue takes graded_per_receiver's gamma.
GRADED_ARMS = {
    "graded_per_connection": ("graded_per_connection", 0.0),
    "graded_per_receiver": ("graded_per_receiver", 0.0),
    "graded_per_receiver_mix": ("graded_per_receiver", None),
    "graded_per_receiver_shared": ("graded_per_receiver", 1.0),
    "graded_switch_queue": ("graded_switch_queue", 0.0),
}
TUNED_ARMS = (
    "graded_per_connection",
    "graded_per_receiver",
    "graded_per_receiver_mix",
    "graded_per_receiver_shared",
)
GAMMA_FROM = {"graded_switch_queue": "graded_per_receiver"}
FIXED_ARMS = ("uniform", "reps", "mrc")
ARMS = tuple(GRADED_ARMS) + FIXED_ARMS
CALIBRATION_ARMS = ("graded_per_connection", "graded_per_receiver", "graded_switch_queue")
SCOPE_OF = {
    "graded_per_connection": "per_connection",
    "graded_per_receiver": "per_receiver",
    "graded_switch_queue": "switch_queue",
}

# ACK redirection at its tuned setting from profiles.py's tuning file, on
# the spine-wide half-rate and blackhole cells; the setting of the fault
# kind its tuning ran under.
REDIRECT_ARM = "redirect_per_connection"
REDIRECT_TUNED_UNDER = {"half_rate": "half_rate", "blackhole": "half_rate", "loss": "gray", "healthy": "half_rate"}

# The faulted cells as (K, kind, geometry, T in us).
CELLS: tuple[tuple[int, str, str, int], ...] = (
    tuple((k, kind, "spinewide", 400) for k in profiles.FAN_INS for kind in ("half_rate", "blackhole"))
    + tuple((32, kind, "spinewide", t) for kind in ("half_rate", "blackhole") for t in DURATIONS_US if t != 400)
    + tuple((32, kind, "downlink0", 400) for kind in ("half_rate", "blackhole"))
    + tuple((32, "loss", geometry, 400) for geometry in GEOMETRIES)
)


def period_ns(duration_us: int) -> int:
    return duration_us * 1000 + GAP_NS


def occurrence_windows(duration_us: int) -> list[tuple[int, int]]:
    """(onset, end) of every occurrence, in simulation time."""
    return [
        (ONSET_NS + i * period_ns(duration_us), ONSET_NS + i * period_ns(duration_us) + duration_us * 1000)
        for i in range(OCCURRENCES)
    ]


def required_run_ns(duration_us: int) -> int:
    """The onset rounded up, the eight occurrences and gaps, and the tail."""
    return ONSET_ROUNDED_NS + OCCURRENCES * duration_us * 1000 + (OCCURRENCES - 1) * GAP_NS + TAIL_NS


def message_bytes(duration_us: int, fan_in: int) -> int:
    volume = required_run_ns(duration_us) * LINE_BYTES_PER_NS
    return math.ceil(volume / fan_in / MIB) * MIB


def line_rate_run_ns(duration_us: int, fan_in: int) -> float:
    """Time for one receiver's K messages at 400 Gbps."""
    return fan_in * message_bytes(duration_us, fan_in) / LINE_BYTES_PER_NS


def failures(kind: str, geometry: str, duration_us: int) -> list[dict[str, Any]]:
    if kind == "healthy":
        return []
    model = {
        "half_rate": {"model": "gray", "rate": "200Gbps"},
        "blackhole": {"model": "silent"},
        "loss": {"model": "gray", "error_rate": LOSS_RATE},
    }[kind]
    entry = {
        **model,
        "start_ns": ONSET_NS,
        "spine": profiles.DEGRADED_SPINE,
        "duration_ns": duration_us * 1000,
        "repeat": OCCURRENCES,
        "period_ns": period_ns(duration_us),
    }
    if geometry == "downlink0":
        entry["leaf"] = 0
    elif geometry != "spinewide":
        raise ValueError(f"unknown geometry {geometry}")
    return [entry]


def _calibrated(arm: str, fan_in: int, calibration: dict | None) -> dict[str, Any]:
    """Thresholds from this study's calibration, once it exists: the
    congestion thresholds, and the trim thresholds for the estimate scopes."""
    scope = SCOPE_OF[GRADED_ARMS[arm][0]]
    values: dict[str, Any] = {}
    entry = (calibration or {}).get(scope, {}).get(str(fan_in), {})
    if "congestion_thresholds" in entry:
        values["congestion_thresholds"] = entry["congestion_thresholds"]
    if scope != "switch_queue" and "trim_thresholds" in entry:
        values["trim_thresholds"] = entry["trim_thresholds"]
    return values


def balancing(
    arm: str,
    fan_in: int,
    calibration: dict | None,
    setting: dict[str, Any] | None,
) -> dict[str, Any]:
    if arm in FIXED_ARMS:
        return profiles.balancing(arm, fan_in, None)
    if arm == REDIRECT_ARM:
        return profiles.balancing(arm, fan_in, None, setting)
    base_arm, weight = GRADED_ARMS[arm]
    values = profiles.balancing(base_arm, fan_in, None)
    values.update(_calibrated(arm, fan_in, calibration))
    values.update(ESTIMATOR)
    values["edge_window_penalty"] = 0
    values["edge_freeze"] = False
    if weight:
        values["sender_pool_weight"] = weight
    values.update(setting or {})
    if weight is None and "sender_pool_weight" not in values:
        raise ValueError(f"{arm} needs a sender pool weight")
    return values


def profile(run: dict[str, Any], calibration: dict | None) -> dict[str, Any]:
    document = profiles.profile(
        run["key"],
        run["k"],
        "healthy",
        balancing(run["arm"], run["k"], calibration, run["setting"]),
        message_bytes=message_bytes(run["t_us"], run["k"]),
    )
    document["workload"] = {
        "kind": "incast",
        "receiver_leaves": list(RECEIVER_LEAVES),
        "receiver_count": RECEIVERS_PER_LEAF,
        "fan_in": run["k"],
        "message_bytes": message_bytes(run["t_us"], run["k"]),
    }
    document["network"]["link_failures"] = failures(run["kind"], run["geometry"], run["t_us"])
    return document


def tuning_settings(arm: str) -> list[dict[str, Any]]:
    if arm == "graded_per_receiver_mix":
        return [{"gamma": g, "sender_pool_weight": w} for g, w in itertools.product(GAMMAS, POOL_WEIGHTS)]
    return [{"gamma": g} for g in GAMMAS]


def tuned_setting(arm: str, tuning: dict[str, Any] | None) -> dict[str, Any] | None:
    """The selected setting of a graded arm, or None before the selection
    exists; switch_queue takes the per-receiver arm's gamma."""
    if tuning is None:
        return None
    source = GAMMA_FROM.get(arm, arm)
    if source not in tuning:
        return None
    if arm in GAMMA_FROM:
        return {"gamma": tuning[source]["gamma"]}
    return dict(tuning[source])


def runs(stage: str, tuning: dict[str, Any] | None, redirect: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Every run of a stage; tuning is this study's selection, redirect
    profiles.py's tuning entry for redirect_per_connection."""
    out: list[dict[str, Any]] = []

    def add(arm, fan_in, kind, geometry, t_us, seed, setting=None, label_setting=False):
        label = arm if not label_setting else f"{arm}-{profiles.setting_name(setting)}"
        out.append(
            {
                "key": f"{stage}-k{fan_in}-{kind}-{geometry}-t{t_us}-{label}-s{seed}",
                "stage": stage,
                "arm": arm,
                "setting": setting,
                "k": fan_in,
                "condition": kind,
                "kind": kind,
                "geometry": geometry,
                "t_us": t_us,
                "seed": seed,
            }
        )

    if stage == "t_calibration":
        for fan_in, seed, arm in itertools.product(profiles.FAN_INS, SEEDS[stage], CALIBRATION_ARMS):
            add(arm, fan_in, "healthy", "none", TUNING_DURATION_US, seed)
    elif stage == "t_tuning":
        for seed, arm in itertools.product(SEEDS[stage], TUNED_ARMS):
            for setting in tuning_settings(arm):
                for kind, geometry in (("half_rate", "spinewide"), ("healthy", "none")):
                    add(arm, TUNING_FAN_IN, kind, geometry, TUNING_DURATION_US, seed, setting, True)
    elif stage == "t_comparison":
        # The graded arms join once the tuning selection exists.
        arms = [a for a in ARMS if a in FIXED_ARMS or tuned_setting(a, tuning) is not None]
        healthy = sorted({(k, t) for k, _, _, t in CELLS})
        for seed, arm in itertools.product(SEEDS[stage], arms):
            setting = None if arm in FIXED_ARMS else tuned_setting(arm, tuning)
            for fan_in, kind, geometry, t_us in CELLS:
                add(arm, fan_in, kind, geometry, t_us, seed, setting)
            for fan_in, t_us in healthy:
                add(arm, fan_in, "healthy", "none", t_us, seed, setting)
        if redirect is not None:
            # The spine-wide half-rate and blackhole cells: those at T = 400 us
            # and the K = 32 T sweep.
            cells = [c for c in CELLS if c[1] in ("half_rate", "blackhole") and c[2] == "spinewide"]
            pairs = sorted({(k, t) for k, _, _, t in cells})
            for seed in SEEDS[stage]:
                for fan_in, kind, geometry, t_us in cells:
                    add(REDIRECT_ARM, fan_in, kind, geometry, t_us, seed, redirect[REDIRECT_TUNED_UNDER[kind]])
                for fan_in, t_us in pairs:
                    add(REDIRECT_ARM, fan_in, "healthy", "none", t_us, seed, redirect[REDIRECT_TUNED_UNDER["healthy"]])
    else:
        raise ValueError(f"unknown stage {stage}")
    return out
