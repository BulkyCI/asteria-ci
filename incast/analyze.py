#!/usr/bin/env python3
"""Tables of the incast study, from run directories holding summary.json.

    analyze.py thresholds RUN_DIR... --scope SCOPE   first threshold from healthy reports
    analyze.py gain RUN_DIR...                       fraction of all-top reports per run
    analyze.py binding RUN_DIR...                    degraded spine's rate toward leaf 0
    analyze.py compare --runs RUNS.json --out-root DIR [--stage S]  per-cell arm table
                                                     (all-to-all runs add per-iteration statistics)
    analyze.py calibrate --runs RUNS.json --out-root DIR [--into FILE]   thresholds per scope and K
    analyze.py gains --runs RUNS.json --out-root DIR --into FILE        gain per scope and K
    analyze.py select --runs RUNS.json --out-root DIR [--stage t_tuning] tuning selection
    analyze.py cost --runs RUNS.json --out-root DIR --reference ARM [--stage S]  transient cost table
    analyze.py versus A B --runs RUNS.json --out-root DIR               seed-noise verdict per cell
    analyze.py a2a_gate --runs RUNS.json... --out-root DIR [--json]     Phase 9 oracle-headroom gate, a row per cell

Phase 9 (all-to-all runs, those with a group): cost pairs each faulted run
with the healthy run of the same arm and setting, group, size and seed, from
any stage of the runs files given (the comparison reuses the gate's uniform
and oracle runs). Primary cost is the mean iteration completion minus the
pair's, tail cost the same on the p99 with linear interpolation; recovered
fraction is 1 - cost / cost(uniform) per seed on the same (group, size,
condition) cell. Those cells follow the transient cells in cost's output. The
gate admits a cell when the oracle's mean primary cost is at most half of
uniform's and the oracle beats uniform on primary cost by the seed rule.

Gain: per scope and K, the largest candidate gain at which at least 99
percent of the healthy reports of every calibration seed give every spine
the top grade, pooled over the seeds; if none reaches 99 percent, the gain
with the largest fraction, flagged. Selection: per redirect arm and tuning
condition, the setting with the lowest collective completion; for the
t_tuning stage, per arm, the setting with the lowest cost.

Cost (transient stages): per (stage, K, kind, geometry, T, arm) cell and
seed, the run's collective completion minus that of the same arm's paired
healthy run (same setting, K, T and seed, no fault); a run that did not
complete costs infinity. Recovered fraction: 1 - cost / cost(uniform), with
uniform on the same cell and seed. Each cell reports the per-seed values,
their means, the seed-noise verdict of every arm against a reference arm on
cost (lower is better) and on recovered fraction (higher is better), and
the means of the per-transient measures of transients.json: detection
latency and restore latency over the occurrences that have one, the
fraction of occurrences detected, and the false-hold time over any healthy
spine.

Thresholds: for every report of a healthy run, each spine's deviation above
the median spine of that report, for the marked and the trimmed fraction
(and, for switch_queue, the marked and the loss fraction). The first of the
congestion thresholds is the 99th percentile of those deviations over all
reports and every metric but the trimmed fraction, the second and third two
and four times it; the trimmed fraction's own 99th percentile gives the
trim thresholds by the same rule. Each first threshold is capped at 0.25,
so the third stays at most one, and the cap is flagged (capped,
trim_capped). A report is one
(time, rank, sequence) group, and under per_connection one (time, rank,
sequence, sender, sender_port) group.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

TOP_GRADE = 3
BASE_RTT_NS = 12_324
DEGRADE_AT_NS = 100 * BASE_RTT_NS
DEGRADED_SPINE = 3
SPINES = 8


def reports(run: Path):
    groups: dict[tuple, list[dict[str, str]]] = defaultdict(list)
    with (run / "ns3" / "spine_reports.csv").open() as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (row["time_ns"], row["rank"], row["sequence"], row.get("sender", ""),
                   row.get("sender_port", ""), row.get("source_leaf", ""))
            groups[key].append(row)
    return groups


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[index]


def scaled_thresholds(first: float) -> tuple[bool, list[float]]:
    """The rule's (t, 2t, 4t) from a first threshold t. The simulator wants
    three increasing fractions in (0, 1], so the 4t can reach 1 and no
    further: a t above a quarter is capped there, and flagged."""
    capped = first > 0.25
    first = min(max(first, 1e-4), 0.25)
    return capped, [round(first, 6), round(2 * first, 6), round(4 * first, 6)]


def thresholds(runs: list[Path], metrics: tuple[str, ...]) -> dict:
    deviations: dict[str, list[float]] = {m: [] for m in metrics}
    count = 0
    for run in runs:
        for rows in reports(run).values():
            count += 1
            for metric in metrics:
                values = [float(r[metric]) for r in rows]
                median = statistics.median(values)
                deviations[metric].extend(max(0.0, v - median) for v in values)
    p99 = {m: percentile(v, 0.99) for m, v in deviations.items()}
    # The trimmed fraction has thresholds of its own; the others share one.
    capped, congestion = scaled_thresholds(max(v for m, v in p99.items() if m != "trim_fraction"))
    result = {
        "reports": count,
        "p99_deviation": p99,
        "capped": capped,
        "congestion_thresholds": congestion,
    }
    if "trim_fraction" in p99:
        result["trim_capped"], result["trim_thresholds"] = scaled_thresholds(p99["trim_fraction"])
    return result


def all_top_fraction(run: Path) -> dict:
    groups = reports(run)
    top = sum(1 for rows in groups.values() if all(int(r["grade"]) == TOP_GRADE for r in rows))
    return {"reports": len(groups), "all_top": top, "fraction": top / len(groups) if groups else None}


def window_rows(run: Path, name: str = "spine_windows.csv"):
    with (run / name).open() as handle:
        return [
            {k: int(v) for k, v in row.items()} for row in csv.DictReader(handle)
        ]


def binding(run: Path, degraded_bps: float) -> dict:
    summary = json.loads((run / "summary.json").read_text())
    end = summary["first_incast_completion_ns"]
    window = summary["window_ns"]
    total = sum(
        r["bytes"]
        for r in window_rows(run)
        if r["spine"] == DEGRADED_SPINE and DEGRADE_AT_NS <= r["start_ns"] and r["start_ns"] + window <= end
    )
    first_window = -(-DEGRADE_AT_NS // window) * window
    last_window = (end // window) * window
    span_ns = max(1, last_window - first_window)
    return {
        "first_incast_completion_ns": end,
        "utilization": total * 8 / (span_ns * 1e-9) / degraded_bps,
    }


def shed_time(run: Path, target_share: float | None, degrade_at_ns: int = DEGRADE_AT_NS) -> float | None:
    """From the degradation until the degraded spine's share of the traffic
    offered to the spines for the receiving leaf falls below the midpoint of
    1/8 and the target, and stays below it for five consecutive windows.
    Offered, not delivered: a half-rate spine delivers at most its fluid
    share whatever the senders do, so its delivered share cannot show them
    shedding."""
    if target_share is None:
        return None
    midpoint = (1 / SPINES + target_share) / 2
    totals: dict[int, int] = defaultdict(int)
    degraded: dict[int, int] = defaultdict(int)
    for r in window_rows(run, "offered_windows.csv"):
        totals[r["start_ns"]] += r["packets"]
        if r["spine"] == DEGRADED_SPINE:
            degraded[r["start_ns"]] += r["packets"]
    starts = sorted(s for s in totals if s >= degrade_at_ns and totals[s] > 0)
    run_start = None
    run_length = 0
    for start in starts:
        if degraded[start] / totals[start] < midpoint:
            if run_length == 0:
                run_start = start
            run_length += 1
            if run_length == 5:
                return run_start - degrade_at_ns
        else:
            run_length = 0
    return None


TARGET_SHARE = {"half_rate": 200 / 3000, "silent": 0.0}


def iteration_stats(summary: dict) -> dict:
    """An all-to-all run's per-iteration completion statistics; nothing for
    any other workload."""
    timing = summary.get("all_to_all")
    if timing is None:
        return {}
    return {
        "iterations_completed": timing["iterations_completed"],
        **{f"iteration_{name}_ns": timing[f"{name}_ns"] for name in ("mean", "p50", "p99", "max")},
    }


def compare(runs_file: Path, out_root: Path, stage: str | None) -> list[dict]:
    listed = json.loads(runs_file.read_text())
    rows = []
    for run in listed:
        if stage and run["stage"] != stage:
            continue
        directory = out_root / run["key"]
        summary_path = directory / "summary.json"
        if not summary_path.exists():
            rows.append({**run, "status": "missing"})
            continue
        summary = json.loads(summary_path.read_text())
        transport = summary["transport"]
        transient = "geometry" in run
        measured = _transients(directory) if transient else None
        rows.append(
            {
                **run,
                "status": "complete" if summary["complete"] else "incomplete",
                "simulator_stop": summary.get("simulator_stop"),
                "completion_ns": summary["collective_completion_ns"],
                "first_incast_ns": summary["first_incast_completion_ns"],
                "trims": sum(v["count"] for k, v in transport.items() if k.startswith("trim_")),
                "shed_ns": None if transient else shed_time(
                    directory,
                    TARGET_SHARE.get(run["condition"]),
                    10 * BASE_RTT_NS if run["stage"] == "short" else DEGRADE_AT_NS,
                ),
                **({} if not transient else transient_means([measured] if measured else [])),
                **iteration_stats(summary),
                "wall_seconds": summary.get("wall_seconds"),
            }
        )
    return rows


def beats(a: list[int], b: list[int]) -> bool:
    """Paired seeds: a beats b by more than seed noise when a is faster on
    every seed and the mean gap exceeds the larger of the two seed ranges."""
    if len(a) != len(b) or not a:
        return False
    if not all(x < y for x, y in zip(a, b)):
        return False
    gap = sum(b) / len(b) - sum(a) / len(a)
    return gap > max(max(a) - min(a), max(b) - min(b))


def versus(runs_file: Path, out_root: Path, arm_a: str, arm_b: str) -> list[dict]:
    """Per cell, the two arms' completions by seed and the verdict. A cell is
    (stage, K, condition), and for the transient stages also (geometry, T),
    so each holds one fault. A run that did not complete counts as slower
    than any that did, and a cell with a missing run has no verdict."""
    cells: dict[tuple, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))
    for run in json.loads(runs_file.read_text()):
        if run["arm"] not in (arm_a, arm_b):
            continue
        path = out_root / run["key"] / "summary.json"
        if not path.exists():
            continue
        completion = json.loads(path.read_text())["collective_completion_ns"]
        cell = (run["stage"], run["k"], run["condition"], run.get("geometry"), run.get("t_us"))
        cells[cell][run["arm"]][run["seed"]] = (
            float("inf") if completion is None else completion
        )
    rows = []
    for (stage, k, condition, geometry, t_us), arms in sorted(cells.items()):
        seeds = sorted(set(arms[arm_a]) & set(arms[arm_b]))
        a = [arms[arm_a][x] for x in seeds]
        b = [arms[arm_b][x] for x in seeds]
        complete = len(seeds) == 3
        rows.append(
            {
                "stage": stage, "k": k, "condition": condition,
                **({} if geometry is None else {"geometry": geometry, "t_us": t_us}),
                "seeds": seeds,
                arm_a: a, arm_b: b,
                "mean_gap_ns": (sum(b) - sum(a)) / len(seeds) if seeds else None,
                "verdict": (
                    None if not complete
                    else f"{arm_a} beats {arm_b}" if beats(a, b)
                    else f"{arm_b} beats {arm_a}" if beats(b, a)
                    else "within seed noise"
                ),
            }
        )
    return rows


SCOPE_OF_ARM = {
    "graded_per_receiver": "per_receiver",
    "graded_per_connection": "per_connection",
    "graded_switch_queue": "switch_queue",
}


def _listed(runs_file: Path, out_root: Path, stage: str):
    for run in json.loads(runs_file.read_text()):
        directory = out_root / run["key"]
        if run["stage"] == stage and (directory / "summary.json").exists():
            yield run, directory


def calibrate(runs_file: Path, out_root: Path, existing: dict, stage: str = "calibration") -> dict:
    groups: dict[tuple[str, int], list[Path]] = defaultdict(list)
    for run, directory in _listed(runs_file, out_root, stage):
        groups[(SCOPE_OF_ARM[run["arm"]], run["k"])].append(directory)
    result = {scope: dict(values) for scope, values in existing.items()}
    for (scope, k), directories in sorted(groups.items()):
        metrics = ("mark_fraction", "loss_fraction") if scope == "switch_queue" else ("mark_fraction", "trim_fraction")
        entry = thresholds(directories, metrics)
        entry["runs"] = [d.name for d in directories]
        previous = result.setdefault(scope, {}).get(str(k), {})
        if "estimator_gain" in previous:
            entry["estimator_gain"] = previous["estimator_gain"]
            entry["gain_fraction"] = previous.get("gain_fraction")
        result[scope][str(k)] = entry
    return result


def gains(runs_file: Path, out_root: Path, existing: dict) -> dict:
    tops: dict[tuple[str, int, float], list[int]] = defaultdict(lambda: [0, 0])
    for run, directory in _listed(runs_file, out_root, "gain"):
        gain = run["setting"]["estimator_gain"]
        counted = all_top_fraction(directory)
        cell = tops[(SCOPE_OF_ARM[run["arm"]], run["k"], gain)]
        cell[0] += counted["all_top"]
        cell[1] += counted["reports"]
    result = {scope: dict(values) for scope, values in existing.items()}
    for scope, k in sorted({(s, k) for s, k, _ in tops}):
        fractions = {g: t / n for (s, kk, g), (t, n) in tops.items() if s == scope and kk == k and n}
        passing = [g for g, f in fractions.items() if f >= 0.99]
        # The largest passing gain; if none passes, the largest of those with
        # the best fraction, so a scope the gain does not move (a pooled
        # interval always fills its sample count, which then sets the
        # weight) gets the largest candidate rather than an arbitrary one.
        best = max(fractions.values())
        chosen = max(passing) if passing else max(g for g, f in fractions.items() if f == best)
        entry = result.setdefault(scope, {}).setdefault(str(k), {})
        entry["estimator_gain"] = chosen
        entry["gain_fraction"] = fractions[chosen]
        entry["gain_fractions"] = {str(g): f for g, f in sorted(fractions.items())}
        entry["gain_meets_rule"] = bool(passing)
    return result


def select(runs_file: Path, out_root: Path) -> dict:
    best: dict[tuple[str, str], tuple[int, dict]] = {}
    table: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for run, directory in _listed(runs_file, out_root, "tuning"):
        summary = json.loads((directory / "summary.json").read_text())
        completion = summary["collective_completion_ns"]
        table[run["arm"]][run["condition"]].append({"setting": run["setting"], "completion_ns": completion})
        if completion is None:
            continue
        key = (run["arm"], run["condition"])
        if key not in best or completion < best[key][0]:
            best[key] = (completion, run["setting"])
    selection: dict[str, dict] = defaultdict(dict)
    for (arm, condition), (_, setting) in best.items():
        selection[arm][condition] = setting
    return {**selection, "table": table}


def _transients(directory: Path) -> dict | None:
    path = directory / "transients.json"
    return json.loads(path.read_text()) if path.exists() else None


def _mean(values) -> float | None:
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def transient_means(measured: list[dict]) -> dict:
    """Means over the runs' transients.json: detection and restore latency
    over every occurrence that has one, the detected fraction and the
    false-hold time over any healthy spine per run."""
    occurrences = [o for m in measured for o in m["occurrences"]]
    return {
        "detection_ns_mean": _mean(o["detection_ns"] for o in occurrences),
        "detected_fraction_mean": _mean(m["detected_fraction"] for m in measured),
        "restore_ns_mean": _mean(o["restore_ns"] for o in occurrences),
        "false_hold_ns_mean": _mean((m["false_hold"] or {}).get("any_ns") for m in measured),
    }


def _label(run: dict) -> str:
    setting = run.get("setting")
    return run["arm"] if not setting else f"{run['arm']} {json.dumps(setting, sort_keys=True)}"


def costs(rows: list[dict]) -> list[dict]:
    """Every faulted row with its cost against the paired healthy row (same
    stage, arm, setting, K, T and seed). A row holds stage, arm, setting, k,
    kind, geometry, t_us, seed and completion_ns (None when the run did not
    complete); cost is None when the healthy run is missing or incomplete,
    and infinite when the faulted run is incomplete."""
    def pair(row):
        return (row["stage"], _label(row), row["k"], row["t_us"], row["seed"])

    healthy = {pair(r): r["completion_ns"] for r in rows if r["kind"] == "healthy"}
    out = []
    for row in rows:
        if row["kind"] == "healthy":
            continue
        reference = healthy.get(pair(row))
        if reference is None:
            cost = None
        elif row["completion_ns"] is None:
            cost = float("inf")
        else:
            cost = row["completion_ns"] - reference
        out.append({**row, "healthy_ns": reference, "cost_ns": cost})
    return out


def recovered_fraction(cost: float | None, uniform_cost: float | None) -> float | None:
    """1 - cost / cost(uniform); None when either is unknown or uniform has
    no finite positive cost to recover."""
    if cost is None or uniform_cost is None or not 0 < uniform_cost < float("inf"):
        return None
    return 1 - cost / uniform_cost


def verdict(a: list[float], b: list[float], name_a: str, name_b: str, higher_is_better: bool = False) -> str | None:
    """The seed-noise rule of beats on paired seeds; None without three."""
    if len(a) != 3 or len(b) != 3 or any(v is None for v in a + b):
        return None
    if higher_is_better:
        a, b = [-v for v in a], [-v for v in b]
    if beats(a, b):
        return f"{name_a} beats {name_b}"
    if beats(b, a):
        return f"{name_b} beats {name_a}"
    return "within seed noise"


def cost_cells(rows: list[dict], reference: str, uniform: str = "uniform") -> list[dict]:
    """Per (stage, K, kind, geometry, T) cell, per arm: per-seed cost and
    recovered fraction, their means, the verdicts against the reference arm
    and the transient means (rows may carry a transients entry). An arm is
    named bare, with its setting as a field, when it runs one setting in the
    cell, so the reference and uniform match by arm name; an arm with
    several settings (the tuning stage) is named with each setting."""
    cells: dict[tuple, dict[str, dict[int, dict]]] = defaultdict(lambda: defaultdict(dict))
    faulted = costs(rows)
    settings: dict[tuple, set[str]] = defaultdict(set)
    for row in faulted:
        cell = (row["stage"], row["k"], row["kind"], row["geometry"], row["t_us"])
        settings[(cell, row["arm"])].add(json.dumps(row["setting"], sort_keys=True))
    for row in faulted:
        cell = (row["stage"], row["k"], row["kind"], row["geometry"], row["t_us"])
        label = row["arm"] if len(settings[(cell, row["arm"])]) == 1 else _label(row)
        cells[cell][label][row["seed"]] = row
    out = []
    for (stage, k, kind, geometry, t_us), arms in sorted(cells.items()):
        uniform_costs = {seed: r["cost_ns"] for seed, r in arms.get(uniform, {}).items()}
        table = {}
        for label, by_seed in sorted(arms.items()):
            seeds = sorted(by_seed)
            cost = [by_seed[x]["cost_ns"] for x in seeds]
            recovered = [recovered_fraction(by_seed[x]["cost_ns"], uniform_costs.get(x)) for x in seeds]
            table[label] = {
                "setting": by_seed[seeds[0]]["setting"],
                "seeds": seeds,
                "cost_ns": cost,
                "cost_ns_mean": None if None in cost or not cost else sum(cost) / len(cost),
                "recovered": recovered,
                "recovered_mean": None if None in recovered or not recovered else sum(recovered) / len(recovered),
                **transient_means([by_seed[x]["transients"] for x in seeds if by_seed[x].get("transients")]),
            }
        if reference in table:
            base = table[reference]
            for label, entry in table.items():
                if label == reference or entry["seeds"] != base["seeds"]:
                    continue
                entry["versus_reference_cost"] = verdict(entry["cost_ns"], base["cost_ns"], label, reference)
                entry["versus_reference_recovered"] = verdict(
                    entry["recovered"], base["recovered"], label, reference, higher_is_better=True
                )
        out.append({"stage": stage, "k": k, "kind": kind, "geometry": geometry, "t_us": t_us,
                    "reference": reference, "arms": table})
    return out


def transient_rows(runs_file: Path, out_root: Path, stage: str | None) -> list[dict]:
    """The transient stages' runs with their completion and transients.json."""
    rows = []
    for run in json.loads(runs_file.read_text()):
        if "geometry" not in run or (stage and run["stage"] != stage):
            continue
        directory = out_root / run["key"]
        if not (directory / "summary.json").exists():
            continue
        summary = json.loads((directory / "summary.json").read_text())
        rows.append({**run, "completion_ns": summary["collective_completion_ns"],
                     "transients": _transients(directory)})
    return rows


def select_by_cost(rows: list[dict]) -> dict:
    """Per arm, the setting with the lowest mean cost over its seeds."""
    by_setting: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    table: dict[str, list] = defaultdict(list)
    for row in costs(rows):
        key = json.dumps(row["setting"], sort_keys=True)
        by_setting[row["arm"]][key].append(row["cost_ns"])
        table[row["arm"]].append({k: row[k] for k in ("setting", "seed", "completion_ns", "healthy_ns", "cost_ns")})
    selection: dict = {}
    for arm, settings in sorted(by_setting.items()):
        means = {key: sum(c) / len(c) for key, c in settings.items() if None not in c}
        if means:
            selection[arm] = json.loads(min(means, key=lambda key: (means[key], key)))
    return {**selection, "table": table}


# Phase 9: all-to-all cost against the paired healthy run, and the
# oracle-headroom gate.

A2A_ORACLE = "graded_switch_queue"


def linear_quantile(values: list[float], q: float) -> float:
    """The q-th quantile with linear interpolation between order statistics,
    so the p99 of 40 values lies between the two largest."""
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _listed_runs(runs_files) -> list[dict]:
    """The runs of one or more runs.json files, each key once."""
    if isinstance(runs_files, Path):
        runs_files = [runs_files]
    seen: dict[str, dict] = {}
    for path in runs_files:
        for run in json.loads(Path(path).read_text()):
            seen.setdefault(run["key"], run)
    return list(seen.values())


def a2a_rows(runs_files, out_root: Path, stage: str | None = None) -> list[dict]:
    """The Phase 9 all-to-all runs (those with a group) that have a summary,
    with their per-iteration completions; iterations_ns is None when the run
    did not complete every iteration."""
    rows = []
    for run in _listed_runs(runs_files):
        if "group" not in run or (stage and run["stage"] != stage):
            continue
        path = out_root / run["key"] / "summary.json"
        if not path.exists():
            continue
        timing = json.loads(path.read_text()).get("all_to_all") or {}
        complete = bool(timing.get("complete"))
        values = timing.get("iteration_completion_ns", {})
        rows.append({**run, "complete": complete,
                     "iterations_ns": [values[i] for i in sorted(values, key=int)] if complete else None})
    return rows


def a2a_costs(rows: list[dict]) -> list[dict]:
    """Every faulted all-to-all row with its primary cost (mean iteration
    completion minus the paired healthy run's) and tail cost (the same on the
    linearly interpolated p99), the pair being the healthy run of the same
    arm and setting, group, size and seed, from any stage. Costs are None
    when the healthy run is missing or incomplete and infinite when the
    faulted run is incomplete."""
    def pair(row):
        return (_label(row), row["group"], row["size"], row["seed"])

    healthy: dict[tuple, dict] = {}
    for row in rows:
        if row["condition"] != "healthy":
            continue
        if pair(row) in healthy:
            raise ValueError(f"{row['key']} and {healthy[pair(row)]['key']} pair the same runs")
        healthy[pair(row)] = row
    out = []
    for row in rows:
        if row["condition"] == "healthy":
            continue
        reference = healthy.get(pair(row))
        if reference is None or reference["iterations_ns"] is None:
            cost = tail = None
        elif row["iterations_ns"] is None:
            cost = tail = float("inf")
        else:
            cost = statistics.fmean(row["iterations_ns"]) - statistics.fmean(reference["iterations_ns"])
            tail = linear_quantile(row["iterations_ns"], 0.99) - linear_quantile(reference["iterations_ns"], 0.99)
        out.append({**row, "healthy_key": None if reference is None else reference["key"],
                    "cost_ns": cost, "tail_cost_ns": tail})
    return out


def a2a_cost_cells(rows: list[dict], reference: str | None = None, uniform: str = "uniform") -> list[dict]:
    """Per (group, size, condition) cell, per arm: per-seed primary and tail
    cost and recovered fraction against uniform on the same cell and seed,
    their means, and the seed-rule verdicts against the reference arm. Arms
    are named as in cost_cells: bare when the cell runs one setting of it."""
    faulted = a2a_costs(rows)
    settings: dict[tuple, set[str]] = defaultdict(set)
    for row in faulted:
        settings[(row["group"], row["size"], row["condition"], row["arm"])].add(
            json.dumps(row["setting"], sort_keys=True))
    cells: dict[tuple, dict[str, dict[int, dict]]] = defaultdict(lambda: defaultdict(dict))
    for row in faulted:
        cell = (row["group"], row["size"], row["condition"])
        label = row["arm"] if len(settings[cell + (row["arm"],)]) == 1 else _label(row)
        cells[cell][label][row["seed"]] = row

    def mean(values):
        return None if not values or None in values else sum(values) / len(values)

    out = []
    for (group, size, condition), arms in sorted(cells.items()):
        uniform_costs = {seed: r["cost_ns"] for seed, r in arms.get(uniform, {}).items()}
        table = {}
        for label, by_seed in sorted(arms.items()):
            seeds = sorted(by_seed)
            cost = [by_seed[x]["cost_ns"] for x in seeds]
            tail = [by_seed[x]["tail_cost_ns"] for x in seeds]
            recovered = [recovered_fraction(c, uniform_costs.get(x)) for c, x in zip(cost, seeds)]
            table[label] = {
                "setting": by_seed[seeds[0]]["setting"],
                "stages": sorted({by_seed[x]["stage"] for x in seeds}),
                "seeds": seeds,
                "cost_ns": cost,
                "cost_ns_mean": mean(cost),
                "tail_cost_ns": tail,
                "tail_cost_ns_mean": mean(tail),
                "recovered": recovered,
                "recovered_mean": mean(recovered),
            }
        if reference in table:
            base = table[reference]
            for label, entry in table.items():
                if label == reference or entry["seeds"] != base["seeds"]:
                    continue
                entry["versus_reference_cost"] = verdict(entry["cost_ns"], base["cost_ns"], label, reference)
                entry["versus_reference_recovered"] = verdict(
                    entry["recovered"], base["recovered"], label, reference, higher_is_better=True
                )
        out.append({"group": group, "size": size, "condition": condition,
                    "reference": reference, "arms": table})
    return out


def a2a_gate(cells: list[dict], oracle: str = A2A_ORACLE, uniform: str = "uniform") -> list[dict]:
    """The oracle-headroom gate per cell: admitted when the oracle's mean
    primary cost is at most half of uniform's (uniform's mean positive) and
    the oracle beats uniform on primary cost by the seed rule, on the same
    three seeds. admitted is None (unreadable) while either arm lacks three
    seeds with a known cost."""
    rows = []
    for cell in cells:
        u, o = cell["arms"].get(uniform), cell["arms"].get(oracle)
        row = {"group": cell["group"], "size": cell["size"], "condition": cell["condition"],
               "uniform_cost_ns": None if u is None else u["cost_ns"],
               "oracle_cost_ns": None if o is None else o["cost_ns"],
               "uniform_mean_ns": None, "oracle_mean_ns": None, "oracle_over_uniform": None,
               "half_rule": None, "seed_rule": None, "admitted": None}
        readable = (
            u is not None and o is not None and u["seeds"] == o["seeds"] and len(u["seeds"]) == 3
            and None not in u["cost_ns"] and None not in o["cost_ns"]
        )
        if readable:
            u_mean, o_mean = u["cost_ns_mean"], o["cost_ns_mean"]
            half = 0 < u_mean and o_mean <= u_mean / 2
            seed = beats(o["cost_ns"], u["cost_ns"])
            row.update(uniform_mean_ns=u_mean, oracle_mean_ns=o_mean,
                       oracle_over_uniform=o_mean / u_mean if 0 < u_mean < float("inf") else None,
                       half_rule=half, seed_rule=seed, admitted=half and seed)
        rows.append(row)
    return rows


def gate_line(row: dict) -> str:
    def us(values):
        if values is None:
            return "-"
        return ", ".join("-" if v is None else f"{v / 1000:,.1f}" for v in values)

    verdict_text = {None: "unreadable", True: "admitted", False: "dropped (non-discriminating)"}[row["admitted"]]
    ratio = "-" if row["oracle_over_uniform"] is None else f"{row['oracle_over_uniform']:.2f}"
    return (f"{row['group']} {row['size']} {row['condition']}: uniform cost us [{us(row['uniform_cost_ns'])}], "
            f"oracle [{us(row['oracle_cost_ns'])}], oracle/uniform mean {ratio}, "
            f"half rule {row['half_rule']}, seed rule {row['seed_rule']}: {verdict_text}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    t = sub.add_parser("thresholds")
    t.add_argument("runs", type=Path, nargs="+")
    t.add_argument("--switch-queue", action="store_true")
    g = sub.add_parser("gain")
    g.add_argument("runs", type=Path, nargs="+")
    b = sub.add_parser("binding")
    b.add_argument("runs", type=Path, nargs="+")
    c = sub.add_parser("compare")
    c.add_argument("--runs", type=Path, required=True)
    c.add_argument("--out-root", type=Path, required=True)
    c.add_argument("--stage")
    v = sub.add_parser("versus")
    v.add_argument("arm_a")
    v.add_argument("arm_b")
    v.add_argument("--runs", type=Path, required=True)
    v.add_argument("--out-root", type=Path, required=True)
    for name in ("calibrate", "gains", "select", "cost", "a2a_gate"):
        x = sub.add_parser(name)
        x.add_argument("--runs", type=Path, required=True, nargs="+" if name in ("cost", "a2a_gate") else None)
        x.add_argument("--out-root", type=Path, required=True)
        if name in ("calibrate", "gains"):
            x.add_argument("--into", type=Path)
        if name in ("calibrate", "select", "cost", "a2a_gate"):
            x.add_argument("--stage")
        if name == "cost":
            x.add_argument("--reference", required=True)
        if name == "a2a_gate":
            x.add_argument("--json", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "cost":
        rows = [r for path in arguments.runs for r in transient_rows(path, arguments.out_root, arguments.stage)]
        a2a = a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
        print(json.dumps(cost_cells(rows, arguments.reference) + a2a_cost_cells(a2a, arguments.reference), indent=1))
        return 0
    if arguments.command == "a2a_gate":
        rows = [r for r in a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
                if r["arm"] in ("uniform", A2A_ORACLE)]
        verdicts = a2a_gate(a2a_cost_cells(rows))
        if arguments.json:
            print(json.dumps(verdicts, indent=1))
        else:
            for row in verdicts:
                print(gate_line(row))
        return 0
    if arguments.command in ("calibrate", "gains"):
        existing = json.loads(arguments.into.read_text()) if arguments.into and arguments.into.exists() else {}
        if arguments.command == "calibrate":
            result = calibrate(arguments.runs, arguments.out_root, existing, arguments.stage or "calibration")
        else:
            result = gains(arguments.runs, arguments.out_root, existing)
        if arguments.into:
            arguments.into.write_text(json.dumps(result, indent=1) + "\n")
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "versus":
        print(json.dumps(versus(arguments.runs, arguments.out_root, arguments.arm_a, arguments.arm_b), indent=1))
        return 0
    if arguments.command == "select":
        if arguments.stage and arguments.stage.startswith("t_"):
            result = select_by_cost(transient_rows(arguments.runs, arguments.out_root, arguments.stage))
        else:
            result = select(arguments.runs, arguments.out_root)
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "thresholds":
        metrics = ("mark_fraction", "loss_fraction") if arguments.switch_queue else ("mark_fraction", "trim_fraction")
        result = thresholds(arguments.runs, metrics)
    elif arguments.command == "gain":
        result = {run.name: all_top_fraction(run) for run in arguments.runs}
    elif arguments.command == "binding":
        result = {
            run.name: binding(run, 200e9 if "half_rate" in run.name else 400e9) for run in arguments.runs
        }
    else:
        result = compare(arguments.runs, arguments.out_root, arguments.stage)
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
