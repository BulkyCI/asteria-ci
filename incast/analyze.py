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
    analyze.py a2a_gate_reps --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     reps_oracle against reps, a row per cell
    analyze.py a2a_candidates --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     cost and reaction per arm and cell
    analyze.py a2a_cycles --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     cost and re-admission per on- and off-period, per arm
    analyze.py a2a_calibrate --runs RUNS.json... --out-root DIR [--into FILE]
                                                     Phase 9 thresholds per scope and (group, size);
                                                     FILE defaults to incast/a2a_calibration.json

Phase 9 (all-to-all runs, those with a group): cost pairs each faulted run
with the healthy run of the same arm and setting, group, size, iteration
count and seed, from any stage of the runs files given (the comparison reuses the gate's uniform
and oracle runs). Primary cost is the mean iteration completion minus the
pair's, tail cost the same on the p99 with linear interpolation; recovered
fraction is 1 - cost / cost(uniform) per seed on the same (group, size,
condition) cell. Those cells follow the transient cells in cost's output. The
gate admits a cell when the oracle's mean primary cost is at most half of
uniform's and the oracle beats uniform on primary cost by the seed rule.
a2a_gate_reps reads the a2a_gate_reps stage's runs alone (--stage) and
gives each cell the mean primary cost of reps_oracle and of reps, and the
verdict "data_helps" when reps_oracle beats reps on primary cost by the
seed rule ("seed_rule_not_met" otherwise, None while either arm lacks three
seeds with a known cost).

a2a_candidates gives every arm of the runs given (all stages unless --stage)
per cell its per-seed and mean primary and tail cost, and these measures per
faulted run:

* reaction_ns: from the fault's onset (100 base RTTs; 0 for structural) to
  the end of the first sender loss interval ending after it in which spine
  3's share of the sends falls below 1/16, from ns3/spine_losses.csv summed
  over the senders of the faulted link: the hosts of leaf 0 for an _up
  fault; for a _down fault the rows with destination_leaf 1 when the trace
  has that column, otherwise the hosts off leaf 1; every host for
  structural. Intervals are the run's loss_interval_ns. None when the run
  writes no trace (ns3/spine_arrivals.csv holds cumulative counters only at
  the link events, so it gives no share over time) or the share never
  falls below 1/16.
* receiver_reaction_ns: from the onset to the end of the first receiver
  hold interval ending after it in which a receiving host holds spine 3
  (hold not none), from ns3/receiver_holds.csv, over the rows of the
  faulted link: sender_leaf 0 for an _up fault, the receiving hosts of leaf
  1 for a _down fault, every row for structural. None when the run writes
  no receiver hold trace or no such interval comes.
* rto_events: the retransmission timeouts the run's queue pairs took, from
  ns3/rto_events.csv, and rto_per_iteration their count per iteration, an
  event counted in the latest iteration started at or before it (the
  first for one before every start) by summary.json's iteration starts.
  None when the run writes no timeout trace.
* early_cost_ns: the sum, over the iterations that start within 500 us of
  the onset, of the iteration's completion minus the paired healthy run's
  completion of the same iteration; infinite when the faulted run is
  incomplete, None when no iteration starts in the window or the healthy
  run lacks one of them.

a2a_cycles reads the runs with a flapping link (a2a_rs3: a "flap" entry of
start_ns, on_ns, period_ns and repeat in runs.json). Occurrence k's
on-period is [start_ns + k period_ns, + on_ns) and its off-period runs from
there to the next occurrence's start. Per faulted run and period:

* cost_ns: the mean, over the iterations that start inside the period (by
  summary.json's iteration starts), of the iteration's completion minus the
  paired healthy run's completion of the same iteration; None when no
  iteration starts in it or the healthy run lacks one of them, infinite
  when the faulted run is incomplete.
* readmission_ns (off-periods): from the off-period's start to the end of
  the first sender loss interval starting at or after it, and ending by
  the next on-period's start, in which spine 3's share of the sends of the
  faulted link's senders (as reaction_ns reads them) is above 1/16; None
  when the run writes no ns3/spine_losses.csv or no such interval comes.

Per arm the table gives each period's per-seed values and their means over
the seeds that have one.

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
    arm and setting, group, size, iteration count and seed, from any stage. Costs are None
    when the healthy run is missing or incomplete and infinite when the
    faulted run is incomplete."""
    def pair(row):
        return (_label(row), row["group"], row["size"], row.get("iterations"), row["seed"])

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
                "keys": [by_seed[x]["key"] for x in seeds],
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


A2A_REPS_ORACLE = "reps_oracle"


def a2a_gate_reps(cells: list[dict], oracle: str = A2A_REPS_ORACLE, reps: str = "reps") -> list[dict]:
    """Per cell, reps_oracle's and reps's per-seed and mean primary cost and
    the verdict: "data_helps" when reps_oracle beats reps by the seed rule
    on the same three seeds, "seed_rule_not_met" when it does not, None
    while either arm lacks three seeds with a known cost."""
    rows = []
    for cell in cells:
        o, r = cell["arms"].get(oracle), cell["arms"].get(reps)
        row = {"group": cell["group"], "size": cell["size"], "condition": cell["condition"],
               "reps_oracle_cost_ns": None if o is None else o["cost_ns"],
               "reps_cost_ns": None if r is None else r["cost_ns"],
               "reps_oracle_mean_ns": None, "reps_mean_ns": None, "verdict": None}
        readable = (
            o is not None and r is not None and o["seeds"] == r["seeds"] and len(o["seeds"]) == 3
            and None not in o["cost_ns"] and None not in r["cost_ns"]
        )
        if readable:
            row.update(reps_oracle_mean_ns=o["cost_ns_mean"], reps_mean_ns=r["cost_ns_mean"],
                       verdict="data_helps" if beats(o["cost_ns"], r["cost_ns"]) else "seed_rule_not_met")
        rows.append(row)
    return rows


def _us(value) -> str:
    if value is None:
        return "-"
    return "inf" if value == float("inf") else f"{value / 1000:,.1f}"


def gate_reps_markdown(rows: list[dict]) -> str:
    lines = [
        "| group | size | condition | reps_oracle cost us (seeds) | reps cost us (seeds) "
        "| reps_oracle mean us | reps mean us | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        def seeds(values):
            return "-" if values is None else ", ".join(_us(v) for v in values)

        lines.append(
            f"| {row['group']} | {row['size']} | {row['condition']} | {seeds(row['reps_oracle_cost_ns'])} "
            f"| {seeds(row['reps_cost_ns'])} | {_us(row['reps_oracle_mean_ns'])} | {_us(row['reps_mean_ns'])} "
            f"| {row['verdict'] or 'unreadable'} |"
        )
    return "\n".join(lines) + "\n"


# Reaction measures of a faulted all-to-all run (a2a_candidates).

A2A_SHARE_LIMIT = 1 / 16
A2A_EARLY_WINDOW_NS = 500_000
A2A_HOSTS_PER_LEAF_OF_TRACE = 8
A2A_DEFAULT_LOSS_INTERVAL_NS = 2 * BASE_RTT_NS


def fault_onset_ns(condition: str) -> int:
    return 0 if condition == "structural" else DEGRADE_AT_NS


def faulted_share_rows(condition: str, leaf_column: bool):
    """The trace rows of the senders of the faulted link, and the basis."""
    hosts = A2A_HOSTS_PER_LEAF_OF_TRACE
    if condition == "structural":
        return (lambda row: True), "every host"
    if condition.endswith("_up"):
        return (lambda row: int(row["rank"]) // hosts == 0), "hosts of leaf 0"
    if leaf_column:
        return (lambda row: int(row["destination_leaf"]) == 1), "destination_leaf 1"
    return (lambda row: int(row["rank"]) // hosts != 1), "hosts off leaf 1"


def reaction(directory: Path, condition: str, interval_ns: int, spine: int = DEGRADED_SPINE) -> dict:
    """reaction_ns (see the module docstring) and the rows' basis."""
    path = directory / "ns3" / "spine_losses.csv"
    if not path.exists():
        return {"reaction_ns": None, "share_basis": None}
    onset = fault_onset_ns(condition)
    totals: dict[int, int] = defaultdict(int)
    faulted: dict[int, int] = defaultdict(int)
    with path.open() as handle:
        reader = csv.DictReader(handle)
        keep, basis = faulted_share_rows(condition, "destination_leaf" in (reader.fieldnames or ()))
        for row in reader:
            if not keep(row):
                continue
            index = (int(row["time_ns"]) - 1) // interval_ns
            totals[index] += int(row["sent"])
            if int(row["spine"]) == spine:
                faulted[index] += int(row["sent"])
    for index in sorted(totals):
        end = (index + 1) * interval_ns
        if end > onset and totals[index] and faulted[index] / totals[index] < A2A_SHARE_LIMIT:
            return {"reaction_ns": end - onset, "share_basis": basis}
    return {"reaction_ns": None, "share_basis": basis}


def receiver_reaction(directory: Path, condition: str, spine: int = DEGRADED_SPINE) -> dict:
    """receiver_reaction_ns (see the module docstring) and the rows' basis."""
    path = directory / "ns3" / "receiver_holds.csv"
    if not path.exists():
        return {"receiver_reaction_ns": None, "receiver_basis": None}
    hosts = A2A_HOSTS_PER_LEAF_OF_TRACE
    if condition == "structural":
        keep, basis = (lambda row: True), "every row"
    elif condition.endswith("_up"):
        keep, basis = (lambda row: int(row["sender_leaf"]) == 0), "sender_leaf 0"
    else:
        keep, basis = (lambda row: int(row["rank"]) // hosts == 1), "receivers on leaf 1"
    onset = fault_onset_ns(condition)
    ends = []
    with path.open() as handle:
        for row in csv.DictReader(handle):
            end = int(row["time_ns"])
            if end > onset and int(row["spine"]) == spine and row["hold"] != "none" and keep(row):
                ends.append(end)
    return {"receiver_reaction_ns": min(ends) - onset if ends else None, "receiver_basis": basis}


def rto_counts(directory: Path) -> dict:
    """rto_events and rto_per_iteration (see the module docstring)."""
    path = directory / "ns3" / "rto_events.csv"
    if not path.exists():
        return {"rto_events": None, "rto_per_iteration": None}
    timing = json.loads((directory / "summary.json").read_text()).get("all_to_all") or {}
    starts = sorted((v, int(i)) for i, v in (timing.get("iteration_start_ns") or {}).items())
    per_iteration = {i: 0 for _, i in starts}
    total = 0
    with path.open() as handle:
        for row in csv.DictReader(handle):
            total += 1
            if not starts:
                continue
            time = int(row["time_ns"])
            iteration = starts[0][1]
            for start, i in starts:
                if start > time:
                    break
                iteration = i
            per_iteration[iteration] += 1
    return {"rto_events": total,
            "rto_per_iteration": [per_iteration[i] for i in sorted(per_iteration)] if starts else None}


def iteration_timing(directory: Path) -> dict[int, tuple[int, int]] | None:
    """{iteration: (start, completion)} of a run's completed iterations, the
    starts from summary.json or, for a summary that has none, from
    telemetry/collective_events.csv; None when the run is incomplete."""
    timing = json.loads((directory / "summary.json").read_text()).get("all_to_all") or {}
    if not timing.get("complete"):
        return None
    completions = {int(i): v for i, v in timing.get("iteration_completion_ns", {}).items()}
    starts = {int(i): v for i, v in (timing.get("iteration_start_ns") or {}).items()}
    events = directory / "telemetry" / "collective_events.csv"
    if not starts and events.exists():
        with events.open() as handle:
            for row in csv.DictReader(handle):
                if row["collective_type"] == "all_to_all":
                    step, start = int(row["training_step"]), int(row["start_time_ns"])
                    starts[step] = min(starts.get(step, start), start)
    return {i: (starts[i], completions[i]) for i in completions if i in starts}


def early_cost(directory: Path, healthy: Path | None, condition: str) -> float | None:
    """early_cost_ns (see the module docstring)."""
    if healthy is None or not (healthy / "summary.json").exists():
        return None
    reference = iteration_timing(healthy)
    if reference is None:
        return None
    timing = iteration_timing(directory)
    if timing is None:
        return float("inf")
    onset = fault_onset_ns(condition)
    window = [i for i, (start, _) in timing.items() if onset <= start <= onset + A2A_EARLY_WINDOW_NS]
    if not window or any(i not in reference for i in window):
        return None
    return sum(timing[i][1] - reference[i][1] for i in window)


def a2a_candidates(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_cost_cells with every arm's reaction_ns and early_cost_ns per seed
    and their means over the seeds that have one."""
    measured = {}
    for row in a2a_costs(rows):
        interval = (row.get("setting") or {}).get("loss_interval_ns", A2A_DEFAULT_LOSS_INTERVAL_NS)
        directory = out_root / row["key"]
        healthy = None if row["healthy_key"] is None else out_root / row["healthy_key"]
        measured[row["key"]] = {**reaction(directory, row["condition"], interval),
                                **receiver_reaction(directory, row["condition"]),
                                **rto_counts(directory),
                                "early_cost_ns": early_cost(directory, healthy, row["condition"])}
    cells = a2a_cost_cells(rows)
    for cell in cells:
        for entry in cell["arms"].values():
            entry["rto_per_iteration"] = [measured[k]["rto_per_iteration"] for k in entry["keys"]]
            for name in ("reaction_ns", "receiver_reaction_ns", "rto_events", "early_cost_ns"):
                values = [measured[k][name] for k in entry["keys"]]
                entry[name] = values
                known = [v for v in values if v is not None]
                entry[f"{name}_mean"] = sum(known) / len(known) if known else None
                entry[f"{name}_count"] = len(known)
            for basis in ("share_basis", "receiver_basis"):
                entry[basis] = next((measured[k][basis] for k in entry["keys"] if measured[k][basis]), None)
    return cells


def _count(value) -> str:
    return "-" if value is None else f"{value:,.1f}"


def candidates_markdown(cells: list[dict]) -> str:
    lines = [
        "| group | size | condition | arm | cost mean us | tail cost mean us "
        "| reaction mean us (seeds) | receiver reaction mean us (seeds) "
        "| early cost mean us (seeds) | RTOs mean (seeds) |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} | {_us(entry['tail_cost_ns_mean'])} "
                f"| {_us(entry['reaction_ns_mean'])} ({entry['reaction_ns_count']}) "
                f"| {_us(entry['receiver_reaction_ns_mean'])} ({entry['receiver_reaction_ns_count']}) "
                f"| {_us(entry['early_cost_ns_mean'])} ({entry['early_cost_ns_count']}) "
                f"| {_count(entry['rto_events_mean'])} ({entry['rto_events_count']}) |"
            )
    return "\n".join(lines) + "\n"


def flap_periods(flap: dict) -> list[tuple[str, int, int, int | None]]:
    """(kind, occurrence, start, end) of every on- and off-period of a
    flapping link, in time order; the last off-period has no end."""
    out = []
    for k in range(flap["repeat"]):
        start = flap["start_ns"] + k * flap["period_ns"]
        out.append(("on", k, start, start + flap["on_ns"]))
        last = k == flap["repeat"] - 1
        out.append(("off", k, start + flap["on_ns"], None if last else start + flap["period_ns"]))
    return out


def _inside(time: int, start: int, end: int | None) -> bool:
    return start <= time and (end is None or time < end)


def period_costs(directory: Path, healthy: Path | None, periods) -> list[dict]:
    """cost_ns and the iteration count of every period (see the module
    docstring)."""
    reference = None
    if healthy is not None and (healthy / "summary.json").exists():
        reference = iteration_timing(healthy)
    timing = iteration_timing(directory)
    out = []
    for _, _, start, end in periods:
        if timing is None:
            out.append({"cost_ns": None if reference is None else float("inf"), "iterations": None})
            continue
        inside = [i for i, (s, _) in timing.items() if _inside(s, start, end)]
        if not inside or reference is None or any(i not in reference for i in inside):
            out.append({"cost_ns": None, "iterations": len(inside)})
            continue
        out.append({"cost_ns": sum(timing[i][1] - reference[i][1] for i in inside) / len(inside),
                    "iterations": len(inside)})
    return out


def readmissions(directory: Path, condition: str, interval_ns: int, periods,
                 spine: int = DEGRADED_SPINE) -> list[int | None]:
    """readmission_ns of every off-period, None for an on-period (see the
    module docstring)."""
    path = directory / "ns3" / "spine_losses.csv"
    if not path.exists():
        return [None for _ in periods]
    totals: dict[int, int] = defaultdict(int)
    faulted: dict[int, int] = defaultdict(int)
    with path.open() as handle:
        reader = csv.DictReader(handle)
        keep, _ = faulted_share_rows(condition, "destination_leaf" in (reader.fieldnames or ()))
        for row in reader:
            if not keep(row):
                continue
            index = (int(row["time_ns"]) - 1) // interval_ns
            totals[index] += int(row["sent"])
            if int(row["spine"]) == spine:
                faulted[index] += int(row["sent"])
    readmitted = sorted(index for index in totals
                        if totals[index] and faulted[index] / totals[index] > A2A_SHARE_LIMIT)
    out = []
    for kind, _, start, end in periods:
        found = None
        if kind == "off":
            for index in readmitted:
                first, last = index * interval_ns, (index + 1) * interval_ns
                if first >= start and (end is None or last <= end):
                    found = last - start
                    break
        out.append(found)
    return out


def a2a_cycles(rows: list[dict], out_root: Path) -> list[dict]:
    """Per (group, size, condition) cell with a flapping link, per arm and
    period: per-seed cost_ns, iteration counts and readmission_ns, and the
    means over the seeds that have one."""
    cells: dict[tuple, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in a2a_costs(rows):
        if "flap" not in row:
            continue
        periods = flap_periods(row["flap"])
        interval = (row.get("setting") or {}).get("loss_interval_ns", A2A_DEFAULT_LOSS_INTERVAL_NS)
        directory = out_root / row["key"]
        healthy = None if row["healthy_key"] is None else out_root / row["healthy_key"]
        costs = period_costs(directory, healthy, periods)
        admitted = readmissions(directory, row["condition"], interval, periods)
        cells[(row["group"], row["size"], row["condition"])][row["arm"]].append(
            {"row": row, "periods": periods, "costs": costs, "readmission": admitted})

    def mean(values):
        known = [v for v in values if v is not None]
        return (sum(known) / len(known) if known else None), len(known)

    out = []
    for (group, size, condition), arms in sorted(cells.items()):
        table = {}
        for arm, runs in sorted(arms.items()):
            runs.sort(key=lambda r: r["row"]["seed"])
            periods = runs[0]["periods"]
            if any(r["periods"] != periods for r in runs):
                raise ValueError(f"{arm} on {group}-{size}-{condition} runs different flap layouts")
            entries = []
            for n, (kind, occurrence, start, end) in enumerate(periods):
                cost = [r["costs"][n]["cost_ns"] for r in runs]
                admitted = [r["readmission"][n] for r in runs]
                cost_mean, cost_count = mean(cost)
                admitted_mean, admitted_count = mean(admitted)
                entries.append({
                    "kind": kind, "occurrence": occurrence, "start_ns": start, "end_ns": end,
                    "iterations": [r["costs"][n]["iterations"] for r in runs],
                    "cost_ns": cost, "cost_ns_mean": cost_mean, "cost_ns_count": cost_count,
                    "readmission_ns": admitted if kind == "off" else None,
                    "readmission_ns_mean": admitted_mean if kind == "off" else None,
                    "readmission_ns_count": admitted_count if kind == "off" else None,
                })
            table[arm] = {"keys": [r["row"]["key"] for r in runs], "seeds": [r["row"]["seed"] for r in runs],
                          "periods": entries}
        out.append({"group": group, "size": size, "condition": condition, "arms": table})
    return out


def _ms(value) -> str:
    return "end" if value is None else f"{value / 1e6:,.3f}"


def cycles_markdown(cells: list[dict]) -> str:
    """A table per arm: a row per cell and period."""
    arms = sorted({arm for cell in cells for arm in cell["arms"]})
    lines = []
    for arm in arms:
        lines += [
            f"### {arm}", "",
            "| group | size | condition | period | start ms | end ms | iterations (seeds) "
            "| cost mean us (seeds) | re-admission mean us (seeds) |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for cell in cells:
            entry = cell["arms"].get(arm)
            if entry is None:
                continue
            for period in entry["periods"]:
                iterations = ", ".join("-" if v is None else str(v) for v in period["iterations"])
                admitted = ("" if period["kind"] == "on" else
                            f"{_us(period['readmission_ns_mean'])} ({period['readmission_ns_count']})")
                lines.append(
                    f"| {cell['group']} | {cell['size']} | {cell['condition']} "
                    f"| {period['kind']} {period['occurrence']} | {_ms(period['start_ns'])} | {_ms(period['end_ns'])} "
                    f"| {iterations} | {_us(period['cost_ns_mean'])} ({period['cost_ns_count']}) | {admitted} |"
                )
        lines.append("")
    return "\n".join(lines)


# Phase 9 calibration: thresholds per scope and (group, size) from the
# healthy runs of a2a_oracle_calibration and a2a_calibration.

A2A_CALIBRATION_STAGES = ("a2a_oracle_calibration", "a2a_calibration")
A2A_SCOPE_OF_ARM = {
    "graded_switch_queue": "switch_queue",
    "graded_per_connection": "per_connection",
    "graded_per_receiver_mix": "per_receiver",
}
A2A_LOSS_INTERVALS_NS = tuple(n * BASE_RTT_NS for n in (2, 10, 40))
A2A_HEADLINE_CELL = "ep64-2m"
A2A_HEADLINE_RATE = 1e-3
A2A_HOSTS_PER_LEAF = 8


def one_loss_thresholds(one_loss: float) -> list[float]:
    """(f/4, f/2, f) for the one-loss fraction f, rounded down to 6 decimals
    (and kept above 0) so that one loss at the median send count reaches the
    last and grades 0."""
    return [max(int(x * 1e6 + 1e-6) / 1e6, 1e-6) for x in (one_loss / 4, one_loss / 2, one_loss)]


def loss_thresholds(first_p99: float, sent_counts: list[int], count_floor: bool = True) -> dict:
    """The lost fraction's thresholds. Without count_floor, (t, 2t, 4t) for
    t the healthy 99th percentile, floored by scaled_thresholds at 1e-4.
    With it, f is the fraction one loss reads at the median per-spine send
    count; a p99 at or above f keeps the p99's (t, 2t, 4t), but a p99 below
    it (healthy loss on this fabric is exactly zero) gives (f/4, f/2, f):
    one loss per interval is the anomaly and grades 0. The x2/x4 shape
    belongs to the marked and trimmed fractions, whose healthy values are
    nonzero. "floored" flags the second case, "capped" the cap of
    scaled_thresholds; "rule" records which rule applied."""
    median_sent = statistics.median(sent_counts) if sent_counts else None
    one_loss = 1 / median_sent if count_floor and median_sent else 0.0
    if one_loss and first_p99 < one_loss:
        capped, values = False, one_loss_thresholds(one_loss)
        rule = "p99 zero: one loss grades 0" if first_p99 == 0 else "p99 below one loss: one loss grades 0"
    else:
        capped, values = scaled_thresholds(first_p99)
        rule = "p99, count floor" if count_floor else "p99, 1e-4 floor, no count floor"
    return {"p99": first_p99, "median_sent": median_sent, "rule": rule,
            "one_loss_fraction": one_loss if count_floor else None,
            "floored": bool(one_loss) and first_p99 < one_loss if count_floor else values[0] > first_p99,
            "capped": capped, "loss_thresholds": values}


def interval_losses(run: Path, interval_ns: int) -> dict[tuple[int, int, int], list[int]]:
    """The sender loss trace (two-base-RTT rows of time_ns, rank, spine, sent,
    lost) summed into intervals of interval_ns, aligned at 0 as the pool's
    are: {(rank, interval index, spine): [sent, lost]}, where a row ending
    at time t belongs to the interval ending at or after it."""
    sums: dict[tuple[int, int, int], list[int]] = defaultdict(lambda: [0, 0])
    with (run / "ns3" / "spine_losses.csv").open() as handle:
        for row in csv.DictReader(handle):
            cell = sums[(int(row["rank"]), (int(row["time_ns"]) - 1) // interval_ns, int(row["spine"]))]
            cell[0] += int(row["sent"])
            cell[1] += int(row["lost"])
    return sums


def trace_loss_thresholds(runs: list[Path]) -> dict[str, dict]:
    """Per candidate interval, loss_thresholds from the healthy lost fraction
    of every (host, interval, spine) with a send."""
    out = {}
    for interval in A2A_LOSS_INTERVALS_NS:
        fractions, sent = [], []
        for run in runs:
            for n, lost in interval_losses(run, interval).values():
                if n:
                    fractions.append(lost / n)
                    sent.append(n)
        out[str(interval)] = loss_thresholds(percentile(fractions, 0.99), sent)
    return out


def report_loss_thresholds(runs: list[Path]) -> dict:
    """The oracle's loss_thresholds, from its ground-truth loss fraction per
    report and spine with arrivals, by the rule of its congestion thresholds:
    the healthy 99th percentile, floored at 1e-4 only. The fraction is a
    probability read from switch state, not a count, so no count floor: one
    over the arrivals would blind the oracle to losses of 1e-2 and 1e-3."""
    fractions, arrivals = [], []
    for run in runs:
        for rows in reports(run).values():
            for r in rows:
                if int(r["arrivals"]):
                    fractions.append(float(r["loss_fraction"]))
                    arrivals.append(int(r["arrivals"]))
    return loss_thresholds(percentile(fractions, 0.99), arrivals, count_floor=False)


def headline(runs: list[Path], spine: int = DEGRADED_SPINE) -> dict:
    """P17: the expected losses at 1e-3 per pool interval of the longest
    candidate on the given spine for a leaf 0 sender, the mean send count
    over the (host, interval) pairs with a send there, times 1e-3. At least
    1 keeps the 1e-3 uplink cell as the headline; otherwise the 1e-2 one."""
    interval = max(A2A_LOSS_INTERVALS_NS)
    sends = [
        n for run in runs for (rank, _, s), (n, _) in interval_losses(run, interval).items()
        if s == spine and rank // A2A_HOSTS_PER_LEAF == 0 and n
    ]
    mean = statistics.fmean(sends) if sends else None
    expected = None if mean is None else mean * A2A_HEADLINE_RATE
    return {"interval_ns": interval, "intervals": len(sends), "mean_sent": mean,
            "expected_losses_at_1e3": expected,
            "condition": "loss1e3_up" if expected is not None and expected >= 1 else "loss1e2_up"}


def a2a_calibrate(runs_files, out_root: Path, existing: dict) -> dict:
    """a2a_calibration.json from the healthy calibration runs that have a
    summary, merged into existing: per scope and (group, size), the Phase 8
    congestion (and, but for switch_queue, trim) thresholds of thresholds(),
    and loss_thresholds. The pooled pass's are a map from each candidate
    interval to its list, from its sender loss trace; per_connection takes
    the same map from the pooled pass of its cell (its own runs write no
    trace); switch_queue's are one list from its ground-truth loss fraction.
    "headline" (<group>-<size>-<condition>) is set by the P17 rule from the
    pooled EP64 2 MiB trace, unless existing holds a "headline_reason" for a
    headline set by hand; "headline_rule" is recorded either way."""
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for run in _listed_runs(runs_files):
        directory = out_root / run["key"]
        if (run["stage"] in A2A_CALIBRATION_STAGES and run["condition"] == "healthy"
                and run["arm"] in A2A_SCOPE_OF_ARM and (directory / "summary.json").exists()):
            groups[(A2A_SCOPE_OF_ARM[run["arm"]], f"{run['group']}-{run['size']}")].append(directory)
    result = {key: (dict(value) if isinstance(value, dict) else value) for key, value in existing.items()}
    traced = {cell: [d for d in dirs if (d / "ns3" / "spine_losses.csv").exists()]
              for (scope, cell), dirs in groups.items() if scope == "per_receiver"}
    for (scope, cell), directories in sorted(groups.items()):
        switch = scope == "switch_queue"
        entry = thresholds(directories, ("mark_fraction", "loss_fraction") if switch else ("mark_fraction", "trim_fraction"))
        entry["runs"] = sorted(d.name for d in directories)
        if switch:
            loss = report_loss_thresholds(directories)
            entry["loss_thresholds"] = loss.pop("loss_thresholds")
            entry["loss_calibration"] = loss
        elif traced.get(cell):
            losses = trace_loss_thresholds(traced[cell])
            entry["loss_thresholds"] = {k: v.pop("loss_thresholds") for k, v in losses.items()}
            entry["loss_calibration"] = losses
            entry["loss_runs"] = sorted(d.name for d in traced[cell])
        result.setdefault(scope, {})[cell] = entry
    if traced.get(A2A_HEADLINE_CELL):
        decision = headline(traced[A2A_HEADLINE_CELL])
        if "headline_reason" not in result:
            result["headline"] = f"{A2A_HEADLINE_CELL}-{decision['condition']}"
        result["headline_rule"] = decision
    return result


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
    for name in ("calibrate", "gains", "select", "cost", "a2a_gate", "a2a_calibrate", "a2a_gate_reps",
                 "a2a_candidates", "a2a_cycles"):
        x = sub.add_parser(name)
        x.add_argument("--runs", type=Path, required=True,
                       nargs="+" if name in ("cost", "a2a_gate", "a2a_calibrate", "a2a_gate_reps", "a2a_candidates",
                                             "a2a_cycles") else None)
        x.add_argument("--out-root", type=Path, required=True)
        if name in ("calibrate", "gains"):
            x.add_argument("--into", type=Path)
        if name in ("calibrate", "select", "cost", "a2a_gate"):
            x.add_argument("--stage")
        if name == "cost":
            x.add_argument("--reference", required=True)
        if name == "a2a_gate":
            x.add_argument("--json", action="store_true")
        if name == "a2a_gate_reps":
            x.add_argument("--stage", default="a2a_gate_reps")
            x.add_argument("--markdown", type=Path)
        if name in ("a2a_candidates", "a2a_cycles"):
            x.add_argument("--stage")
            x.add_argument("--markdown", type=Path)
        if name == "a2a_calibrate":
            x.add_argument("--into", type=Path, default=Path(__file__).resolve().parent / "a2a_calibration.json")
    arguments = parser.parse_args()
    if arguments.command == "a2a_calibrate":
        existing = json.loads(arguments.into.read_text()) if arguments.into.exists() else {}
        result = a2a_calibrate(arguments.runs, arguments.out_root, existing)
        arguments.into.write_text(json.dumps(result, indent=1) + "\n")
        print(json.dumps(result, indent=1))
        return 0
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
    if arguments.command == "a2a_gate_reps":
        rows = [r for r in a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
                if r["arm"] in (A2A_REPS_ORACLE, "reps")]
        verdicts = a2a_gate_reps(a2a_cost_cells(rows))
        if arguments.markdown:
            arguments.markdown.write_text(gate_reps_markdown(verdicts))
        print(json.dumps(verdicts, indent=1))
        return 0
    if arguments.command == "a2a_candidates":
        cells = a2a_candidates(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(candidates_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_cycles":
        cells = a2a_cycles(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(cycles_markdown(cells))
        print(json.dumps(cells, indent=1))
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
