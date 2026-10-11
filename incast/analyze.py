#!/usr/bin/env python3
"""Tables of the incast study, from run directories holding summary.json.

    analyze.py thresholds RUN_DIR... --scope SCOPE   first threshold from healthy reports
    analyze.py gain RUN_DIR...                       fraction of all-top reports per run
    analyze.py binding RUN_DIR...                    degraded spine's rate toward leaf 0
    analyze.py compare --runs RUNS.json --out-root DIR [--stage S]  per-cell arm table
    analyze.py calibrate --runs RUNS.json --out-root DIR [--into FILE]   thresholds per scope and K
    analyze.py gains --runs RUNS.json --out-root DIR --into FILE        gain per scope and K
    analyze.py select --runs RUNS.json --out-root DIR                   tuning selection

Gain: per scope and K, the largest candidate gain at which at least 99
percent of the healthy reports of every calibration seed give every spine
the top grade, pooled over the seeds; if none reaches 99 percent, the gain
with the largest fraction, flagged. Selection: per redirect arm and tuning
condition, the setting with the lowest collective completion.

Thresholds: for every report of a healthy run, each spine's deviation above
the median spine of that report, for the marked and the trimmed fraction
(and, for switch_queue, the loss fraction); the first threshold is the 99th
percentile of those deviations over all reports and metrics, the second and
third two and four times it, each capped below one. A report is one
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
    first = max(p99.values())
    # The simulator wants three increasing fractions in (0, 1].
    first = min(max(first, 1e-4), 0.24)
    return {
        "reports": count,
        "p99_deviation": p99,
        "congestion_thresholds": [round(first, 6), round(2 * first, 6), round(4 * first, 6)],
    }


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


def shed_time(run: Path, target_share: float | None) -> float | None:
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
    starts = sorted(s for s in totals if s >= DEGRADE_AT_NS and totals[s] > 0)
    run_start = None
    run_length = 0
    for start in starts:
        if degraded[start] / totals[start] < midpoint:
            if run_length == 0:
                run_start = start
            run_length += 1
            if run_length == 5:
                return run_start - DEGRADE_AT_NS
        else:
            run_length = 0
    return None


TARGET_SHARE = {"half_rate": 200 / 3000, "silent": 0.0}


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
        rows.append(
            {
                **run,
                "status": "complete" if summary["complete"] else "incomplete",
                "completion_ns": summary["collective_completion_ns"],
                "first_incast_ns": summary["first_incast_completion_ns"],
                "trims": sum(v["count"] for k, v in transport.items() if k.startswith("trim_")),
                "shed_ns": shed_time(directory, TARGET_SHARE.get(run["condition"])),
                "wall_seconds": summary.get("wall_seconds"),
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


def calibrate(runs_file: Path, out_root: Path, existing: dict) -> dict:
    groups: dict[tuple[str, int], list[Path]] = defaultdict(list)
    for run, directory in _listed(runs_file, out_root, "calibration"):
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
        chosen = max(passing) if passing else max(fractions, key=fractions.get)
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
    for name in ("calibrate", "gains", "select"):
        x = sub.add_parser(name)
        x.add_argument("--runs", type=Path, required=True)
        x.add_argument("--out-root", type=Path, required=True)
        if name != "select":
            x.add_argument("--into", type=Path)
    arguments = parser.parse_args()
    if arguments.command in ("calibrate", "gains"):
        existing = json.loads(arguments.into.read_text()) if arguments.into and arguments.into.exists() else {}
        compute = calibrate if arguments.command == "calibrate" else gains
        result = compute(arguments.runs, arguments.out_root, existing)
        if arguments.into:
            arguments.into.write_text(json.dumps(result, indent=1) + "\n")
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "select":
        print(json.dumps(select(arguments.runs, arguments.out_root), indent=1))
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
