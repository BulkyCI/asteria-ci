#!/usr/bin/env python3
"""Reduce one incast run to summary.json, then drop the raw event stream.

    summarize.py RUN_DIR --profile PROFILE.json [--keep-events]

Reads RUN_DIR/ns3: fct.txt for completion, transport_summary.csv for event
totals, and the transport event segments, binned per spine in windows of two
base RTTs: every data packet arriving at a receiving leaf from a spine
(what the spines delivered, summed over the receiving leaves), and every
data packet for a receiving host arriving at a spine (what the senders
offered each spine, per receiving leaf). The receivers are every host of
workload.receiver_leaf, or the first workload.receiver_count hosts of each
of workload.receiver_leaves. Writes RUN_DIR/summary.json,
RUN_DIR/spine_windows.csv and RUN_DIR/offered_windows.csv, and deletes the
segments unless --keep-events.

When the profile's link failure is time-bounded (duration_ns), also writes
RUN_DIR/transients.json: per occurrence, from the offered windows of the
leaves the failure affects (every receiving leaf under a whole-spine
failure, the named leaf under a single link's), the degraded spine's share
of the offered packets, and

* detection: from the onset to the start of the first run of five
  consecutive windows with the share below the midpoint of 1/8 and the
  fluid-optimal share, counted when the run starts before the occurrence
  ends;
* restore, for a detected occurrence: from its end to the start of the
  first run of five consecutive windows with the share above the midpoint
  of 1/8 and the detected level (the mean share from detection to the
  end), starting before the next onset;
* false hold: per healthy spine (every other spine, and under a single
  link's failure the degraded spine's share toward the other receiving
  leaves), the time in windows outside every occurrence with its share
  below the midpoint.

The fluid-optimal share of a spine at rate r among spines at the link rate
R is r / (7R + r): 1/15 at half rate, 0 for a silent spine. Under loss no
fluid-optimal share exists; only the shares are reported. Windows with no
offered packet are skipped.

For workload kind all_to_all, completion comes from
RUN_DIR/telemetry/collective_events.csv instead of fct.txt: per iteration
(training_step), the collective's completion is the latest end_time_ns over
the group's ranks minus the earliest start_time_ns, and summary.json's
"all_to_all" holds every iteration's value and their mean, p50, p99 and max,
and every iteration's start (the earliest start_time_ns).
The run is complete when every iteration has a row from every rank of the
group, two when workload.combine_bytes is set, since the dispatch and the
combine all-to-all both carry the iteration's training_step and the span
runs from the dispatch's first start to the combine's last end. The
windows are taken for leaf 0, where the probe's faulted link is. Workload
kind sequential_dp_all_reduce, one chained all-reduce per step over every
host, is read the same way from its all_reduce rows, one per rank and step,
over the profile's steps, into the same "all_to_all" entry.

When RUN_DIR/ns3/spine_losses.csv exists (the sender loss trace, written
under spray_policy with loss_thresholds and a sender pool weight above 0),
summary.json's "spine_losses" holds, over every host, each spine's total
sends and losses, and the detection evidence for the faulted spine (the
spine of the profile's link failure, spine 3 in a run with none): the end
of the first loss interval at which that spine's cumulative lost count,
summed over the hosts, reaches 1, 5 and 20, absolute and after the
failure's start (None when it never does).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
from pathlib import Path

BASE_RTT_NS = 12_324
WINDOW_NS = 2 * BASE_RTT_NS
RUN_LENGTH = 5


def node_of(address: str) -> int:
    # 11.0.x.1 with x the node id, as the backend writes it in hex.
    return int(address[2:6], 16)


def read_flows(path: Path) -> list[dict[str, int]]:
    flows = []
    for line in path.read_text().split("\n"):
        fields = line.split()
        if len(fields) != 8:
            continue
        flows.append(
            {
                "source": node_of(fields[0]),
                "destination": node_of(fields[1]),
                "bytes": int(fields[4]),
                "start_ns": int(fields[5]),
                "end_ns": int(fields[5]) + int(fields[6]),
            }
        )
    return flows


class Concatenation(io.RawIOBase):
    """The files' bytes in order, as one readable stream."""

    def __init__(self, paths):
        self._paths = list(paths)
        self._handle = None

    def readable(self):
        return True

    def readinto(self, buffer):
        while True:
            if self._handle is None:
                if not self._paths:
                    return 0
                self._handle = open(self._paths.pop(0), "rb")
            count = self._handle.readinto(buffer)
            if count:
                return count
            self._handle.close()
            self._handle = None

    def close(self):
        if self._handle is not None:
            self._handle.close()
        super().close()


def windows(ns3: Path, leaf_nodes: dict[int, int], first_spine_port: int, first_spine_node: int,
            spines: int, receivers: set[int], hosts_per_leaf: int):
    """Packets and bytes delivered to the receiving leaves from each spine,
    per (window, spine), and offered to each spine for the receiving hosts,
    per (window, receiving leaf, spine). leaf_nodes maps a receiving leaf's
    node id to its leaf index."""
    import zstandard

    delivered: dict[tuple[int, int], list[int]] = {}
    offered: dict[tuple[int, int, int], list[int]] = {}
    spine_nodes = range(first_spine_node, first_spine_node + spines)
    # The segments are one zstd stream cut at byte boundaries, so they are
    # decompressed as their concatenation, never one by one.
    segments = sorted(ns3.glob("transport_events.csv.zst.*"))
    with Concatenation(segments) as raw:
        reader = io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(raw))
        header = reader.readline().strip().split(",")
        if header and header[0] == "time_ns":
            for line in reader:
                if ",data_arrival," not in line:
                    continue
                f = line.split(",")
                node = int(f[4])
                window = int(f[0]) // WINDOW_NS
                if node in leaf_nodes:
                    spine = int(f[6]) - first_spine_port
                    if not 0 <= spine < spines:
                        continue
                    cell = delivered.setdefault((window, spine), [0, 0])
                elif node in spine_nodes and int(f[8]) in receivers:
                    leaf = int(f[8]) // hosts_per_leaf
                    cell = offered.setdefault((window, leaf, node - first_spine_node), [0, 0])
                else:
                    continue
                cell[0] += 1
                cell[1] += int(f[11])
    return delivered, offered


def write_windows(path: Path, counts, spines: int) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["window", "start_ns", "spine", "packets", "bytes"])
        for window in sorted({w for w, _ in counts}):
            for spine in range(spines):
                packets, size = counts.get((window, spine), (0, 0))
                writer.writerow([window, window * WINDOW_NS, spine, packets, size])


def write_offered(path: Path, counts, leaves: list[int], spines: int) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["window", "start_ns", "leaf", "spine", "packets", "bytes"])
        for window in sorted({w for w, _, _ in counts}):
            for leaf in leaves:
                for spine in range(spines):
                    packets, size = counts.get((window, leaf, spine), (0, 0))
                    writer.writerow([window, window * WINDOW_NS, leaf, spine, packets, size])


def receiving_hosts(workload: dict, hosts_per_leaf: int) -> tuple[list[int], set[int]]:
    """The receiving leaves and the receiving hosts' node ids."""
    if "receiver_leaves" in workload:
        leaves = list(workload["receiver_leaves"])
        count = workload.get("receiver_count", hosts_per_leaf)
    else:
        leaves = [workload["receiver_leaf"]]
        count = hosts_per_leaf
    return leaves, {leaf * hosts_per_leaf + p for p in range(count) for leaf in leaves}


def all_to_all_group(workload: dict, hosts: int, hosts_per_leaf: int) -> list[int]:
    """The all-to-all's members: every host, those of the group's leaves, or
    with the group's hosts the first that many of each."""
    if workload["group"] == "all":
        return list(range(hosts))
    first = workload["group"].get("hosts", hosts_per_leaf)
    return [h for h in range(hosts)
            if h // hosts_per_leaf in workload["group"]["leaves"] and h % hosts_per_leaf < first]


def nearest_rank(values: list[int], q: float) -> int:
    """The q-th quantile of values by the nearest rank, ceil(q n)."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def all_to_all_timing(path: Path, iterations: int, group: list[int], collectives: int = 1,
                      collective_type: str = "all_to_all") -> dict:
    """Per-iteration completion of the all-to-all (or of collective_type)
    from collective_events.csv: max end over the ranks minus min start over
    the ranks. An iteration is done once every rank of the group has written
    collectives rows of the type for it."""
    spans: dict[int, list[int]] = {}
    ranks: dict[int, dict[int, int]] = {}
    if path.is_file():
        with path.open() as handle:
            for row in csv.DictReader(handle):
                if row["collective_type"] != collective_type:
                    continue
                step = int(row["training_step"])
                start, end = int(row["start_time_ns"]), int(row["end_time_ns"])
                span = spans.setdefault(step, [start, end])
                span[0] = min(span[0], start)
                span[1] = max(span[1], end)
                rows = ranks.setdefault(step, {})
                rows[int(row["rank"])] = rows.get(int(row["rank"]), 0) + 1
    members = set(group)
    done = [i for i in range(1, iterations + 1)
            if set(ranks.get(i, {})) == members and min(ranks[i].values()) >= collectives]
    times = [spans[i][1] - spans[i][0] for i in done]
    return {
        "group_size": len(group),
        "iterations_expected": iterations,
        "iterations_completed": len(done),
        "complete": len(done) == iterations,
        "first_start_ns": min((s[0] for s in spans.values()), default=None),
        "last_end_ns": max((s[1] for s in spans.values()), default=None),
        "iteration_completion_ns": {str(i): t for i, t in zip(done, times)},
        "iteration_start_ns": {str(i): spans[i][0] for i in done},
        "mean_ns": sum(times) / len(times) if times else None,
        "p50_ns": nearest_rank(times, 0.50) if times else None,
        "p99_ns": nearest_rank(times, 0.99) if times else None,
        "max_ns": max(times) if times else None,
    }


DETECTION_COUNTS = (1, 5, 20)
DEFAULT_FAULTED_SPINE = 3


def spine_losses(path: Path, spine: int | None, onset_ns: int | None) -> dict:
    """Per-spine sent and lost totals of the sender loss trace, and the first
    interval end at which the spine's cumulative lost count reaches each of
    DETECTION_COUNTS; never, for no spine."""
    totals: dict[int, list[int]] = {}
    lost_at: dict[int, int] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            s = int(row["spine"])
            lost = int(row["lost"])
            cell = totals.setdefault(s, [0, 0])
            cell[0] += int(row["sent"])
            cell[1] += lost
            if s == spine and lost:
                time = int(row["time_ns"])
                lost_at[time] = lost_at.get(time, 0) + lost
    reached: dict[str, int | None] = {str(n): None for n in DETECTION_COUNTS}
    cumulative = 0
    for time in sorted(lost_at):
        cumulative += lost_at[time]
        for n in DETECTION_COUNTS:
            if reached[str(n)] is None and cumulative >= n:
                reached[str(n)] = time
    return {
        "spines": {str(s): {"sent": c[0], "lost": c[1]} for s, c in sorted(totals.items())},
        "faulted_spine": spine,
        "onset_ns": onset_ns,
        "cumulative_lost_reached_ns": reached,
        "cumulative_lost_reached_after_onset_ns": {
            n: None if t is None or onset_ns is None else t - onset_ns for n, t in reached.items()
        },
    }


# Per-transient measures, on offered packets per (window start, leaf, spine).


def occurrences(failure: dict) -> list[tuple[int, int]]:
    """(onset, end) of every occurrence of a time-bounded failure."""
    repeat = failure.get("repeat", 1)
    period = failure.get("period_ns", 0)
    return [
        (failure["start_ns"] + i * period, failure["start_ns"] + i * period + failure["duration_ns"])
        for i in range(repeat)
    ]


def rate_bps(rate: str) -> float:
    units = {"Gbps": 1e9, "Mbps": 1e6, "Kbps": 1e3, "bps": 1.0}
    for unit, scale in units.items():
        if rate.endswith(unit):
            return float(rate[: -len(unit)]) * scale
    raise ValueError(f"unknown rate {rate}")


def fluid_share(failure: dict, link_rate: str, spines: int) -> float | None:
    """The degraded spine's share of a leaf's traffic when every spine runs
    at its capacity; None under loss, which takes no capacity."""
    if failure["model"] == "silent":
        return 0.0
    if failure["model"] == "gray" and "rate" in failure:
        degraded = rate_bps(failure["rate"])
        return degraded / ((spines - 1) * rate_bps(link_rate) + degraded)
    return None


def share_series(offered: dict[tuple[int, int, int], int], leaves, spine: int) -> list[tuple[int, float]]:
    """(window start, the spine's share of the leaves' offered packets) for
    every window with an offered packet to the leaves."""
    totals: dict[int, int] = {}
    own: dict[int, int] = {}
    for (start, leaf, s), packets in offered.items():
        if leaf not in leaves:
            continue
        totals[start] = totals.get(start, 0) + packets
        if s == spine:
            own[start] = own.get(start, 0) + packets
    return [(start, own.get(start, 0) / totals[start]) for start in sorted(totals) if totals[start] > 0]


def first_run(series, holds, start_from: int, start_before: int, length: int = RUN_LENGTH) -> int | None:
    """Start of the first run of `length` consecutive windows, at or after
    start_from, whose share satisfies holds; None unless it starts before
    start_before."""
    run_start = None
    run_length = 0
    for start, share in series:
        if start < start_from:
            continue
        if run_length == 0 and start >= start_before:
            return None
        if holds(share):
            if run_length == 0:
                run_start = start
            run_length += 1
            if run_length == length:
                return run_start
        else:
            run_length = 0
    return None


def transient_measures(series, schedule: list[tuple[int, int]], midpoint: float | None,
                       uniform: float) -> list[dict]:
    """Detection, detected level and restore per occurrence."""
    measures = []
    for index, (onset, end) in enumerate(schedule):
        next_onset = schedule[index + 1][0] if index + 1 < len(schedule) else float("inf")
        inside = [share for start, share in series if onset <= start < end]
        entry = {
            "onset_ns": onset,
            "end_ns": end,
            "mean_share": sum(inside) / len(inside) if inside else None,
            "detected": False,
            "detection_ns": None,
            "detected_share": None,
            "restore_ns": None,
        }
        if midpoint is not None:
            detected_at = first_run(series, lambda share: share < midpoint, onset, end)
            if detected_at is not None:
                level = [share for start, share in series if detected_at <= start < end]
                detected_share = sum(level) / len(level)
                threshold = (detected_share + uniform) / 2
                restored_at = first_run(series, lambda share: share > threshold, end, next_onset)
                entry.update(
                    detected=True,
                    detection_ns=detected_at - onset,
                    detected_share=detected_share,
                    restore_ns=None if restored_at is None else restored_at - end,
                )
        measures.append(entry)
    return measures


def outside(start: int, schedule: list[tuple[int, int]], window_ns: int = WINDOW_NS) -> bool:
    """Whether the window starting at start overlaps no occurrence."""
    return all(start + window_ns <= onset or start >= end for onset, end in schedule)


def false_hold(series_by_name: dict[str, list[tuple[int, float]]], schedule, midpoint: float,
               window_ns: int = WINDOW_NS) -> dict:
    """Per series, and for any of them, the time in windows outside every
    occurrence with the share below the midpoint."""
    held: dict[str, set[int]] = {
        name: {start for start, share in series if share < midpoint and outside(start, schedule, window_ns)}
        for name, series in series_by_name.items()
    }
    return {
        "per_spine_ns": {name: len(starts) * window_ns for name, starts in held.items()},
        "any_ns": len(set().union(*held.values())) * window_ns if held else 0,
    }


def transients(profile: dict, offered_counts: dict[tuple[int, int, int], list[int]],
               receiving_leaves: list[int]) -> dict | None:
    """The per-transient measures of a run whose failure is time-bounded,
    or None when it has none."""
    network = profile["network"]
    bounded = [f for f in network.get("link_failures", []) if "duration_ns" in f]
    if not bounded:
        return None
    if len(bounded) != 1 or len(network["link_failures"]) != 1:
        raise ValueError("transients.json takes one time-bounded failure")
    failure = bounded[0]
    spines = network["spine_count"]
    spine = failure["spine"]
    affected = [failure["leaf"]] if "leaf" in failure else list(receiving_leaves)
    offered = {(w * WINDOW_NS, leaf, s): c[0] for (w, leaf, s), c in offered_counts.items()}
    schedule = occurrences(failure)
    uniform = 1 / spines
    target = fluid_share(failure, network["link_rate"], spines)
    midpoint = None if target is None else (uniform + target) / 2
    degraded = share_series(offered, affected, spine)
    measures = transient_measures(degraded, schedule, midpoint, uniform)
    result = {
        "window_ns": WINDOW_NS,
        "spine": spine,
        "leaf": failure.get("leaf"),
        "affected_leaves": affected,
        "fluid_share": target,
        "midpoint": midpoint,
        "occurrences": measures,
        "detected_fraction": (
            None if midpoint is None else sum(m["detected"] for m in measures) / len(measures)
        ),
        "false_hold": None,
        "degraded_share": [[start, round(share, 6)] for start, share in degraded],
    }
    if midpoint is not None:
        healthy = {f"spine{s}": share_series(offered, affected, s) for s in range(spines) if s != spine}
        others = [leaf for leaf in receiving_leaves if leaf not in affected]
        if others:
            healthy[f"spine{spine}_leaves{'_'.join(map(str, others))}"] = share_series(offered, others, spine)
        result["false_hold"] = false_hold(healthy, schedule, midpoint)
    return result


def incast_summary(flows: list[dict[str, int]], workload: dict, hosts_per_leaf: int) -> dict:
    """Completion of an incast run from its flows."""
    fan_in = workload["fan_in"]
    _, receivers = receiving_hosts(workload, hosts_per_leaf)
    expected = len(receivers) * fan_in
    first_start = min((f["start_ns"] for f in flows), default=0)
    per_receiver: dict[int, int] = {}
    for flow in flows:
        per_receiver[flow["destination"]] = max(per_receiver.get(flow["destination"], 0), flow["end_ns"])
    completed_receivers = {
        r for r in per_receiver
        if sum(1 for f in flows if f["destination"] == r) == fan_in
    }
    return {
        "flows_expected": expected,
        "flows_completed": len(flows),
        "complete": len(flows) == expected,
        "collective_completion_ns": (max(f["end_ns"] for f in flows) - first_start) if len(flows) == expected else None,
        "incast_completion_ns": {
            str(r): per_receiver[r] - first_start for r in sorted(completed_receivers)
        },
        "first_incast_completion_ns": (
            min(per_receiver[r] for r in completed_receivers) - first_start if completed_receivers else None
        ),
        "transport": {},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--keep-events", action="store_true")
    arguments = parser.parse_args()
    profile = json.loads(arguments.profile.read_text())
    network = profile["network"]
    hosts_per_leaf = network["hosts_per_leaf"]
    spines = network["spine_count"]
    hosts = profile["parallelism"]["dp"]
    workload = profile["workload"]
    # A leaf's devices follow topology.txt: its hosts, then its spines.
    first_spine_port = hosts_per_leaf + 1
    ns3 = arguments.run_dir / "ns3"

    flows = read_flows(ns3 / "fct.txt")
    if workload["kind"] in ("all_to_all", "sequential_dp_all_reduce"):
        receiving_leaves = [0]
        receivers = set(range(hosts_per_leaf))
        events = arguments.run_dir / "telemetry" / "collective_events.csv"
        if workload["kind"] == "all_to_all":
            timing = all_to_all_timing(events, workload["iterations"],
                                       all_to_all_group(workload, hosts, hosts_per_leaf),
                                       1 + bool(workload.get("combine_bytes")))
        else:
            timing = all_to_all_timing(events, profile["steps"], list(range(hosts)), collective_type="all_reduce")
        summary = {
            "flows_expected": None,
            "flows_completed": len(flows),
            "complete": timing["complete"],
            "collective_completion_ns": (
                timing["last_end_ns"] - timing["first_start_ns"] if timing["complete"] else None
            ),
            "incast_completion_ns": {},
            "first_incast_completion_ns": None,
            "all_to_all": timing,
            "transport": {},
        }
    else:
        summary = incast_summary(flows, workload, hosts_per_leaf)
        receiving_leaves, receivers = receiving_hosts(workload, hosts_per_leaf)
    with (ns3 / "transport_summary.csv").open() as handle:
        for row in csv.DictReader(handle):
            summary["transport"][f"{row['event']}:{row['plane']}"] = {
                "count": int(row["event_count"]),
                "bytes": int(row["total_bytes"]),
            }

    losses = ns3 / "spine_losses.csv"
    if losses.is_file():
        failures = network.get("link_failures", [])
        # A host's link names no spine, and no spine is faulted.
        summary["spine_losses"] = spine_losses(
            losses,
            failures[0].get("spine") if failures else DEFAULT_FAULTED_SPINE,
            failures[0]["start_ns"] if failures else None,
        )

    leaves = hosts // hosts_per_leaf
    # Node ids follow topology.txt: hosts, then leaves, then spines.
    leaf_nodes = {hosts + leaf: leaf for leaf in receiving_leaves}
    delivered, offered = windows(ns3, leaf_nodes, first_spine_port, hosts + leaves, spines,
                                 receivers, hosts_per_leaf)
    write_windows(arguments.run_dir / "spine_windows.csv", delivered, spines)
    write_offered(arguments.run_dir / "offered_windows.csv", offered, receiving_leaves, spines)
    measured = transients(profile, offered, receiving_leaves)
    if measured is not None:
        (arguments.run_dir / "transients.json").write_text(json.dumps(measured, indent=1) + "\n")
    summary["window_ns"] = WINDOW_NS
    summary["leaf_arrival_packets"] = sum(c[0] for c in delivered.values())
    summary["spine_offered_packets"] = sum(c[0] for c in offered.values())

    (arguments.run_dir / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    if not arguments.keep_events:
        for segment in ns3.glob("transport_events.csv.zst.*"):
            segment.unlink()
        for bulky in ("mix.tr",):
            (ns3 / bulky).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
