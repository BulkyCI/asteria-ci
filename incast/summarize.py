#!/usr/bin/env python3
"""Reduce one incast run to summary.json, then drop the raw event stream.

    summarize.py RUN_DIR --profile PROFILE.json [--keep-events]

Reads RUN_DIR/ns3: fct.txt for completion, transport_summary.csv for event
totals, and the transport event segments for every data packet arriving at
the receiving leaf, binned per spine in windows of two base RTTs. Writes
RUN_DIR/summary.json and RUN_DIR/spine_windows.csv, and deletes the segments
unless --keep-events.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path

import zstandard

BASE_RTT_NS = 12_324
WINDOW_NS = 2 * BASE_RTT_NS


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


def leaf_arrivals(ns3: Path, leaf_node: int, first_spine_port: int, spines: int):
    """Packets and bytes per (window, spine) arriving at the leaf from spines."""
    counts: dict[tuple[int, int], list[int]] = {}
    prefix_node = f",{leaf_node},"
    for segment in sorted(ns3.glob("transport_events.csv.zst.*")):
        with segment.open("rb") as raw:
            reader = io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(raw))
            header = reader.readline().strip().split(",")
            if not header or header[0] != "time_ns":
                continue
            for line in reader:
                if "data_arrival" not in line or prefix_node not in line:
                    continue
                f = line.split(",")
                if f[1] != "data_arrival" or int(f[4]) != leaf_node:
                    continue
                spine = int(f[6]) - first_spine_port
                if not 0 <= spine < spines:
                    continue
                cell = counts.setdefault((int(f[0]) // WINDOW_NS, spine), [0, 0])
                cell[0] += 1
                cell[1] += int(f[11])
    return counts


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
    receiver_leaf = profile["workload"]["receiver_leaf"]
    fan_in = profile["workload"]["fan_in"]
    leaf_node = hosts + receiver_leaf
    # A leaf's devices follow topology.txt: its hosts, then its spines.
    first_spine_port = hosts_per_leaf + 1
    ns3 = arguments.run_dir / "ns3"

    flows = read_flows(ns3 / "fct.txt")
    expected = hosts_per_leaf * fan_in
    first_start = min((f["start_ns"] for f in flows), default=0)
    per_receiver: dict[int, int] = {}
    for flow in flows:
        per_receiver[flow["destination"]] = max(per_receiver.get(flow["destination"], 0), flow["end_ns"])
    completed_receivers = {
        r for r in per_receiver
        if sum(1 for f in flows if f["destination"] == r) == fan_in
    }
    summary = {
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
    with (ns3 / "transport_summary.csv").open() as handle:
        for row in csv.DictReader(handle):
            summary["transport"][f"{row['event']}:{row['plane']}"] = {
                "count": int(row["event_count"]),
                "bytes": int(row["total_bytes"]),
            }

    counts = leaf_arrivals(ns3, leaf_node, first_spine_port, spines)
    windows = sorted({w for w, _ in counts})
    with (arguments.run_dir / "spine_windows.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["window", "start_ns", "spine", "packets", "bytes"])
        for window in windows:
            for spine in range(spines):
                packets, size = counts.get((window, spine), (0, 0))
                writer.writerow([window, window * WINDOW_NS, spine, packets, size])
    summary["window_ns"] = WINDOW_NS
    summary["leaf_arrival_packets"] = sum(c[0] for c in counts.values())

    (arguments.run_dir / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    if not arguments.keep_events:
        for segment in ns3.glob("transport_events.csv.zst.*"):
            segment.unlink()
        for bulky in ("mix.tr",):
            (ns3 / bulky).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
