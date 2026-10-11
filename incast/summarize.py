#!/usr/bin/env python3
"""Reduce one incast run to summary.json, then drop the raw event stream.

    summarize.py RUN_DIR --profile PROFILE.json [--keep-events]

Reads RUN_DIR/ns3: fct.txt for completion, transport_summary.csv for event
totals, and the transport event segments, binned per spine in windows of two
base RTTs: every data packet arriving at the receiving leaf from a spine
(what the spines delivered), and every data packet for a receiving host
arriving at a spine (what the senders offered each spine). Writes
RUN_DIR/summary.json, RUN_DIR/spine_windows.csv and RUN_DIR/offered_windows.csv,
and deletes the segments unless --keep-events.
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


def windows(ns3: Path, leaf_node: int, first_spine_port: int, first_spine_node: int,
            spines: int, receivers: range):
    """Packets and bytes per (window, spine): delivered to the leaf from each
    spine, and offered to each spine for the receiving hosts."""
    delivered: dict[tuple[int, int], list[int]] = {}
    offered: dict[tuple[int, int], list[int]] = {}
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
                if node == leaf_node:
                    spine = int(f[6]) - first_spine_port
                    if not 0 <= spine < spines:
                        continue
                    table = delivered
                elif node in spine_nodes and int(f[8]) in receivers:
                    spine = node - first_spine_node
                    table = offered
                else:
                    continue
                cell = table.setdefault((int(f[0]) // WINDOW_NS, spine), [0, 0])
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

    leaves = hosts // hosts_per_leaf
    receivers = range(receiver_leaf * hosts_per_leaf, (receiver_leaf + 1) * hosts_per_leaf)
    # Node ids follow topology.txt: hosts, then leaves, then spines.
    delivered, offered = windows(ns3, leaf_node, first_spine_port, hosts + leaves, spines, receivers)
    write_windows(arguments.run_dir / "spine_windows.csv", delivered, spines)
    write_windows(arguments.run_dir / "offered_windows.csv", offered, spines)
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
