#!/usr/bin/env python3
"""Markdown of a compare table: one row per (stage, K, condition, geometry, T, arm), seeds side by side.

    report.py TABLE.json

Geometry and T (the transient length in us) are blank for runs of the
stages that have none. Detected is the per-seed fraction of a run's
transients detected, for the transient stages.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict


def us(ns):
    return "-" if ns is None else f"{ns / 1000:,.1f}"


def main() -> int:
    rows = json.loads(open(sys.argv[1]).read())
    cells = defaultdict(list)
    for row in rows:
        label = row["arm"] if not row.get("setting") else f"{row['arm']} {json.dumps(row['setting'], sort_keys=True)}"
        if "compute_ns" in row:
            # The all-to-all probe's compute between iterations.
            label = f"{label}, compute {row['compute_ns'] // 1000} us"
        cells[(row["stage"], row["k"], row["condition"], row.get("geometry", ""), row.get("t_us", 0), label)].append(row)
    done = sum(1 for r in rows if r.get("status") == "complete")
    print(f"{done} of {len(rows)} runs complete.\n")
    print("| Stage | K | Condition | Geometry | T (us) | Arm | Completion (us) per seed | Mean | Shed (us) | Detected | Trims |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for (stage, k, condition, geometry, t_us, label), group in sorted(cells.items()):
        group.sort(key=lambda r: r["seed"])
        times = [r.get("completion_ns") for r in group]
        known = [t for t in times if t is not None]
        mean = sum(known) / len(known) if known and len(known) == len(times) else None
        sheds = ", ".join(us(r.get("shed_ns")) for r in group)
        detected = ", ".join(
            "-" if r.get("detected_fraction_mean") is None else f"{r['detected_fraction_mean']:.2f}" for r in group
        )
        trims = ", ".join(str(r.get("trims", "-")) for r in group)
        per_seed = ", ".join(f"{r['seed']}: {us(r.get('completion_ns'))}" if r.get("status") == "complete" else f"{r['seed']}: {r.get('status')}" for r in group)
        print(f"| {stage} | {k} | {condition} | {geometry} | {t_us or ''} | {label} | {per_seed} | {us(mean)} | {sheds} | {detected} | {trims} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
