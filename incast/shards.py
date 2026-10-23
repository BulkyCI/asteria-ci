#!/usr/bin/env python3
"""Split the planned runs into shards for the workflow matrix.

    shards.py PROFILES_DIR --runs-per-shard N [--at-once M] --out FILE

Reads every PROFILES_DIR/<stage>/runs.json and writes a JSON list of
{"id": "s000", "keys": "key key ..."}. The workflow's record_filter selects
the runs where the profiles are written (profiles.py write --filter), so
every run listed is sharded. Runs are ordered by fan-in, largest
first, so the longest runs start first and shards stay balanced.

--at-once M refuses more than M shards. On the cluster every shard's
runner is provisioned up front, and one whose shard has not started within
its idle guard (30 minutes) tears itself down, so a shard beyond the M that
run at once would wait for a runner that is gone.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("profiles", type=Path)
    parser.add_argument("--runs-per-shard", type=int, required=True)
    parser.add_argument("--at-once", type=int)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    runs = [
        run
        for listing in sorted(arguments.profiles.glob("*/runs.json"))
        for run in json.loads(listing.read_text())
    ]
    runs.sort(key=lambda run: (-run["k"], run["key"]))
    size = max(1, arguments.runs_per_shard)
    shards = [
        {"id": f"s{index:03d}", "keys": " ".join(run["key"] for run in runs[start : start + size])}
        for index, start in enumerate(range(0, len(runs), size))
    ]
    if not shards:
        raise SystemExit("no run matches")
    if len(shards) > 256:
        raise SystemExit(f"{len(shards)} shards exceed the 256-job matrix; raise --runs-per-shard")
    if arguments.at_once is not None and len(shards) > arguments.at_once:
        raise SystemExit(
            f"{len(shards)} shards, but {arguments.at_once} run at once and the rest would outwait their"
            " runners; raise runs_per_shard or max_parallel"
        )
    arguments.out.write_text(json.dumps(shards))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
