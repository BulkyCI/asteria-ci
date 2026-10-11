#!/usr/bin/env python3
"""Split the planned runs into shards for the workflow matrix.

    shards.py PROFILES_DIR --filter REGEX --runs-per-shard N --out FILE

Reads every PROFILES_DIR/<stage>/runs.json and writes a JSON list of
{"id": "s000", "keys": "key key ..."}. Runs are ordered by fan-in, largest
first, so the longest runs start first and shards stay balanced.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("profiles", type=Path)
    parser.add_argument("--filter", default="")
    parser.add_argument("--runs-per-shard", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    arguments = parser.parse_args()
    pattern = re.compile(arguments.filter)
    runs = [
        run
        for listing in sorted(arguments.profiles.glob("*/runs.json"))
        for run in json.loads(listing.read_text())
        if pattern.search(run["key"])
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
    arguments.out.write_text(json.dumps(shards))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
