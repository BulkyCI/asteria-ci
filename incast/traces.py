"""Readers of the run traces the backend can write sparsely.

Under network.sparse_hold_trace the backend writes a row of
ns3/spine_losses.csv only where its series sent or lost in the closed
interval or its grade changed, and one row (spine 0's) of an interval that
would write none, which marks the interval closed (SPINE_LOSS_SPARSE).
open_trace rebuilds the dense trace from it, row for row and byte for byte:
a row left out is no send and no loss at the series' last grade. The run's
manifest.json says whether the trace is sparse (physical_topology
sparse_hold_trace) and over how many spines (physical_topology
spine_count). Every other trace, and a dense one, is read as it is.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Iterator


def _manifest(path: Path) -> dict:
    manifest = path.parent.parent / "manifest.json"
    if not manifest.is_file():
        return {}
    return json.loads(manifest.read_text()).get("physical_topology", {})


def is_sparse(path: Path) -> bool:
    """Whether path is a sender loss trace the backend wrote sparsely."""
    return path.name == "spine_losses.csv" and bool(_manifest(path).get("sparse_hold_trace"))


def dense_lines(lines: Iterator[str], spines: int) -> Iterator[str]:
    """The dense trace's lines from a sparse trace's, header first."""
    header = next(lines, None)
    if header is None:
        return
    yield header
    # The series columns before the spine: rank, and the destination where
    # the trace names one.
    keyed = len(header.rstrip("\n").split(",")) == 7
    last: dict[tuple[str, ...], str] = {}

    def group_lines(prefix: tuple[str, ...], rows: dict[int, str]) -> Iterator[str]:
        head = ",".join(prefix)
        for spine in range(spines):
            line = rows.get(spine)
            if line is None:
                grade = last[(*prefix[1:], str(spine))]
                line = f"{head},{spine},0,0,{grade}\n"
            else:
                grade = line.rstrip("\n").rsplit(",", 1)[1]
            last[(*prefix[1:], str(spine))] = grade
            yield line

    current: tuple[str, ...] | None = None
    rows: dict[int, str] = {}
    for line in lines:
        fields = line.rstrip("\n").split(",")
        prefix = tuple(fields[:3] if keyed else fields[:2])
        spine = int(fields[3] if keyed else fields[2])
        if prefix != current:
            if current is not None:
                yield from group_lines(current, rows)
            current, rows = prefix, {}
        rows[spine] = line
    if current is not None:
        yield from group_lines(current, rows)


@contextlib.contextmanager
def open_trace(path: Path):
    """path opened for reading as csv lines, a sparse sender loss trace
    rebuilt dense (see the module docstring)."""
    with path.open() as handle:
        if is_sparse(path):
            yield dense_lines(iter(handle), int(_manifest(path)["spine_count"]))
        else:
            yield handle
