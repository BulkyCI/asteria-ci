"""Readers of the run traces the backend can write sparsely.

Under network.sparse_hold_trace the backend writes a row of
ns3/spine_losses.csv only where its series sent or lost in the closed
interval or its grade changed, and one row (spine 0's) of an interval that
would write none, which marks the interval closed (SPINE_LOSS_SPARSE); and a
row of ns3/receiver_holds.csv likewise where its series had an arrival or a
missing packet or its hold changed (RECEIVER_HOLD_SPARSE). open_trace
rebuilds the dense trace from either, row for row and byte for byte: a row
left out is no count at the series' last grade or hold. The run's
manifest.json says whether the trace is sparse (physical_topology
sparse_hold_trace) and over how many spines (physical_topology
spine_count). Every other trace, and a dense one, is read as it is.

A reader that sums a trace's counts reads its rows as written, since a row
left out counts nothing; one that follows a series' grade or hold reads
them by closed interval (intervals), as dense and sparse traces alike give
the same, carrying each series' value from its last row. Neither rebuilds
the dense trace, whose rows grow with the spines.

A run of a sparse hold trace keeps its hold traces zstd-compressed beside
where they were written (run_one.sh), <trace>.zst; exists and open_rows
take a trace from either.
"""

from __future__ import annotations

import contextlib
import io
import json
import subprocess
from pathlib import Path
from typing import Iterator


def _manifest(path: Path) -> dict:
    manifest = path.parent.parent / "manifest.json"
    if not manifest.is_file():
        return {}
    return json.loads(manifest.read_text()).get("physical_topology", {})


# The hold traces the backend writes sparsely under sparse_hold_trace.
SPARSE_TRACES = ("spine_losses.csv", "receiver_holds.csv")


def compressed(path: Path) -> Path:
    """Where a run keeps path zstd-compressed."""
    return path.with_name(path.name + ".zst")


def exists(path: Path) -> bool:
    """Whether the run has the trace, as written or compressed."""
    return path.exists() or compressed(path).exists()


@contextlib.contextmanager
def open_rows(path: Path):
    """The trace's lines as written, from its compressed form (zstandard
    where it is installed, the zstd command otherwise) where the run keeps
    that."""
    if path.exists() or not compressed(path).exists():
        with path.open() as handle:
            yield handle
        return
    try:
        import zstandard
    except ImportError:
        zstandard = None
    if zstandard is not None:
        with compressed(path).open("rb") as raw:
            yield io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(raw))
        return
    process = subprocess.Popen(["zstd", "-dcq", str(compressed(path))], stdout=subprocess.PIPE, text=True)
    try:
        yield process.stdout
    finally:
        process.stdout.close()
        if process.wait() not in (0, -13):
            raise OSError(f"zstd could not read {compressed(path)}")


def is_sparse(path: Path) -> bool:
    """Whether path is a hold trace the backend wrote sparsely."""
    return path.name in SPARSE_TRACES and bool(_manifest(path).get("sparse_hold_trace"))


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


# A hold trace's columns that are neither its series' nor the time: the
# counts and the series' value.
VALUE_COLUMNS = ("sent", "arrivals", "lost", "grade", "hold")


def intervals(reader) -> Iterator[tuple[tuple[str, ...], list[dict]]]:
    """A hold trace's csv.DictReader, dense or sparse, by closed interval of
    one group (a host, and its destination or sender leaf where the trace
    names one): the group's columns but the spine, and its rows as written,
    with the time. Every series of the group has its value at the
    interval's close from its last row: one in this interval or an earlier
    one, as a sparse trace writes the first interval of each group whole."""
    group = [column for column in reader.fieldnames or ()
             if column not in VALUE_COLUMNS and column != "spine"]
    current: tuple[str, ...] | None = None
    rows: list[dict] = []
    for row in reader:
        key = tuple(row[column] for column in group)
        if key != current:
            if current is not None:
                yield current, rows
            current, rows = key, []
        rows.append(row)
    if current is not None:
        yield current, rows


@contextlib.contextmanager
def open_trace(path: Path):
    """path opened for reading as csv lines, a sparse hold trace rebuilt
    dense (see the module docstring)."""
    with open_rows(path) as handle:
        if is_sparse(path):
            yield dense_lines(iter(handle), int(_manifest(path)["spine_count"]))
        else:
            yield handle
