"""The sparse hold traces, rebuilt dense and read as written."""

import json
import random
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analyze  # noqa: E402
import traces  # noqa: E402

HEADER = "time_ns,rank,destination_leaf,spine,sent,lost,grade\n"
# Three spines, host 9 toward leaf 0: two closed intervals and one of the
# pooled form's columns.
DENSE = (HEADER
         + "10,9,0,0,4,0,3\n10,9,0,1,0,0,3\n10,9,0,2,2,1,1\n"
         + "20,9,0,0,0,0,3\n20,9,0,1,0,0,3\n20,9,0,2,0,0,1\n"
         + "30,9,0,0,0,0,3\n30,9,0,1,3,0,3\n30,9,0,2,0,0,0\n")
# What the backend writes of it under SPINE_LOSS_SPARSE: the first interval
# whole (every grade new), the second marked by spine 0's row alone, the
# third the send on spine 1 and the grade change on spine 2.
SPARSE = (HEADER
          + "10,9,0,0,4,0,3\n10,9,0,1,0,0,3\n10,9,0,2,2,1,1\n"
          + "20,9,0,0,0,0,3\n"
          + "30,9,0,1,3,0,3\n30,9,0,2,0,0,0\n")


def sparse_of(dense: str) -> str:
    """What the backend writes of a dense hold trace sparsely: per closed
    interval of a group, the rows with a count or a value other than the
    series' last, spine 0's where none is."""
    lines = dense.splitlines(True)
    out, last, group, rows = [lines[0]], {}, None, []

    def close():
        kept = [line for line in rows if line.split(",")[-3:-1] != ["0", "0"]
                or last.get(tuple(line.split(",")[1:-3])) != line.rstrip("\n").rsplit(",", 1)[1]]
        out.extend(kept or rows[:1])
        for line in rows:
            last[tuple(line.split(",")[1:-3])] = line.rstrip("\n").rsplit(",", 1)[1]

    for line in lines[1:]:
        key = tuple(line.split(",")[:-4])
        if key != group and group is not None:
            close()
            rows = []
        group = key
        rows.append(line)
    if rows:
        close()
    return "".join(out)


def random_trace(header: str, values: tuple[str, ...], seed: int, spines: int = 4) -> str:
    """A dense hold trace of two hosts, on leaves 0 and 2, toward leaves 0
    and 1 over 30 intervals of 10 ns about the fault's onset, each series'
    value holding for a while and its counts mostly 0."""
    draw = random.Random(seed)
    state = {}
    lines = [header]
    onset = analyze.DEGRADE_AT_NS
    for end in range(onset - 150, onset + 150, 10):
        for rank in (3, 20):
            for leaf in (0, 1):
                for spine in range(spines):
                    key = (rank, leaf, spine)
                    if key not in state or draw.random() < 0.1:
                        state[key] = draw.choice(values)
                    sent = draw.choice((0, 0, 0, 1, 2))
                    lost = 1 if sent and draw.random() < 0.3 else 0
                    lines.append(f"{end},{rank},{leaf},{spine},{sent},{lost},{state[key]}\n")
    return "".join(lines)


class SparseTraceTest(unittest.TestCase):
    def _run(self, directory: Path, trace: str, sparse: bool) -> Path:
        (directory / "ns3").mkdir(parents=True)
        path = directory / "ns3" / "spine_losses.csv"
        path.write_text(trace)
        topology = {"spine_count": 3, "hosts_per_leaf": 8,
                    **({"sparse_hold_trace": True} if sparse else {})}
        (directory / "manifest.json").write_text(json.dumps({
            "ranks": 64, "physical_topology": topology,
            "link_failures": [{"model": "silent", "leaf": 0, "spine": 3, "direction": "up"}]}))
        return path

    def test_the_rebuilt_trace_is_the_dense_one(self):
        self.assertEqual("".join(traces.dense_lines(iter(SPARSE.splitlines(True)), 3)), DENSE)
        # The pooled form names no destination.
        rows = ["time_ns,rank,spine,sent,lost,grade\n", "10,9,0,4,0,3\n", "10,9,1,0,0,3\n",
                "20,9,1,0,0,3\n"]
        self.assertEqual("".join(traces.dense_lines(iter(rows), 2)),
                         rows[0] + "10,9,0,4,0,3\n10,9,1,0,0,3\n20,9,0,0,0,3\n20,9,1,0,0,3\n")

    def test_readers_take_the_sparse_trace_as_the_dense(self):
        with tempfile.TemporaryDirectory() as root:
            dense = self._run(Path(root) / "dense", DENSE, False)
            sparse = self._run(Path(root) / "sparse", SPARSE, True)
            with traces.open_trace(sparse) as handle:
                self.assertEqual("".join(handle), DENSE)
            with traces.open_trace(dense) as handle:
                self.assertEqual("".join(handle), DENSE)
            for reader in (lambda p: analyze.sender_hold_reading(p, "blackhole_up", "spray"),
                           lambda p: analyze.spine_holds(p, lambda r: r["grade"] == "0"),
                           lambda p: analyze.interval_losses(p.parent.parent, 20)):
                self.assertEqual(reader(sparse), reader(dense))

    def test_sparse_of_is_the_backends_rule(self):
        self.assertEqual(sparse_of(DENSE), SPARSE)

    def test_every_reader_reads_the_sparse_traces_as_the_dense(self):
        sender = random_trace(HEADER, ("3", "0", "1"), 1)
        receiver = random_trace("time_ns,rank,sender_leaf,spine,arrivals,lost,hold\n", ("none", "leaf", "host"), 2)
        with tempfile.TemporaryDirectory() as root:
            runs = {}
            for name, sparse in (("dense", False), ("sparse", True)):
                directory = Path(root) / name
                self._run(directory, sparse_of(sender) if sparse else sender, sparse)
                (directory / "ns3" / "receiver_holds.csv").write_text(sparse_of(receiver) if sparse else receiver)
                manifest = json.loads((directory / "manifest.json").read_text())
                manifest["physical_topology"]["spine_count"] = 4
                (directory / "manifest.json").write_text(json.dumps(manifest))
                runs[name] = directory
            self.assertLess(len(sparse_of(sender)), len(sender))
            with traces.open_trace(runs["sparse"] / "ns3" / "receiver_holds.csv") as handle:
                self.assertEqual("".join(handle), receiver)
            for condition in ("blackhole_up", "loss1e2_down"):
                readers = {
                    "sender": lambda d: analyze.sender_hold_reading(d / "ns3" / "spine_losses.csv", condition, "spray"),
                    "receiver": lambda d: analyze.receiver_hold_reading(d / "ns3" / "receiver_holds.csv", condition),
                    "spine_holds": lambda d: analyze.spine_holds(d / "ns3" / "spine_losses.csv",
                                                                 lambda r: r["grade"] == "0"),
                    "reaction": lambda d: analyze.reaction(d, condition, 20),
                    "receiver_reaction": lambda d: analyze.receiver_reaction(d, condition, 1),
                    "first_hold": lambda d: [analyze.first_hold(d, condition, end, 1) for end in (False, True)],
                    "spine_lost": lambda d: analyze.spine_lost(d, condition, 1),
                    "interval_losses": lambda d: analyze.interval_losses(d, 30),
                }
                for name, reader in readers.items():
                    with self.subTest(condition=condition, reader=name):
                        self.assertEqual(reader(runs["sparse"]), reader(runs["dense"]))
            # Kept compressed, as run_one.sh keeps a sparse run's traces.
            try:
                import zstandard
            except ImportError:
                zstandard = None
            if zstandard is None and shutil.which("zstd") is None:
                return
            if zstandard is None:
                self.skipTest("no zstd decompressor")
            for name in ("spine_losses.csv", "receiver_holds.csv"):
                path = runs["sparse"] / "ns3" / name
                traces.compressed(path).write_bytes(zstandard.ZstdCompressor().compress(path.read_bytes()))
                path.unlink()
                self.assertTrue(traces.exists(path))
            self.assertEqual(analyze.sender_hold_reading(runs["sparse"] / "ns3" / "spine_losses.csv", "blackhole_up",
                                                         "spray"),
                             analyze.sender_hold_reading(runs["dense"] / "ns3" / "spine_losses.csv", "blackhole_up",
                                                         "spray"))
            self.assertEqual(analyze.first_hold(runs["sparse"], "blackhole_up", True),
                             analyze.first_hold(runs["dense"], "blackhole_up", True))

    def test_other_traces_are_read_as_they_are(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / "ns3").mkdir()
            path = directory / "ns3" / "answer_holds.csv"
            path.write_text("time_ns,rank\n1,2\n")
            (directory / "manifest.json").write_text(
                json.dumps({"physical_topology": {"spine_count": 3, "sparse_hold_trace": True}}))
            with traces.open_trace(path) as handle:
                self.assertEqual("".join(handle), "time_ns,rank\n1,2\n")


if __name__ == "__main__":
    unittest.main()
