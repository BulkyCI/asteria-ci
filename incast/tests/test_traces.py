"""The sparse sender loss trace, rebuilt dense."""

import json
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


class SparseTraceTest(unittest.TestCase):
    def _run(self, directory: Path, trace: str, sparse: bool) -> Path:
        (directory / "ns3").mkdir(parents=True)
        path = directory / "ns3" / "spine_losses.csv"
        path.write_text(trace)
        topology = {"spine_count": 3, **({"sparse_hold_trace": True} if sparse else {})}
        (directory / "manifest.json").write_text(json.dumps({"physical_topology": topology}))
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

    def test_other_traces_are_read_as_they_are(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / "ns3").mkdir()
            path = directory / "ns3" / "receiver_holds.csv"
            path.write_text("time_ns,rank\n1,2\n")
            (directory / "manifest.json").write_text(
                json.dumps({"physical_topology": {"spine_count": 3, "sparse_hold_trace": True}}))
            with traces.open_trace(path) as handle:
                self.assertEqual("".join(handle), "time_ns,rank\n1,2\n")


if __name__ == "__main__":
    unittest.main()
