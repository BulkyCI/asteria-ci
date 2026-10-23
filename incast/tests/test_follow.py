"""The arrival windows counted while the simulator writes its event
segments, against the run's whole stream read afterwards."""

import io
import json
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

TESTS = Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS.parent))

import summarize  # noqa: E402

try:
    import zstandard
except ImportError:
    zstandard = None

HEADER = ("time_ns,event,plane,protocol,node,node_type,interface,source_host,"
          "destination_host,source_port,sequence,packet_bytes,queue\n")
# Two leaves of four hosts under two spines: hosts 0 to 7, leaves 8 and 9,
# spines 10 and 11, a leaf's spine ports 5 and 6; leaf 1 receives.
PROFILE = {
    "network": {"hosts_per_leaf": 4, "spine_count": 2},
    "parallelism": {"dp": 8},
    "workload": {"kind": "incast", "receiver_leaf": 1, "fan_in": 4},
}
ROWS_PER_SEGMENT = 2000


def rows(count: int, seed: int = 1) -> list[str]:
    """Event rows over about 25 windows: data arrivals at the receiving
    leaf (on spine ports and others), at the other leaf and at the spines
    for every host, among other events."""
    draw = random.Random(seed)
    return [
        f"{i * 50 + draw.randrange(50)},{draw.choice(('data_arrival', 'data_arrival', 'data_deliver'))},"
        f"data,rdma,{draw.choice((8, 9, 9, 10, 11))},1,{draw.randrange(1, 8)},{draw.randrange(8)},"
        f"{draw.randrange(8)},{draw.randrange(1000)},{i},{draw.choice((64, 1000, 4160))},0\n"
        for i in range(count)
    ]


def segment_texts(count: int = 12_000, seed: int = 1) -> list[str]:
    """The text of each segment, rotated at a row boundary; the header
    opens the first."""
    events = rows(count, seed)
    texts = ["".join(events[start:start + ROWS_PER_SEGMENT]) for start in range(0, count, ROWS_PER_SEGMENT)]
    texts[0] = HEADER + texts[0]
    return texts


def cut_row(text: str) -> int:
    """Where to cut text: in the last field of an arrival at the receiving
    leaf from spine 0 past its middle, so that the row, cut, would still
    parse."""
    arrival = text.index(",data_arrival,data,rdma,9,1,5,", len(text) // 2)
    return text.index("\n", arrival) - 1


def frames(texts: list[str], seed: int = 2, cut: bool = False) -> tuple[list[list[bytes]], str]:
    """What the writer lands of each segment, in order: the text in pieces
    cut anywhere in a row, each flushed as a block and landed in pieces cut
    anywhere in it, the frame ended at the segment's end. With cut, the
    last segment stops after a row cut short, with half of a block more
    and no frame end, as a killed simulator leaves it. Also the text the
    landed bytes decode to."""
    draw = random.Random(seed)
    segments = []
    decodable = ""
    for index, text in enumerate(texts):
        last = cut and index == len(texts) - 1
        stop = cut_row(text) if last else len(text)
        compressor = zstandard.ZstdCompressor().compressobj()
        pieces = []
        position = 0
        while position < stop:
            end = min(stop, position + draw.randrange(1, 3000))
            block = compressor.compress(text[position:end].encode()) + compressor.flush(
                zstandard.COMPRESSOBJ_FLUSH_BLOCK)
            while block:
                size = draw.randrange(1, 1000)
                pieces.append(block[:size])
                block = block[size:]
            position = end
        decodable += text[:stop]
        if last:
            block = compressor.compress(text[stop:].encode()) + compressor.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK)
            pieces.append(block[:len(block) // 2])
        else:
            pieces.append(compressor.flush())
        segments.append(pieces)
    return segments, decodable


def one_stream(texts: list[str], segment_bytes: int = 20_000) -> list[list[bytes]]:
    """The segments of the stream as one zstd frame cut at byte
    boundaries."""
    data = zstandard.ZstdCompressor().compress("".join(texts).encode())
    return [[data[start:start + segment_bytes]] for start in range(0, len(data), segment_bytes)]


def play(directories: list[Path], segments: list[list[bytes]], pace: float = 0.0,
         before_rotate=None) -> None:
    """Write the segments into each directory's ns3 as the simulator does,
    each piece landing pace seconds after the last; before_rotate(index)
    runs before segment index (above 0) is opened."""
    for directory in directories:
        (directory / "ns3").mkdir(parents=True, exist_ok=True)
    for index, pieces in enumerate(segments):
        if index and before_rotate is not None:
            before_rotate(index)
        handles = [summarize.segment(directory / "ns3", index).open("wb") for directory in directories]
        for piece in pieces:
            for handle in handles:
                handle.write(piece)
                handle.flush()
            time.sleep(pace)
        for handle in handles:
            handle.close()


def finish(run: Path) -> None:
    """What the simulator writes at its end besides the segments."""
    (run / "ns3").mkdir(parents=True, exist_ok=True)
    (run / "ns3" / "fct.txt").write_text("")
    (run / "ns3" / "transport_summary.csv").write_text("event,plane,event_count,total_bytes\n")


def main(*arguments) -> int:
    with mock.patch.object(sys, "argv", ["summarize.py", *map(str, arguments)]):
        return summarize.main()


def outputs(run: Path) -> dict[str, bytes]:
    return {name: (run / name).read_bytes() for name in ("spine_windows.csv", "offered_windows.csv")}


def layout() -> dict:
    return summarize.window_layout(PROFILE)[1]


def counted(text: str):
    """count_arrivals' counts of text as one whole stream."""
    counts = ({}, {})
    summarize.count_arrivals(io.BytesIO(zstandard.ZstdCompressor().compress(text.encode())), counts, **layout())
    return counts


def remaining(run: Path) -> list[str]:
    return sorted(path.name for path in (run / "ns3").glob(summarize.SEGMENTS))


def names(indices) -> list[str]:
    return [summarize.segment(Path(), index).name for index in indices]


@unittest.skipIf(zstandard is None, "no zstandard")
class Following(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.profile = self.root / "profile.json"
        self.profile.write_text(json.dumps(PROFILE))
        self.done = self.root / "simulated"
        interval = mock.patch.object(summarize, "FOLLOW_INTERVAL_S", 0.002)
        interval.start()
        self.addCleanup(interval.stop)

    def start(self, target):
        """target run on a thread, and the errors it raised."""
        errors = []

        def run():
            try:
                target()
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread, errors

    def joined(self, thread, errors):
        thread.join(30)
        self.assertFalse(thread.is_alive(), "the follower did not finish")
        self.assertEqual(errors, [])

    def test_a_followed_run_summarizes_as_its_whole_stream(self):
        followed, whole = self.root / "followed", self.root / "whole"
        segments, _ = frames(segment_texts())
        self.assertGreater(len(segments), 4)
        # The follower starts before run.py has made the run's directory.
        with mock.patch.object(sys, "argv", ["summarize.py", str(followed), "--profile", str(self.profile),
                                             "--follow", str(self.done)]):
            thread, errors = self.start(summarize.main)
            time.sleep(0.02)
            kept = []

            def before_rotate(index):
                # Opening segment index seals index - 1. Every segment
                # before that one was sealed and written whole earlier, and
                # is to be gone while the simulator still writes.
                deadline = time.monotonic() + 10
                while len(remaining(followed)) > 1 and time.monotonic() < deadline:
                    time.sleep(0.002)
                kept.append(remaining(followed))

            play([followed, whole], segments, pace=0.0002, before_rotate=before_rotate)
            self.done.touch()
            self.joined(thread, errors)
        self.assertEqual(kept, [names([index]) for index in range(len(segments) - 1)])
        self.assertEqual(remaining(followed), [])
        self.assertEqual(remaining(whole), names(range(len(segments))))
        saved, next_segment = summarize.load_counts(followed / "ns3" / summarize.COUNTS)
        self.assertEqual(next_segment, len(segments))
        self.assertEqual(saved, summarize.windows(whole / "ns3", layout()))
        for run in (followed, whole):
            finish(run)
            main(run, "--profile", self.profile)
        self.assertEqual(outputs(followed), outputs(whole))
        self.assertGreater(json.loads((followed / "summary.json").read_text())["leaf_arrival_packets"], 0)
        self.assertEqual(sorted(path.name for path in (followed / "ns3").iterdir()),
                         ["fct.txt", "transport_summary.csv"])

    def test_a_last_frame_cut_short_is_counted_to_its_last_whole_row(self):
        followed, whole = self.root / "followed", self.root / "whole"
        segments, decodable = frames(segment_texts(), cut=True)
        self.assertFalse(decodable.endswith("\n"))
        thread, errors = self.start(lambda: summarize.follow(followed / "ns3", self.done, layout()))
        play([followed, whole], segments, pace=0.0002)
        self.done.touch()
        self.joined(thread, errors)
        expected = counted(decodable[:decodable.rindex("\n") + 1])
        # Counted, the cut row would add a packet.
        self.assertNotEqual(counted(decodable + "\n"), expected)
        self.assertEqual(summarize.windows(followed / "ns3", layout()), expected)
        self.assertEqual(summarize.windows(whole / "ns3", layout()), expected)
        for run in (followed, whole):
            finish(run)
            main(run, "--profile", self.profile)
        self.assertEqual(outputs(followed), outputs(whole))

    def test_a_simulator_that_wrote_no_rows(self):
        # It died before its first segment, or with the first empty.
        for segments in ([], [[]]):
            with self.subTest(segments=len(segments)):
                run = self.root / f"run{len(segments)}"
                self.done.unlink(missing_ok=True)
                thread, errors = self.start(lambda: summarize.follow(run / "ns3", self.done, layout()))
                time.sleep(0.02)
                if segments:
                    play([run], segments)
                self.done.touch()
                self.joined(thread, errors)
                self.assertEqual(remaining(run), [])
                finish(run)
                main(run, "--profile", self.profile)
                self.assertEqual((run / "spine_windows.csv").read_text(), "window,start_ns,spine,packets,bytes\n")

    def test_a_stopped_follower_leaves_the_rest_to_the_summary(self):
        segments, _ = frames(segment_texts())
        whole = self.root / "whole"
        play([whole], segments)
        finish(whole)
        main(whole, "--profile", self.profile)
        count_arrivals, save_counts = summarize.count_arrivals, summarize.save_counts
        calls = []

        def reading_segment_2(*arguments, **keywords):
            calls.append(1)
            if len(calls) == 3:
                raise OSError("stopped")
            return count_arrivals(*arguments, **keywords)

        def saving_segment_2(*arguments, **keywords):
            save_counts(*arguments, **keywords)
            calls.append(1)
            if len(calls) == 3:
                raise OSError("stopped")

        # Stopped reading segment 2, it has saved through segment 1; stopped
        # between saving through segment 2 and deleting it, it keeps it.
        for name, stop, kept, next_segment in (("count_arrivals", reading_segment_2, 2, 2),
                                               ("save_counts", saving_segment_2, 2, 3)):
            with self.subTest(stopped_in=name):
                run = self.root / name
                calls.clear()
                play([run], segments)
                self.done.touch()
                with mock.patch.object(summarize, name, stop), self.assertRaises(OSError):
                    summarize.follow(run / "ns3", self.done, layout())
                self.assertEqual(remaining(run), names(range(kept, len(segments))))
                self.assertEqual(summarize.load_counts(run / "ns3" / summarize.COUNTS)[1], next_segment)
                finish(run)
                main(run, "--profile", self.profile)
                self.assertEqual(outputs(run), outputs(whole))
                self.assertEqual(remaining(run), [])
                self.assertFalse((run / "ns3" / summarize.COUNTS).exists())

    def test_a_stream_cut_within_a_frame_is_read_whole(self):
        run, whole = self.root / "cut", self.root / "whole"
        segments = one_stream(segment_texts())
        self.assertGreater(len(segments), 2)
        play([run, whole], segments)
        self.done.touch()
        with self.assertRaises(ValueError):
            summarize.follow(run / "ns3", self.done, layout())
        # Nothing is deleted or saved: the summary reads the stream whole.
        self.assertEqual(len(remaining(run)), len(segments))
        self.assertFalse((run / "ns3" / summarize.COUNTS).exists())
        self.assertEqual(summarize.windows(run / "ns3", layout()), counted("".join(segment_texts())))
        for directory in (run, whole):
            finish(directory)
            main(directory, "--profile", self.profile)
        self.assertEqual(outputs(run), outputs(whole))


class Reading(unittest.TestCase):
    def test_bytes_landed_as_the_segment_is_sealed_are_read(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "transport_events.csv.zst.000"
            path.write_bytes(b"first")

            class Sealing:
                """The next segment, which the writer opens just after the
                last bytes of this one land, as the reader looks for it."""

                landed = False

                def exists(self):
                    if not self.landed:
                        with path.open("ab") as handle:
                            handle.write(b", last")
                        self.landed = True
                    return True

            with summarize.Following(path, Sealing(), Path(root) / "simulated") as raw:
                self.assertEqual(raw.read(), b"first, last")


# run.py of an asteria checkout as run_one.sh calls it: after a while it
# replaces the output directory, writes the stream FAKE_STREAM names into
# it and, for frames, records in FAKE_RECORD the segments left once the
# follower has caught up, before the simulator's other outputs.
FAKE_RUN = """
import argparse, json, os, shutil, sys, time
from pathlib import Path

sys.path.insert(0, os.environ["FAKE_TESTS"])
import test_follow

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path)
parser.add_argument("--clean", action="store_true")
arguments, _ = parser.parse_known_args()
time.sleep(0.3)
if arguments.clean and arguments.output.exists():
    shutil.rmtree(arguments.output)
(arguments.output / "ns3").mkdir(parents=True)
(arguments.output / "manifest.json").write_text(json.dumps({"physical_topology": {}}))
texts = test_follow.segment_texts(6000)
if os.environ["FAKE_STREAM"] == "frames":
    test_follow.play([arguments.output], test_follow.frames(texts)[0], pace=0.0002)
    deadline = time.monotonic() + 10
    while len(test_follow.remaining(arguments.output)) > 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    Path(os.environ["FAKE_RECORD"]).write_text(json.dumps(test_follow.remaining(arguments.output)))
else:
    test_follow.play([arguments.output], test_follow.one_stream(texts))
test_follow.finish(arguments.output)
"""


@unittest.skipIf(zstandard is None, "no zstandard")
class RunOne(unittest.TestCase):
    def test_a_run_is_followed_while_it_simulates(self):
        for stream in ("frames", "one_stream"):
            with self.subTest(stream=stream), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "asteria" / "experiments" / "ring_3d").mkdir(parents=True)
                (root / "asteria" / "experiments" / "ring_3d" / "run.py").write_text(FAKE_RUN)
                profiles = root / "profiles"
                profiles.mkdir()
                (profiles / "runs.json").write_text(json.dumps([{"key": "k", "seed": 1}]))
                (profiles / "k.json").write_text(json.dumps(PROFILE))
                # An interrupted attempt's segments, its last unsealed, and
                # its marker: neither is this run's.
                results = root / "results"
                stale, _ = frames(segment_texts(4000, seed=7))
                play([results / "k"], stale)
                (results / "k.simulated").touch()
                completed = subprocess.run(
                    ["bash", str(TESTS.parent / "run_one.sh"), str(profiles), "k", str(results)],
                    env={**os.environ, "ASTERIA": str(root / "asteria"), "PYTHON": sys.executable,
                         "FAKE_TESTS": str(TESTS), "FAKE_STREAM": stream, "FAKE_RECORD": str(root / "kept.json")},
                    capture_output=True, text=True, timeout=120)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertIn("k: simulator status 0,", completed.stdout)
                reference = root / "reference"
                texts = segment_texts(6000)
                play([reference], frames(texts)[0] if stream == "frames" else one_stream(texts))
                finish(reference)
                main(reference, "--profile", profiles / "k.json")
                self.assertEqual(outputs(results / "k"), outputs(reference))
                self.assertEqual(remaining(results / "k"), [])
                self.assertFalse((results / "k" / "ns3" / summarize.COUNTS).exists())
                self.assertFalse((results / "k.simulated").exists())
                if stream == "frames":
                    # The sealed segments went while the simulator ran.
                    self.assertEqual(json.loads((root / "kept.json").read_text()), names([2]))
                    self.assertNotIn("follower status", completed.stdout)
                else:
                    # Cut within a frame, the stream stops the follower at
                    # its first segment and is read whole.
                    self.assertIn("k: follower status 1", completed.stdout)


class Layout(unittest.TestCase):
    def test_the_receivers_of_each_workload(self):
        leaves, parameters = summarize.window_layout(PROFILE)
        self.assertEqual(leaves, [1])
        self.assertEqual(parameters, {"leaf_nodes": {9: 1}, "first_spine_port": 5, "first_spine_node": 10,
                                      "spines": 2, "receivers": {4, 5, 6, 7}, "hosts_per_leaf": 4})
        for kind in ("all_to_all", "sequential_dp_all_reduce"):
            leaves, parameters = summarize.window_layout({**PROFILE, "workload": {"kind": kind}})
            self.assertEqual(leaves, [0])
            self.assertEqual((parameters["leaf_nodes"], parameters["receivers"]), ({8: 0}, {0, 1, 2, 3}))


if __name__ == "__main__":
    unittest.main()
