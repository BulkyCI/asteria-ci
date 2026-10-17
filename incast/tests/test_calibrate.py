"""Threshold calibration on synthetic healthy reports."""

import csv
import json
import random
import shutil
import statistics
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import analyze  # noqa: E402


def write_reports(run: Path, reports: int, mark_excess: float, trim_excess: float) -> None:
    """Reports of eight spines in which spine 3 marks and trims above the
    others, by mark_excess and trim_excess, and every other spine sits at
    the median."""
    (run / "ns3").mkdir(parents=True)
    with (run / "ns3" / "spine_reports.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["time_ns", "rank", "sequence", "spine", "mark_fraction", "trim_fraction", "grade"])
        for report in range(reports):
            for spine in range(8):
                hot = spine == 3
                writer.writerow([report * 1000, 0, report, spine,
                                 0.1 + (mark_excess if hot else 0), 0.01 + (trim_excess if hot else 0), 3])


class Calibrate(unittest.TestCase):
    def test_trim_thresholds_apart_from_mark_thresholds(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "run"
            # One spine in eight deviates, so the 99th percentile of the
            # deviations is that spine's excess.
            write_reports(run, 100, 0.5, 0.05)
            result = analyze.thresholds([run], ("mark_fraction", "trim_fraction"))
        self.assertTrue(result["capped"])
        self.assertEqual(result["congestion_thresholds"], [0.25, 0.5, 1.0])
        self.assertFalse(result["trim_capped"])
        self.assertEqual(result["trim_thresholds"], [0.05, 0.1, 0.2])

    def test_trims_do_not_raise_the_mark_thresholds(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "run"
            write_reports(run, 100, 0.02, 0.05)
            result = analyze.thresholds([run], ("mark_fraction", "trim_fraction"))
        self.assertEqual(result["congestion_thresholds"], [0.02, 0.04, 0.08])
        self.assertEqual(result["trim_thresholds"], [0.05, 0.1, 0.2])

    def test_scaling_and_cap(self):
        self.assertEqual(analyze.scaled_thresholds(0.3), (True, [0.25, 0.5, 1.0]))
        self.assertEqual(analyze.scaled_thresholds(0.1), (False, [0.1, 0.2, 0.4]))


def in_memory_thresholds(runs, metrics):
    """thresholds() as it read the runs before report_deviations: every
    report grouped whole and every deviation listed."""
    deviations, count = {m: [] for m in metrics}, 0
    for run in runs:
        for rows in analyze.reports(run).values():
            count += 1
            for metric in metrics:
                values = [float(r[metric]) for r in rows]
                median = statistics.median(values)
                deviations[metric].extend(max(0.0, v - median) for v in values)
    p99 = {m: analyze.percentile(v, 0.99) for m, v in deviations.items()}
    capped, congestion = analyze.scaled_thresholds(max(v for m, v in p99.items() if m != "trim_fraction"))
    result = {"reports": count, "p99_deviation": p99, "capped": capped, "congestion_thresholds": congestion}
    if "trim_fraction" in p99:
        result["trim_capped"], result["trim_thresholds"] = analyze.scaled_thresholds(p99["trim_fraction"])
    return result


def shuffled(run: Path, into: Path) -> Path:
    """run's report rows in another order, so its reports interleave."""
    with (run / "ns3" / "spine_reports.csv").open() as handle:
        header, *rows = handle.read().splitlines()
    random.Random(7).shuffle(rows)
    (into / "ns3").mkdir(parents=True)
    (into / "ns3" / "spine_reports.csv").write_text("\n".join([header, *rows]) + "\n")
    return into


class Streamed(unittest.TestCase):
    """thresholds() reads the reports one at a time (report_deviations) and
    gives what the whole-run grouping gave, to the byte."""

    def assertSame(self, runs, metrics):
        self.assertEqual(json.dumps(analyze.thresholds(runs, metrics)), json.dumps(in_memory_thresholds(runs, metrics)))

    def test_real_slice(self):
        # Six consecutive reports, of 128 spines each, of
        # a2a_wide128_calibration_4k-ep64-256k-healthy-graded_per_receiver_mix-s102.
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "run"
            (run / "ns3").mkdir(parents=True)
            shutil.copy(Path(__file__).parent / "data" / "spine_reports_wide128.csv", run / "ns3" / "spine_reports.csv")
            mixed = shuffled(run, Path(directory) / "mixed")
            with self.assertRaises(analyze.InterleavedReports):
                list(analyze.consecutive_reports(mixed))
            result = analyze.thresholds([run], ("mark_fraction", "trim_fraction"))
            self.assertEqual(result["reports"], 6)
            self.assertTrue(all(v > 0 for v in result["p99_deviation"].values()))
            for metrics in (("mark_fraction", "trim_fraction"), ("mark_fraction",)):
                self.assertSame([run], metrics)
                self.assertSame([mixed], metrics)
                self.assertSame([run, mixed], metrics)

    def test_synthetic_runs(self):
        # Three runs of six-place fractions, the third per_connection (a
        # report per sender and port too) with its reports interleaved.
        generator = random.Random(11)
        with tempfile.TemporaryDirectory() as directory:
            runs = []
            for index, spines in enumerate((8, 32, 16)):
                run = Path(directory) / f"run{index}"
                (run / "ns3").mkdir(parents=True)
                rows = []
                for report in range(300):
                    time_ns = report * 500 + generator.randrange(3)
                    senders = ((report % 3, 4000 + report % 2),) if index == 2 else ((None, None),)
                    for sender, port in senders:
                        for spine in range(spines):
                            hot = generator.random() < 0.05
                            row = [time_ns, report % 7, report, spine,
                                   f"{min(1.0, generator.random() * (0.6 if hot else 0.05)):.6f}",
                                   f"{generator.choice((0.0, 0.0, 0.0, 1 / 64, 1 / 8)) if hot else 0.0:.6f}",
                                   f"{generator.random() * 0.01:.6f}"]
                            rows.append(row + ([sender, port] if index == 2 else []))
                if index == 2:
                    generator.shuffle(rows)
                with (run / "ns3" / "spine_reports.csv").open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["time_ns", "rank", "sequence", "spine", "mark_fraction", "trim_fraction",
                                     "loss_fraction"] + (["sender", "sender_port"] if index == 2 else []))
                    writer.writerows(rows)
                runs.append(run)
            with self.assertRaises(analyze.InterleavedReports):
                list(analyze.consecutive_reports(runs[2]))
            self.assertEqual(len(list(analyze.consecutive_reports(runs[1]))), 300)
            for metrics in (("mark_fraction", "trim_fraction"), ("mark_fraction", "loss_fraction")):
                self.assertSame(runs, metrics)
                self.assertSame(runs[:2], metrics)
                for run in runs:
                    self.assertSame([run], metrics)

    def test_no_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "run"
            (run / "ns3").mkdir(parents=True)
            (run / "ns3" / "spine_reports.csv").write_text("time_ns,rank,sequence,spine,mark_fraction,trim_fraction\n")
            self.assertSame([run], ("mark_fraction", "trim_fraction"))


if __name__ == "__main__":
    unittest.main()
