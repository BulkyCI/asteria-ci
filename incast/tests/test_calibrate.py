"""Threshold calibration on synthetic healthy reports."""

import csv
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


if __name__ == "__main__":
    unittest.main()
