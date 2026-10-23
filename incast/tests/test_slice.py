"""The cluster's fixed slice command: options, branch resolution, refusals."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "dcs" / "slice.sh"
REPO = "BulkyCI/asteria-ci"
OTHER = "caddyreverse/asteria"
MAIN = "a" * 40
FEATURE = "b" * 40

# ls-remote notes the URL and prints every branch (the script picks its
# own); curl serves raw/<repo>/<commit>/<path>; sbatch keeps what it was
# handed.
GIT = """#!/bin/bash
[[ "$1" == ls-remote && ! -e "$HOME/offline" ]] || exit 128
echo "$2" > "$HOME/ls-remote-url"
cat "$HOME/branches"
"""
CURL = """#!/bin/bash
while (( $# > 1 )); do [[ "$1" == -o ]] && out=$2; shift; done
cp "$HOME/raw/${1#https://raw.githubusercontent.com/}" "$out" 2>/dev/null || exit 22
"""
SBATCH = """#!/bin/bash
[[ -e "$HOME/sbatch-fails" ]] && exit 1
printf '%s\\n' "$@" > "$HOME/sbatch-args"
cp "${@: -2:1}" "$HOME/sbatch-script"
cp "${@: -1}" "$HOME/sbatch-payload"
echo 4242
"""


class SliceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.home = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.home)
        (self.home / "asteria-ci" / "jobs").mkdir(parents=True)
        (self.home / "branches").write_text(
            f"{FEATURE}\trefs/heads/x/refs/heads/main\n{MAIN}\trefs/heads/main\n{FEATURE}\trefs/heads/feat/x\n"
        )
        for repo, sha, path, text in (
            (REPO, MAIN, "ci/dcs/runner-job.sbatch", "main runner\n"),
            (REPO, FEATURE, "ci/dcs/runner-job.sbatch", "feature runner\n"),
            (REPO, FEATURE, "ci/dcs/other.sh", "other script\n"),
            (OTHER, MAIN, "ci/dcs/runner-job.sbatch", "other repository's runner\n"),
        ):
            file = self.home / "raw" / repo / sha / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(text)
        stubs = self.home / "stubs"
        stubs.mkdir()
        for name, body in (("git", GIT), ("curl", CURL), ("sbatch", SBATCH)):
            (stubs / name).write_text(body)
            (stubs / name).chmod(0o755)
        self.path = f"{stubs}:{os.environ['PATH']}"

    def slice(self, command: str, payload: str = "jitconfig") -> subprocess.CompletedProcess:
        env = {"HOME": str(self.home), "PATH": self.path, "SSH_ORIGINAL_COMMAND": command}
        return subprocess.run(
            ["bash", str(SCRIPT)], input=payload, env=env, capture_output=True, text=True
        )

    def submitted(self) -> list[str]:
        return (self.home / "sbatch-args").read_text().splitlines()

    def jobs(self) -> list[str]:
        return sorted(p.name.split(".")[0] for p in (self.home / "asteria-ci" / "jobs").iterdir())

    def test_bare_name_runs_mains_runner_with_its_own_resources(self) -> None:
        result = self.slice("asteria/dcs-1-1-s000")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "4242\n")
        args = self.submitted()
        self.assertEqual(
            args[:3],
            [
                "--parsable",
                "--job-name=asteriadcs-1-1-s000",
                f"--export=ALL,DCS_SLICE_REPO={REPO},DCS_SLICE_BRANCH=main,DCS_SLICE_SHA={MAIN}",
            ],
        )
        self.assertEqual(len(args), 5)
        self.assertEqual((self.home / "ls-remote-url").read_text(), f"https://github.com/{REPO}\n")
        self.assertEqual((self.home / "sbatch-script").read_text(), "main runner\n")
        self.assertEqual((self.home / "sbatch-payload").read_text(), "jitconfig")
        # The payload waits for the job; the script copy is gone.
        self.assertEqual(self.jobs(), ["payload"])

    def test_options_become_sbatch_options(self) -> None:
        result = self.slice(
            "asteria/dcs-1-1-s000 branch=feat/x script=ci/dcs/other.sh cpus=64 mem=256G"
            " time=4-20:00:00 nodes=1 ntasks=2 constraint=EPYC_9634|AMD_Epyc9754"
            " partition=p1 qos=normal gres=gpu:a100:2"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.submitted()
        self.assertEqual(
            sorted(args[2:-3]),
            sorted(
                [
                    "--cpus-per-task=64",
                    "--mem=262144M",
                    "--time=4-20:00:00",
                    "--nodes=1",
                    "--ntasks=2",
                    "--constraint=EPYC_9634|AMD_Epyc9754",
                    "--partition=p1",
                    "--qos=normal",
                    "--gres=gpu:a100:2",
                ]
            ),
        )
        self.assertIn(f"DCS_SLICE_BRANCH=feat/x,DCS_SLICE_SHA={FEATURE}", args[-3])
        self.assertEqual((self.home / "sbatch-script").read_text(), "other script\n")
        self.assertIn(f"ci/dcs/other.sh at {REPO} feat/x {FEATURE}", result.stderr)

    def test_the_other_repository(self) -> None:
        result = self.slice(f"name=n repo={OTHER}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.home / "ls-remote-url").read_text(), f"https://github.com/{OTHER}\n")
        self.assertIn(f"DCS_SLICE_REPO={OTHER},", self.submitted()[2])
        self.assertEqual((self.home / "sbatch-script").read_text(), "other repository's runner\n")

    def test_memory_units(self) -> None:
        for mem, expected in (("4096", "4096M"), ("512M", "512M"), ("2048G", "2097152M"), ("064G", "65536M")):
            with self.subTest(mem=mem):
                result = self.slice(f"name=n mem={mem}")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"--mem={expected}", self.submitted())

    def test_test_only_submits_nothing_and_keeps_no_payload(self) -> None:
        result = self.slice("name=probe cpus=64 mem=256G test=1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--test-only", self.submitted())
        self.assertEqual(self.jobs(), [])

    def test_refusals_submit_nothing_and_leave_nothing(self) -> None:
        for command, reason in (
            ("cpus=0", "cpus 0 outside"),
            ("cpus=257", "cpus 257 outside"),
            ("cpus=1x", "cpus 1x outside"),
            ("mem=2049G", "mem 2049G outside"),
            ("mem=1T", "bad mem"),
            ("time=abc", "bad time"),
            ("nodes=9", "nodes 9 outside"),
            ("ntasks=0", "ntasks 0 outside"),
            ("constraint=a;b", "bad constraint"),
            ("gres=$(id)", "bad gres"),
            ("output=/tmp/x", "unknown option output"),
            ("repo=someone/asteria-ci", "repo someone/asteria-ci is not one of"),
            ("test=2", "test takes 1"),
            ("branch=../x", "bad branch"),
            ("branch=-x", "bad branch"),
            ("script=/etc/passwd", "bad script"),
            ("script=ci/../x", "bad script"),
            ("cpus=2 cpus=4", "cpus given twice"),
            ("one two", "name given twice"),
            ("branch=nope", "no branch nope"),
            ("branch=x", "no branch x"),
            ("script=ci/dcs/missing.sh", "no ci/dcs/missing.sh"),
        ):
            with self.subTest(command=command):
                result = self.slice(command)
                self.assertEqual(result.returncode, 1)
                self.assertIn(reason, result.stderr)
                self.assertFalse((self.home / "sbatch-args").exists())
                self.assertEqual(self.jobs(), [])

    def test_unreachable_repository_is_refused(self) -> None:
        (self.home / "offline").touch()
        result = self.slice("name=n")
        self.assertEqual(result.returncode, 1)
        self.assertIn(f"cannot reach https://github.com/{REPO}", result.stderr)
        self.assertEqual(self.jobs(), [])

    def test_failed_submission_keeps_no_payload(self) -> None:
        (self.home / "sbatch-fails").touch()
        result = self.slice("name=n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.jobs(), [])


if __name__ == "__main__":
    unittest.main()
