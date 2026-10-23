"""The cluster's fixed slice command: the words it reads, the job it hands
sbatch, what it leaves behind."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "dcs" / "slice.sh"
REPO = "BulkyCI/asteria-ci"
OTHER = "BulkyCI/other-tool"
COMMIT = "a" * 40

# curl serves $FX/raw/<repo>/<ref>/<path> and, like any careless reader,
# swallows whatever stdin it is given. sbatch keeps its arguments, the
# script it reads from stdin, the payload its job would read, and where it
# ran. Outputs go to $FX/out, never under HOME.
CURL = r"""#!/bin/bash
cat > /dev/null
[[ -e "$FX/offline" ]] && exit 6
url=${!#}
cat "$FX/raw/${url#https://raw.githubusercontent.com/}" 2>/dev/null || exit 22
"""
SBATCH = r"""#!/bin/bash
[[ -e "$FX/sbatch-fails" ]] && exit 1
printf '%s\n' "$@" > "$FX/out/args"
cat > "$FX/out/script"
env | grep '^DCS_SLICE_' | sort > "$FX/out/env"
cp "$DCS_SLICE_PAYLOAD" "$FX/out/payload"
pwd > "$FX/out/cwd"
echo 4242
"""


class SliceTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        self.home, self.fx, stubs = root / "home", root / "fx", root / "stubs"
        for path in (self.home, self.fx / "out", stubs):
            path.mkdir(parents=True)
        for repo, ref, path, text in (
            (REPO, "main", "ci/dcs/runner-job.sbatch", "main runner\n"),
            (REPO, COMMIT, "ci/dcs/runner-job.sbatch", "pinned runner\n"),
            (REPO, COMMIT, "ci/dcs/other.sh", "other script\n"),
            (OTHER, "main", "ci/dcs/runner-job.sbatch", "other tool's runner\n"),
        ):
            file = self.fx / "raw" / repo / ref / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(text)
        for name, body in (("curl", CURL), ("sbatch", SBATCH)):
            (stubs / name).write_text(body)
            (stubs / name).chmod(0o755)
        self.path = f"{stubs}:{os.environ['PATH']}"
        # A file the word --constraint=* would match were the words globbed.
        (root / "--constraint=globbed").touch()
        self.cwd = root

    def slice(self, command: str, stdin: str = "jitconfig") -> subprocess.CompletedProcess:
        env = {"HOME": str(self.home), "PATH": self.path, "FX": str(self.fx), "SSH_ORIGINAL_COMMAND": command}
        return subprocess.run(
            ["bash", str(SCRIPT)], input=stdin, env=env, cwd=self.cwd, capture_output=True, text=True
        )

    def out(self, name: str) -> str:
        return (self.fx / "out" / name).read_text()

    def left(self) -> list[str]:
        """What stays under ~/slices, by repository and kind."""
        slices = self.home / "slices"
        return sorted(f"{p.parent.name}/{p.name.split('.')[0]}" for p in slices.rglob("*") if p.is_file())

    def test_defaults_run_mains_runner_at_its_own_resources(self) -> None:
        result = self.slice(f"repo={REPO}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "4242\n")
        directory = self.home / "slices" / "asteria-ci"
        # No --export: a variable list there makes SLURM load the login
        # environment on the node; the job's variables ride the default ALL.
        self.assertEqual(self.out("args").splitlines(), ["--parsable"])
        self.assertRegex(
            self.out("env"),
            rf"^DCS_SLICE_PAYLOAD={directory}/payload\.\w+\nDCS_SLICE_REF=main\nDCS_SLICE_REPO={REPO}\n$",
        )
        self.assertEqual(self.out("script"), "main runner\n")
        # The payload is stdin, whole, although curl tried to read stdin too.
        self.assertEqual(self.out("payload"), "jitconfig")
        self.assertEqual(self.out("cwd").strip(), str(directory))
        self.assertEqual(self.left(), ["asteria-ci/payload"])

    def test_sbatch_options_pass_through_in_order(self) -> None:
        words = "--job-name=dcs-1-1-s000 --cpus-per-task=64 --mem=256G --constraint=EPYC_9634|AMD_Epyc9754 --exclusive --constraint=*"
        result = self.slice(f"repo={REPO} ref={COMMIT} script=ci/dcs/other.sh {words}")
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.out("args").splitlines()
        self.assertEqual(args[1:], words.split())
        self.assertIn(f"DCS_SLICE_REF={COMMIT}\n", self.out("env"))
        self.assertEqual(self.out("script"), "other script\n")
        self.assertIn(f"ci/dcs/other.sh at {REPO} {COMMIT}", result.stderr)

    def test_any_repository_of_the_organization(self) -> None:
        result = self.slice(f"repo={OTHER}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.out("script"), "other tool's runner\n")
        self.assertEqual(self.left(), ["other-tool/payload"])

    def test_test_only_keeps_no_payload(self) -> None:
        result = self.slice(f"repo={REPO} --cpus-per-task=64 --mem=256G --test-only")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--test-only", self.out("args").splitlines())
        self.assertEqual(self.left(), [])

    def test_refusals_submit_nothing_and_leave_nothing(self) -> None:
        for command, reason in (
            ("", "repo must be BulkyCI/<name>"),
            ("--mem=1G", "repo must be BulkyCI/<name>"),
            ("repo=someone/asteria-ci", "repo must be BulkyCI/<name>"),
            ("repo=BulkyCI/a/b", "repo must be BulkyCI/<name>"),
            ("repo=BulkyCI/..", "bad name BulkyCI/.."),
            (f"repo={REPO} ref=../x", "bad name ../x"),
            (f"repo={REPO} ref=-x", "bad name -x"),
            (f"repo={REPO} script=/etc/passwd", "bad name /etc/passwd"),
            (f"repo={REPO} script=ci/../x", "bad name ci/../x"),
            (f"repo={REPO} asteria/dcs-1", "unknown word asteria/dcs-1"),
            (f"repo={REPO} cpus=4", "unknown word cpus=4"),
            (f"repo={REPO} ref=nope", "cannot fetch"),
            (f"repo={REPO} script=ci/dcs/missing.sh", "cannot fetch"),
            ("repo=BulkyCI/absent", "cannot fetch"),
        ):
            with self.subTest(command=command):
                result = self.slice(command)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(reason, result.stderr)
                self.assertFalse((self.fx / "out" / "args").exists())
                self.assertFalse((self.home / "slices").exists())

    def test_unreachable_github_is_refused(self) -> None:
        (self.fx / "offline").touch()
        result = self.slice(f"repo={REPO}")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot fetch", result.stderr)
        self.assertFalse((self.home / "slices").exists())

    def test_failed_submission_keeps_no_payload(self) -> None:
        (self.fx / "sbatch-fails").touch()
        result = self.slice(f"repo={REPO}")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.left(), [])


class ProvisionActionTests(unittest.TestCase):
    ACTION = Path(__file__).resolve().parents[2] / ".github" / "actions" / "dcs-provision" / "action.yml"

    def test_no_variable_the_runner_sets_over_a_steps_own(self) -> None:
        # The runner sets RUNNER_* and GITHUB_* itself, over a step's env, so
        # a step variable of that name silently holds the runner's value.
        keys = [line.split(":")[0].strip() for line in self.ACTION.read_text().splitlines() if line.startswith("      ") and ":" in line]
        self.assertFalse([k for k in keys if k.startswith(("RUNNER_", "GITHUB_"))])

    def test_command_word_is_the_slice_commands(self) -> None:
        text = self.ACTION.read_text()
        self.assertIn('"repo=$GITHUB_REPOSITORY ref=$GITHUB_SHA --job-name=$NAME${SLICE:+ $SLICE}"', text)


if __name__ == "__main__":
    unittest.main()
