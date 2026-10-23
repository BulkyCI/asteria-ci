"""The cluster's fixed slice command: options, branch resolution, private
repositories, refusals."""

import base64
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "ci" / "dcs" / "slice.sh"
REPO = "BulkyCI/asteria-ci"
OTHER = "BulkyCI/other-tool"
PRIVATE = "BulkyCI/private-tool"
MAIN = "a" * 40
FEATURE = "b" * 40
TOKEN = "ghs_" + "t" * 36

# curl serves $FX/refs/<repo> and $FX/raw/<repo>/<commit>/<path>; for a
# repository listed under $FX/private it wants the token's header, read
# from stdin (-H @-). sbatch keeps what it was handed. Outputs go to
# $FX/out, never under HOME.
CURL = r"""#!/bin/bash
headers=""
while (( $# > 1 )); do
    case "$1" in
        -o) out=$2; shift ;;
        -H) [[ "$2" == @- ]] && headers=$(cat); shift ;;
    esac
    shift
done
url=$1
[[ -e "$FX/offline" ]] && exit 6
case "$url" in
    https://github.com/*.git/info/refs\?service=git-upload-pack)
        repo=${url#https://github.com/}; repo=${repo%.git/info/refs*}
        src="$FX/refs/$repo"; want="$FX/want-basic" ;;
    https://raw.githubusercontent.com/*)
        rest=${url#https://raw.githubusercontent.com/}
        repo=$(cut -d/ -f1-2 <<< "$rest")
        src="$FX/raw/$rest"; want="$FX/want-token" ;;
    *) exit 3 ;;
esac
if [[ -e "$FX/private/$repo" ]]; then
    [[ "$headers" == "$(cat "$want")" ]] || exit 22
    echo "$repo authorized" >> "$FX/out/auth"
fi
cp "$src" "$out" 2>/dev/null || exit 22
"""
SBATCH = r"""#!/bin/bash
[[ -e "$FX/sbatch-fails" ]] && exit 1
printf '%s\n' "$@" > "$FX/out/sbatch-args"
cp "${@: -2:1}" "$FX/out/sbatch-script"
cp "${@: -1}" "$FX/out/sbatch-payload"
pwd > "$FX/out/sbatch-cwd"
echo 4242
"""


def advertisement(*refs: tuple[str, str]) -> bytes:
    """A git-upload-pack ref advertisement as GitHub sends it."""

    def pkt(text: bytes) -> bytes:
        return b"%04x" % (len(text) + 4) + text

    lines = [pkt(b"# service=git-upload-pack\n"), b"0000"]
    lines.append(pkt(MAIN.encode() + b" HEAD\0multi_ack symref=HEAD:refs/heads/main\n"))
    lines += [pkt(f"{sha} {ref}\n".encode()) for sha, ref in refs]
    return b"".join(lines) + b"0000"


class SliceTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root)
        self.home, self.fx, stubs = root / "home", root / "fx", root / "stubs"
        for path in (self.home, self.fx / "out", self.fx / "private" / "BulkyCI", stubs):
            path.mkdir(parents=True)
        for repo in (REPO, OTHER, PRIVATE):
            refs = self.fx / "refs" / repo
            refs.parent.mkdir(parents=True, exist_ok=True)
            refs.write_bytes(
                advertisement(
                    (FEATURE, "refs/heads/x/refs/heads/main"),
                    (MAIN, "refs/heads/main"),
                    (FEATURE, "refs/heads/feat/x"),
                )
            )
        for repo, sha, path, text in (
            (REPO, MAIN, "ci/dcs/runner-job.sbatch", "main runner\n"),
            (REPO, FEATURE, "ci/dcs/other.sh", "other script\n"),
            (OTHER, MAIN, "ci/dcs/runner-job.sbatch", "other tool's runner\n"),
            (PRIVATE, MAIN, "ci/dcs/runner-job.sbatch", "private runner\n"),
        ):
            file = self.fx / "raw" / repo / sha / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(text)
        (self.fx / "private" / PRIVATE).touch()
        basic = base64.b64encode(f"x-access-token:{TOKEN}".encode()).decode()
        (self.fx / "want-basic").write_text(f"Authorization: Basic {basic}")
        (self.fx / "want-token").write_text(f"Authorization: token {TOKEN}")
        for name, body in (("curl", CURL), ("sbatch", SBATCH)):
            (stubs / name).write_text(body)
            (stubs / name).chmod(0o755)
        self.path = f"{stubs}:{os.environ['PATH']}"

    def slice(self, command: str, stdin: str = "jitconfig") -> subprocess.CompletedProcess:
        env = {"HOME": str(self.home), "PATH": self.path, "FX": str(self.fx), "SSH_ORIGINAL_COMMAND": command}
        return subprocess.run(["bash", str(SCRIPT)], input=stdin, env=env, capture_output=True, text=True)

    def out(self, name: str) -> str:
        return (self.fx / "out" / name).read_text()

    def submitted(self) -> list[str]:
        return self.out("sbatch-args").splitlines()

    def left(self, repo: str = REPO) -> list[str]:
        """What stays in the repository's directory, by kind; a directory
        left empty is removed."""
        directory = self.home / "slices" / repo.split("/")[1]
        if not directory.exists():
            return []
        left = sorted(p.name.split(".")[0] for p in directory.iterdir())
        self.assertTrue(left, f"{directory} left empty")
        return left

    def test_runner_from_main_at_its_own_resources(self) -> None:
        result = self.slice(f"asteria/dcs-1-1-s000 repo={REPO}")
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
        self.assertEqual(self.out("sbatch-script"), "main runner\n")
        self.assertEqual(self.out("sbatch-payload"), "jitconfig")
        self.assertEqual(self.out("sbatch-cwd").strip(), str(self.home / "slices" / "asteria-ci"))
        # The payload waits for the job; the script copy and refs are gone.
        self.assertEqual(self.left(), ["payload"])

    def test_any_repository_of_the_organization(self) -> None:
        result = self.slice(f"name=n repo={OTHER}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"DCS_SLICE_REPO={OTHER},", self.submitted()[2])
        self.assertEqual(self.out("sbatch-script"), "other tool's runner\n")
        self.assertEqual(self.left(OTHER), ["payload"])

    def test_options_become_sbatch_options(self) -> None:
        result = self.slice(
            f"asteria/dcs-1-1-s000 repo={REPO} branch=feat/x script=ci/dcs/other.sh cpus=64 mem=256G"
            " time=4-20:00:00 nodes=1 ntasks=2 array=0-99%10 exclusive=1"
            " constraint=EPYC_9634|AMD_Epyc9754 partition=p1 qos=normal gres=gpu:a100:2"
            " account=lab dependency=afterok:41:42"
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
                    "--array=0-99%10",
                    "--exclusive",
                    "--constraint=EPYC_9634|AMD_Epyc9754",
                    "--partition=p1",
                    "--qos=normal",
                    "--gres=gpu:a100:2",
                    "--account=lab",
                    "--dependency=afterok:41:42",
                ]
            ),
        )
        self.assertIn(f"DCS_SLICE_BRANCH=feat/x,DCS_SLICE_SHA={FEATURE}", args[-3])
        self.assertEqual(self.out("sbatch-script"), "other script\n")
        self.assertIn(f"ci/dcs/other.sh at {REPO} feat/x {FEATURE}", result.stderr)

    def test_memory_units(self) -> None:
        for mem, expected in (("4096", "4096M"), ("512M", "512M"), ("2048G", "2097152M"), ("064G", "65536M")):
            with self.subTest(mem=mem):
                result = self.slice(f"repo={REPO} mem={mem}")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"--mem={expected}", self.submitted())

    def test_test_only_submits_nothing_and_keeps_no_payload(self) -> None:
        result = self.slice(f"name=probe repo={REPO} cpus=64 mem=256G test=1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--test-only", self.submitted())
        self.assertEqual(self.left(), [])

    def test_private_repository_with_a_token_that_is_never_kept(self) -> None:
        result = self.slice(f"name=n repo={PRIVATE} auth=1", stdin=f"{TOKEN}\njitconfig")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.out("auth").splitlines(), [f"{PRIVATE} authorized"] * 2)
        self.assertEqual(self.out("sbatch-script"), "private runner\n")
        self.assertEqual(self.out("sbatch-payload"), "jitconfig")
        self.assertNotIn(TOKEN, self.out("sbatch-args"))
        for file in self.home.rglob("*"):
            if file.is_file():
                self.assertNotIn(TOKEN.encode(), file.read_bytes(), file)

    def test_private_repository_without_a_token_is_refused(self) -> None:
        result = self.slice(f"name=n repo={PRIVATE}")
        self.assertEqual(result.returncode, 1)
        self.assertIn(f"cannot read {PRIVATE}", result.stderr)
        self.assertEqual(self.left(PRIVATE), [])

    def test_refusals_submit_nothing_and_leave_nothing(self) -> None:
        for command, stdin, reason in (
            ("name=n", "x", "repo (none) is not BulkyCI/<name>"),
            ("repo=someone/asteria-ci", "x", "repo someone/asteria-ci is not BulkyCI/<name>"),
            ("repo=BulkyCI/../x", "x", "is not BulkyCI/<name>"),
            ("repo=BulkyCI/a/b", "x", "is not BulkyCI/<name>"),
            (f"repo={REPO} cpus=0", "x", "cpus 0 outside"),
            (f"repo={REPO} cpus=257", "x", "cpus 257 outside"),
            (f"repo={REPO} cpus=1x", "x", "cpus 1x outside"),
            (f"repo={REPO} mem=2049G", "x", "mem 2049G outside"),
            (f"repo={REPO} mem=1T", "x", "bad mem"),
            (f"repo={REPO} time=abc", "x", "bad time"),
            (f"repo={REPO} nodes=9", "x", "nodes 9 outside"),
            (f"repo={REPO} ntasks=0", "x", "ntasks 0 outside"),
            (f"repo={REPO} array=x1", "x", "bad array"),
            (f"repo={REPO} constraint=a;b", "x", "bad constraint"),
            (f"repo={REPO} gres=$(id)", "x", "bad gres"),
            (f"repo={REPO} output=/tmp/x", "x", "unknown option output"),
            (f"repo={REPO} wrap=id", "x", "unknown option wrap"),
            (f"repo={REPO} test=2", "x", "test takes 1"),
            (f"repo={REPO} exclusive=yes", "x", "exclusive takes 1"),
            (f"repo={REPO} auth=1", "short\njitconfig", "auth=1 needs a token"),
            (f"repo={REPO} branch=../x", "x", "bad branch"),
            (f"repo={REPO} branch=-x", "x", "bad branch"),
            (f"repo={REPO} script=/etc/passwd", "x", "bad script"),
            (f"repo={REPO} script=ci/../x", "x", "bad script"),
            (f"repo={REPO} cpus=2 cpus=4", "x", "cpus given twice"),
            (f"repo={REPO} one two", "x", "name given twice"),
            (f"repo={REPO} branch=nope", "x", "no branch nope"),
            (f"repo={REPO} branch=x", "x", "no branch x"),
            (f"repo={REPO} script=ci/dcs/missing.sh", "x", "no ci/dcs/missing.sh"),
            ("repo=BulkyCI/absent", "x", "cannot read BulkyCI/absent"),
        ):
            with self.subTest(command=command):
                result = self.slice(command, stdin)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(reason, result.stderr)
                self.assertFalse((self.fx / "out" / "sbatch-args").exists())
                slices = self.home / "slices"
                self.assertEqual(list(slices.iterdir()) if slices.exists() else [], [])

    def test_unreachable_github_is_refused(self) -> None:
        (self.fx / "offline").touch()
        result = self.slice(f"name=n repo={REPO}")
        self.assertEqual(result.returncode, 1)
        self.assertIn(f"cannot read {REPO}", result.stderr)
        self.assertEqual(self.left(), [])

    def test_failed_submission_keeps_no_payload(self) -> None:
        (self.fx / "sbatch-fails").touch()
        result = self.slice(f"name=n repo={REPO}")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.left(), [])


if __name__ == "__main__":
    unittest.main()
