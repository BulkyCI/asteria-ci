# asteria-ci

Simulation runs for [asteria](https://github.com/caddyreverse/asteria) on the
DCS cluster, in the shape of BulkyCI/astra-sim's runners: a hosted job builds
the ns-3 runtime once, each shard of runs gets one just-in-time runner
through the forced-command SSH key, and results are archived as release
assets.

## Incast study

`incast/profiles.py` writes the study's profiles: eight leaves of eight
hosts under eight spines at 400 Gbps (base RTT 12,324 ns), UEC switch
preset, NSCC, trimming and selective repair; every host on leaf 0 receives
K messages of 32 MiB at once (K in 8, 32, 64); from 100 base RTTs spine 3
is healthy, at half rate, at 1 percent loss, or silent. Stages:

| Stage | Runs | What it is |
| --- | --- | --- |
| `binding` | 6 | `spray_uniform` on the half-rate and gray fixtures, to check the degraded spine binds |
| `calibration` | 27 | healthy runs of the three graded arms, three seeds, every K |
| `gain` | 90 | healthy graded runs over five estimator gains, after the thresholds are set |
| `tuning` | 126 | every ACK-redirect setting at K = 32 on the half-rate and gray fixtures, one seed |
| `comparison` | 324 | nine arms, every K and condition, three seeds |
| `kill` | 18 | the K = 32 half-rate and gray cells of the two graded scopes and uniform |
| `edge` | 24 | the two graded scopes with the edge bit's NSCC window penalty on (64), K = 32, every condition |

The graded arms run with `edge_window_penalty: 0`, so every arm runs the
same NSCC and only the spray weights differ. Calibration runs of
workflow run 38112665214 used the default penalty of 64 and are not used
for calibration.

`incast/calibration.json` and `incast/tuning.json`, once committed, feed the
later stages.

### Transient-fault stages

`incast/transient.py` (called through `profiles.py write --stage t_*`)
writes the transient-fault stages on the same fabric and arms. Every host
on each of leaves 0 and 1 receives K messages from the other leaves (each
receiving leaf's full spine capacity). Spine 3 fails eight
times, each occurrence lasting T, the first at 100 base RTTs and one every
T + 1 ms (`duration_ns`, `repeat`, `period_ns`): half rate, silent
(`blackhole`) or 5 percent loss, on every link of spine 3 (`spinewide`) or
on its link to leaf 0 (`downlink0`). The message size is
(1.3 ms + 8T + 8 ms) x 50 GB/s / K, rounded up to a whole MiB. Every graded
arm, the calibration runs included, runs with `edge_window_penalty: 0`,
`edge_freeze: false`, `estimator_interval_samples: 0` and
`estimator_gain: 0.25`; the arms are `graded_per_connection`, `graded_per_receiver`,
`graded_per_receiver_mix` (`sender_pool_weight` tuned),
`graded_per_receiver_shared` (`sender_pool_weight: 1`),
`graded_switch_queue`, `uniform`, `reps` and `mrc`, and in `t_comparison`
`redirect_per_connection` at the setting `incast/tuning.json` holds for it
(the half-rate one for half rate and blackhole).

| Stage | Runs | What it is |
| --- | --- | --- |
| `t_calibration` | 27 | healthy runs of the three graded scopes, K in 8, 32, 64, seeds 101 to 103 |
| `t_tuning` | 36 | gamma in 0.25, 0.5, 1.0 per graded arm, by `sender_pool_weight` in 0.25, 0.5, 0.75 for the mixed arm, K = 32, spine-wide half rate, T = 400 us, seed 201; each with a healthy run of the same setting |
| `t_comparison` | 663 | 18 faulted cells by 8 arms by seeds 301 to 303 (432), and a healthy run per arm, K, T and seed (168); `redirect_per_connection` on the 14 spine-wide half-rate and blackhole cells (42) with its healthy runs (21) |

The faulted cells: K in 8, 32, 64 at T = 400 us, spine-wide, half rate and
blackhole; T in 100, 200, 800, 1600 us at K = 32, spine-wide, half rate and
blackhole; the single downlink at K = 32, T = 400 us, half rate and
blackhole; and 5 percent loss at K = 32, T = 400 us on both geometries. A
run key is `<stage>-k<K>-<kind>-<geometry>-t<T in us>-<arm>[-<setting>]-s<seed>`,
with kind `healthy` and geometry `none` for a run without fault.

`analyze.py calibrate --stage t_calibration --into incast/transient_calibration.json`
sets the thresholds; `analyze.py select --stage t_tuning` picks, per
graded arm, the setting with the lowest cost, for
`incast/transient_tuning.json` (`graded_switch_queue` takes
`graded_per_receiver`'s gamma). Until that file exists `t_comparison` lists
only the ungraded arms (and `redirect_per_connection`). `analyze.py cost` tabulates cost (completion minus
the paired healthy run's), recovered fraction against `uniform`, seed-noise
verdicts against a reference arm, and the means of the per-transient
measures `summarize.py` writes to `transients.json`. `t_comparison` needs
`runs_per_shard` of at least 3 to fit the 256-job matrix. Tests:
`python3 -m unittest discover incast/tests`. Dispatch `.github/workflows/incast.yml` with a comma-separated
list of stages; `simulation_timeout_seconds`, `run_timeout_minutes` and
`collect_timeout_minutes` (6 h, 12 h and 60 min by default) lift the limits of
one simulation, a shard's job and the tabulating job for long runs, and
`max_parallel` (64 by default) sets how many shards are provisioned and run
at once.

The asteria commit under test is `incast/asteria.ref`.

## Cluster

The DCS key's forced command is `ci/dcs/slice.sh`, installed once wherever
the key's `authorized_keys` line points (on DCS, the account's
`~/astra-ci/bin/accept-runner.sh`) and never edited on the cluster. It hands
sbatch a job script read from any BulkyCI repository, so what runs on the
cluster, and on how large a slice, changes with a push. The SSH command
word is `repo=BulkyCI/<name>`, optionally `ref=<commit or branch>`
(default `main`) and `script=<path>` (default `ci/dcs/runner-job.sbatch`),
and any sbatch options as `--<option>=<value>`; whatever those leave out
comes from the script's own `#SBATCH` lines. stdin is the job's payload,
which it finds at `$DCS_SLICE_PAYLOAD`; a runner's is its JIT config. Each
repository's jobs start in `~/slices/<name>`, where their logs land. A
branch is read through GitHub's raw cache, which can trail a push by a few
minutes, so the provision action passes its run's commit, with
`repo=$GITHUB_REPOSITORY`: a copy of the action in another repository runs
that repository's scripts. This repository's runner script keeps job
scratch, store and outbox under `<scratch base>/asteria/`.

The key is as strong as a shell on the account, since a runner job runs
whatever its JIT config points it at; keeping it secret is what guards the
account, and the script does not pretend otherwise.

Install, or update the installed copy, from a pinned commit, over the
file the key's line names:

```sh
f=~/astra-ci/bin/accept-runner.sh
curl -fsSL -o "$f.new" https://raw.githubusercontent.com/BulkyCI/asteria-ci/<commit>/ci/dcs/slice.sh
chmod 700 "$f.new" && bash -n "$f.new" && mv "$f.new" "$f"
```
