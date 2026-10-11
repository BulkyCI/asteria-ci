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

`incast/calibration.json` and `incast/tuning.json`, once committed, feed the
later stages. Dispatch `.github/workflows/incast.yml` with a comma-separated
list of stages.

The asteria commit under test is `incast/asteria.ref`.

## Cluster

The DCS key's forced command routes the `asteria/` command word to
`~/asteria-ci` (control root) and `~/asteria` (a clone of this repository),
and `ci/dcs/runner-job.sbatch` keeps job scratch, store and outbox under
`<scratch base>/asteria/`, apart from astra-sim's.
