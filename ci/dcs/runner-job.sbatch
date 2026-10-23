#!/bin/bash
#SBATCH --job-name=asteria-gh-runner
# Two, not one: the simulator is single-threaded, but the runner's own
# streaming threads would otherwise steal cycles from the sim for days.
# (The per-job LTO link that once wanted more cores is gone: jobs
# download the prebuilt runtime bundle instead of compiling.) Two, not
# four: allocation - not load - is what blocks other cluster users. A
# slice's own sbatch options override this and --mem (slice.sh).
#SBATCH --cpus-per-task=2
# The simulator needs <1 GB; the rest is headroom for the last-resort
# case where scratch selection below lands on tmpfs /tmp, whose bytes
# (unpacked runtime bundle, sealed raw-log segments awaiting upload) are
# RAM charged to this limit.
#SBATCH --mem=16G
# Root of the timeout chain: 4 h under SLURM's 5-day cap, and 60 min above
# the Actions job ceiling (6900 min in ns3-evaluation.yml), which in turn
# bounds the comparison and per-sim timeouts in evaluation-matrix.json.
#SBATCH --time=4-20:00:00
#SBATCH --output=runner-%j.log
# Anything but the 2018 Threadrippers: the simulator is single-threaded and
# their Zen+ cores run it roughly half as fast as the modern nodes. A hard
# constraint is safe - the allowed set has 1184 CPUs and a full fleet needs
# 64 - and unlike --nodelist it never serializes behind specific hosts.
#SBATCH --constraint=EPYC_9634|AMD_Epyc7453|XeonGold-6348|AMD_Epyc9754
#
# Runs one just-in-time GitHub Actions runner for exactly one workflow job.
# The runner deregisters itself and exits when its job ends - completed,
# failed, or cancelled - so the allocation is returned the moment the work
# stops. The walltime above stays under SLURM's 5-day cap and above the
# 5-day Actions self-hosted job limit will never be hit in practice.
set -euo pipefail

# The slice's payload is the runner's JIT config (slice.sh).
jit_file=$DCS_SLICE_PAYLOAD
jit=$(cat "$jit_file")
rm -f "$jit_file"

# The cluster's sanctioned scratch is /scratch/scratch-space/expires-<date>
# (support.cs.toronto.edu/systems/slurmresource.html): network-backed,
# shared by every node, wiped on expiry. The newest directory is guaranteed
# to expire more than five days out, which outlives this job's 4d20h
# walltime. Month names do not sort lexically, so rank by parsed date.
newest_scratch_space() {
    local dir stamp y m d epoch best="" best_epoch=0
    for dir in "${DCS_SCRATCH_BASE:-/scratch/scratch-space}"/expires-*; do
        [[ -d "$dir" ]] || continue
        stamp="${dir##*expires-}"
        read -r y m d <<< "${stamp//-/ }"
        epoch=$(date -d "$d $m $y" +%s 2>/dev/null) || continue
        if (( epoch > best_epoch )); then
            best="$dir"
            best_epoch=$epoch
        fi
    done
    [[ -n "$best" ]] && printf '%s\n' "$best"
}

# Everything mutable lives under this job's private directory: runner
# state, checkout, build tree. Candidates, best first. NFS home is quota'd
# (observed: provision failing on a 10 KB write); /var/tmp is a small root
# partition that co-located jobs fill (observed: runners dying logless on
# ENOSPC); /tmp is tmpfs billed to the job's memory limit (observed: the
# OOM killer taking the simulator). The 16G ask is sized to survive the
# tmpfs last resort.
# One tidy parent per scratch base - asteria/<slurm job id> - so a
# 16-job wave presents the lab as a single directory, not sixteen
# scattered entries. The parent is this repository's own: another
# repository's runners on the same account (astra-sim/ under the same
# base, ~/astra-ci and ~/astra-sim in home) keep theirs, and no job, store,
# outbox or reaper here ever names a path of theirs.
network_scratch="$(newest_scratch_space || true)"
# Single definition site for the shared runtime store's location: the
# run's seed job publishes sealed bundle extractions under it, and every
# evaluation job resolves against it (ci/dcs/seed-store.sh and
# resolve-runtime.sh). Only the network scratch base is shareable, so the
# variable is empty - store disabled, jobs extract privately - whenever
# the node has none; job-local fallback bases below are never shared.
export ASTRA_STORE_ROOT="${network_scratch:+${network_scratch}/asteria/store}"
# The outbox is the store's outbound twin: an arm places its results here
# (ci/dcs/outbox.sh) and a freshly tokened courier job collects them,
# because no GitHub upload survives a job's 24-hour token ceiling. This
# export is only the placement fallback: outbox.sh re-resolves the
# freshest generation at placement time, since a 4d20h arm outlives most
# of the margin this job-start resolution carries. Like `store`, the name
# never parses as a numeric job id, so the sibling reaper below cannot
# touch it; orphans die with the scratch generation.
export ASTRA_OUTBOX_ROOT="${network_scratch:+${network_scratch}/asteria/outbox}"
dir=""
for base in "${SLURM_TMPDIR:-}" "$network_scratch" /var/tmp /tmp; do
    [[ -n "$base" ]] || continue
    if mkdir -p "$base/asteria/$SLURM_JOB_ID" 2>/dev/null; then
        dir="$base/asteria/$SLURM_JOB_ID"
        break
    fi
done
if [[ -z "$dir" ]]; then
    echo "no writable scratch base found" >&2
    exit 1
fi
# Canonical physical path: /scratch and /u are symlinks, and any consumer
# that compares this path against what a process or tool reports must see
# one spelling, not two.
dir=$(cd "$dir" && pwd -P)
echo "scratch: $dir"
# The trap also removes the shared asteria parent when this is the last
# job out - non-empty is expected while siblings run, hence the ignore.
cleanup() {
    rm -rf "$dir"
    rmdir --ignore-fail-on-non-empty "$(dirname "$dir")" 2>/dev/null || true
}
trap cleanup EXIT
# Responsible tenancy: the EXIT trap wipes this job's tree, but a job that
# dies without running it (node crash, OOM-killed shell, SIGKILL landing
# before a multi-gigabyte NFS rm finishes its KillWait grace) leaves an
# orphan the lab admin should never have to clean. Each new job therefore
# reaps dead siblings - numeric-id dirs under the shared parent, and
# nothing beside it, where other repositories' trees live - dead iff SLURM
# no longer knows the job id. The shared `store` directory is untouched by construction: its
# name never parses as a numeric job id, and its reaping (a pure age
# sweep) belongs to the single-writer seed job in seed-store.sh. A live job always answers squeue, so this cannot touch a running
# sibling; any other squeue failure skips the sweep - the next job retries.
for sibling in "$(dirname "$dir")"/*; do
    [[ -d "$sibling" ]] || continue
    sibling_id="${sibling##*/}"
    [[ "$sibling_id" == "$SLURM_JOB_ID" ]] && continue
    [[ "$sibling_id" =~ ^[0-9]+$ ]] || continue
    if sibling_state=$(squeue -h -j "$sibling_id" -o %A 2>&1); then
        [[ -n "$sibling_state" ]] && continue
        echo "reaping orphaned scratch: $sibling"
        rm -rf "$sibling" || true
    elif [[ "$sibling_state" == *"Invalid job id"* ]]; then
        echo "reaping orphaned scratch: $sibling"
        rm -rf "$sibling" || true
    fi
done
# SLURM propagates the submitter's entire login environment into this job
# (sbatch --export defaults to ALL), and every convention-following tool
# derives its write paths from HOME and the XDG base dirs. Re-root both
# into the job's scratch before the runner starts: the runner daemon and
# every workflow step inherit them, so incidental writes - uv caches,
# micromamba's ~/.mamba fallback and its NFS lock, git config, runner
# telemetry - land inside $dir by construction and die with the cleanup
# trap. The quota'd NFS home stays control plane only. This is the single
# definition site for the job's isolation; workflow steps must not
# redefine HOME or XDG_* themselves.
export HOME="$dir/home"
export XDG_CACHE_HOME="$HOME/.cache"
export XDG_CONFIG_HOME="$HOME/.config"
export XDG_DATA_HOME="$HOME/.local/share"
export XDG_STATE_HOME="$HOME/.local/state"
export XDG_RUNTIME_DIR="$HOME/.runtime"
mkdir -p "$HOME"
mkdir -m 700 -p "$XDG_RUNTIME_DIR"
# The runner is downloaded per job - the home directory is strictly control
# plane and holds no runtime payloads; the network is unmetered. Pinned
# version and digest, extracted straight onto this job's scratch.
RUNNER_VERSION=2.336.0
RUNNER_SHA256=04cf0be1aff4c3ec3554466c39124ca250e3effd8873bb7e8d68535aa9505d5d
curl -fsSL -o "$dir/runner.tar.gz" \
    "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
echo "${RUNNER_SHA256}  $dir/runner.tar.gz" | sha256sum -c -
tar -xzf "$dir/runner.tar.gz" -C "$dir"
rm "$dir/runner.tar.gz"

cd "$dir"
./run.sh --jitconfig "$jit" &
runner_pid=$!
trap 'kill "$runner_pid" 2>/dev/null || true' TERM INT

# Leak guard: a JIT runner whose workflow job was cancelled while still
# queued would listen forever for work that never arrives. The evidence
# that a job WAS assigned is the Worker diagnostic log the runner writes
# on pickup - a filesystem fact. Never process-table matching here: pgrep
# against $dir once missed every worker because the pattern spelled the
# path through the /scratch symlink while cmdlines carried the resolved
# path, and the guard then killed all 16 jobs at exactly minute 30 while
# their cleanup traps deleted trees under live compilers.
(
    sleep "${DCS_IDLE_GRACE_SECONDS:-1800}"
    if compgen -G "$dir/_diag/Worker_*" >/dev/null; then
        echo "guard: worker log present, job was assigned; standing down"
    else
        echo "guard: no job assigned after ${DCS_IDLE_GRACE_SECONDS:-1800}s; tearing down"
        kill "$runner_pid" 2>/dev/null
    fi
) &
guard_pid=$!

status=0
wait "$runner_pid" || status=$?
kill "$guard_pid" 2>/dev/null || true
exit "$status"
