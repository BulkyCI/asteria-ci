#!/bin/bash
# Forced SSH command for the CI deploy key, installed once (README,
# Cluster) and never edited on the cluster. It applies to SLURM for a slice
# (the options below, each validated) and runs a script from a branch of
# one of the repositories below on it, so changing what a slice does is a
# push, not a cluster edit.
#
# The command word is space-separated key=value pairs, plus at most one bare
# word, the job's display name (the provision actions send it first). stdin,
# at most 64 KiB, is saved to a private file whose path is the script's
# first argument: a runner's JIT config, or any other payload.
#
#   name=        SLURM job name, sanitized to [A-Za-z0-9._-], cut to 64
#   repo=        BulkyCI/asteria-ci (default) or caddyreverse/asteria
#   branch=      branch of the repository (default main)
#   script=      path in the repository (default ci/dcs/runner-job.sbatch)
#   cpus=        --cpus-per-task, 1 to 256
#   mem=         --mem, <n>[M|G], at most 2048G
#   time=        --time, [D-]H[:MM[:SS]]
#   nodes=       --nodes, 1 to 8
#   ntasks=      --ntasks, 1 to 1024
#   constraint= partition= qos= gres=   passed on after a charset check
#   test=1       sbatch --test-only: validate and estimate, submit nothing
#
# An option left out comes from the script's own #SBATCH lines. The job
# sees DCS_SLICE_REPO, DCS_SLICE_BRANCH and DCS_SLICE_SHA, so a script can
# fetch the rest of the repository at the commit it was taken from.
set -euo pipefail

ROOT="$HOME/asteria-ci"
REPOS=(BulkyCI/asteria-ci caddyreverse/asteria)
umask 077
mkdir -p "$ROOT/jobs"
cd "$ROOT/jobs"

payload=$(mktemp "$ROOT/jobs/payload.XXXXXX")
job_script=""
submitted=0
# The payload outlives this script only when a job was submitted to read it.
trap '[[ -z "$job_script" ]] || rm -f "$job_script"; (( submitted )) || rm -f "$payload"' EXIT
head -c 65536 > "$payload"

refuse() {
    echo "slice: $*" >&2
    exit 1
}

declare -A opt=()
read -ra words <<< "${SSH_ORIGINAL_COMMAND:-}"
for word in ${words[@]+"${words[@]}"}; do
    if [[ "$word" == *=* ]]; then
        key=${word%%=*}
        value=${word#*=}
    else
        key=name
        value=$word
    fi
    [[ -z "${opt[$key]+set}" ]] || refuse "$key given twice"
    opt[$key]=$value
done

in_range() {  # value, low, high: a decimal of at most 7 digits within bounds
    [[ "$1" =~ ^[0-9]{1,7}$ ]] && (( 10#$1 >= $2 && 10#$1 <= $3 ))
}

name=$(printf '%s' "${opt[name]:-slice}" | tr -cd 'A-Za-z0-9._-' | head -c 64)
REPO=${opt[repo]:-${REPOS[0]}}
[[ " ${REPOS[*]} " == *" $REPO "* ]] || refuse "repo $REPO is not one of ${REPOS[*]}"
branch=${opt[branch]:-main}
script=${opt[script]:-ci/dcs/runner-job.sbatch}
[[ "$branch" =~ ^[A-Za-z0-9._][A-Za-z0-9._/-]{0,127}$ && "$branch" != *..* ]] \
    || refuse "bad branch $branch"
[[ "$script" =~ ^[A-Za-z0-9._][A-Za-z0-9._/-]{0,255}$ && "$script" != *..* ]] \
    || refuse "bad script $script"

sbatch_args=()
passthrough='^[][A-Za-z0-9_.,:|&*()-]{1,256}$'
for key in ${opt[@]+"${!opt[@]}"}; do
    value=${opt[$key]}
    case "$key" in
        name | repo | branch | script) ;;
        cpus)
            in_range "$value" 1 256 || refuse "cpus $value outside 1 to 256"
            sbatch_args+=(--cpus-per-task="$((10#$value))") ;;
        mem)
            [[ "$value" =~ ^([0-9]{1,7})([MG]?)$ ]] || refuse "bad mem $value"
            mb=$((10#${BASH_REMATCH[1]}))
            [[ "${BASH_REMATCH[2]}" == G ]] && mb=$((mb * 1024))
            (( mb >= 1 && mb <= 2048 * 1024 )) || refuse "mem $value outside 1M to 2048G"
            sbatch_args+=(--mem="${mb}M") ;;
        time)
            [[ "$value" =~ ^([0-9]{1,2}-)?[0-9]{1,3}(:[0-9]{2}){0,2}$ ]] || refuse "bad time $value"
            sbatch_args+=(--time="$value") ;;
        nodes)
            in_range "$value" 1 8 || refuse "nodes $value outside 1 to 8"
            sbatch_args+=(--nodes="$((10#$value))") ;;
        ntasks)
            in_range "$value" 1 1024 || refuse "ntasks $value outside 1 to 1024"
            sbatch_args+=(--ntasks="$((10#$value))") ;;
        constraint | partition | qos | gres)
            [[ "$value" =~ $passthrough ]] || refuse "bad $key $value"
            sbatch_args+=(--"$key"="$value") ;;
        test)
            [[ "$value" == 1 ]] || refuse "test takes 1"
            sbatch_args+=(--test-only) ;;
        *) refuse "unknown option $key" ;;
    esac
done

# The branch's head now, and the script at exactly that commit: the raw path
# names a commit taken from the repository's own branches, never one the
# caller supplies, so no commit outside them (a fork's, say) can run.
sha=$(git ls-remote "https://github.com/$REPO" "refs/heads/$branch" \
    | awk -v ref="refs/heads/$branch" '$2 == ref { print $1 }') \
    || refuse "cannot reach https://github.com/$REPO"
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || refuse "no branch $branch in $REPO"
job_script=$(mktemp "$ROOT/jobs/script.XXXXXX")
curl -fsSL --max-time 60 -o "$job_script" \
    "https://raw.githubusercontent.com/$REPO/$sha/$script" \
    || refuse "no $script at $REPO $sha"

echo "slice: $script at $REPO $branch $sha" >&2
# sbatch copies the script at submission, so the copy above goes on exit.
sbatch --parsable --job-name="$name" ${sbatch_args[@]+"${sbatch_args[@]}"} \
    --export="ALL,DCS_SLICE_REPO=$REPO,DCS_SLICE_BRANCH=$branch,DCS_SLICE_SHA=$sha" \
    "$job_script" "$payload"
# --test-only submits nothing, so nothing will read the payload.
[[ -n "${opt[test]+set}" ]] || submitted=1
