#!/bin/bash
# Forced SSH command for a CI deploy key, installed once and never edited on
# the cluster. It applies to SLURM for a slice (the options below, each
# validated) and runs a job script on it, taken from a branch of a
# repository of the BulkyCI organization at the branch's current head, so
# what a slice does changes with a push to that repository, never here.
#
# The command word is space-separated key=value pairs plus at most one bare
# word, the job's display name. stdin, at most 64 KiB, is saved to a private
# file whose path is the job script's first argument: a runner's JIT config,
# or any other payload. With auth=1 the first line of stdin is a GitHub
# token instead, used only to read a private repository, never stored or
# passed on.
#
#   repo=        BulkyCI/<name>, required
#   branch=      branch of the repository (default main)
#   script=      path in the repository (default ci/dcs/runner-job.sbatch)
#   name=        SLURM job name, sanitized to [A-Za-z0-9._-], cut to 64
#   cpus=        --cpus-per-task, 1 to 256
#   mem=         --mem, <n>[M|G], at most 2048G
#   time=        --time, [D-]H[:MM[:SS]]
#   nodes=       --nodes, 1 to 8
#   ntasks=      --ntasks, 1 to 1024
#   array=       --array, e.g. 0-99%10
#   exclusive=1  --exclusive
#   constraint= partition= qos= gres= account= dependency=
#                passed on after a charset check
#   test=1       sbatch --test-only: validate and estimate, submit nothing
#   auth=1       stdin's first line is a token (above)
#
# An option left out comes from the job script's own #SBATCH lines. The job
# sees DCS_SLICE_REPO, DCS_SLICE_BRANCH and DCS_SLICE_SHA, so it can fetch
# the rest of the repository at the commit its script came from. Each
# repository has its own directory, ~/slices/<name>: the job's working
# directory at submission, where its payloads and SLURM logs land.
set -euo pipefail

OWNER=BulkyCI
umask 077

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

repo=${opt[repo]:-}
[[ "$repo" =~ ^$OWNER/[A-Za-z0-9_-][A-Za-z0-9._-]{0,99}$ ]] \
    || refuse "repo ${repo:-(none)} is not $OWNER/<name>"
name=$(printf '%s' "${opt[name]:-slice}" | tr -cd 'A-Za-z0-9._-' | head -c 64)
branch=${opt[branch]:-main}
script=${opt[script]:-ci/dcs/runner-job.sbatch}
[[ "$branch" =~ ^[A-Za-z0-9._][A-Za-z0-9._/-]{0,127}$ && "$branch" != *..* ]] \
    || refuse "bad branch $branch"
[[ "$script" =~ ^[A-Za-z0-9._][A-Za-z0-9._/-]{0,255}$ && "$script" != *..* ]] \
    || refuse "bad script $script"

sbatch_args=()
passthrough='^[][A-Za-z0-9_.,:|&*()?-]{1,256}$'
for key in ${opt[@]+"${!opt[@]}"}; do
    value=${opt[$key]}
    case "$key" in
        repo | name | branch | script) ;;
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
        array)
            [[ "$value" =~ ^[0-9][0-9,:-]{0,63}(%[0-9]{1,4})?$ ]] || refuse "bad array $value"
            sbatch_args+=(--array="$value") ;;
        constraint | partition | qos | gres | account | dependency)
            [[ "$value" =~ $passthrough ]] || refuse "bad $key $value"
            sbatch_args+=(--"$key"="$value") ;;
        exclusive | test | auth)
            [[ "$value" == 1 ]] || refuse "$key takes 1"
            case "$key" in
                exclusive) sbatch_args+=(--exclusive) ;;
                test) sbatch_args+=(--test-only) ;;
            esac ;;
        *) refuse "unknown option $key" ;;
    esac
done

dir="$HOME/slices/${repo#*/}"
payload=""
work=""
submitted=0
# The payload outlives this script only when a job was submitted to read
# it, and the directory only when something is left in it.
trap '[[ -z "$work" ]] || rm -rf "$work"; (( submitted )) || [[ -z "$payload" ]] || rm -f "$payload"; rmdir "$dir" 2>/dev/null || true' EXIT
mkdir -p "$dir"
cd "$dir"
token=""
if [[ -n "${opt[auth]+set}" ]]; then
    IFS= read -r -n 4096 token || true
    [[ "$token" =~ ^[A-Za-z0-9_]{20,255}$ ]] || refuse "auth=1 needs a token on stdin's first line"
fi
payload=$(mktemp "$dir/payload.XXXXXX")
work=$(mktemp -d "$dir/work.XXXXXX")
head -c 65536 > "$payload"

get() {  # url, output file, authorization scheme; the token goes by stdin
    if [[ -z "$token" ]]; then
        curl -fsSL --max-time 60 -o "$2" "$1"
    elif [[ "$3" == basic ]]; then
        printf 'Authorization: Basic %s\n' "$(printf 'x-access-token:%s' "$token" | base64 -w0)" \
            | curl -fsSL --max-time 60 -H @- -o "$2" "$1"
    else
        printf 'Authorization: token %s\n' "$token" | curl -fsSL --max-time 60 -H @- -o "$2" "$1"
    fi
}

# The branch's head now, from the repository's own ref advertisement, and
# the script at exactly that commit: no caller-supplied commit (a fork's,
# say) can run.
get "https://github.com/$repo.git/info/refs?service=git-upload-pack" "$work/refs" basic \
    || refuse "cannot read $repo"
sha=$(grep -aoE '[0-9a-f]{40} refs/heads/[^[:space:][:cntrl:]]+' "$work/refs" \
    | awk -v ref="refs/heads/$branch" '$2 == ref { print $1 }') || true
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || refuse "no branch $branch in $repo"
get "https://raw.githubusercontent.com/$repo/$sha/$script" "$work/job" token \
    || refuse "no $script at $repo $sha"

echo "slice: $script at $repo $branch $sha" >&2
# sbatch copies the script at submission, so the copy above goes on exit.
sbatch --parsable --job-name="$name" ${sbatch_args[@]+"${sbatch_args[@]}"} \
    --export="ALL,DCS_SLICE_REPO=$repo,DCS_SLICE_BRANCH=$branch,DCS_SLICE_SHA=$sha" \
    "$work/job" "$payload"
# --test-only submits nothing, so nothing will read the payload.
[[ -n "${opt[test]+set}" ]] || submitted=1
