#!/bin/bash
# Forced command for the DCS deploy key, installed once and never edited on
# the cluster. It hands sbatch a job script read from a BulkyCI repository,
# so what runs on the cluster, and on how large a slice, changes with a
# push.
#
# The SSH command word is a list of words:
#   repo=BulkyCI/<name>   the repository (required)
#   ref=<commit|branch>   where the script is read (default main)
#   script=<path>         the job script (default ci/dcs/runner-job.sbatch)
#   --<option>[=<value>]  any sbatch option: --cpus-per-task=64 --mem=256G
#                         --job-name=... --test-only ...
# Whatever the options leave out comes from the script's #SBATCH lines.
# This command's stdin, at most 64 KiB, is kept for the job, which finds it
# at $DCS_SLICE_PAYLOAD beside DCS_SLICE_REPO and DCS_SLICE_REF. They reach
# it in sbatch's default --export=ALL: naming variables in --export would
# make SLURM load the login environment on the node, which DCS fails to do.
# A repository's jobs start in ~/slices/<name>, where their logs land.
#
# A branch is read through GitHub's raw cache, which can trail a push by a
# few minutes; a commit is exact, so workflows pass theirs.
#
# The key is as strong as a shell: a runner job runs whatever its JIT config
# points it at. Keeping the key secret is what guards the account; this
# script only makes the common case one line.
set -euf -o pipefail
umask 077

die() {
    echo "slice: $*" >&2
    exit 1
}

repo="" ref=main script=ci/dcs/runner-job.sbatch options=(--parsable)
for word in ${SSH_ORIGINAL_COMMAND:-}; do
    case "$word" in
        repo=*) repo=${word#*=} ;;
        ref=*) ref=${word#*=} ;;
        script=*) script=${word#*=} ;;
        --*) options+=("$word") ;;
        *) die "unknown word $word" ;;
    esac
done
# The three name a URL path; nothing may step out of it.
[[ "$repo" =~ ^BulkyCI/[^/]+$ ]] || die "repo must be BulkyCI/<name>"
for part in "$repo" "$ref" "$script"; do
    [[ "$part" =~ ^[A-Za-z0-9_][A-Za-z0-9_./-]*$ && "$part" != *..* ]] || die "bad name $part"
done

url="https://raw.githubusercontent.com/$repo/$ref/$script"
job=$(curl -fsSL --max-time 60 "$url" < /dev/null) || die "cannot fetch $url"

dir="$HOME/slices/${repo#*/}"
mkdir -p "$dir"
cd "$dir"
payload=$(mktemp "$dir/payload.XXXXXX")
head -c 65536 > "$payload"
echo "slice: $script at $repo $ref" >&2
export DCS_SLICE_PAYLOAD=$payload DCS_SLICE_REPO=$repo DCS_SLICE_REF=$ref
sbatch "${options[@]}" <<< "$job" || { rm -f "$payload"; exit 1; }
# --test-only submits nothing, so nothing will read the payload.
[[ " ${options[*]} " != *" --test-only "* ]] || rm -f "$payload"
