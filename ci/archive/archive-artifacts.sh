#!/usr/bin/env bash
# Copies the unexpired artifacts of earlier runs onto releases, so that their
# outputs outlive the artifact retention period. Run <id> gets the release
# incast-<id> with these assets:
#   incast-<id>-artifacts-NN.tar  whole artifact zips, each tar under 1.9 GiB
#   <artifact>.zip.partNN         an artifact over 1.9 GiB, split; concatenate
#                                 the parts in order to rebuild the zip
#   incast-<id>-artifacts.tsv     one line per artifact: id, name, bytes,
#                                 SHA-256 and the asset(s) holding it; written
#                                 last, so a run that has it is complete
#
#   archive-artifacts.sh group G   every run of group G in ci/archive/runs.txt
#   archive-artifacts.sh run ID    one run (a separate process, so that
#                                  errexit holds inside it)
#
# Needs GH_TOKEN with contents: write and actions: read. The Actions token
# allows about 1,000 requests an hour per repository, shared by every job, so
# each request first waits for budget. ARCHIVE_PART_BYTES overrides the
# 1.9 GiB bundle and part size (small values exercise the packing in a test).
set -euo pipefail

repo="$GITHUB_REPOSITORY"
limit="${ARCHIVE_PART_BYTES:-$((1900 * 1024 * 1024))}"

budget() {
  local remaining reset pause
  read -r remaining reset < <(gh api rate_limit --jq '.resources.core | "\(.remaining) \(.reset)"' </dev/null)
  if (( remaining < 25 )); then
    pause=$(( reset - $(date +%s) + 10 ))
    (( pause > 10 )) || pause=10
    echo "rate limit: $remaining requests left, waiting ${pause}s"
    sleep "$pause"
  fi
}

# Uploads files to the release, retrying, then deletes them locally.
upload() {
  local attempt
  for attempt in 1 2 3 4; do
    budget
    if gh release upload "$tag" "$@" --repo "$repo" --clobber </dev/null; then
      rm -f "$@"
      return 0
    fi
    sleep $(( attempt * 30 ))
  done
  echo "::error::upload to $tag failed: $*"
  return 1
}

# Packs the pending zips into the next tar, uploads it and records it.
flush() {
  (( ${#pack_files[@]} > 0 )) || return 0
  local tarname line
  tarname="$(printf 'incast-%s-artifacts-%02d.tar' "$run" "$bundle")"
  tar -cf "$tarname" "${pack_files[@]}"
  rm -f "${pack_files[@]}"
  for line in "${pending[@]}"; do
    printf '%s\t%s\n' "$line" "$tarname" >> manifest.tsv
  done
  upload "$tarname"
  bundle=$(( bundle + 1 ))
  pack_files=()
  pending=()
  pack_bytes=0
}

archive_run() {
  run="$1"
  tag="incast-$run"
  local id name size got sum asset parts
  budget
  if gh api "repos/$repo/releases/tags/$tag" --jq '.assets[].name' > assets.txt 2>/dev/null </dev/null; then
    if grep -qx "incast-$run-artifacts.tsv" assets.txt; then
      echo "$run: already archived"
      return 0
    fi
    # Only assets this script names may be replaced; anything else means the
    # release was made another way and is left alone.
    if grep -qvE "^(incast-$run-artifacts-[0-9]+\.tar|.+\.zip\.part[0-9]+)$" assets.txt; then
      echo "::error::$tag has assets this script did not make"
      return 1
    fi
    while read -r asset <&3; do
      budget
      gh release delete-asset "$tag" "$asset" --repo "$repo" --yes </dev/null
    done 3< assets.txt
  else
    budget
    local sha notes
    sha="$(gh api "repos/$repo/actions/runs/$run" --jq .head_sha </dev/null)"
    notes="Artifacts of run $GITHUB_SERVER_URL/$repo/actions/runs/$run, copied before they expire. incast-$run-artifacts-NN.tar files hold whole artifact zips; an artifact over 1.9 GiB is split into <artifact>.zip.partNN (concatenate the parts in order). incast-$run-artifacts.tsv lists each artifact's id, name, bytes, SHA-256 and the asset that holds it."
    budget
    gh release create "$tag" --repo "$repo" --target "$sha" --title "Incast study: artifacts of run $run" \
      --notes "$notes" </dev/null \
      || gh release create "$tag" --repo "$repo" --title "Incast study: artifacts of run $run" \
           --notes "$notes" </dev/null
  fi

  budget
  gh api --paginate "repos/$repo/actions/runs/$run/artifacts?per_page=100" \
    --jq '.artifacts[] | select(.expired | not) | "\(.id) \(.name) \(.size_in_bytes)"' </dev/null \
    | sort -k2 > artifacts.txt
  echo "$run: $(wc -l < artifacts.txt) artifacts, $(awk '{s += $3} END {printf "%.2f", s / 1e9}' artifacts.txt) GB"

  : > manifest.tsv
  pack_files=()
  pending=()
  pack_bytes=0
  bundle=0
  while read -r id name size <&3; do
    budget
    gh api "repos/$repo/actions/artifacts/$id/zip" > "$name.zip" </dev/null
    got="$(stat -c %s "$name.zip")"
    if [[ "$got" != "$size" ]]; then
      echo "::error::$run $name: downloaded $got of $size bytes"
      return 1
    fi
    sum="$(sha256sum "$name.zip" | cut -d' ' -f1)"
    if (( size > limit )); then
      split -b "$limit" -d -a 2 "$name.zip" "$name.zip.part"
      rm -f "$name.zip"
      parts=("$name".zip.part*)
      printf '%s\t%s\t%s\t%s\t%s\n' "$id" "$name" "$size" "$sum" "$(IFS=,; echo "${parts[*]}")" >> manifest.tsv
      upload "${parts[@]}"
      continue
    fi
    if (( pack_bytes + size > limit )); then
      flush
    fi
    pack_files+=("$name.zip")
    pending+=("$id"$'\t'"$name"$'\t'"$size"$'\t'"$sum")
    pack_bytes=$(( pack_bytes + size ))
  done 3< artifacts.txt
  flush
  mv manifest.tsv "incast-$run-artifacts.tsv"
  upload "incast-$run-artifacts.tsv"
  echo "$run: archived on $tag"
}

case "$1" in
  run)
    archive_run "$2"
    ;;
  group)
    failed=()
    for run in $(awk -v g="$2" '$1 == g {print $2}' "$GITHUB_WORKSPACE/ci/archive/runs.txt"); do
      if ! bash "$0" run "$run"; then
        failed+=("$run")
      fi
    done
    {
      echo "Group $2: $(awk -v g="$2" '$1 == g' "$GITHUB_WORKSPACE/ci/archive/runs.txt" | wc -l) runs, ${#failed[@]} failed"
      for run in "${failed[@]}"; do echo "* failed: $run"; done
    } | tee -a "$GITHUB_STEP_SUMMARY"
    (( ${#failed[@]} == 0 ))
    ;;
  *)
    echo "usage: $0 group G | run ID" >&2
    exit 2
    ;;
esac
