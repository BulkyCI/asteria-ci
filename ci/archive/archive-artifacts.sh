#!/usr/bin/env bash
# Copies the unexpired artifacts of earlier runs onto releases, so that their
# outputs outlive the artifact retention period. Run <id> gets the release
# incast-<id> with these assets:
#   incast-<id>-artifacts-NN.tar  whole artifact zips, each tar under 1.9 GiB
#   <file>.partNN                 a zip over 1.9 GiB, split; concatenate the
#                                 parts in order to rebuild it
#   incast-<id>-artifacts.tsv     one line per artifact: id, name, bytes,
#                                 SHA-256, file and the asset(s) holding it;
#                                 uploaded last, after the uploads check out
# A zip is stored as <name>.zip, or as <name>-<id>.zip when re-run jobs left
# two artifacts with the same name.
#
#   archive-artifacts.sh group G   every run of group G in ci/archive/runs.txt
#   archive-artifacts.sh run ID    one run (a separate process, so that
#                                  errexit holds inside it)
#
# A release the run published itself keeps its own assets. A run whose
# release already has a manifest is checked against it and
# archived again only if the check fails. Needs GH_TOKEN with contents: write
# and actions: read. The Actions token allows about 1,000 requests an hour per
# repository, shared by every job, so each request first waits for budget.
# ARCHIVE_PART_BYTES overrides the 1.9 GiB bundle and part size (small values
# exercise the packing in a test).
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

# Lists the release's assets as "name bytes" lines into the named file.
list_assets() {
  budget
  gh api "repos/$repo/releases/tags/$tag" --jq '.assets[] | "\(.name) \(.size)"' > "$1" </dev/null
}

# Checks a manifest against the release's assets: every asset it names
# exists, no two lines share a file, each tar is at least as large as the
# zips it holds, and the parts of a split zip add up to its size. A manifest
# without the file column (five fields) names its files <name>.zip.
verify() {
  awk -F'\t' -v assets="$2" '
    BEGIN {
      while ((getline line < assets) > 0) {
        split(line, a, " ")
        size[a[1]] = a[2]
      }
    }
    /^#/ { next }
    {
      file = (NF >= 6) ? $5 : $2 ".zip"
      asset = (NF >= 6) ? $6 : $5
      if (seen[file]++) { print "two artifacts stored as " file; bad = 1 }
      n = split(asset, parts, ",")
      if (asset ~ /\.part[0-9]+$/) {
        total = 0
        for (i = 1; i <= n; i++) {
          if (!(parts[i] in size)) { print "missing asset " parts[i]; bad = 1 }
          total += size[parts[i]]
        }
        if (total != $3) { print "parts of " file " hold " total " of " $3 " bytes"; bad = 1 }
      } else {
        if (!(asset in size)) { print "missing asset " asset; bad = 1 }
        held[asset] += $3
      }
    }
    END {
      for (t in held) {
        if (size[t] < held[t]) { print t " has " size[t] " bytes for " held[t] " bytes of zips"; bad = 1 }
      }
      exit bad
    }' "$1"
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
  local id name size got sum file parts asset
  budget
  if gh api "repos/$repo/releases/tags/$tag" --jq '.assets[] | "\(.name) \(.size)"' > assets.txt 2>/dev/null </dev/null; then
    if grep -q "^incast-$run-artifacts\.tsv " assets.txt; then
      budget
      gh release download "$tag" --repo "$repo" --pattern "incast-$run-artifacts.tsv" \
        --output archived.tsv --clobber </dev/null
      if verify archived.tsv assets.txt; then
        echo "$run: already archived"
        return 0
      fi
      echo "$run: the archived copy fails its check; archiving it again"
    fi
    # Only assets this script names are replaced. Others, such as the results
    # tarball the run published itself, stay beside them.
    local ours="^(incast-$run-artifacts(-[0-9]+\.tar|\.tsv)|.+\.zip\.part[0-9]+)\$"
    while read -r asset _ <&3; do
      [[ "$asset" =~ $ours ]] || continue
      budget
      gh release delete-asset "$tag" "$asset" --repo "$repo" --yes </dev/null
    done 3< assets.txt
  else
    budget
    local sha notes
    sha="$(gh api "repos/$repo/actions/runs/$run" --jq .head_sha </dev/null)"
    notes="Artifacts of run $GITHUB_SERVER_URL/$repo/actions/runs/$run, copied before they expire. incast-$run-artifacts-NN.tar files hold whole artifact zips; a zip over 1.9 GiB is split into <file>.partNN (concatenate the parts in order). incast-$run-artifacts.tsv lists each artifact's id, name, bytes, SHA-256, file name and the asset that holds it."
    budget
    gh release create "$tag" --repo "$repo" --target "$sha" --title "Incast study: artifacts of run $run" \
      --notes "$notes" </dev/null \
      || gh release create "$tag" --repo "$repo" --title "Incast study: artifacts of run $run" \
           --notes "$notes" </dev/null
  fi

  budget
  gh api --paginate "repos/$repo/actions/runs/$run/artifacts?per_page=100" \
    --jq '.artifacts[] | select(.expired | not) | "\(.id) \(.name) \(.size_in_bytes)"' </dev/null \
    | sort -k2,2 -k1,1n > artifacts.txt
  awk '{count[$2]++} END {for (n in count) if (count[n] > 1) print n}' artifacts.txt > repeated.txt
  echo "$run: $(wc -l < artifacts.txt) artifacts ($(wc -l < repeated.txt) names repeated)," \
    "$(awk '{s += $3} END {printf "%.2f", s / 1e9}' artifacts.txt) GB"

  printf '# id\tname\tbytes\tsha256\tfile\tasset\n' > manifest.tsv
  pack_files=()
  pending=()
  pack_bytes=0
  bundle=0
  while read -r id name size <&3; do
    file="$name.zip"
    if grep -qxF -- "$name" repeated.txt; then
      file="$name-$id.zip"
    fi
    budget
    gh api "repos/$repo/actions/artifacts/$id/zip" > "$file" </dev/null
    got="$(stat -c %s "$file")"
    if [[ "$got" != "$size" ]]; then
      echo "::error::$run $name ($id): downloaded $got of $size bytes"
      return 1
    fi
    sum="$(sha256sum "$file" | cut -d' ' -f1)"
    if (( size > limit )); then
      split -b "$limit" -d -a 2 "$file" "$file.part"
      rm -f "$file"
      parts=("$file".part*)
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$name" "$size" "$sum" "$file" "$(IFS=,; echo "${parts[*]}")" >> manifest.tsv
      upload "${parts[@]}"
      continue
    fi
    if (( pack_bytes + size > limit )); then
      flush
    fi
    pack_files+=("$file")
    pending+=("$id"$'\t'"$name"$'\t'"$size"$'\t'"$sum"$'\t'"$file")
    pack_bytes=$(( pack_bytes + size ))
  done 3< artifacts.txt
  flush

  # The manifest marks the run complete, so it goes up only after the
  # uploads check out against it.
  list_assets assets.txt
  if ! verify manifest.tsv assets.txt; then
    echo "::error::$tag does not match its manifest"
    return 1
  fi
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
