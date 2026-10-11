#!/bin/bash
# run_one.sh PROFILE_DIR KEY OUT_ROOT: one run of the incast study.
#
# Needs ASTERIA (an asteria checkout with a locked environment), and the
# simulator as ASTRA_SIM_BINARY or the checkout's own build. Writes
# OUT_ROOT/KEY with the run's small outputs and summary.json; the raw event
# stream is reduced and deleted. A run already summarized is skipped, so a
# shard can be re-run after an interruption.
set -euo pipefail
key=$2
profile_dir=$(realpath "$1")
mkdir -p "$3"
out_root=$(realpath "$3")
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
asteria="${ASTERIA:?set ASTERIA to an asteria checkout}"
python="${PYTHON:-$asteria/.venv/bin/python}"
out="$out_root/$key"
[[ -f "$out/summary.json" ]] && { echo "$key: done already"; exit 0; }
seed=$("$python" -c 'import json,sys; print(next(r["seed"] for r in json.load(open(sys.argv[1])) if r["key"]==sys.argv[2]))' "$profile_dir/runs.json" "$key")
binary=()
[[ -n "${ASTRA_SIM_BINARY:-}" ]] && binary=(--binary "$ASTRA_SIM_BINARY")
start=$(date +%s)
status=0
(cd "$asteria" && "$python" experiments/ring_3d/run.py --profile "$profile_dir/$key.json" \
    --output "$out" --clean --seed "$seed" --skip-analysis "${binary[@]}" \
    --simulation-timeout-seconds "${SIMULATION_TIMEOUT_SECONDS:-21600}") > "$out_root/$key.log" 2>&1 || status=$?
wall=$(( $(date +%s) - start ))
# A run the transport gives up on (a queue pair exhausting its retries) is a
# result for its arm, not an error of the study: it is summarized like any
# other, with the simulator's status and its stated reason, and the run
# fails only when nothing was simulated at all.
if [[ -f "$out/ns3/fct.txt" ]]; then
    "$python" "$here/summarize.py" "$out" --profile "$profile_dir/$key.json"
    "$python" - "$out/summary.json" "$wall" "$status" "$out_root/$key.log" <<'PY'
import json, sys
path, wall, status, log = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
summary = json.load(open(path))
summary["wall_seconds"] = wall
summary["simulator_status"] = status
reasons = [line.strip() for line in open(log, errors="replace") if "Simulation stopped after" in line]
summary["simulator_stop"] = reasons[-1] if reasons else None
json.dump(summary, open(path, "w"), indent=1)
PY
    rm -rf "$out/workload"
    echo "$key: simulator status $status, ${wall}s"
    exit 0
fi
echo "$key: status $status, ${wall}s, nothing simulated"
exit "$status"
