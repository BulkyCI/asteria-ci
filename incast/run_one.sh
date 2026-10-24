#!/bin/bash
# run_one.sh PROFILE_DIR KEY OUT_ROOT: one run of the incast study.
#
# Needs ASTERIA (an asteria checkout with a locked environment), and the
# simulator as ASTRA_SIM_BINARY or the checkout's own build. Writes
# OUT_ROOT/KEY with the run's small outputs and summary.json; the raw event
# stream is reduced and deleted as it is written. A run already summarized
# is skipped, so a shard can be re-run after an interruption.
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
# While the simulator runs, summarize.py --follow bins its event segments
# and deletes each one it has read, so the run keeps about one segment on
# scratch rather than its whole stream; it returns once the file
# $simulated marks the simulator's exit. The segments of an earlier,
# interrupted attempt would be taken for this one's, so its directory goes
# first, as --clean would remove it. The follower yields the core to the
# simulator.
simulated="$out_root/$key.simulated"
rm -rf "$out" "$simulated"
nice "$python" "$here/summarize.py" "$out" --profile "$profile_dir/$key.json" --follow "$simulated" &
follower=$!
# Should this script stop before writing $simulated, the follower would wait
# for it forever.
trap 'kill "$follower" 2>/dev/null || true' EXIT
start=$(date +%s)
status=0
(cd "$asteria" && "$python" experiments/ring_3d/run.py --profile "$profile_dir/$key.json" \
    --output "$out" --clean --seed "$seed" --skip-analysis "${binary[@]}" \
    --simulation-timeout-seconds "${SIMULATION_TIMEOUT_SECONDS:-21600}") > "$out_root/$key.log" 2>&1 || status=$?
wall=$(( $(date +%s) - start ))
touch "$simulated"
follower_status=0
wait "$follower" || follower_status=$?
rm -f "$simulated"
# A follower that stopped early leaves its counts and the segments it had
# not read, from which summarize.py takes the rest.
(( follower_status == 0 )) || echo "$key: follower status $follower_status"
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
    # A run of a sparse hold trace (manifest physical_topology
    # sparse_hold_trace) keeps its hold traces zstd-compressed, as
    # traces.py reads them; every run keeps so its flow, completion and
    # queue logs, which only analyze.py's validity table reads again, so
    # that a shard's finished runs hold little of the cluster's scratch.
    "$python" - "$out" <<'PY'
import json, sys
from pathlib import Path
import zstandard
run = Path(sys.argv[1])
paths = [run / "telemetry" / "flow_events.csv", run / "ns3" / "fct.txt", run / "ns3" / "qlen.txt"]
if json.loads((run / "manifest.json").read_text())["physical_topology"].get("sparse_hold_trace"):
    paths += [run / "ns3" / "spine_losses.csv", run / "ns3" / "receiver_holds.csv"]
for path in paths:
    if path.exists():
        with path.open("rb") as source, path.with_name(path.name + ".zst").open("wb") as target:
            zstandard.ZstdCompressor(level=10).copy_stream(source, target)
        path.unlink()
PY
    rm -rf "$out/workload"
    echo "$key: simulator status $status, ${wall}s"
    exit 0
fi
echo "$key: status $status, ${wall}s, nothing simulated"
exit "$status"
