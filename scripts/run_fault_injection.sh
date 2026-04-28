#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_fault_injection.sh
#
# Drives a live 3-node Raft cluster via the multinode binary, injects faults
# at deterministic points, and captures RaftScope JSONL logs in logs/ for
# each scenario.
#
# Prerequisites
#   1.  Build the project:
#         mkdir -p build && cd build && cmake .. && make -j$(nproc)
#   2.  Run this script from the repo root:
#         bash scripts/run_fault_injection.sh [scenario]
#
#   scenarios: split_brain | stale_read | livelock | all (default: all)
#
# After the script finishes, load the JSONL files from logs/ into
# visualizer/index.html (or via the Load from URL button) to inspect
# the causal diagrams.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
MULTINODE="$BUILD_DIR/app/multinode"
LOG_DIR="$REPO_ROOT/logs"
SCENARIO="${1:-all}"

mkdir -p "$LOG_DIR"

# Run everything from build/ so raft_node processes write scope logs to
# build/logs/ (RaftEventLogger writes relative to CWD).
cd "$BUILD_DIR"

CTRL_PIPE=""
MPID=""

# ─── Cleanup ─────────────────────────────────────────────────────────────────

cleanup() {
  # Close the control pipe if open.
  { exec 3>&-; } 2>/dev/null || true
  [ -n "$CTRL_PIPE" ] && rm -f "$CTRL_PIPE"
  # Kill any stray processes.
  [ -n "$MPID" ] && kill -9 "$MPID" 2>/dev/null || true
  pkill -9 raft_node 2>/dev/null || true
}
# Run cleanup on normal exit AND on SIGTERM/SIGINT (e.g. from timeout(1)).
trap cleanup EXIT SIGTERM SIGINT

kill_cluster() {
  cleanup
  sleep 0.5
  CTRL_PIPE=""
  MPID=""
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

# Start a 3-node cluster via multinode. Opens a named FIFO for control.
# After calling, write commands to fd 3 and call end_scenario when done.
start_cluster() {
  local name="$1"
  local desc="$2"
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  printf  "║  Scenario: %-50s║\n" "$desc"
  echo "╚══════════════════════════════════════════════════════════════╝"

  kill_cluster

  CTRL_PIPE="$(mktemp -u /tmp/raftscope_ctrl.XXXXXX)"
  mkfifo "$CTRL_PIPE"

  # Launch multinode first: its child will block on the FIFO read-open.
  # Then open the write end (fd 3) which unblocks multinode's stdin open.
  "$MULTINODE" --num 3 < "$CTRL_PIPE" \
    > "$LOG_DIR/multinode_${name}.log" 2>&1 &
  MPID=$!
  exec 3>"$CTRL_PIPE"

  # Give nodes time to bind ports and report ready.
  sleep 1

  echo "r" >&3
  echo "  waiting 2s for leader election..."
  sleep 2
}

# Copy this scenario's scope logs from build/logs/ to repo logs/ with a
# scenario-name suffix, then kill the cluster.
end_scenario() {
  local name="$1"

  echo "k" >&3
  # Give multinode time to kill nodes and exit cleanly.
  sleep 2
  { exec 3>&-; } 2>/dev/null || true
  # Wait up to 10 s for multinode to exit, then force-kill.
  local i
  for i in $(seq 1 10); do
    kill -0 "$MPID" 2>/dev/null || break
    sleep 1
  done
  kill -9 "$MPID" 2>/dev/null || true
  pkill -9 raft_node 2>/dev/null || true
  wait "$MPID" 2>/dev/null || true
  sleep 0.3

  for f in "$BUILD_DIR/logs/raft_scope_node_"*.jsonl; do
    [ -f "$f" ] || continue
    base="$(basename "$f" .jsonl)"
    dest="$LOG_DIR/${base}_${name}.jsonl"
    cp "$f" "$dest"
    echo "  captured: $dest ($(wc -l < "$dest") events)"
  done
}

# ─── Scenario functions ───────────────────────────────────────────────────────

scenario_split_brain() {
  start_cluster "split_brain" "Split-Brain from Delayed Heartbeat"

  echo "  [+] Disconnecting node 0 (simulating delayed heartbeat)..."
  echo "dis 0" >&3
  sleep 2   # followers time out and elect a new leader

  echo "  [+] Reconnecting node 0 (it will step down on seeing higher term)..."
  echo "conn 0" >&3
  sleep 1

  end_scenario "split_brain"
}

scenario_stale_read() {
  start_cluster "stale_read" "Stale Read from Lagging Follower"

  echo "  [+] Proposing initial entry..."
  echo "prop initial_value" >&3
  sleep 0.5

  echo "  [+] Isolating follower node 1..."
  echo "dis 1" >&3
  sleep 0.5

  echo "  [+] Committing new entry while node 1 is isolated..."
  echo "prop updated_value" >&3
  sleep 1   # leader+node2 reach majority; node 1 still stale

  echo "  [+] Reconnecting node 1 (it now catches up)..."
  echo "conn 1" >&3
  sleep 1

  end_scenario "stale_read"
}

scenario_livelock() {
  start_cluster "livelock" "Election Livelock (simultaneous timeouts)"
  # Capture the natural election; livelock is visible in the causal diagram
  # when two candidates fire simultaneously and split the vote.
  sleep 3
  end_scenario "livelock"
}

# ─── Main ─────────────────────────────────────────────────────────────────────
echo "RaftScope Fault Injection Harness"
echo "Build dir:  $BUILD_DIR"
echo "Log dir:    $LOG_DIR"

case "$SCENARIO" in
  split_brain)   scenario_split_brain ;;
  stale_read)    scenario_stale_read ;;
  livelock)      scenario_livelock ;;
  all)
    scenario_split_brain
    scenario_stale_read
    scenario_livelock
    ;;
  *)
    echo "Unknown scenario: $SCENARIO.  Use split_brain | stale_read | livelock | all"
    exit 1
    ;;
esac

echo ""
echo "Fault injection complete."
echo "Logs are in $LOG_DIR/raft_scope_node_*_<scenario>.jsonl"
echo "Open visualizer/index.html and load those files to inspect the diagrams."
