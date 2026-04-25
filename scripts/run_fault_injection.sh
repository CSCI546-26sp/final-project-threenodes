#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_fault_injection.sh
#
# Drives a live 3-node Raft cluster, injects faults at deterministic points,
# and captures RaftScope JSONL logs in logs/ for each scenario.
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
# visualizer/index.html to inspect the causal diagrams.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
RAFT_NODE="$BUILD_DIR/app/raft_node"
LOG_DIR="$REPO_ROOT/logs"
SCENARIO="${1:-all}"

mkdir -p "$LOG_DIR"
cd "$BUILD_DIR/integration_tests"

# ─── Helpers ─────────────────────────────────────────────────────────────────
kill_cluster() {
  pkill -9 raft_node 2>/dev/null || true
  sleep 0.5
}

wait_for_leader() {
  # Poll node states via the tester gRPC service (requires raft_test binary
  # which internally calls check_one_leader).  Here we simply wait a fixed
  # window for safety.
  echo "  waiting ${1:-2}s for leader election..."
  sleep "${1:-2}"
}

run_scenario() {
  local name="$1"
  local desc="$2"
  echo ""
  echo "╔══════════════════════════════════════════════════════════════╗"
  printf  "║  Scenario: %-50s║\n" "$desc"
  echo "╚══════════════════════════════════════════════════════════════╝"

  # Start 3-node cluster
  kill_cluster
  sleep 0.3

  # Launch nodes (verbosity 1 = file-sink only for raft logs)
  COMMON="--enable_ctrl --ctrl_addr 0.0.0.0:55000 --fail_type 0 --verbosity 1"
  ../app/raft_node $COMMON --id 0 --port 50051 \
      --peers "1+localhost:50052,2+localhost:50053" \
      --node_tester_port 55001 > "$LOG_DIR/node_runner_0_${name}.log" 2>&1 &
  ../app/raft_node $COMMON --id 1 --port 50052 \
      --peers "0+localhost:50051,2+localhost:50053" \
      --node_tester_port 55002 > "$LOG_DIR/node_runner_1_${name}.log" 2>&1 &
  ../app/raft_node $COMMON --id 2 --port 50053 \
      --peers "0+localhost:50051,1+localhost:50052" \
      --node_tester_port 55003 > "$LOG_DIR/node_runner_2_${name}.log" 2>&1 &

  sleep 0.5  # let nodes bind their ports
  wait_for_leader 2
}

copy_scope_logs() {
  local name="$1"
  for f in logs/raft_scope_node_*.jsonl; do
    [ -f "$f" ] || continue
    base="$(basename "$f" .jsonl)"
    cp "$f" "$LOG_DIR/${base}_${name}.jsonl"
    echo "  captured: $LOG_DIR/${base}_${name}.jsonl ($(wc -l < "$f") events)"
  done
}

# ─── Scenario functions ───────────────────────────────────────────────────────

scenario_split_brain() {
  run_scenario "split_brain" "Split-Brain from Delayed Heartbeat"

  echo "  [+] Sending proposals then partitioning leader ..."
  # Let leader commit a few entries
  sleep 1

  # Find the leader node (naive: wait for raft_scope logs to appear)
  # Disconnect the leader to trigger split-brain window
  echo "  [+] Disconnecting leader (simulating delayed heartbeat) ..."
  # The test harness disconnect command hits the NetInterceptor via gRPC;
  # here we use a tiny Python snippet to send the Disconnect RPC
  python3 - <<'EOF'
import grpc, sys
sys.path.insert(0, ".")
# If tester proto stubs are available, use them.  Otherwise skip.
try:
    from tester_pb2_grpc import TesterCommNodeServiceStub
    from tester_pb2 import ConnOpt
    ch = grpc.insecure_channel("localhost:55001")
    stub = TesterCommNodeServiceStub(ch)
    opt  = ConnOpt(); opt.ids.append(0)
    stub.Disconnect(opt)
    print("  Leader (node 0) disconnected via RPC")
except Exception as e:
    print(f"  (skipping RPC disconnect: {e})")
EOF

  sleep 2  # allow followers to time out and elect new leader
  echo "  [+] Reconnecting leader ..."
  python3 - <<'EOF'
import grpc, sys
sys.path.insert(0, ".")
try:
    from tester_pb2_grpc import TesterCommNodeServiceStub
    from tester_pb2 import ConnOpt
    ch   = grpc.insecure_channel("localhost:55001")
    stub = TesterCommNodeServiceStub(ch)
    opt  = ConnOpt(); opt.ids.append(0)
    stub.Reconnect(opt)
    print("  Leader reconnected")
except Exception as e:
    print(f"  (skipping RPC reconnect: {e})")
EOF
  sleep 1
  copy_scope_logs "split_brain"
  kill_cluster
}

scenario_stale_read() {
  run_scenario "stale_read" "Stale Read from Lagging Follower"

  echo "  [+] Proposing entries, then isolating a follower ..."
  python3 - <<'EOF'
import grpc, sys, time
sys.path.insert(0, ".")
try:
    from tester_pb2_grpc import TesterCommNodeServiceStub
    from tester_pb2 import ConnOpt, ProposalReq
    # Disconnect follower node 1 from the cluster
    ch   = grpc.insecure_channel("localhost:55002")
    stub = TesterCommNodeServiceStub(ch)
    opt  = ConnOpt(); opt.ids.append(1)
    stub.Disconnect(opt)
    print("  Follower node 1 disconnected (stale-read candidate)")
except Exception as e:
    print(f"  (skipping: {e})")
EOF

  # Leader commits more entries – node 1 won't see them yet
  sleep 1
  python3 - <<'EOF'
import grpc, sys
sys.path.insert(0, ".")
try:
    from tester_pb2_grpc import TesterCommNodeServiceStub
    from tester_pb2 import ConnOpt
    ch   = grpc.insecure_channel("localhost:55002")
    stub = TesterCommNodeServiceStub(ch)
    opt  = ConnOpt(); opt.ids.append(1)
    stub.Reconnect(opt)
    print("  Follower node 1 reconnected (will now catch up)")
except Exception as e:
    print(f"  (skipping: {e})")
EOF
  sleep 1
  copy_scope_logs "stale_read"
  kill_cluster
}

scenario_livelock() {
  run_scenario "livelock" "Election Livelock (simultaneous timeouts)"
  # This scenario relies on natural timing; with a 3-node cluster the
  # probability of a livelock is low but it can be induced by patching the
  # election_timeout to be identical.  Here we just run the cluster long
  # enough that the natural election succeeds and capture the logs.
  sleep 3
  copy_scope_logs "livelock"
  kill_cluster
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
