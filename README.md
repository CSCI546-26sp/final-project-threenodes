[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/yGZxMl3D)
# Raft Lab Skeleton + RaftScope

RaftScope is a causal log visualization and debugging tool for Raft consensus. It instruments a running Raft cluster, records causal event ordering across nodes, and renders an interactive space-time diagram so you can diagnose split-brain, stale reads, and election livelock in seconds.

---

## Prerequisites

- `cmake` >= 3.22.1
- `g++` >= 13.1.0
- Python 3.8+ (for demo log generation and fault injection)
- Docker + Docker Compose (optional — required only for the OTel/Jaeger live mode)

> [!IMPORTANT]
> The lab will be built with **C++20** standard and extension disabled. (`-std=c++20` is used). Please avoid incompatible APIs. For more details, you can refer to the cmake files in the project.

> [!WARNING]
> You are not supposed to modify any build files. You can do that for your own testing purposes, but the grader will use the unmodified build files. Make sure your code compiles correctly with the provided build files.

---

## Build

### 1. Install dependencies

```bash
# From the repo root
./setup.sh
```

This clones gRPC, googletest, and spdlog as submodules and installs gRPC binaries/headers to `$HOME/.local`.

### 2. Configure and build

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

To build with OpenTelemetry tracing enabled (required for the Jaeger live mode):

```bash
cmake -DTRACING=ON ..
make -j$(nproc)
```

---

## RaftScope: Quick Start (Demo Mode)

The fastest way to see RaftScope in action — no cluster required.

### Option A: Load pre-generated demo logs

```bash
# Open the visualizer in your browser
open visualizer/index.html       # macOS
xdg-open visualizer/index.html  # Linux
```

Click **Load JSONL logs**, then select one or more files from `demo_logs/`:

| File set | Scenario |
|---|---|
| `demo_logs/split_brain_node_*.jsonl` | Split-brain from a delayed leader heartbeat |
| `demo_logs/stale_read_node_*.jsonl` | Stale read from a lagging follower |
| `demo_logs/livelock_node_*.jsonl` | Election livelock from simultaneous timeouts |

Load all files for a scenario at once (multi-select) to see all node timelines together.

### Option B: Generate fresh demo logs

```bash
python3 scripts/generate_demo_logs.py --output-dir demo_logs
```

Then load the new files in the visualizer as above.

### Option C: Click "Load Demo" in the visualizer

The **Load Demo** button in the visualizer toolbar loads a built-in synthetic three-node scenario (initial election → log replication → partition → new election) without any files.

---

## RaftScope: Live Cluster Mode (JSONL)

Run a real Raft cluster, inject faults, and visualize the causal log.

### 1. Start a cluster

```bash
cd build
./app/multinode --num 3
```

At the `>` prompt, start the cluster:

```
> r
```

The cluster writes per-node event logs to `logs/raft_scope_node_<id>.jsonl` in the **build directory**.

### 2. Inject faults interactively

```
> dis 0          # disconnect node 0 (simulates delayed heartbeat / partition)
> conn 0         # reconnect node 0
> prop hello     # propose a log entry
```

### 3. Load the logs in the visualizer

Open `visualizer/index.html` in your browser and drag the `logs/raft_scope_node_*.jsonl` files from the build directory onto the **Load JSONL logs** drop zone, or click it to open a file picker.

### 4. Run the automated fault injection harness

The script drives all three canonical bug scenarios and saves the JSONL logs:

```bash
# From the repo root
bash scripts/run_fault_injection.sh          # runs all three scenarios
bash scripts/run_fault_injection.sh split_brain
bash scripts/run_fault_injection.sh stale_read
bash scripts/run_fault_injection.sh livelock
```

After each scenario, load the `logs/raft_scope_node_*_<scenario>.jsonl` files into the visualizer.

To force a deterministic election livelock (all nodes use the same election timeout):

```bash
export RAFT_FIXED_ELECTION_TIMEOUT_MS=300
bash scripts/run_fault_injection.sh livelock
unset RAFT_FIXED_ELECTION_TIMEOUT_MS
```

---

## RaftScope: Live Mode with OTel + Jaeger

For near-real-time rendering and W3C TraceContext causal ordering, run the cluster with tracing enabled and query the Jaeger HTTP API directly from the visualizer.

### 1. Start Jaeger and the OTel collector

```bash
cd tools/jaeger-suite
docker compose up -d
```

- Jaeger UI: http://localhost:16686
- OTel collector OTLP gRPC endpoint: `localhost:4317`

### 2. Build and run nodes with tracing

```bash
mkdir -p build && cd build
cmake -DTRACING=ON ..
make -j$(nproc)
./app/multinode --num 3
> r
```

Each node registers itself as a service named `raft-node-<id>` in Jaeger. All gRPC spans (AppendEntries, RequestVote) are enriched with Raft attributes:

| Span attribute | Description |
|---|---|
| `raft.node_id` | Node that handled the RPC |
| `raft.term` | Current term at decision time |
| `raft.log_index` | Last log index |
| `raft.commit_index` | Commit index |
| `raft.role` | `follower` / `candidate` / `leader` |

Named span events mark key decision points: `vote_granted`, `vote_denied`, `log_append_recv`, `commit`.

### 3. Query Jaeger from the visualizer

Open `visualizer/index.html`. In the **Jaeger** control row:

1. Set the URL to `http://localhost:16686` (default).
2. Choose a lookback window (15m, 1h, 3h, 24h).
3. Click **Fetch** — the visualizer queries `GET /api/services` to find all `raft-node-*` services, fetches their traces, and renders the space-time diagram.

For **near-real-time rendering**, click **▶ Live** and select a polling interval (3s, 5s, 10s, 30s). The diagram refreshes automatically as new events arrive.

> **CORS note:** Jaeger's query service allows `*` by default. If you see CORS errors, serve the visualizer from a local HTTP server: `python3 -m http.server 8080` and open `http://localhost:8080/visualizer/index.html`.

---

## Visualizer Reference

Open `visualizer/index.html` in any modern browser.

### Controls

| Control | Description |
|---|---|
| **Load JSONL logs** | Drag-and-drop or click to load one or more `*.jsonl` node files |
| **Nodes: □ N0 □ N1 …** | Per-node visibility toggles (populated after loading data) |
| **Show: □ vote_request …** | Filter events by type |
| **Time range sliders** | Restrict the diagram to a Lamport timestamp range |
| **All / None** | Toggle all event-type or node filters at once |
| **Load Demo** | Load built-in synthetic scenario without any files |
| **Jaeger URL** | OTel/Jaeger query endpoint (default `http://localhost:16686`) |
| **Lookback** | How far back to fetch Jaeger traces |
| **Fetch** | Pull latest traces from Jaeger and re-render |
| **▶ Live** | Start/stop auto-refresh polling at the selected interval |

### Reading the diagram

- **Vertical lines** — one timeline per node; time flows downward.
- **Colored circles** — individual events; see the legend in the sidebar.
- **Arrows** — causal message flows: `vote_request → vote_recv`, `vote_granted → vote_granted_recv`, `vote_denied → vote_denied_recv`, `log_append → log_append_recv`.
- **Left margin numbers** — Lamport timestamps (causal ordering).
- **Click any circle** — shows full event metadata in the sidebar (term, log index, commit index, role, wall time).
- **Hover** — tooltip with event type, nodes, Lamport timestamps, and term.

### Event color legend

| Color | Event types |
|---|---|
| Amber | `vote_request`, `vote_recv` |
| Emerald | `vote_granted`, `vote_granted_recv` |
| Red | `vote_denied`, `vote_denied_recv` |
| Blue | `log_append`, `log_append_recv` |
| Violet | `commit` |
| Purple | `leader_change` |
| Slate | `become_follower` |
| Orange | `become_candidate` |
| Bright red | `node_crash` |
| Orange / Cyan | `partition_start` / `partition_end` |

---

## Diagnosing the Three Canonical Bugs

### (a) Split-brain from a delayed heartbeat

Load `demo_logs/split_brain_node_*.jsonl`. Look for the window where **two nodes both show `leader_change`** at different terms. The arrow from `vote_request` to `vote_recv` on the old leader (which arrived but was denied) makes the causal gap visible — the heartbeat that should have prevented the re-election is simply absent.

### (b) Stale read from a lagging follower

Load `demo_logs/stale_read_node_*.jsonl`. Find the `commit` event on the leader (Node 0). Compare its Lamport timestamp to the `log_append_recv` on Node 1 — Node 1's event arrives *after* the commit, so any read from Node 1 before that point returns stale data. The gap is invisible in per-node logs but immediately obvious in the diagram.

### (c) Election livelock from simultaneous timeouts

Load `demo_logs/livelock_node_*.jsonl`. Multiple `become_candidate` events at the same Lamport tick, with `vote_denied` arrows crossing between all nodes, show the collision. Watch it repeat for two rounds before jitter breaks the tie in round 3.

---

## JSONL Event Format

Each line in a `*.jsonl` file is a JSON object:

```json
{
  "lamport_ts":   4,
  "wall_time_us": 1745000000000000,
  "node_id":      0,
  "event_type":   "vote_request",
  "from_node":    0,
  "to_node":      1,
  "term":         1,
  "log_index":    0,
  "commit_index": 0,
  "role":         "candidate"
}
```

`from_node == to_node == node_id` for local events (no message). `to_node == -1` for broadcast-style local events (e.g., `leader_change`, `node_crash`).

---

## Stopping Jaeger

```bash
cd tools/jaeger-suite
docker compose down
```
