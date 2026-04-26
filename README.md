[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/yGZxMl3D)

# RaftScope

**Causal Log Visualization and Debugging for Raft Consensus**  
CSCI 546 – Distributed Systems | USC | Spring 2026

RaftScope instruments a running Raft cluster, records causal event ordering across nodes using Lamport clocks, and renders an interactive space-time diagram so you can diagnose split-brain, stale reads, and election livelock in seconds — without manually correlating per-node logs.

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| `cmake` | ≥ 3.22.1 | Build system |
| `g++` | ≥ 13.1.0 | C++20 compiler |
| Python | ≥ 3.8 | Demo log generation, fault injection harness |
| Docker + Compose | any recent | OTel/Jaeger live mode (optional) |

> [!IMPORTANT]
> The project is built with **C++20** (`-std=c++20`, extensions disabled). Avoid incompatible APIs.

> [!WARNING]
> Do not modify any build files. The grader uses the unmodified build files.

---

## Accessing the Visualizer from a Multipass / Headless VM

The visualizer is a plain HTML file — browsers can't open `file://` paths on a remote VM. Serve it over HTTP instead:

```bash
# On the VM, from the repo root:
python3 -m http.server 8080

# Find the VM's IP:
hostname -I | awk '{print $1}'
```

Then open this URL **on your Mac** (or any machine that can reach the VM):

```
http://<vm-ip>:8080/visualizer/index.html
```

The `demo_logs/` directory is served alongside the visualizer, so you can load scenario files directly from the file picker.

---

## Build

### 1. Install dependencies

```bash
# From the repo root
./setup.sh
```

Clones gRPC, googletest, and spdlog as submodules and installs gRPC binaries/headers to `$HOME/.local`.

### 2. Standard build

```bash
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### 3. Build with OpenTelemetry tracing (required for Jaeger export)

```bash
mkdir -p build && cd build
cmake -DTRACING=ON ..
make -j$(nproc)
```

With tracing enabled, every gRPC span (AppendEntries, RequestVote) is enriched with Raft attributes (`raft.term`, `raft.log_index`, `raft.commit_index`, `raft.role`, `raft.node_id`) and named span events (`vote_granted`, `vote_denied`, `log_append_recv`, `commit`) before being exported to Jaeger via OTLP gRPC.

---

## Quick Start — Demo Mode (No Cluster Needed)

### Option A: Pre-generated demo logs

Start the HTTP server (see [Accessing the Visualizer](#accessing-the-visualizer-from-a-multipass--headless-vm) above), then load a scenario using one of these methods:

**Multipass VM — use the URL loader.** Paste one of the patterns below into the **Load from URL** field in the visualizer and click **Fetch** (replace `<vm-ip>` with your VM's IP):

| Scenario | URL pattern |
|---|---|
| Split-brain | `http://<vm-ip>:8080/demo_logs/split_brain_node_*.jsonl` |
| Stale read | `http://<vm-ip>:8080/demo_logs/stale_read_node_*.jsonl` |
| Livelock | `http://<vm-ip>:8080/demo_logs/livelock_node_*.jsonl` |

**Local machine — use the file picker.** Click **Load JSONL logs** and multi-select all files for a scenario from `demo_logs/`.

### Option B: Regenerate demo logs

```bash
python3 scripts/generate_demo_logs.py --output-dir demo_logs
```

Then load the files as in Option A.

### Option C: Built-in demo (no files)

Click **Load Demo** in the visualizer toolbar. It loads a built-in synthetic 3-node scenario — initial election, log replication, partition, and re-election — with no files required.

---

## Live Cluster Mode — JSONL

Run a real cluster, inject faults interactively, then load the logs into the visualizer.

### 1. Start a 3-node cluster

```bash
cd build
./app/multinode --num 3
```

At the `>` prompt, type `r` to start all nodes:

```
> r
```

The cluster writes one log file per node to **`build/logs/raft_scope_node_<id>.jsonl`** as events occur.

### 2. Available commands

| Command | Effect |
|---|---|
| `r` | Start the cluster |
| `dis <id>` | Disconnect a node (simulates partition / dropped heartbeat) |
| `conn <id>` | Reconnect a node |
| `prop <data>` | Propose a log entry to the leader |
| `k` | Kill all nodes and exit |

Example session:

```
> r
> prop hello
> dis 0          # partition the leader
> conn 0         # reconnect — it will step down
> k
```

### 3. Load the logs

**From a Multipass / headless VM** — the browser can't open a file picker on the VM, so use the URL loader instead. Make sure the HTTP server is running from the repo root (`python3 -m http.server 8080`), then paste this into the **Load from URL** field in the visualizer and click **Fetch**:

```
http://<vm-ip>:8080/build/logs/raft_scope_node_*.jsonl
```

The `*` expands to node IDs 0–9 automatically; nodes that don't exist are skipped.

**From a local machine** — drag the `build/logs/raft_scope_node_*.jsonl` files onto the **Load JSONL logs** drop zone, or click it to open a file picker. Load all node files for a run together.

---

## Automated Fault Injection Harness

The script starts a 3-node cluster, injects faults at deterministic points, captures JSONL logs, and prints time-to-diagnosis metrics for each scenario.

```bash
# From the repo root
bash scripts/run_fault_injection.sh           # all three scenarios
bash scripts/run_fault_injection.sh split_brain
bash scripts/run_fault_injection.sh stale_read
bash scripts/run_fault_injection.sh livelock
```

Logs are saved to `logs/raft_scope_node_*_<scenario>.jsonl`. Load them in the visualizer after the script finishes.

**Scenarios:**

| Scenario | What is injected | What to look for in the diagram |
|---|---|---|
| `split_brain` | Leader is disconnected; followers time out and elect a new leader | Two `leader_change` events at different terms; the old leader's heartbeat arrow is absent |
| `stale_read` | A follower is isolated while the leader commits new entries | `commit` on Node 0 has a lower Lamport ts than `log_append_recv` on the isolated node |
| `livelock` | All nodes start with a short timeout; simultaneous elections | Repeated `become_candidate` events at the same tick, crossed `vote_denied` arrows each round |

---

## OTel + Jaeger Export

For full distributed tracing with W3C TraceContext causal ordering, export spans to Jaeger.

### 1. Start Jaeger and the OTel collector

```bash
cd tools/jaeger-suite
docker compose up -d
```

- **Jaeger UI:** http://localhost:16686  
- **OTel OTLP gRPC endpoint:** `localhost:4317`

For Multipass VMs, replace `localhost` with the VM's IP when opening the Jaeger UI on your Mac.

### 2. Run the cluster with tracing

```bash
cd build          # must be the TRACING=ON build
./app/multinode --num 3
> r
```

Each node registers as `raft-node-<id>` in Jaeger. Search for any of those service names in the Jaeger UI to inspect individual RPC spans with their Raft attributes and span events.

### 3. Stop Jaeger

```bash
cd tools/jaeger-suite
docker compose down
```

---

## Visualizer Reference

Open `visualizer/index.html` via the HTTP server described above.

### Controls

| Control | Description |
|---|---|
| **Load JSONL logs** | Drag-and-drop or click to load `*.jsonl` files — local machine only |
| **Load from URL** | Paste a URL (use `*` as a node-ID wildcard) and click **Fetch** to pull logs over HTTP — works from Multipass VMs |
| **Show: □ vote_request …** | Toggle individual event types on/off |
| **All / None** | Select or deselect all event-type filters at once |
| **Load Demo** | Load the built-in synthetic scenario without any files |

### Reading the diagram

- **Vertical lines** — one timeline per node; time flows downward.
- **Colored circles** — individual events. Click any circle to see its full metadata in the sidebar.
- **Arrows** — causal message flows between nodes:
  - `vote_request` → `vote_recv`
  - `vote_granted` → `vote_granted_recv`
  - `vote_denied` → `vote_denied_recv`
  - `log_append` → `log_append_recv`
- **Left-margin numbers** — Lamport timestamps used for causal ordering.
- **Hover** — tooltip showing event type, nodes involved, Lamport timestamps, and term.
- **Sidebar legend** — color key for all event types.

### Event colors

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

### Split-brain from a delayed heartbeat

Load `demo_logs/split_brain_node_*.jsonl`.

Find the two `leader_change` events — one on Node 0 (term 1) and one on Node 1 (term 2). The causal arrow that *should* connect Node 0's heartbeat to Node 1's timeline is simply absent: Node 1 never received a heartbeat, timed out, and started a new election while Node 0 still believed it was leader. The overlapping leader window is the split-brain.

### Stale read from a lagging follower

Load `demo_logs/stale_read_node_*.jsonl`.

Find the `commit` event on Node 0 at index 2. Then trace the `log_append` arrow toward Node 1 — it arrives at a Lamport timestamp *after* the commit. Any client read served by Node 1 between those two timestamps sees stale data (`x=1` instead of `x=2`). Per-node logs show the same sequence of events on each node but hide this cross-node gap entirely.

### Election livelock from simultaneous timeouts

Load `demo_logs/livelock_node_*.jsonl`.

The diagram shows Nodes 0 and 1 both entering `become_candidate` at the same Lamport tick in rounds 1 and 2. Each round ends with crossed `vote_denied` arrows — neither candidate reaches a majority. In round 3, random jitter means Node 0 sends its `vote_request` before Node 1 starts its election, and all followers grant it the vote.

---

## JSONL Event Format

Each line in a `*.jsonl` file is a self-contained JSON record:

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

| Field | Description |
|---|---|
| `lamport_ts` | Lamport logical clock value — use this for causal ordering across nodes |
| `wall_time_us` | Wall-clock time in microseconds since epoch |
| `node_id` | The node that recorded this event |
| `event_type` | One of the 15 event types listed in the color table above |
| `from_node` | Sender node for message events; same as `node_id` for local events |
| `to_node` | Recipient node for message events; `-1` for local/broadcast events |
| `term` | Raft term at the time of the event |
| `log_index` | Last log index at the time of the event |
| `commit_index` | Commit index at the time of the event |
| `role` | `follower`, `candidate`, or `leader` |

Heartbeats and empty AppendEntries are intentionally excluded to keep the diagram focused on causally significant decisions.
