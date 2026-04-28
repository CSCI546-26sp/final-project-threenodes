[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/yGZxMl3D)

# RaftScope

Causal space-time visualization for Raft consensus — see split-brain, stale reads, and election livelock in seconds.

---

## Requirements

- `cmake` ≥ 3.22.1, `g++` ≥ 13.1.0, Python 3
- Build tools for grpc/googletest: run `./setup.sh` once

---

## Try it now (no build needed)

The demo logs are already in the repo. This is the fastest way to see RaftScope.

**On the VM — start a file server:**
```bash
cd /home/ubuntu/final_project/lab1-akshay-karthik
python3 -m http.server 8080
```
Press `Ctrl+C` to stop it, or from another terminal:
```bash
pkill -f "http.server 8080"
```

**On your Mac — open the visualizer in a browser:**
```
http://192.168.2.3:8080/visualizer/index.html
```

**Inside the visualizer — click `🌐 Load from URL…`**

A dialog box appears. Type this URL and click OK:
```
http://192.168.2.3:8080/demo_logs/split_brain_node_*.jsonl
```

You will see a space-time diagram showing a split-brain scenario across 3 nodes.

> To try the other scenarios, click `🌐 Load from URL…` again and use:
> - `http://192.168.2.3:8080/demo_logs/stale_read_node_*.jsonl`
> - `http://192.168.2.3:8080/demo_logs/livelock_node_*.jsonl`

---

## How to read the diagram

| Element | Meaning |
|---|---|
| Vertical line | One node's timeline — time flows downward |
| Colored dot | An event (click it to see full details in the sidebar) |
| Arrow between nodes | A message was sent and received causally |
| Number on the left | Lamport timestamp — used for causal ordering |

Use the checkboxes at the top to show/hide event types.

---

## Run a live cluster and visualize it

**1. Build:**
```bash
cd /home/ubuntu/final_project/lab1-akshay-karthik
mkdir -p build && cd build
cmake .. && make -j$(nproc)
```

**2. Start a 3-node cluster:**
```bash
./app/multinode --num 3
```
Type `r` at the prompt and press Enter to start the nodes.

**3. Inject faults:**
```
> dis 0      ← disconnect node 0 (triggers a new election)
> conn 0     ← reconnect it (it steps down)
> prop hello ← propose a log entry to the leader
> k          ← kill everything when done
```

**4. View the logs** — the file server must still be running on port 8080. Click `🌐 Load from URL…` in the visualizer and enter:
```
http://192.168.2.3:8080/build/logs/raft_scope_node_*.jsonl
```

---

## Run all three bug scenarios automatically

```bash
cd /home/ubuntu/final_project/lab1-akshay-karthik
bash scripts/run_fault_injection.sh
```

This runs split-brain, stale-read, and livelock in sequence. Load the results:
```
http://192.168.2.3:8080/logs/raft_scope_node_*_split_brain.jsonl
http://192.168.2.3:8080/logs/raft_scope_node_*_stale_read.jsonl
http://192.168.2.3:8080/logs/raft_scope_node_*_livelock.jsonl
```

---

## What each event means

| Event | What happened |
|---|---|
| `become_candidate` | Node timed out waiting for a heartbeat, started an election |
| `vote_request` | Candidate sent a RequestVote RPC to a peer |
| `vote_recv` | Peer received a RequestVote RPC |
| `vote_granted` | Peer decided to grant its vote |
| `vote_denied` | Peer rejected the vote request |
| `vote_granted_recv` / `vote_denied_recv` | Candidate received the peer's decision |
| `leader_change` | Node won the election and became leader |
| `log_append` | Leader sent log entries to a follower |
| `log_append_recv` | Follower received and applied log entries |
| `commit` | Node advanced its commit index |
| `become_follower` | Node stepped down after seeing a higher term |
| `node_crash` | Node was killed |
| `partition_start` / `partition_end` | Network disconnect / reconnect injected |

Heartbeats and empty AppendEntries are excluded — only causally significant events appear.
