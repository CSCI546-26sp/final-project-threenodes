#!/usr/bin/env python3
"""
generate_demo_logs.py
─────────────────────
Generates synthetic RaftScope JSONL logs that demonstrate three canonical
Raft bug scenarios.  Each scenario writes one JSONL file per node into the
output directory so they can be loaded directly by visualizer/index.html.

Usage:
  python3 scripts/generate_demo_logs.py --output-dir /tmp/demo_logs
  # Then open visualizer/index.html and load the generated .jsonl files.

Scenarios
─────────
  1. split_brain   – delayed leader heartbeat causes a follower to elect
                     itself, briefly creating two concurrent leaders.
  2. stale_read    – a follower that is lagging behind the leader serves a
                     read before it applies the latest committed entry.
  3. livelock      – two candidates start elections simultaneously with
                     identical Lamport offsets, repeatedly splitting the
                     vote until random timeouts break the tie.
"""

import json
import os
import sys
import argparse
import time

BASE_WALL = int(time.time() * 1_000_000)

def event(lts, wall_offset_ms, node_id, event_type,
          from_node, to_node, term, log_index, commit_index, role,
          **extra):
    """Return a dict representing one JSONL event."""
    e = {
        "lamport_ts":   lts,
        "wall_time_us": BASE_WALL + wall_offset_ms * 1000,
        "node_id":      node_id,
        "event_type":   event_type,
        "from_node":    from_node,
        "to_node":      to_node,
        "term":         term,
        "log_index":    log_index,
        "commit_index": commit_index,
        "role":         role,
    }
    e.update(extra)
    return e


def write_scenario(name: str, events: list[dict], out_dir: str):
    """Partition events by node_id and write one JSONL per node."""
    nodes: dict[int, list] = {}
    for e in events:
        nodes.setdefault(e["node_id"], []).append(e)
    for nid, evs in nodes.items():
        evs.sort(key=lambda e: e["lamport_ts"])
        path = os.path.join(out_dir, f"{name}_node_{nid}.jsonl")
        with open(path, "w") as f:
            for e in evs:
                f.write(json.dumps(e) + "\n")
        print(f"  Wrote {len(evs)} events → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: Split-Brain from Delayed Heartbeat
# ─────────────────────────────────────────────────────────────────────────────
def scenario_split_brain() -> list[dict]:
    """
    Node 0 is the initial leader.  Its heartbeat to Node 1 is delayed past
    Node 1's election timeout.  Node 1 starts a new election (term 2) and
    wins with Node 2's vote.  Briefly both Node 0 (term 1) and Node 1 (term 2)
    believe they are the leader – this is the split-brain window.
    Node 0 eventually receives a term-2 message and steps down.
    """
    ev = []
    # ── Phase 0: initial election, Node 0 wins ───────────────────────
    ev += [
        event(1,  100,  0, "become_candidate", 0, -1,  1, 0, 0, "candidate"),
        event(2,  110,  0, "vote_request",     0,  1,  1, 0, 0, "candidate"),
        event(3,  110,  0, "vote_request",     0,  2,  1, 0, 0, "candidate"),
        event(4,  130,  1, "vote_recv",        0,  1,  1, 0, 0, "follower"),
        event(5,  135,  1, "vote_granted",     1,  0,  1, 0, 0, "follower"),
        event(6,  130,  2, "vote_recv",        0,  2,  1, 0, 0, "follower"),
        event(7,  135,  2, "vote_granted",     2,  0,  1, 0, 0, "follower"),
        event(8,  145,  0, "vote_granted_recv",1,  0,  1, 0, 0, "candidate"),
        event(9,  147,  0, "leader_change",    0, -1,  1, 0, 0, "leader"),
        event(10, 148,  0, "vote_granted_recv",2,  0,  1, 0, 0, "leader"),
    ]
    # ── Phase 1: Node 0 replicates two commands ───────────────────────
    ev += [
        event(11, 200,  0, "log_append",       0,  1,  1, 1, 0, "leader", data="x=1"),
        event(12, 200,  0, "log_append",       0,  2,  1, 1, 0, "leader", data="x=1"),
        event(13, 230,  1, "log_append_recv",  0,  1,  1, 1, 0, "follower"),
        event(14, 230,  2, "log_append_recv",  0,  2,  1, 1, 0, "follower"),
        event(15, 260,  0, "commit",           0, -1,  1, 1, 1, "leader"),
        event(16, 265,  1, "commit",           1, -1,  1, 1, 1, "follower"),
        event(17, 265,  2, "commit",           2, -1,  1, 1, 1, "follower"),
    ]
    # ── Phase 2: SPLIT-BRAIN – heartbeat to Node 1 lost ──────────────
    # Node 0's heartbeat does NOT reach Node 1 (simulated delay > timeout).
    # Node 1 triggers election while Node 0 still thinks it is leader.
    ev += [
        # Node 1 times out and starts election (term 2)
        event(18, 550,  1, "become_candidate", 1, -1,  2, 1, 1, "candidate"),
        event(19, 560,  1, "vote_request",     1,  0,  2, 1, 1, "candidate"),
        event(20, 560,  1, "vote_request",     1,  2,  2, 1, 1, "candidate"),
        # Note: Node 0's vote_recv arrives but Node 0 is still leader at term 1
        event(21, 580,  0, "vote_recv",        1,  0,  2, 1, 1, "leader"),
        event(22, 582,  2, "vote_recv",        1,  2,  2, 1, 1, "follower"),
        event(23, 585,  2, "vote_granted",     2,  1,  2, 1, 1, "follower"),
        event(24, 590,  0, "vote_denied",      0,  1,  2, 1, 1, "follower"),  # steps down
        event(25, 590,  0, "become_follower",  0, -1,  2, 1, 1, "follower"),
        event(26, 600,  1, "vote_granted_recv",2,  1,  2, 1, 1, "candidate"),
        event(27, 602,  1, "leader_change",    1, -1,  2, 1, 1, "leader"),
        event(28, 604,  1, "vote_denied_recv", 0,  1,  2, 1, 1, "leader"),
    ]
    return ev


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: Stale Read from Lagging Follower
# ─────────────────────────────────────────────────────────────────────────────
def scenario_stale_read() -> list[dict]:
    """
    Leader (Node 0) commits a second entry (x=2) which is acked by Node 2
    but NOT yet replicated to Node 1 due to a slow network link.
    A client reads from Node 1 and observes x=1 (stale), missing x=2.
    This is invisible in per-node logs but obvious in the causal diagram:
    the commit event on Node 0 has a Lamport timestamp that causally
    PRECEDES Node 1's response to the client.
    """
    ev = []
    # Phase 0: initial election
    ev += [
        event(1,  50,  0, "become_candidate", 0, -1,  1, 0, 0, "candidate"),
        event(2,  60,  0, "vote_request",     0,  1,  1, 0, 0, "candidate"),
        event(3,  60,  0, "vote_request",     0,  2,  1, 0, 0, "candidate"),
        event(4,  80,  1, "vote_recv",        0,  1,  1, 0, 0, "follower"),
        event(5,  85,  1, "vote_granted",     1,  0,  1, 0, 0, "follower"),
        event(6,  80,  2, "vote_recv",        0,  2,  1, 0, 0, "follower"),
        event(7,  85,  2, "vote_granted",     2,  0,  1, 0, 0, "follower"),
        event(8,  95,  0, "vote_granted_recv",1,  0,  1, 0, 0, "candidate"),
        event(9,  97,  0, "leader_change",    0, -1,  1, 0, 0, "leader"),
    ]
    # Phase 1: first commit (x=1) – all nodes up to date
    ev += [
        event(10, 130,  0, "log_append",       0,  1,  1, 1, 0, "leader", data="x=1"),
        event(11, 130,  0, "log_append",       0,  2,  1, 1, 0, "leader", data="x=1"),
        event(12, 155,  1, "log_append_recv",  0,  1,  1, 1, 0, "follower"),
        event(13, 155,  2, "log_append_recv",  0,  2,  1, 1, 0, "follower"),
        event(14, 175,  0, "commit",           0, -1,  1, 1, 1, "leader"),
        event(15, 180,  1, "commit",           1, -1,  1, 1, 1, "follower"),
        event(16, 180,  2, "commit",           2, -1,  1, 1, 1, "follower"),
    ]
    # Phase 2: second commit (x=2) – Node 1 link is slow; only Node 2 acks
    # The stale-read window: commit is recorded on Node 0 at lts=22,
    # but Node 1 hasn't received the entry yet.
    ev += [
        event(17, 250,  0, "log_append",       0,  2,  1, 2, 1, "leader", data="x=2"),
        event(18, 250,  0, "log_append",       0,  1,  1, 2, 1, "leader", data="x=2"),  # slow
        event(19, 275,  2, "log_append_recv",  0,  2,  1, 2, 1, "follower"),
        # Node 2 replies → majority → commit
        event(20, 290,  0, "commit",           0, -1,  1, 2, 2, "leader"),
        event(21, 295,  2, "commit",           2, -1,  1, 2, 2, "follower"),
        # *** STALE READ WINDOW *** Node 1 still at commit_index=1 while
        # global commit is at index 2.
        # The log_append_recv finally arrives at Node 1 much later:
        event(22, 450,  1, "log_append_recv",  0,  1,  1, 2, 1, "follower"),
        event(23, 460,  1, "commit",           1, -1,  1, 2, 2, "follower"),
    ]
    return ev


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: Election Livelock
# ─────────────────────────────────────────────────────────────────────────────
def scenario_livelock() -> list[dict]:
    """
    Three-node cluster with all nodes starting at the same time.
    Nodes 0 and 1 both time out simultaneously (same random seed) and
    each votes for itself.  In term 1, each gets only 1 vote → no majority.
    They time out again in term 2 with the same collision.
    In term 3, random jitter breaks the tie: Node 0 wins alone.
    This is invisible in per-node logs (both just see "election failed"),
    but the causal diagram shows the interleaved vote_request events and
    the vote_denied responses at every step, making the collision obvious.
    """
    ev = []
    # ── Round 1: both Node 0 and Node 1 start election in term 1 ─────
    ev += [
        event(1,  300,  0, "become_candidate", 0, -1,  1, 0, 0, "candidate"),
        event(1,  300,  1, "become_candidate", 1, -1,  1, 0, 0, "candidate"),
        # Each sends vote_request to both peers
        event(2,  310,  0, "vote_request",     0,  1,  1, 0, 0, "candidate"),
        event(2,  310,  0, "vote_request",     0,  2,  1, 0, 0, "candidate"),
        event(2,  310,  1, "vote_request",     1,  0,  1, 0, 0, "candidate"),
        event(2,  310,  1, "vote_request",     1,  2,  1, 0, 0, "candidate"),
        # Node 2 receives both requests but grants vote to the first (Node 0)
        event(3,  330,  2, "vote_recv",        0,  2,  1, 0, 0, "follower"),
        event(4,  332,  2, "vote_granted",     2,  0,  1, 0, 0, "follower"),
        event(5,  335,  2, "vote_recv",        1,  2,  1, 0, 0, "follower"),
        event(6,  337,  2, "vote_denied",      2,  1,  1, 0, 0, "follower"),  # already voted
        # Node 0 receives Node 1's vote_request → already candidate → deny
        event(3,  330,  0, "vote_recv",        1,  0,  1, 0, 0, "candidate"),
        event(4,  332,  0, "vote_denied",      0,  1,  1, 0, 0, "candidate"),
        # Node 1 receives Node 0's vote_request → already candidate → deny
        event(3,  330,  1, "vote_recv",        0,  1,  1, 0, 0, "candidate"),
        event(4,  332,  1, "vote_denied",      1,  0,  1, 0, 0, "candidate"),
        # Vote replies arrive
        event(7,  345,  0, "vote_granted_recv",2,  0,  1, 0, 0, "candidate"),
        event(7,  345,  1, "vote_denied_recv", 2,  1,  1, 0, 0, "candidate"),
        event(8,  350,  0, "vote_denied_recv", 1,  0,  1, 0, 0, "candidate"),
        event(8,  350,  1, "vote_denied_recv", 0,  1,  1, 0, 0, "candidate"),
        # No majority! Node 0 has 2 (self + Node 2), wait – Node 0 wins!
        # Actually with 3 nodes, 2 votes = majority.
    ]
    # On reflection: with 3 nodes, Node 0 gets 2 votes (self + Node 2) and
    # should win.  Let's model an even more pathological 4-node cluster:
    # Nodes 0 and 1 collide; each gets 2 votes out of 4 → tie.
    # (We add Node 3 to create a true 4-node cluster.)
    ev = []  # restart with 4 nodes

    ev += [
        # ── Round 1 (term 1): Nodes 0 and 1 collide ─────────────────
        event(1,  300,  0, "become_candidate", 0, -1,  1, 0, 0, "candidate"),
        event(1,  300,  1, "become_candidate", 1, -1,  1, 0, 0, "candidate"),
        event(2,  310,  0, "vote_request",     0,  1,  1, 0, 0, "candidate"),
        event(2,  310,  0, "vote_request",     0,  2,  1, 0, 0, "candidate"),
        event(2,  310,  0, "vote_request",     0,  3,  1, 0, 0, "candidate"),
        event(2,  310,  1, "vote_request",     1,  0,  1, 0, 0, "candidate"),
        event(2,  310,  1, "vote_request",     1,  2,  1, 0, 0, "candidate"),
        event(2,  310,  1, "vote_request",     1,  3,  1, 0, 0, "candidate"),
        # Node 2 votes for Node 0; Node 3 votes for Node 1 (arrived first)
        event(3,  330,  2, "vote_recv",        0,  2,  1, 0, 0, "follower"),
        event(4,  335,  2, "vote_granted",     2,  0,  1, 0, 0, "follower"),
        event(5,  331,  3, "vote_recv",        1,  3,  1, 0, 0, "follower"),
        event(6,  336,  3, "vote_granted",     3,  1,  1, 0, 0, "follower"),
        event(5,  331,  2, "vote_recv",        1,  2,  1, 0, 0, "follower"),
        event(6,  337,  2, "vote_denied",      2,  1,  1, 0, 0, "follower"),
        event(5,  330,  3, "vote_recv",        0,  3,  1, 0, 0, "follower"),
        event(6,  336,  3, "vote_denied",      3,  0,  1, 0, 0, "follower"),
        # Cross-vote denials between candidates
        event(3,  328,  0, "vote_recv",        1,  0,  1, 0, 0, "candidate"),
        event(4,  330,  0, "vote_denied",      0,  1,  1, 0, 0, "candidate"),
        event(3,  328,  1, "vote_recv",        0,  1,  1, 0, 0, "candidate"),
        event(4,  330,  1, "vote_denied",      1,  0,  1, 0, 0, "candidate"),
        # Vote results: 0 gets 2 (self+2), 1 gets 2 (self+3) → TIE, no majority
        event(7,  350,  0, "vote_granted_recv",2,  0,  1, 0, 0, "candidate"),
        event(8,  352,  0, "vote_denied_recv", 3,  0,  1, 0, 0, "candidate"),
        event(8,  352,  0, "vote_denied_recv", 1,  0,  1, 0, 0, "candidate"),
        event(7,  350,  1, "vote_granted_recv",3,  1,  1, 0, 0, "candidate"),
        event(8,  352,  1, "vote_denied_recv", 2,  1,  1, 0, 0, "candidate"),
        event(8,  352,  1, "vote_denied_recv", 0,  1,  1, 0, 0, "candidate"),

        # ── Round 2 (term 2): same collision again ───────────────────
        event(9,  650,  0, "become_candidate", 0, -1,  2, 0, 0, "candidate"),
        event(9,  651,  1, "become_candidate", 1, -1,  2, 0, 0, "candidate"),
        event(10, 660,  0, "vote_request",     0,  1,  2, 0, 0, "candidate"),
        event(10, 660,  0, "vote_request",     0,  2,  2, 0, 0, "candidate"),
        event(10, 660,  0, "vote_request",     0,  3,  2, 0, 0, "candidate"),
        event(10, 661,  1, "vote_request",     1,  0,  2, 0, 0, "candidate"),
        event(10, 661,  1, "vote_request",     1,  2,  2, 0, 0, "candidate"),
        event(10, 661,  1, "vote_request",     1,  3,  2, 0, 0, "candidate"),
        event(11, 680,  2, "vote_recv",        0,  2,  2, 0, 0, "follower"),
        event(12, 685,  2, "vote_granted",     2,  0,  2, 0, 0, "follower"),
        event(11, 680,  3, "vote_recv",        1,  3,  2, 0, 0, "follower"),
        event(12, 685,  3, "vote_granted",     3,  1,  2, 0, 0, "follower"),
        event(13, 686,  2, "vote_recv",        1,  2,  2, 0, 0, "follower"),
        event(14, 688,  2, "vote_denied",      2,  1,  2, 0, 0, "follower"),
        event(13, 686,  3, "vote_recv",        0,  3,  2, 0, 0, "follower"),
        event(14, 688,  3, "vote_denied",      3,  0,  2, 0, 0, "follower"),
        event(11, 678,  0, "vote_recv",        1,  0,  2, 0, 0, "candidate"),
        event(12, 680,  0, "vote_denied",      0,  1,  2, 0, 0, "candidate"),
        event(11, 678,  1, "vote_recv",        0,  1,  2, 0, 0, "candidate"),
        event(12, 680,  1, "vote_denied",      1,  0,  2, 0, 0, "candidate"),
        event(15, 700,  0, "vote_granted_recv",2,  0,  2, 0, 0, "candidate"),
        event(16, 702,  0, "vote_denied_recv", 3,  0,  2, 0, 0, "candidate"),
        event(16, 702,  0, "vote_denied_recv", 1,  0,  2, 0, 0, "candidate"),
        event(15, 700,  1, "vote_granted_recv",3,  1,  2, 0, 0, "candidate"),
        event(16, 702,  1, "vote_denied_recv", 2,  1,  2, 0, 0, "candidate"),
        event(16, 702,  1, "vote_denied_recv", 0,  1,  2, 0, 0, "candidate"),

        # ── Round 3 (term 3): jitter breaks the tie – Node 0 wins ───
        event(17, 1010,  0, "become_candidate", 0, -1,  3, 0, 0, "candidate"),
        # Node 1 is slightly later this time
        event(17, 1010,  0, "vote_request",     0,  1,  3, 0, 0, "candidate"),
        event(17, 1010,  0, "vote_request",     0,  2,  3, 0, 0, "candidate"),
        event(17, 1010,  0, "vote_request",     0,  3,  3, 0, 0, "candidate"),
        event(18, 1030,  1, "vote_recv",        0,  1,  3, 0, 0, "follower"),
        event(19, 1035,  1, "vote_granted",     1,  0,  3, 0, 0, "follower"),
        event(18, 1030,  2, "vote_recv",        0,  2,  3, 0, 0, "follower"),
        event(19, 1035,  2, "vote_granted",     2,  0,  3, 0, 0, "follower"),
        event(18, 1030,  3, "vote_recv",        0,  3,  3, 0, 0, "follower"),
        event(19, 1035,  3, "vote_granted",     3,  0,  3, 0, 0, "follower"),
        event(20, 1050,  0, "vote_granted_recv",1,  0,  3, 0, 0, "candidate"),
        event(21, 1052,  0, "leader_change",    0, -1,  3, 0, 0, "leader"),
        event(22, 1055,  0, "vote_granted_recv",2,  0,  3, 0, 0, "leader"),
        event(22, 1055,  0, "vote_granted_recv",3,  0,  3, 0, 0, "leader"),
        # Node 1 starts its own election too late – gets DENIED everywhere
        event(22, 1060,  1, "become_candidate", 1, -1,  3, 0, 0, "candidate"),
        event(23, 1070,  1, "vote_request",     1,  0,  3, 0, 0, "candidate"),
        event(24, 1080,  0, "vote_recv",        1,  0,  3, 0, 0, "leader"),
        event(25, 1082,  0, "vote_denied",      0,  1,  3, 0, 0, "leader"),
        event(26, 1090,  1, "vote_denied_recv", 0,  1,  3, 0, 0, "candidate"),
        event(27, 1095,  1, "become_follower",  1, -1,  3, 0, 0, "follower"),
    ]
    return ev


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-dir", default="demo_logs",
                        help="Directory to write JSONL files into")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    scenarios = [
        ("split_brain",  scenario_split_brain,  "Split-Brain from Delayed Heartbeat"),
        ("stale_read",   scenario_stale_read,   "Stale Read from Lagging Follower"),
        ("livelock",     scenario_livelock,     "Election Livelock"),
    ]

    for name, fn, desc in scenarios:
        print(f"\n[{name}] {desc}")
        events = fn()
        write_scenario(name, events, args.output_dir)

    print(f"\nDone.  Open visualizer/index.html and load files from {args.output_dir}/")


if __name__ == "__main__":
    main()
