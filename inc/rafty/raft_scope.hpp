#pragma once

// RaftScope: instrumentation layer for causal log collection.
//
// Each Raft node creates a RaftEventLogger that writes newline-delimited JSON
// (JSONL) events to logs/raft_scope_node_<id>.jsonl.  Every event is tagged
// with a Lamport timestamp so the visualizer can reconstruct causal ordering
// across nodes.
//
// Event types captured (heartbeats / empty AppendEntries intentionally omitted):
//   vote_request   – candidate sends RequestVote to a peer
//   vote_recv      – peer receives RequestVote (always before vote decision)
//   vote_granted   – peer decides to grant the vote (sent back to candidate)
//   vote_denied    – peer decides to deny the vote  (sent back to candidate)
//   vote_granted_recv / vote_denied_recv – candidate receives the decision
//   log_append     – leader sends entries to a follower
//   log_append_recv – follower receives entries
//   commit         – node advances its commitIndex
//   leader_change  – a node becomes leader
//   become_follower – a node steps down to follower
//   become_candidate – a node starts an election
//   node_crash     – kill() is called on a node
//   partition_start / partition_end – network partition events

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>

namespace rafty {

// ---------------------------------------------------------------------------
// Lamport clock
// ---------------------------------------------------------------------------
class LamportClock {
public:
  LamportClock() : time_(0) {}

  // Tick for a local event or outgoing message: returns the new timestamp.
  uint64_t tick() { return ++time_; }

  // Update upon receiving a message: time = max(local, received) + 1.
  uint64_t update(uint64_t received) {
    uint64_t expected = time_.load();
    uint64_t desired;
    do {
      desired = std::max(expected, received) + 1;
    } while (!time_.compare_exchange_weak(expected, desired));
    return desired;
  }

  uint64_t get() const { return time_.load(); }

private:
  std::atomic<uint64_t> time_;
};

// ---------------------------------------------------------------------------
// Event type enum and string conversion
// ---------------------------------------------------------------------------
enum class RaftEventType : uint8_t {
  VOTE_REQUEST,
  VOTE_RECV,
  VOTE_GRANTED,
  VOTE_DENIED,
  VOTE_GRANTED_RECV,
  VOTE_DENIED_RECV,
  LOG_APPEND,
  LOG_APPEND_RECV,
  COMMIT,
  LEADER_CHANGE,
  BECOME_FOLLOWER,
  BECOME_CANDIDATE,
  NODE_CRASH,
  PARTITION_START,
  PARTITION_END,
};

inline const char *event_type_str(RaftEventType t) {
  switch (t) {
  case RaftEventType::VOTE_REQUEST:      return "vote_request";
  case RaftEventType::VOTE_RECV:         return "vote_recv";
  case RaftEventType::VOTE_GRANTED:      return "vote_granted";
  case RaftEventType::VOTE_DENIED:       return "vote_denied";
  case RaftEventType::VOTE_GRANTED_RECV: return "vote_granted_recv";
  case RaftEventType::VOTE_DENIED_RECV:  return "vote_denied_recv";
  case RaftEventType::LOG_APPEND:        return "log_append";
  case RaftEventType::LOG_APPEND_RECV:   return "log_append_recv";
  case RaftEventType::COMMIT:            return "commit";
  case RaftEventType::LEADER_CHANGE:     return "leader_change";
  case RaftEventType::BECOME_FOLLOWER:   return "become_follower";
  case RaftEventType::BECOME_CANDIDATE:  return "become_candidate";
  case RaftEventType::NODE_CRASH:        return "node_crash";
  case RaftEventType::PARTITION_START:   return "partition_start";
  case RaftEventType::PARTITION_END:     return "partition_end";
  }
  return "unknown";
}

inline const char *role_str(int role) {
  switch (role) {
  case 0: return "follower";
  case 1: return "candidate";
  case 2: return "leader";
  }
  return "unknown";
}

// ---------------------------------------------------------------------------
// RaftEventLogger – writes JSONL events to disk
// ---------------------------------------------------------------------------
class RaftEventLogger {
public:
  explicit RaftEventLogger(uint64_t node_id,
                           const std::string &log_dir = "logs")
      : node_id_(node_id) {
    std::string path =
        log_dir + "/raft_scope_node_" + std::to_string(node_id) + ".jsonl";
    file_.open(path, std::ios::out | std::ios::trunc);
  }

  ~RaftEventLogger() {
    if (file_.is_open()) {
      file_.flush();
      file_.close();
    }
  }

  // --- Local event (no message; from_node == to_node == node_id) ---
  void log_local(RaftEventType type, uint64_t term, uint64_t log_index,
                 uint64_t commit_index, int role,
                 const std::string &extra = "") {
    uint64_t ts = clock_.tick();
    write(ts, type, static_cast<int64_t>(node_id_),
          static_cast<int64_t>(node_id_), -1LL, term, log_index,
          commit_index, role, extra);
  }

  // --- Outgoing message: returns the Lamport ts to embed in the RPC ---
  uint64_t log_send(RaftEventType type, uint64_t to_node, uint64_t term,
                    uint64_t log_index, uint64_t commit_index, int role,
                    const std::string &extra = "") {
    uint64_t ts = clock_.tick();
    write(ts, type, static_cast<int64_t>(node_id_),
          static_cast<int64_t>(node_id_), static_cast<int64_t>(to_node),
          term, log_index, commit_index, role, extra);
    return ts;
  }

  // --- Incoming message: updates clock from the received timestamp ---
  void log_recv(RaftEventType type, uint64_t from_node, uint64_t recv_ts,
                uint64_t term, uint64_t log_index, uint64_t commit_index,
                int role, const std::string &extra = "") {
    uint64_t ts = clock_.update(recv_ts);
    write(ts, type, static_cast<int64_t>(node_id_),
          static_cast<int64_t>(from_node), static_cast<int64_t>(node_id_),
          term, log_index, commit_index, role, extra);
  }

  LamportClock &clock() { return clock_; }

private:
  void write(uint64_t lamport_ts, RaftEventType type, int64_t node_id,
             int64_t from_node, int64_t to_node, uint64_t term,
             uint64_t log_index, uint64_t commit_index, int role,
             const std::string &extra) {
    auto now = std::chrono::system_clock::now();
    int64_t wall_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          now.time_since_epoch())
                          .count();

    std::lock_guard<std::mutex> lk(mtx_);
    if (!file_.is_open())
      return;

    file_ << '{'
          << "\"lamport_ts\":"   << lamport_ts    << ','
          << "\"wall_time_us\":" << wall_us        << ','
          << "\"node_id\":"      << node_id        << ','
          << "\"event_type\":\""  << event_type_str(type) << "\","
          << "\"from_node\":"    << from_node      << ','
          << "\"to_node\":"      << to_node        << ','
          << "\"term\":"         << term           << ','
          << "\"log_index\":"    << log_index      << ','
          << "\"commit_index\":" << commit_index   << ','
          << "\"role\":\""       << role_str(role) << '"';

    if (!extra.empty())
      file_ << ',' << extra;

    file_ << "}\n";
    file_.flush();
  }

  uint64_t      node_id_;
  LamportClock  clock_;
  std::ofstream file_;
  std::mutex    mtx_;
};

} // namespace rafty
