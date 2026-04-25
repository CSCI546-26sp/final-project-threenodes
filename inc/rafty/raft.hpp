#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "rafty/raft_scope.hpp"
#include "toolings/msg_queue.hpp"

// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

class Raft : public raftpb::RaftService::Service {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run(); /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const; /* lab 2 */

  // lab3: sync propose
  ProposalResult propose_sync(const std::string &data);

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

  // gRPC service overrides (AppendEntries / RequestVote)
  grpc::Status AppendEntries(grpc::ServerContext *ctx,
                             const raftpb::AppendEntriesArgs *req,
                             raftpb::AppendEntriesReply *resp) override;

  grpc::Status RequestVote(grpc::ServerContext *ctx,
                           const raftpb::RequestVoteArgs *req,
                           raftpb::RequestVoteReply *resp) override;

private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;

  // ----- Raft consensus state (all guarded by mtx unless noted) -----

  enum class Role : uint8_t { Follower = 0, Candidate = 1, Leader = 2 };

  struct RaftEntry {
    uint64_t    term;
    uint64_t    index;
    std::string data;
  };

  Role     role_;
  uint64_t current_term_;
  int64_t  voted_for_; // -1 means "none"

  std::vector<RaftEntry> log_; // log_[0] is sentinel {term=0, index=0, data=""}

  uint64_t commit_index_;
  uint64_t last_applied_;

  // Leader-only: reinitialized on becoming leader
  std::unordered_map<uint64_t, uint64_t> next_index_;
  std::unordered_map<uint64_t, uint64_t> match_index_;

  // Election timer
  std::chrono::steady_clock::time_point last_heartbeat_;
  uint64_t election_timeout_ms_;
  std::mt19937 rng_;

  // Background threads
  std::thread election_thread_;
  std::thread heartbeat_thread_;
  std::thread apply_thread_;

  // Condition variables
  std::condition_variable apply_cv_;    // signaled when commit_index_ advances
  std::condition_variable heartbeat_cv_; // signaled to wake heartbeat thread
  std::mutex              heartbeat_mtx_;

  // RaftScope instrumentation
  std::unique_ptr<RaftEventLogger> scope_;

  // ----- Private helpers -----
  void become_follower_locked(uint64_t term);
  void become_leader_locked();
  void reset_election_timeout_locked();
  bool is_log_up_to_date_locked(uint64_t last_idx, uint64_t last_term) const;
  uint64_t last_log_index_locked() const;
  uint64_t last_log_term_locked() const;
  void try_advance_commit_index_locked();

  void run_election_timer();
  void run_heartbeat();
  void run_apply();
  void send_append_entries_to(uint64_t peer_id, uint64_t my_term);
};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
