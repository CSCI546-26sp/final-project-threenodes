//
// raft.cpp – complete Raft consensus implementation (Labs 1-3)
//
// Design overview
// ───────────────
// Three long-lived detached threads are started by run():
//   election_thread_  – fires elections when the heartbeat timer expires
//   heartbeat_thread_ – sends AppendEntries RPCs on a fixed interval (leader)
//   apply_thread_     – applies committed log entries to the state machine
//
// All Raft state is protected by `mtx` (declared in raft.hpp).
// RPC handlers acquire `mtx` for the duration of their execution.
// Background threads hold `mtx` only while reading/mutating state; they
// release it before any outgoing gRPC call to avoid blocking the server.
//
// Lamport timestamps are maintained by `scope_` (RaftEventLogger).
// Every non-heartbeat outgoing RPC embeds its Lamport timestamp in the
// request; every incoming handler updates the local clock from it.
//

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

// ============================================================
// Constructor / Destructor
// ============================================================

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)),
      id(config.id),
      listening_addr(config.addr),
      peer_addrs(config.peer_addrs),
      dead(false),
      ready_queue(ready),
      role_(Role::Follower),
      current_term_(0),
      voted_for_(-1),
      commit_index_(0),
      last_applied_(0) {
  // Sentinel entry: index 0, term 0.  Real entries begin at index 1.
  log_.push_back({0, 0, ""});

  // Seed RNG deterministically per-node so elections don't always collide.
  std::random_device rd;
  rng_ = std::mt19937(rd() ^ (config.id * 2654435761ULL));
  reset_election_timeout_locked();
  last_heartbeat_ = std::chrono::steady_clock::now();

  // Create the RaftScope event logger (writes to logs/ directory).
  scope_ = std::make_unique<RaftEventLogger>(config.id);
}

Raft::~Raft() { this->stop_server(); }

// ============================================================
// Public API
// ============================================================

void Raft::run() {
  election_thread_  = std::thread([this] { run_election_timer(); });
  heartbeat_thread_ = std::thread([this] { run_heartbeat(); });
  apply_thread_     = std::thread([this] { run_apply(); });
  election_thread_.detach();
  heartbeat_thread_.detach();
  apply_thread_.detach();
}

State Raft::get_state() const {
  std::lock_guard<std::mutex> lk(mtx);
  return {current_term_, role_ == Role::Leader};
}

ProposalResult Raft::propose(const std::string &data) {
  std::lock_guard<std::mutex> lk(mtx);
  if (role_ != Role::Leader || is_dead()) {
    return {0, 0, false};
  }

  uint64_t idx  = static_cast<uint64_t>(log_.size());
  uint64_t term = current_term_;
  log_.push_back({term, idx, data});
  match_index_[id] = idx;

  scope_->log_local(RaftEventType::LOG_APPEND, term, idx, commit_index_, 2);

  // Wake the heartbeat thread so entries are replicated without waiting the
  // full 50 ms interval.
  heartbeat_cv_.notify_all();

  return {idx, term, true};
}

ProposalResult Raft::propose_sync(const std::string &data) {
  auto result = propose(data);
  if (!result.is_leader) {
    return result;
  }

  uint64_t target_idx  = result.index;
  uint64_t target_term = result.term;

  std::unique_lock<std::mutex> lk(mtx);
  apply_cv_.wait(lk, [&] {
    return is_dead() ||
           last_applied_ >= target_idx ||
           current_term_ != target_term ||
           role_ != Role::Leader;
  });

  if (is_dead() || current_term_ != target_term || role_ != Role::Leader) {
    return {0, 0, false};
  }
  return {target_idx, target_term, true};
}

// ============================================================
// RPC Handlers (implement raftpb::RaftService::Service)
// ============================================================

grpc::Status Raft::AppendEntries(grpc::ServerContext *ctx,
                                  const raftpb::AppendEntriesArgs *req,
                                  raftpb::AppendEntriesReply *resp) {
  (void)ctx;
  std::lock_guard<std::mutex> lk(mtx);

  if (is_dead()) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "dead");
  }

  // Update Lamport clock from incoming message.
  scope_->clock().update(req->lamport_ts());

  uint64_t sender_term = req->term();

  // Rule 1: reject stale leader.
  if (sender_term < current_term_) {
    resp->set_term(current_term_);
    resp->set_success(false);
    resp->set_lamport_ts(scope_->clock().tick());
    return grpc::Status::OK;
  }

  // Any valid AppendEntries resets the election timer.
  last_heartbeat_ = std::chrono::steady_clock::now();
  reset_election_timeout_locked();

  // Step down if we see a higher term or we're a stale candidate/leader.
  if (sender_term > current_term_ || role_ != Role::Follower) {
    become_follower_locked(sender_term);
  }

  uint64_t prev_log_index = req->prev_log_index();
  uint64_t prev_log_term  = req->prev_log_term();
  bool     has_entries    = req->entries_size() > 0;

  // Rule 2: log consistency check.
  if (prev_log_index > 0) {
    if (prev_log_index >= log_.size()) {
      // Our log is too short.
      resp->set_term(current_term_);
      resp->set_success(false);
      resp->set_conflict_term(0);
      resp->set_conflict_index(static_cast<uint64_t>(log_.size()));
      resp->set_lamport_ts(scope_->clock().tick());
      return grpc::Status::OK;
    }
    if (log_[prev_log_index].term != prev_log_term) {
      // Conflicting term: help the leader skip the whole conflicting term.
      uint64_t ct = log_[prev_log_index].term;
      uint64_t ci = prev_log_index;
      while (ci > 0 && log_[ci - 1].term == ct)
        ci--;
      resp->set_term(current_term_);
      resp->set_success(false);
      resp->set_conflict_term(ct);
      resp->set_conflict_index(ci);
      resp->set_lamport_ts(scope_->clock().tick());
      return grpc::Status::OK;
    }
  }

  // Rules 3-5: merge incoming entries into local log.
  for (int i = 0; i < req->entries_size(); i++) {
    const auto &e   = req->entries(i);
    uint64_t    idx = e.index();
    if (idx < log_.size()) {
      if (log_[idx].term != e.term()) {
        // Truncate conflicting suffix.
        log_.resize(idx);
        log_.push_back({e.term(), idx, std::string(e.data())});
      }
      // Otherwise the entry is already present; skip.
    } else {
      log_.push_back({e.term(), idx, std::string(e.data())});
    }
  }

  // Log receive event only for real entries (heartbeats are skipped).
  if (has_entries) {
    uint64_t last_new =
        static_cast<uint64_t>(req->entries(req->entries_size() - 1).index());
    scope_->log_recv(RaftEventType::LOG_APPEND_RECV, req->leader_id(),
                     req->lamport_ts(), current_term_, last_new,
                     commit_index_, 0);
  }

  // Rule 6: advance commitIndex.
  if (req->leader_commit() > commit_index_) {
    uint64_t last_new_idx = prev_log_index + static_cast<uint64_t>(req->entries_size());
    uint64_t new_ci = std::min(req->leader_commit(), last_new_idx);
    if (new_ci > commit_index_) {
      commit_index_ = new_ci;
      apply_cv_.notify_all();
    }
  }

  resp->set_term(current_term_);
  resp->set_success(true);
  resp->set_lamport_ts(scope_->clock().tick());
  return grpc::Status::OK;
}

grpc::Status Raft::RequestVote(grpc::ServerContext *ctx,
                                const raftpb::RequestVoteArgs *req,
                                raftpb::RequestVoteReply *resp) {
  (void)ctx;
  std::lock_guard<std::mutex> lk(mtx);

  if (is_dead()) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "dead");
  }

  // Update Lamport clock.
  scope_->clock().update(req->lamport_ts());

  uint64_t candidate_term = req->term();
  uint64_t candidate_id   = req->candidate_id();

  // Log receipt of the vote request.
  scope_->log_recv(RaftEventType::VOTE_RECV, candidate_id, req->lamport_ts(),
                   current_term_, last_log_index_locked(), commit_index_,
                   static_cast<int>(role_));

  // Reject stale candidates.
  if (candidate_term < current_term_) {
    resp->set_term(current_term_);
    resp->set_vote_granted(false);
    resp->set_lamport_ts(
        scope_->log_send(RaftEventType::VOTE_DENIED, candidate_id,
                         current_term_, last_log_index_locked(),
                         commit_index_, static_cast<int>(role_)));
    return grpc::Status::OK;
  }

  // Step down if we see a higher term.
  if (candidate_term > current_term_) {
    become_follower_locked(candidate_term);
  }

  bool can_vote = (voted_for_ == -1 ||
                   voted_for_ == static_cast<int64_t>(candidate_id));
  bool log_ok   = is_log_up_to_date_locked(req->last_log_index(),
                                            req->last_log_term());
  bool grant    = can_vote && log_ok;

  if (grant) {
    voted_for_ = static_cast<int64_t>(candidate_id);
    // Granting a vote counts as receiving a heartbeat (resets election timer).
    last_heartbeat_ = std::chrono::steady_clock::now();
    reset_election_timeout_locked();

    resp->set_lamport_ts(
        scope_->log_send(RaftEventType::VOTE_GRANTED, candidate_id,
                         current_term_, last_log_index_locked(),
                         commit_index_, static_cast<int>(role_)));
  } else {
    resp->set_lamport_ts(
        scope_->log_send(RaftEventType::VOTE_DENIED, candidate_id,
                         current_term_, last_log_index_locked(),
                         commit_index_, static_cast<int>(role_)));
  }

  resp->set_term(current_term_);
  resp->set_vote_granted(grant);
  return grpc::Status::OK;
}

// ============================================================
// Private helpers (all called while holding mtx)
// ============================================================

void Raft::become_follower_locked(uint64_t term) {
  scope_->log_local(RaftEventType::BECOME_FOLLOWER, term,
                    last_log_index_locked(), commit_index_,
                    static_cast<int>(role_));
  role_      = Role::Follower;
  current_term_ = term;
  voted_for_ = -1;
}

void Raft::become_leader_locked() {
  if (role_ != Role::Candidate)
    return;
  role_ = Role::Leader;

  uint64_t last_idx = last_log_index_locked();
  for (auto &[pid, _] : peer_addrs) {
    next_index_[pid]  = last_idx + 1;
    match_index_[pid] = 0;
  }
  match_index_[id] = last_idx;

  scope_->log_local(RaftEventType::LEADER_CHANGE, current_term_,
                    last_idx, commit_index_, 2);

  // Send immediate heartbeats.
  heartbeat_cv_.notify_all();
}

void Raft::reset_election_timeout_locked() {
  std::uniform_int_distribution<uint64_t> dist(250, 500);
  election_timeout_ms_ = dist(rng_);
}

bool Raft::is_log_up_to_date_locked(uint64_t last_idx,
                                     uint64_t last_term) const {
  uint64_t my_last_idx  = last_log_index_locked();
  uint64_t my_last_term = last_log_term_locked();
  if (last_term != my_last_term)
    return last_term > my_last_term;
  return last_idx >= my_last_idx;
}

uint64_t Raft::last_log_index_locked() const {
  return static_cast<uint64_t>(log_.size()) - 1;
}

uint64_t Raft::last_log_term_locked() const { return log_.back().term; }

void Raft::try_advance_commit_index_locked() {
  // Find the highest N > commitIndex where:
  //   log[N].term == currentTerm  AND  majority of matchIndex[i] >= N
  uint64_t total = static_cast<uint64_t>(peer_addrs.size()) + 1;

  for (uint64_t n = last_log_index_locked(); n > commit_index_; n--) {
    if (n >= log_.size())
      continue;
    if (log_[n].term != current_term_)
      continue;

    uint64_t count = 1; // count self
    for (auto &[pid, _] : peer_addrs) {
      if (match_index_.count(pid) && match_index_.at(pid) >= n)
        count++;
    }

    if (count * 2 > total) {
      commit_index_ = n;
      scope_->log_local(RaftEventType::COMMIT, current_term_, n,
                        commit_index_, 2);
      apply_cv_.notify_all();
      break;
    }
  }
}

// ============================================================
// Background threads
// ============================================================

void Raft::run_election_timer() {
  while (!is_dead()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (is_dead())
      break;

    bool   should_elect = false;
    uint64_t my_term, last_idx, last_term_val;

    {
      std::lock_guard<std::mutex> lk(mtx);
      if (is_dead() || role_ == Role::Leader)
        continue;

      auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - last_heartbeat_)
              .count();

      if (static_cast<uint64_t>(elapsed) >= election_timeout_ms_) {
        // Become candidate atomically under the lock.
        current_term_++;
        role_         = Role::Candidate;
        voted_for_    = static_cast<int64_t>(id);
        last_heartbeat_ = std::chrono::steady_clock::now();
        reset_election_timeout_locked();

        my_term      = current_term_;
        last_idx     = last_log_index_locked();
        last_term_val = last_log_term_locked();

        scope_->log_local(RaftEventType::BECOME_CANDIDATE, my_term,
                          last_idx, commit_index_, 1);
        should_elect = true;
      }
    }

    if (!should_elect)
      continue;

    // Send RequestVote to all peers in parallel, each in its own thread.
    size_t total    = peer_addrs.size() + 1;
    size_t majority = total / 2 + 1;

    auto vote_count    = std::make_shared<std::atomic<int>>(1); // self-vote
    auto became_leader = std::make_shared<std::atomic<bool>>(false);

    for (auto &[peer_id, _] : peer_addrs) {
      std::thread([this, peer_id, my_term, last_idx, last_term_val,
                   vote_count, majority, became_leader]() mutable {
        if (is_dead())
          return;

        raftpb::RequestVoteArgs args;
        uint64_t send_ts;
        {
          std::lock_guard<std::mutex> lk(mtx);
          if (is_dead() || role_ != Role::Candidate || current_term_ != my_term)
            return;
          send_ts = scope_->log_send(RaftEventType::VOTE_REQUEST, peer_id,
                                     my_term, last_idx, commit_index_, 1);
        }
        args.set_term(my_term);
        args.set_candidate_id(id);
        args.set_last_log_index(last_idx);
        args.set_last_log_term(last_term_val);
        args.set_lamport_ts(send_ts);

        raftpb::RequestVoteReply reply;
        auto ctx = create_context(peer_id);
        ctx->set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds(200));

        auto status = peers_[peer_id]->RequestVote(ctx.get(), args, &reply);
        if (!status.ok() || is_dead())
          return;

        // Process the reply under the lock.
        std::lock_guard<std::mutex> lk(mtx);
        if (is_dead() || role_ != Role::Candidate || current_term_ != my_term)
          return;

        scope_->clock().update(reply.lamport_ts());

        if (reply.term() > current_term_) {
          become_follower_locked(reply.term());
          return;
        }

        if (reply.vote_granted()) {
          scope_->log_recv(RaftEventType::VOTE_GRANTED_RECV, peer_id,
                           reply.lamport_ts(), current_term_,
                           last_log_index_locked(), commit_index_, 1);
          int cnt = ++(*vote_count);
          if (cnt >= static_cast<int>(majority) &&
              !became_leader->exchange(true)) {
            become_leader_locked();
          }
        } else {
          scope_->log_recv(RaftEventType::VOTE_DENIED_RECV, peer_id,
                           reply.lamport_ts(), current_term_,
                           last_log_index_locked(), commit_index_, 1);
        }
      }).detach();
    }
  }
}

void Raft::run_heartbeat() {
  while (!is_dead()) {
    {
      std::unique_lock<std::mutex> lk(heartbeat_mtx_);
      heartbeat_cv_.wait_for(lk, std::chrono::milliseconds(50));
    }
    if (is_dead())
      break;

    uint64_t my_term;
    bool     is_leader;
    std::vector<uint64_t> peer_ids;

    {
      std::lock_guard<std::mutex> lk(mtx);
      is_leader = (role_ == Role::Leader);
      my_term   = current_term_;
      for (auto &[pid, _] : peer_addrs)
        peer_ids.push_back(pid);
    }

    if (!is_leader)
      continue;

    for (auto pid : peer_ids) {
      std::thread([this, pid, my_term] {
        send_append_entries_to(pid, my_term);
      }).detach();
    }
  }
}

void Raft::send_append_entries_to(uint64_t peer_id, uint64_t my_term) {
  if (is_dead())
    return;

  raftpb::AppendEntriesArgs args;
  bool is_heartbeat = true;

  {
    std::lock_guard<std::mutex> lk(mtx);
    if (is_dead() || role_ != Role::Leader || current_term_ != my_term)
      return;

    if (!next_index_.count(peer_id))
      next_index_[peer_id] = last_log_index_locked() + 1;

    uint64_t ni              = next_index_.at(peer_id);
    uint64_t prev_log_index  = ni - 1;
    uint64_t prev_log_term   =
        (prev_log_index < log_.size()) ? log_[prev_log_index].term : 0;

    args.set_term(my_term);
    args.set_leader_id(id);
    args.set_prev_log_index(prev_log_index);
    args.set_prev_log_term(prev_log_term);
    args.set_leader_commit(commit_index_);

    if (ni < log_.size()) {
      is_heartbeat = false;
      for (uint64_t i = ni; i < log_.size(); i++) {
        auto *e = args.add_entries();
        e->set_term(log_[i].term);
        e->set_index(i);
        e->set_data(log_[i].data);
      }
    }

    // Embed Lamport timestamp (log only non-heartbeat sends).
    uint64_t ts;
    if (!is_heartbeat) {
      ts = scope_->log_send(RaftEventType::LOG_APPEND, peer_id, my_term,
                            last_log_index_locked(), commit_index_, 2);
    } else {
      ts = scope_->clock().tick(); // advance clock but don't write event
    }
    args.set_lamport_ts(ts);
  }

  raftpb::AppendEntriesReply reply;
  auto ctx = create_context(peer_id);
  ctx->set_deadline(std::chrono::system_clock::now() +
                    std::chrono::milliseconds(100));

  auto status = peers_[peer_id]->AppendEntries(ctx.get(), args, &reply);
  if (!status.ok() || is_dead())
    return;

  std::lock_guard<std::mutex> lk(mtx);
  if (is_dead() || role_ != Role::Leader || current_term_ != my_term)
    return;

  scope_->clock().update(reply.lamport_ts());

  if (reply.term() > current_term_) {
    become_follower_locked(reply.term());
    return;
  }

  if (reply.success()) {
    uint64_t last_sent =
        args.prev_log_index() + static_cast<uint64_t>(args.entries_size());
    if (!match_index_.count(peer_id) || last_sent > match_index_.at(peer_id)) {
      match_index_[peer_id] = last_sent;
    }
    next_index_[peer_id] = match_index_.at(peer_id) + 1;
    try_advance_commit_index_locked();
  } else {
    // Fast-backup: skip the whole conflicting term if possible.
    if (reply.conflict_term() > 0) {
      uint64_t new_ni = reply.conflict_index();
      for (uint64_t i = log_.size(); i > 0; i--) {
        if (log_[i - 1].term == reply.conflict_term()) {
          new_ni = i; // first index AFTER the conflicting term
          break;
        }
      }
      next_index_[peer_id] = std::max(new_ni, uint64_t(1));
    } else if (reply.conflict_index() > 0) {
      next_index_[peer_id] = reply.conflict_index();
    } else {
      if (next_index_[peer_id] > 1)
        next_index_[peer_id]--;
    }
  }
}

void Raft::run_apply() {
  while (!is_dead()) {
    std::vector<ApplyResult> pending;

    {
      std::unique_lock<std::mutex> lk(mtx);
      apply_cv_.wait_for(lk, std::chrono::milliseconds(10), [this] {
        return is_dead() || last_applied_ < commit_index_;
      });

      if (is_dead())
        break;

      while (last_applied_ < commit_index_ &&
             last_applied_ + 1 < log_.size()) {
        last_applied_++;
        auto &e = log_[last_applied_];
        pending.push_back({true, e.data, last_applied_});
      }
    }

    for (auto &r : pending) {
      if (is_dead())
        break;
      this->apply(r);
    }
  }
}

} // namespace rafty
