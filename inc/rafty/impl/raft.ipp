#pragma once

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "rafty/raft.hpp"

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

inline void Raft::start_server() {
  grpc::EnableDefaultHealthCheckService(false);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  ServerBuilder builder;
  builder.AddListeningPort(this->listening_addr,
                           grpc::InsecureServerCredentials());

#ifdef TRACING
  builder.experimental().SetInterceptorCreators(
      tracing::CreateServerTracingInterceptors());
#endif

  // Register this Raft instance as the RaftService implementation.
  builder.RegisterService(static_cast<raftpb::RaftService::Service *>(this));

  std::unique_ptr<Server> server(builder.BuildAndStart());
  logger->info("Raft server {} listening on {}", id, listening_addr);

  this->server_ = std::move(server);

  std::thread([this] { this->server_->Wait(); }).detach();
}

inline void Raft::stop_server() {
  if (this->server_) {
    this->server_->Shutdown();
  }
}

inline void Raft::connect_peers() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);

  for (const auto &peer_addr : peer_addrs) {
    logger->info("Connecting to peer {} at {}", peer_addr.first,
                 peer_addr.second);
    std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>>
        interceptor_creators;
    interceptor_creators.push_back(
        std::make_unique<ByteCountingInterceptorFactory>());
    interceptor_creators.push_back(std::make_unique<NetInterceptorFactory>());
#ifdef TRACING
    interceptor_creators.push_back(std::make_unique<tracing::TracingClientInterceptorFactory>());
#endif
    auto channel = CreateCustomChannelWithInterceptors(
        peer_addr.second, grpc::InsecureChannelCredentials(), args,
        std::move(interceptor_creators));
    auto stub = raftpb::RaftService::NewStub(std::move(channel));
    peers_[peer_addr.first] = std::move(stub);
  }
}

inline bool Raft::is_dead() const { return this->dead.load(); }

inline void Raft::kill() {
  this->dead.store(true);
  // Log crash event
  if (scope_) {
    uint64_t term, log_idx, ci;
    int role_i;
    {
      std::lock_guard<std::mutex> lk(mtx);
      term    = current_term_;
      log_idx = last_log_index_locked();
      ci      = commit_index_;
      role_i  = static_cast<int>(role_);
    }
    scope_->log_local(RaftEventType::NODE_CRASH, term, log_idx, ci, role_i);
  }
  // Wake blocked threads so they can observe the dead flag and exit.
  apply_cv_.notify_all();
  heartbeat_cv_.notify_all();
}

inline std::unique_ptr<grpc::ClientContext>
Raft::create_context(uint64_t to) const {
  std::unique_ptr<grpc::ClientContext> context =
      std::make_unique<grpc::ClientContext>();
  context->AddMetadata("from", std::to_string(this->id));
  context->AddMetadata("to", std::to_string(to));
  return context;
}

inline void Raft::apply(const ApplyResult &result) {
  this->ready_queue.enqueue(result);
}

} // namespace rafty
