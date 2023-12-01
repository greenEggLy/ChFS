#pragma once

#include <stdarg.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs {
enum class RaftRole {
  Follower,
  Candidate,
  Leader
};

struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {
#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count() & 0xffff;                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __SHORT_FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);

  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool>& network_availablility);

  void set_reliable(bool flag);

  int get_list_state_log_num();

  int rpc_count();

  std::vector<u8> get_snapshot_direct();

private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered
   * before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current
   * term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() const -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends
   * the log, false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size)
    -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;

  auto append_entries(const RpcAppendEntriesArgs& arg) -> AppendEntriesReply;

  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, const RequestVoteArgs& arg);

  void handle_request_vote_reply(int target, const RequestVoteArgs& arg,
                                 const RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);

  void handle_append_entries_reply(int target,
                                   const AppendEntriesArgs<Command> arg,
                                   const AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);

  void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                     const InstallSnapshotReply reply);

  /*  tools */
  RequestVoteArgs create_request_vote();

  // node_id_t available_count();
  void become_follower(term_id_t term, node_id_t leader);

  void become_candidate();

  void become_leader();

  bool is_more_up_to_data(term_id_t termId, int index);

  /* background workers */
  void run_background_ping();

  void run_background_election();

  void run_background_commit();

  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state; /*  The state machine that applies the
                                          raft log, e.g. a kv store. */
  std::map<int, node_id_t>
  applied_num_; /* The applied num of a log entry, if is the majority, then
                       apply it to state machine*/

  std::unique_ptr<RpcServer>
  rpc_server; /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>>
  rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs; /* Configuration for all nodes */
  int my_id; /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  term_id_t current_term;
  node_id_t leader_id;
  commit_id_t commit_idx = 0;
  node_id_t vote_for_;
  node_id_t vote_gain_;
  std::map<node_id_t, commit_id_t> next_index_map_;
  IntervalTimer rand_timer{};
  IntervalTimer fixed_timer{300};
  std::chrono::milliseconds apply_interval =
      std::chrono::milliseconds(100);

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id,
                                          std::vector<RaftNodeConfig> configs)
  : network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server =
      std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER,
                   [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED,
                   [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) {
                     return this->new_command(data, cmd_size);
                   });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT,
                   [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT,
                   [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) {
    return this->request_vote(arg);
  });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) {
    return this->append_entries(arg);
  });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) {
    return this->install_snapshot(arg);
  });

  /* Lab3: Your code here */
  thread_pool = std::make_unique<ThreadPool>(3);
  auto bm = std::make_shared<BlockManager>("/tmp/raft_log/" +
                                           std::to_string(my_config.node_id));
  log_storage = std::make_unique<RaftLog<Command>>(bm);
  state = std::make_unique<StateMachine>();

  rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();
  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */
  auto cnt = 0;
  for (const auto& config : this->node_configs) {
    if (!cnt && config.node_id == my_id) {
      become_candidate();
    } else {
      become_follower(0, INVALID_NODE_ID);
    }
    this->rpc_clients_map[config.node_id] =
        std::make_unique<RpcClient>(config.ip_address, config.port, true);
    cnt++;
  }
  stopped = false;

  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  stopped.store(true);
  background_apply->join();
  background_commit->join();
  background_election->join();
  background_ping->join();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  if (role == RaftRole::Leader) {
    return {true, current_term};
  }
  return {false, current_term};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() const -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data,
                                                  int cmd_size)
  -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  std::lock_guard lockGuard(mtx);
  if (role != RaftRole::Leader) {
    return {false, current_term, log_storage->size() - 1};
  }
  Command command;
  command.deserialize(cmd_data, cmd_size);
  auto store_idx = log_storage->append_log(current_term, command);
  // LOG_FORMAT_INFO("return new cmd idx: {}", store_idx);
  return {true, current_term, store_idx};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
  -> RequestVoteReply {
  auto nodeId = args.node_id_;
  // first: request's term < my term
  if (args.term_id_ < current_term) {
    return {false, current_term};
  }
  // second: request's term > my term
  if (args.term_id_ > current_term) {
    become_follower(args.term_id_, INVALID_NODE_ID);
    vote_for_ = nodeId;
    return {true, current_term};
  }
  // third: same
  if (vote_for_ != INVALID_NODE_ID) {
    // haven't voted before
    if (!is_more_up_to_data(args.last_log_term_, args.last_log_idx_)) {
      RAFT_LOG("up to date, vote for %d", args.node_id_);
      become_follower(args.term_id_, INVALID_NODE_ID);
      vote_for_ = nodeId;
      return {true, current_term};
    }
    return {false, current_term};
  } else {
    if (vote_for_ == args.node_id_) {
      RAFT_LOG("voted %d before", args.node_id_)
      become_follower(args.term_id_, INVALID_NODE_ID);
      vote_for_ = nodeId;
      return {true, current_term};
    }
    return {false, current_term};
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs& arg, const RequestVoteReply reply) {
  //  std::lock_guard<std::mutex> lockGuard(mtx);
  if (reply.term_id_ > current_term) {
    become_follower(reply.term_id_, INVALID_NODE_ID);
  } else if (reply.is_vote_ && role == RaftRole::Candidate &&
             current_term == arg.term_id_) {
    vote_gain_++;
    RAFT_LOG("get vote from %d, gain: %d", target, vote_gain_)
    if (vote_gain_ > node_configs.size() / 2) {
      become_leader();
    }
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(
    const RpcAppendEntriesArgs& rpc_arg) -> AppendEntriesReply {
  AppendEntriesArgs<Command> args = transform_rpc_append_entries_args<
    Command>(rpc_arg);
  // ping info
  if (current_term > args.term_) {
    return {false, current_term};
  }
  if (args.heartbeat_) {
    if (current_term < args.term_) {
      become_follower(args.term_, args.leader_id_);
    }
    leader_id = args.leader_id_;
    rand_timer.receive();
    return {true, current_term};
  }
  // compare the prev
  if (auto entry = log_storage->get_nth(args.prev_log_idx_);
    entry.term_id_ == args.prev_log_term_) {
    auto curr_idx = args.prev_log_idx_ + 1;
    for (auto& entry_item : args.entries_) {
      log_storage->insert_or_rewrite(entry_item, curr_idx++);
    }
    commit_idx = std::min(log_storage->size(), args.leader_commit_);
    return {true, current_term};
  }
  log_storage->delete_after_nth(args.prev_log_idx_);
  commit_idx = std::min(log_storage->size(), args.leader_commit_);
  return {false, current_term};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    const int target, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  if (!reply.accept_) {
    if (reply.term_ > current_term) {
      become_follower(reply.term_, target);
      return;
    }
    // update next_idx_map info
    next_index_map_[target] = arg.prev_log_idx_;
    // send again
    return;
  }
  // if accept
  if (arg.heartbeat_) {
    fixed_timer.receive();
    return;
  }
  auto agree_idx = arg.prev_log_idx_ + arg.entries_.size();
  const auto cnt = node_configs.size();
  next_index_map_[target] = agree_idx + 1;
  for (auto idx = arg.prev_log_idx_ + 1; idx <= agree_idx; ++idx) {
    if (!applied_num_[idx]) {
      applied_num_[idx] = 1;
    }
    ++applied_num_[idx];
    if (idx <= commit_idx || applied_num_[idx] > cnt / 2) {
      // can be committed
      if (role != RaftRole::Leader) continue;
      int cmd = (int)log_storage->get_nth(idx).command_.value;
      RAFT_LOG("commit %lu, %d", idx,
               cmd);
      this->commit_idx = std::max(idx, commit_idx);
    }
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
  -> InstallSnapshotReply {
  /* Lab3: Your code here */
  return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(const int target,
  const RequestVoteArgs& arg) {
  if (rpc_clients_map[target] == nullptr ||
      rpc_clients_map[target]->get_connection_state() !=
      rpc::client::connection_state::connected) {
    return;
  }

  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  const auto res = rpc_clients_map[target]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target, arg,
                              res.unwrap()->template as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(
    const int target, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target] == nullptr ||
      rpc_clients_map[target]->get_connection_state() !=
      rpc::client::connection_state::connected) {
    return;
  }

  const RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  const auto res = rpc_clients_map[target]->
      call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(
        target, arg, res.unwrap()->as<AppendEntriesReply>());
  } else {
    // RPC fails
    RAFT_LOG("rpc fails")
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(
    const int target, const InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target] == nullptr ||
      rpc_clients_map[target]->get_connection_state() !=
      rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(
        target, arg, res.unwrap()->template as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Tool Functions

*******************************************************************/

template <typename StateMachine, typename Command>
RequestVoteArgs RaftNode<StateMachine, Command>::create_request_vote() {
  auto currentTerm = current_term;
  auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
  return {lastLogIdx, lastLogTerm, currentTerm, my_id};
};


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_follower(const term_id_t term,
  const node_id_t leader) {
  role = RaftRole::Follower;
  rand_timer.start();
  current_term = term;
  vote_for_ = INVALID_NODE_ID;
  vote_gain_ = 0;
  leader_id = leader;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_candidate() {
  //  fixed_timer.stop();
  role = RaftRole::Candidate;
  rand_timer.start();
  vote_for_ = my_id;
  vote_gain_ = 1;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_leader() {
  if (role == RaftRole::Leader) return;
  role = RaftRole::Leader;
  vote_for_ = INVALID_NODE_ID;
  vote_gain_ = 0;
  leader_id = my_id;
  // initialize the next_idx_map
  auto lastLogIdx = log_storage->size() - 1;
  for (const auto& [idx, cli] : rpc_clients_map) {
    next_index_map_[idx] = lastLogIdx + 1;
  }
  RAFT_LOG("become leader, commit idx: %lu", commit_idx)
  rand_timer.stop();
  fixed_timer.start();
}

template <typename StateMachine, typename Command>
bool RaftNode<StateMachine, Command>::is_more_up_to_data(chfs::term_id_t termId,
  int index) {
  auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
  return lastLogTerm > termId || (lastLogTerm == termId && lastLogIdx > index);
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  while (true) {
    {
      if (is_stopped()) {
        return;
      }

      std::this_thread::sleep_for(rand_timer.sleep_for());
      if (role == RaftRole::Leader) continue;
      if (!rpc_clients_map[my_id]) continue;
      const auto receive = rand_timer.check_receive();
      const auto timeout = rand_timer.timeout();
      if (role == RaftRole::Follower) {
        if (timeout && !receive) {
          RAFT_LOG("follower become candidate")
          become_candidate();
        }
      }
      if (!timeout || receive) continue;
      current_term++;
      auto args = create_request_vote();
      for (const auto& [idx, cli] : rpc_clients_map) {
        if (idx == my_id) continue;
        if (!cli) continue;
        send_request_vote(idx, args);
        // thread_pool->enqueue(&RaftNode::send_request_vote, this, idx,
        // args);
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      std::this_thread::sleep_for(fixed_timer.sleep_for());
      // Only work for the leader.
      if (role != RaftRole::Leader) continue;
      if (!fixed_timer.check_receive()) continue;
      fixed_timer.reset();
      // if (commit_idx >= log_storage->get_last().second) continue;
      for (const auto& [node_id, client] : rpc_clients_map) {
        if (node_id == my_id) continue;
        const auto next_idx = next_index_map_[node_id];
        auto prev_idx = next_idx - 1;
        if (log_storage->size() - 1 < prev_idx) {
          continue;
        }
        auto [prev_term, _] = log_storage->get_nth(prev_idx);
        auto entries = log_storage->get_after_nth(prev_idx);
        auto arg = AppendEntriesArgs<Command>{
            current_term, my_id, commit_idx, prev_term,
            prev_idx, entries, false};
        // RAFT_LOG("send to %d, prev idx: %lu", node_id, prev_idx)
        send_append_entries(node_id, arg);
        // thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id,
        // arg);
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      std::this_thread::sleep_for(fixed_timer.sleep_for());
      auto last_applied = state->store.size();
      const int this_applied = commit_idx;
      // RAFT_LOG("commit idx: %lu last idx: %lu", this_applied, last_applied)
      for (int idx = last_applied; idx <= this_applied; ++idx) {
        auto cmd = log_storage->get_nth(idx).command_;
        state->apply_log(cmd);
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */

      std::this_thread::sleep_for(fixed_timer.sleep_for());
      if (role != RaftRole::Leader) {
        continue;
      }
      AppendEntriesArgs<Command> args{
          current_term,
          my_id,
          commit_idx,
          0,
          0,
          std::vector<LogEntry<Command>>(),
          true
      };
      for (const auto& [idx, cli] : rpc_clients_map) {
        if (idx == my_id) continue;
        if (!cli) continue;
        send_append_entries(idx, args);
        // thread_pool->enqueue(&RaftNode::send_append_entries, this, idx,
        // args);
      }
    };
  }
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(
    std::map<int, bool>& network_availability) {
  std::unique_lock clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto&& client : rpc_clients_map) {
      if (client.second != nullptr) client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id)
          target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(
          target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto&& client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto&& client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>();
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}
} // namespace chfs
