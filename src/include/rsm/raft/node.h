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

  void become_follower(term_id_t term, node_id_t leader);

  void become_candidate();

  void become_leader();

  bool is_more_up_to_data(term_id_t termId, int index);

  void recover_from_disk();

  void recover_state_from_disk();

  int get_last_include_idx() const {
    return log_storage->get_last_include_idx();
  }

  /* background workers */
  void run_background_ping();

  void run_background_election();

  void run_background_commit();

  void run_background_apply();

  void run_background_snapshot();

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
  int last_applied_idx = 1;
  node_id_t vote_for_;
  node_id_t vote_gain_;
  std::map<node_id_t, commit_id_t> next_index_map_;
  IntervalTimer rand_timer{};
  IntervalTimer fixed_timer{100};
  std::vector<u8> installing_data;
  int apply_cnt = 0;

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;
  std::unique_ptr<std::thread> background_snapshot;


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
                                           std::to_string(node_id) +
                                           ".log");
  log_storage = std::make_unique<RaftLog<Command>>(node_id, bm);
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
  // LOG_FORMAT_DEBUG("ok");
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  for (const auto& config : this->node_configs) {
    this->rpc_clients_map[config.node_id] =
        std::make_unique<RpcClient>(config.ip_address, config.port, true);
    // cnt++;
  }
  RAFT_LOG("recovering")
  become_follower(0, INVALID_NODE_ID);
  recover_from_disk();
  recover_state_from_disk();
  stopped = false;

  background_election =
      std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping =
      std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit =
      std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply =
      std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
  background_snapshot = std::make_unique<std::thread>(
      &RaftNode::run_background_snapshot, this);

  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  stopped.store(true);
  background_apply->join();
  background_commit->join();
  background_election->join();
  background_ping->join();
  background_snapshot->join();
  rpc_server->stop();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  if (role == RaftRole::Leader && fixed_timer.check_receive() && rpc_clients_map
      [my_id]) {
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
  if (is_stopped()) {
    return {false, 0, 0};
  }
  std::lock_guard lockGuard(mtx);
  const auto last_included_idx = get_last_include_idx();
  if (role != RaftRole::Leader) {
    return {false, current_term, last_included_idx + log_storage->size() - 1};
  }
  Command command;
  command.deserialize(cmd_data, cmd_size);
  auto store_idx = log_storage->append_log(current_term, command);
  log_storage->persist(current_term, vote_for_);
  RAFT_LOG("log_idx: %lu + %d", store_idx, last_included_idx);
  return {true, current_term, store_idx + last_included_idx};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  std::lock_guard lockGuard(mtx);
  const auto idx = commit_idx - get_last_include_idx();
  auto [term, _] = log_storage->get_nth(idx);
  auto data = log_storage->create_snap(commit_idx);
  state->clear();
  state->apply_snapshot(data);
  log_storage->write_snapshot(0, data);
  log_storage->last_snapshot(term, commit_idx);
  log_storage->persist(current_term, vote_for_);
  // RAFT_LOG("saved snapshot, last-included-term %d, last-included-idx %d", term,
  // get_last_include_idx())
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  std::lock_guard lockGuard(mtx);
  return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args)
  -> RequestVoteReply {
  if (is_stopped()) {
    LOG_FORMAT_WARN("hasn't begin");
    return {false, 0};
  }
  const auto nodeId = args.node_id_;
  if (args.term_id_ < current_term) {
    return {false, current_term};
  }
  if (vote_for_ == nodeId) {
    if (args.term_id_ > current_term) {
      become_follower(args.term_id_, INVALID_NODE_ID);
      vote_for_ = nodeId;
      RAFT_LOG("voted before, vote to %d", args.node_id_);

      return {true, current_term};
    }
    vote_for_ = INVALID_NODE_ID;
    return {false, current_term};
  }
  if (!is_more_up_to_data(args.last_log_term_, args.last_log_idx_)) {
    if (vote_for_ == my_id && role == RaftRole::Candidate) {
      vote_gain_--;
    }
    become_follower(args.term_id_, INVALID_NODE_ID);
    vote_for_ = nodeId;
    RAFT_LOG("up to date, vote to %d", args.node_id_);
    return {true, current_term};
  }
  return {false, current_term};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(
    int target, const RequestVoteArgs& arg, const RequestVoteReply reply) {
  if (reply.term_id_ > current_term) {
    current_term = reply.term_id_;
  }
  if (!reply.is_vote_ && reply.term_id_ > current_term) {
    become_follower(reply.term_id_, INVALID_NODE_ID);
    vote_for_ = target;
  } else if (reply.is_vote_ && role == RaftRole::Candidate) {
    current_term = std::max(current_term, reply.term_id_);
    vote_gain_++;
    RAFT_LOG("get vote from %d, vote gain: %d", target, vote_gain_)
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
  current_term = args.term_;
  if (args.heartbeat_) {
    if (current_term <= args.term_) {
      vote_for_ = INVALID_NODE_ID;
      become_follower(args.term_, args.leader_id_);
    }
    leader_id = args.leader_id_;
    rand_timer.receive();
    return {true, current_term};
  }
  auto last_included_idx = get_last_include_idx();
  // fill 0 from last_included_idx to commit_idx
  if (last_included_idx + log_storage->size() - 1 < args.last_included_idx) {
    // missing snapshot before
    // insert 0s
    for (int i = last_included_idx; i < args.last_included_idx; ++i) {
      log_storage->insert(0, ListCommand{0});
    }
  } else if (last_included_idx > args.prev_log_idx_ + args.last_included_idx) {
    // this node has snapshotted a committed value
    // so insert 0s
    // RAFT_LOG("has more snapshot")
    auto insert_num = static_cast<int>(args.prev_log_idx_) - (
                        last_included_idx - args.last_included_idx);
    for (int i = 0; i < insert_num; ++i) {
      log_storage->insert(0, ListCommand(0));
    }
    auto curr_idx = log_storage->size();
    for (auto& entry : args.entries_) {
      log_storage->insert_or_rewrite(entry, curr_idx++);
    }
    log_storage->persist(current_term, vote_for_);
    commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                          args.leader_commit_);
    return {true, current_term};
  }
  if (auto entry = log_storage->get_nth(
        args.prev_log_idx_);
    entry.term_id_ == args.prev_log_term_ || args.prev_log_idx_ == 0) {
    auto curr_idx = args.prev_log_idx_ + 1;
    for (auto& entry_item : args.entries_) {
      RAFT_LOG("append %d: %d", entry_item.term_id_,
               (int)entry_item.command_.value)
      log_storage->insert_or_rewrite(entry_item, curr_idx++);
    }
    vote_for_ = INVALID_NODE_ID;
    log_storage->persist(current_term, vote_for_);
    // RAFT_LOG("1: last included: %d + size:%lu, 2: %lu", last_included_idx,
    //          log_storage->size()-1,
    //          args.leader_commit_);
    commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                          args.leader_commit_);
    // RAFT_LOG("accept, commit idx: %lu", commit_idx);
    return {true, current_term};
  }
  log_storage->delete_after_nth(args.prev_log_idx_);
  commit_idx = std::min(last_included_idx + log_storage->size() - 1,
                        args.leader_commit_);
  return {false, current_term};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(
    const int target, const AppendEntriesArgs<Command> arg,
    const AppendEntriesReply reply) {
  if (!reply.accept_) {
    if (reply.term_ > current_term) {
      // RAFT_LOG("target term is larger")
      become_follower(reply.term_, target);
      return;
    }
    // update next_idx_map info
    next_index_map_[target] = arg.prev_log_idx_;
    return;
  }
  // if accept
  if (arg.heartbeat_) {
    fixed_timer.receive();
    return;
  }
  auto last_included_idx = get_last_include_idx();
  auto agree_idx = arg.prev_log_idx_ + arg.entries_.size();
  const auto cnt = node_configs.size();
  next_index_map_[target] = agree_idx + 1;
  for (auto idx = last_included_idx + arg.prev_log_idx_ + 1;
       idx <= last_included_idx + agree_idx; ++idx) {
    if (!applied_num_[idx]) {
      applied_num_[idx] = 1;
    }
    ++applied_num_[idx];
    if (idx <= commit_idx || applied_num_[idx] > cnt / 2) {
      if (role != RaftRole::Leader) continue;
      // RAFT_LOG("commit %lu", idx);
      this->commit_idx = std::max(idx, commit_idx);
    }
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args)
  -> InstallSnapshotReply {
  if (current_term > args.term_id_) {
    return {current_term};
  }
  if (args.last_included_idx_ == get_last_include_idx()) {
    return {current_term};
  }
  // RAFT_LOG("install snapshot, last-included-term %d, last-included-idx %d",
  // args.last_included_term_, args.last_included_idx_)
  if (args.offset_ == 0) {
    installing_data.clear();
  }
  // append args.data to installing_data
  installing_data.insert(installing_data.end(), args.data_.begin(),
                         args.data_.end());
  log_storage->write_snapshot(
      args.offset_,
      args.data_
      );
  if (args.done) {
    log_storage->last_snapshot(args.last_included_term_,
                               args.last_included_idx_);
    log_storage->persist(current_term, vote_for_);
    state->clear();
    this->state->apply_snapshot(installing_data);
    installing_data.clear();
  }
  std::string str(args.data_.begin(), args.data_.end());
  std::stringstream ss(str);
  int size, tmp;
  ss >> size;
  printf("size: %d\t", size);
  for (int i = 0; i < size; ++i) {
    ss >> tmp;
    printf("%d ", tmp);
  }
  // RAFT_LOG("install snapshot done, last_included_idx: %d",
  //          get_last_include_idx());
  return {current_term};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(
    int node_id, const InstallSnapshotArgs arg,
    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
  if (reply.term_ > current_term) {
    current_term = reply.term_;
  }
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
  RAFT_LOG("become leader, commit idx: %lu", commit_idx)
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
  // rand_timer.stop();
  fixed_timer.start();
}

template <typename StateMachine, typename Command>
bool RaftNode<StateMachine, Command>::is_more_up_to_data(chfs::term_id_t termId,
  int index) {
  auto [lastLogTerm, lastLogIdx] = log_storage->get_last();
  RAFT_LOG("my term: %d, my idx: %lu, term: %d, idx: %d", lastLogTerm,
           lastLogIdx, termId, index)
  return lastLogTerm > termId || (lastLogTerm == termId && lastLogIdx > index);
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::recover_from_disk() {
  auto res = log_storage->recover();
  // RAFT_LOG("recover, term %d, vote %d", res.first, res.second)
  std::tie(current_term, vote_for_) = res;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::recover_state_from_disk() {
  // std::lock_guard lockGuard(clients_mtx);
  auto data = log_storage->get_snapshot_data();
  if (data.first == 0) return;
  std::stringstream ss;
  ss << data.first;
  for (int i = 0; i < data.first; ++i) {
    ss << ' ' << data.second[i];
  }
  std::string str = ss.str();
  std::vector<u8> buffer(str.begin(), str.end());
  state->apply_snapshot(buffer);

  RAFT_LOG(
      "recover state from disk, last-included-term %d, last-included-idx %d, log size: %lu",
      log_storage->get_last_include_idx(), log_storage->get_last_include_term(),
      log_storage->size())
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
      if (role == RaftRole::Leader) {
        continue;
      }
      if (!rpc_clients_map[my_id]) {
        role = RaftRole::Follower;
        continue;
      }
      const auto receive = rand_timer.check_receive();
      const auto timeout = rand_timer.timeout();
      if (role == RaftRole::Follower) {
        if (timeout && !receive) {
          become_candidate();
        }
      }
      if (!timeout || receive) {
        continue;
      }
      current_term++;
      // RAFT_LOG("term++")
      vote_for_ = my_id;
      vote_gain_ = 1;
      auto args = create_request_vote();
      // RAFT_LOG("new term")
      for (const auto& [idx, cli] : rpc_clients_map) {
        if (idx == my_id) continue;
        // send_request_vote(idx, args);
        thread_pool->enqueue(&RaftNode::send_request_vote, this, idx,
                             args);
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
      if (!fixed_timer.check_receive()) {
        continue;
      }
      fixed_timer.reset();
      for (const auto& [node_id, client] : rpc_clients_map) {
        if (node_id == my_id) continue;
        const auto next_idx = next_index_map_[node_id];
        auto prev_idx = next_idx - 1;
        if (log_storage->size() - 1 < prev_idx) {
          continue;
        }
        // RAFT_LOG(
        //     "[background commit]: send to %d prev idx %lu, existing idx %zu",
        //     node_id,
        //     prev_idx,
        //     log_storage->size())
        auto [prev_term, _] = log_storage->get_nth(prev_idx);
        auto entries = log_storage->get_after_nth(prev_idx);
        auto arg = AppendEntriesArgs<Command>{
            current_term, my_id, commit_idx, prev_term,
            prev_idx, entries, false, get_last_include_idx()};
        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id,
                             arg);
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
      auto last_included_idx = get_last_include_idx();
      auto last_applied = state->get_applied_size();
      const int this_applied = commit_idx;
      // RAFT_LOG("last applied: %lu, this applied: %d", last_applied,
      //          this_applied)
      for (int idx = last_applied - last_included_idx;
           idx <= this_applied - last_included_idx; ++idx) {
        if (idx < 0) continue;
        auto term = log_storage->get_nth(idx).term_id_;
        if (term == 0) continue;
        auto cmd = log_storage->get_nth(idx).command_;
        // RAFT_LOG("apply idx: %d", idx);
        state->apply_log(cmd);
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_snapshot() {
  while (true) {
    if (is_stopped()) {
      return;
    }
    std::this_thread::sleep_for(fixed_timer.sleep_for());
    if (role != RaftRole::Leader) {
      continue;
    }
    auto data = log_storage->get_snapshot_data();
    if (data.first == 0) continue;
    std::stringstream ss;
    ss << data.first;
    for (int i = 0; i < data.first; ++i) {
      ss << ' ' << data.second[i];
    }
    std::string str = ss.str();
    std::vector<u8> buffer(str.begin(), str.end());
    InstallSnapshotArgs args{
        current_term, my_id, log_storage->get_last_include_idx(),
        log_storage->get_last_include_term(), 0, buffer, true};
    for (const auto& [cli_idx, cli] : rpc_clients_map) {
      next_index_map_[cli_idx] = log_storage->size();
      if (cli_idx != my_id) {
        thread_pool->enqueue(&RaftNode::send_install_snapshot, this, cli_idx,
                             args);
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
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
          true,
          get_last_include_idx()
      };
      for (const auto& [idx, cli] : rpc_clients_map) {
        if (idx == my_id) continue;
        if (!cli) continue;
        // RAFT_LOG("ping to %d", idx)
        // send_append_entries(idx, args);
        thread_pool->enqueue(&RaftNode::send_append_entries, this, idx,
                             args);
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
