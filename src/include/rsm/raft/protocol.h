#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {
const node_id_t INVALID_NODE_ID = -1;

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
  /* Lab3: Your code here */
  commit_id_t last_log_idx_;
  term_id_t last_log_term_;
  term_id_t term_id_;
  node_id_t node_id_;
  MSGPACK_DEFINE(last_log_idx_, last_log_term_, term_id_, node_id_)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  bool is_vote_;
  term_id_t term_id_;

  MSGPACK_DEFINE(is_vote_, term_id_)
};

template <typename Command>
struct AppendEntriesArgs {
  term_id_t term_;            /* Leader's term */
  node_id_t leader_id_;       /* for followers to redirect clients */
  commit_id_t leader_commit_; /* Leader's commit index */
  term_id_t prev_log_term_;
  commit_id_t prev_log_idx_;
  std::vector<LogEntry<Command>> entries_;
  bool heartbeat_;
  int last_included_idx;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  std::vector<u8> data_;
  MSGPACK_DEFINE(data_)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(
    const AppendEntriesArgs<Command>& arg) {
  std::stringstream ss;
  ss << arg.term_ << ' ' << arg.leader_id_ << ' ' << arg.leader_commit_ << ' '
      << arg.prev_log_term_
      << ' ' << arg.prev_log_idx_ << ' ' << arg.heartbeat_ << ' ' << arg.
      last_included_idx << ' ' << static_cast<int>(arg.entries_.size());
  for (const auto& entry : arg.entries_) {
    ss << ' ' << entry.term_id_ << ' ' << entry.command_.
                                                value;
  }
  std::string str = ss.str();
  return RpcAppendEntriesArgs{{str.begin(), str.end()}};
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(
    const RpcAppendEntriesArgs& rpc_arg) {
  AppendEntriesArgs<Command> args;
  std::string str;
  str.assign(rpc_arg.data_.begin(), rpc_arg.data_.end());
  std::stringstream ss(str);
  int size;
  term_id_t term;
  int cmd;
  ss >> args.term_ >> args.leader_id_ >> args.leader_commit_ >>
      args.prev_log_term_ >> args.prev_log_idx_ >> args.heartbeat_ >> args.
      last_included_idx >> size;
  for (int i = 0; i < size; ++i) {
    ss >> term >> cmd;
    args.entries_.emplace_back(LogEntry<Command>{term, cmd});
  }
  return args;
}

struct AppendEntriesReply {
  bool accept_;
  term_id_t term_;

  MSGPACK_DEFINE(accept_, term_)
};

struct InstallSnapshotArgs {
  term_id_t term_id_;
  node_id_t leader_id_;
  int last_included_idx_;
  term_id_t last_included_term_;
  int offset_; /*byte offset where chunk is positioned in the snapshot file*/
  std::vector<u8> data_;
  bool done;

  MSGPACK_DEFINE(
      term_id_, leader_id_, last_included_idx_, last_included_term_, offset_,
      data_, done
      )
};

struct InstallSnapshotReply {
  term_id_t term_;

  MSGPACK_DEFINE(
      term_
      )
};
} /* namespace chfs */
