#pragma once

#include <cstring>
#include <mutex>
#include <utility>
#include <vector>

#include "node.h"
#include "block/manager.h"
#include "common/macros.h"

namespace chfs {
using term_id_t = int;
using node_id_t = int;
using commit_id_t = u64;

template <typename Command>
struct LogEntry {
  term_id_t term_id_;
  Command command_;

  LogEntry(term_id_t term, Command cmd)
    : term_id_(term), command_(cmd) {
  }

  LogEntry()
    : term_id_(0) {
  }
};

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
  explicit RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();
  bool validate_log(term_id_t last_log_term, commit_id_t last_log_idx);
  void insert_or_rewrite(LogEntry<Command>& entry, int idx);
  commit_id_t append_log(term_id_t term, Command command);
  std::pair<term_id_t, commit_id_t> get_last();
  LogEntry<Command> get_nth(int n);
  std::vector<LogEntry<Command>> get_after_nth(int n);
  void delete_after_nth(int n);
  void apply_committed(commit_id_t leader_commit, commit_id_t prev_log_idx,
                       std::vector<LogEntry<Command>>& entries);
  size_t size() const;

  /* Lab3: Your code here */

private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  std::vector<LogEntry<Command>> logs_;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) {
  /* Lab3: Your code here */
  bm_ = std::move(bm);
  logs_.emplace_back(LogEntry<Command>());
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/**
 * return the back's term id and the index of it
 * @tparam Command
 * @return
 */
template <typename Command>
std::pair<term_id_t, commit_id_t> RaftLog<Command>::get_last() {
  //  std::lock_guard<std::mutex> lockGuard(mtx);
  return {logs_.back().term_id_, logs_.size() - 1};
}

/**
 * get the nth term id and command content
 * @tparam Command
 * @param n
 * @return
 */
template <typename Command>
LogEntry<Command> RaftLog<Command>::get_nth(int n) {
  std::lock_guard<std::mutex> lockGuard(mtx);
  while (logs_.size() - 1 < n) {
    logs_.emplace_back(LogEntry<Command>());
  }
  return logs_[n];
}

template <typename Command>
std::vector<LogEntry<Command>> RaftLog<Command>::get_after_nth(int n) {
  if (logs_.size() <= n) return {};
  auto begin_ = logs_.begin() + n + 1;
  auto ret_val = std::vector<LogEntry<Command>>(begin_, logs_.end());
  return ret_val;
}

template <typename Command>
void RaftLog<Command>::delete_after_nth(int n) {
  if (logs_.size() <= n) return;
  // delete after nth and include nth
  auto begin_ = logs_.begin() + n;
  logs_.erase(begin_, logs_.end());
}

template <typename Command>
bool RaftLog<Command>::validate_log(term_id_t last_log_term,
                                    commit_id_t last_log_idx) {
  std::lock_guard<std::mutex> lockGuard(mtx);
  if (logs_.size() - 1 < last_log_term) return false;
  if (logs_.size() - 1 == last_log_idx) {
    if (logs_.back().term_id_ == last_log_term) return true;
  }
  auto it = logs_.begin();
  it += last_log_idx;
  logs_.erase(it, logs_.end());
  return false;
}

template <typename Command>
commit_id_t RaftLog<
  Command>::append_log(term_id_t term, Command command) {
  std::lock_guard lockGuard(mtx);
  logs_.emplace_back(LogEntry<Command>(term, command));
  return logs_.size() - 1;
}

template <typename Command>
void RaftLog<Command>::insert_or_rewrite(LogEntry<Command>& entry, int idx) {
  std::lock_guard lockGuard(mtx);
  while (logs_.size() - 1 < idx) {
    logs_.emplace_back(LogEntry<Command>());
  }
  logs_[idx] = entry;
}

template <typename Command>
void RaftLog<Command>::apply_committed(
    const commit_id_t leader_commit, const commit_id_t prev_log_idx,
    std::vector<LogEntry<Command>>& entries) {
  std::lock_guard lockGuard(mtx);
  auto it = entries.begin();
  auto curr_log_idx = prev_log_idx + 1;
  while (curr_log_idx <= leader_commit) {
    LOG_FORMAT_DEBUG("append commited log[{}]", curr_log_idx);
    insert_or_rewrite(*it, curr_log_idx++);
  }
}

template <typename Command>
size_t RaftLog<Command>::size() const {
  return logs_.size();
}

/* Lab3: Your code here */

class IntervalTimer {
public:
  explicit IntervalTimer(const long long interval) {
    start_time = std::chrono::steady_clock::now();
    interval_ = interval;
  }

  IntervalTimer() {
    start_time = std::chrono::steady_clock::now();
    interval_ = std::rand() % 400 + 500;
  }

  std::chrono::milliseconds sleep_for() const {
    return std::chrono::milliseconds(interval_);
  }

  void start() {
    started_ = true;
    start_time = std::chrono::steady_clock::now();
  }

  void stop() {
    started_.store(false);
  };

  void reset() {
    start_time = std::chrono::steady_clock::now();
    receive_from_leader_ = false;
  }

  void receive() { receive_from_leader_ = true; }
  bool check_receive() const { return receive_from_leader_.load(); }

  bool timeout() {
    if (started_) {
      curr_time = std::chrono::steady_clock::now();
      if (const auto duration = std::chrono::duration_cast<
            std::chrono::milliseconds>(
              curr_time - start_time)
          .count(); duration > interval_) {
        reset();
        return true;
      }
    }
    return false;
  };

private:
  std::mutex mtx;
  long long interval_;
  std::atomic<bool> started_ = false;
  std::atomic<bool> receive_from_leader_ = false;
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point curr_time;
};
} /* namespace chfs */
