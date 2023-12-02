#pragma once

#include <mutex>
#include <utility>
#include <vector>

#include "block/manager.h"

namespace chfs {
using term_id_t = int;
using node_id_t = int;
using commit_id_t = u64;

constexpr int MAGIC_NUM = 114514;


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

template <typename Command>
struct PersistEntry {
  term_id_t term_id_;
  int vote_for_;
  std::vector<LogEntry<Command>> logs_;
};


/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
  explicit RaftLog(const std::shared_ptr<BlockManager>& bm);
  ~RaftLog();
  [[nodiscard]] size_t size() const;
  // check whether the log is valid
  bool validate_log(term_id_t last_log_term, commit_id_t last_log_idx);
  // insert
  void insert_or_rewrite(LogEntry<Command>& entry, int idx);
  commit_id_t append_log(term_id_t term, Command command);
  // get
  std::pair<term_id_t, commit_id_t> get_last();
  LogEntry<Command> get_nth(int n);
  std::vector<LogEntry<Command>> get_after_nth(int n);
  // delete
  void delete_after_nth(int n);

  /* persist */
  void persist(term_id_t current_term, int vote_for);
  std::pair<term_id_t, int> recover();

  /* Lab3: Your code here */


private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  std::vector<LogEntry<Command>> logs_;
};

template <typename Command>
RaftLog<Command>::RaftLog(const std::shared_ptr<BlockManager>& bm) {
  bm_ = bm;
  logs_.emplace_back(LogEntry<Command>());
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}


template <typename Command>
std::pair<term_id_t, commit_id_t> RaftLog<Command>::get_last() {
  //  std::lock_guard<std::mutex> lockGuard(mtx);
  return {logs_.back().term_id_, logs_.size() - 1};
}


template <typename Command>
LogEntry<Command> RaftLog<Command>::get_nth(int n) {
  std::lock_guard lockGuard(mtx);
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
void RaftLog<
  Command>::persist(const term_id_t current_term, const int vote_for) {
  std::lock_guard lockGuard(mtx);
  std::stringstream ss;
  ss << MAGIC_NUM << ' ' << current_term << ' ' << vote_for << ' ' << (int)(
    logs_.
    size());
  for (const auto& log : logs_) {
    ss << ' ' << log.term_id_ << ' ' << (int)(log.command_.value);
  }
  // write ss into block
  std::string str = ss.str();
  const std::vector<u8> data(str.begin(), str.end());
  bm_->write_partial_block(0, data.data(), 0, data.size());
}

template <typename Command>
std::pair<term_id_t, int> RaftLog<Command>::recover() {
  std::lock_guard lockGurad(mtx);
  term_id_t current_term;
  int vote_for, size, magic_num;
  int value;
  term_id_t term;
  std::vector<u8> buffer(bm_->block_size());
  logs_.clear();
  bm_->read_block(0, buffer.data());
  std::string str;
  str.assign(buffer.begin(), buffer.end());
  std::stringstream ss(str);
  ss >> magic_num >> current_term >> vote_for;
  ss >> size;
  if (magic_num != MAGIC_NUM) {
    // first start
    logs_.push_back(LogEntry<Command>());
    return {0, -1};
  }
  // LOG_FORMAT_INFO("read from disk: current_term: {}, vote_for: {}, size: {}",
  //                 current_term, vote_for, size);
  for (int i = 0; i < size; i++) {
    ss >> term >> value;
    logs_.emplace_back(LogEntry<Command>(term, Command(value)));
  }
  return {current_term, vote_for};
}

template <typename Command>
bool RaftLog<Command>::validate_log(term_id_t last_log_term,
                                    commit_id_t last_log_idx) {
  std::lock_guard lockGuard(mtx);
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
size_t RaftLog<Command>::size() const {
  return logs_.size();
}

/* Lab3: Your code here */


class IntervalTimer {
public:
  explicit IntervalTimer(const long long interval) {
    fixed = true;
    start_time = std::chrono::steady_clock::now();
    gen = std::mt19937(rd());
    dist = std::uniform_int_distribution(500, 1000);
    interval_ = interval;
  }

  IntervalTimer() {
    start_time = std::chrono::steady_clock::now();
    gen = std::mt19937(rd());
    dist = std::uniform_int_distribution(500, 1000);
    interval_ = dist(gen);
  }

  [[nodiscard]] std::chrono::milliseconds sleep_for() const {
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
    if (fixed) {
      return;
    }
    interval_ = dist(gen);
  }

  void receive() { receive_from_leader_.store(true); }
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
      reset();
    }
    return false;
  };

private:
  std::mutex mtx;
  long long interval_;
  bool fixed = false;
  std::atomic<bool> started_ = false;
  std::atomic<bool> receive_from_leader_ = false;
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point curr_time;
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<int> dist;
};
} /* namespace chfs */
