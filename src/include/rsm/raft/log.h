#pragma once

#include <mutex>
#include <utility>
#include <vector>

#include "block/manager.h"

namespace chfs {
using term_id_t = int;
using node_id_t = int;
using commit_id_t = unsigned long;

constexpr int MAGIC_NUM = 123123123;


template <typename Command>
struct LogEntry {
  term_id_t term_id_;
  Command command_;

  LogEntry(term_id_t term, Command cmd)
    : term_id_(term), command_(cmd) {
  }

  LogEntry()
    : term_id_(0) {
    command_ = ListCommand(0);
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
  explicit RaftLog(const node_id_t nodeId,
                   const std::shared_ptr<BlockManager>& bm);
  ~RaftLog();
  [[nodiscard]] size_t size();

  // check whether the log is valid
  bool validate_log(term_id_t last_log_term, commit_id_t last_log_idx);
  // insert
  void insert_or_rewrite(LogEntry<Command>& entry, int idx);
  commit_id_t append_log(term_id_t term, Command command);
  void insert(term_id_t term, Command command);
  // get
  std::pair<term_id_t, commit_id_t> get_last();
  LogEntry<Command> get_nth(int n);
  std::vector<LogEntry<Command>> get_after_nth(int n);
  // delete
  void delete_after_nth(int n);
  void delete_before_nth(int n);

  /* persist */
  void persist(term_id_t current_term, int vote_for);
  void write_meta(term_id_t current_term, int vote_for);
  void write_data();
  std::pair<term_id_t, int> recover();
  std::tuple<term_id_t, int, int> get_meta();
  void get_data(int size);

  /* snapshot */
  [[nodiscard]] int get_last_include_idx() const { return last_include_idx_; }

  [[nodiscard]] term_id_t get_last_include_term() const {
    return last_inclued_term_;
  }

  [[nodiscard]] std::pair<int, std::vector<int>> get_snapshot_data() const;
  std::vector<u8> create_snap(int commit_idx);
  void write_snapshot(
      int offset,
      std::vector<u8> data);
  void last_snapshot(term_id_t last_included_term,
                     int last_included_idx);

  /* Lab3: Your code here */


private:
  std::shared_ptr<BlockManager> bm_;
  std::shared_ptr<BlockManager> log_meta_bm_, log_data_bm_, snap_bm_;
  std::mutex mtx;
  /* Lab3: Your code here */
  std::vector<LogEntry<Command>> logs_;
  int meta_str_size;
  int per_entry_size;
  int snapshot_idx_ = 0;
  node_id_t node_id_;
  term_id_t last_inclued_term_ = 0;
  int last_include_idx_ = 0;
};

template <typename Command>
RaftLog<Command>::RaftLog(const node_id_t nodeId,
                          const std::shared_ptr<BlockManager>& bm)
  : node_id_(nodeId) {
  bm_ = bm;
  log_meta_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/meta_" +
                                                std::to_string(node_id_));
  log_data_bm_ = std::make_shared<BlockManager>(
      "/tmp/raft_log/data_" + std::to_string(node_id_));
  logs_.emplace_back(LogEntry<Command>());
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}


template <typename Command>
std::pair<term_id_t, commit_id_t> RaftLog<Command>::get_last() {
  std::lock_guard lockGuard(mtx);
  if (logs_.size() <= 1) {
    LOG_FORMAT_WARN("return last included");
    return {last_inclued_term_, last_include_idx_};
  }
  return {logs_.back().term_id_, last_include_idx_ + logs_.size() - 1};
}


template <typename Command>
LogEntry<Command> RaftLog<Command>::get_nth(int n) {
  std::lock_guard lockGuard(mtx);
  if (n <= 0) {
    return LogEntry<Command>{
        last_inclued_term_, ListCommand(0)};
  }
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
void RaftLog<Command>::delete_before_nth(int n) {
  if (logs_.size() <= n) return;
  // delete before nth and include nth
  auto begin_ = logs_.begin();
  logs_.erase(begin_ + 1, begin_ + n + 1);
}

template <typename Command>
void RaftLog<
  Command>::persist(const term_id_t current_term, const int vote_for) {
  std::lock_guard lockGuard(mtx);
  write_meta(current_term, vote_for);
  write_data();
}

template <typename Command>
void RaftLog<Command>::write_meta(const term_id_t current_term,
                                  const int vote_for) {
  std::stringstream ss;
  ss << MAGIC_NUM << ' ' << current_term << ' ' << vote_for << ' ' << (int)(
        logs_.
        size()) << ' ' << snapshot_idx_ << ' ' << last_include_idx_ << ' ' <<
      last_inclued_term_;
  std::string str = ss.str();
  const std::vector<u8> data(str.begin(), str.end());
  log_meta_bm_->write_partial_block(0, data.data(), 0, data.size());
}

template <typename Command>
void RaftLog<Command>::write_data() {
  bool first = true;
  std::stringstream ss;
  int used_bytes = 0, block_idx = 0;
  for (const auto& log : logs_) {
    ss << log.term_id_ << ' ' << (int)(log.command_.value) << ' ';
    used_bytes += per_entry_size;
    if (first) {
      first = false;
      per_entry_size = ss.str().size();
      used_bytes = per_entry_size;
    }
    if (used_bytes + per_entry_size > bm_->block_size() ) {
      std::string str = ss.str();
      const std::vector<u8> data(str.begin(), str.end());
      log_data_bm_->write_partial_block(block_idx++, data.data(), 0,
                                        data.size());
      ss.clear();
      used_bytes = 0;
    }
  }

  std::string str = ss.str();
  const std::vector<u8> data(str.begin(), str.end());
  log_data_bm_->write_partial_block(block_idx++, data.data(), 0,
                                    data.size());

}

template <typename Command>
std::pair<term_id_t, int> RaftLog<Command>::recover() {
  std::lock_guard lockGurad(mtx);
  std::vector<u8> buffer(bm_->block_size());
  logs_.clear();
  auto [cur_term, vote_for, size] = get_meta();
  if (size == 0) {
    LOG_FORMAT_WARN("size is zero");
    return {cur_term, vote_for};
  }
  get_data(size);
  return {cur_term, vote_for};
}

template <typename Command>
std::tuple<term_id_t, int, int> RaftLog<Command>::get_meta() {
  std::vector<u8> buffer(log_meta_bm_->block_size());
  log_meta_bm_->read_block(0, buffer.data());
  std::string str;
  str.assign(buffer.begin(), buffer.end());
  std::stringstream ss(str);
  int magic_num, current_term, vote_for, size, snapshot_index, last_included_idx
      , last_included_term;
  ss >> magic_num;
  if (magic_num != MAGIC_NUM) {
    logs_.emplace_back(LogEntry<Command>());
    return {0, -1, 0};
  }
  ss >> current_term >> vote_for >> size >> snapshot_index >> last_included_idx
      >> last_included_term;
  this->snapshot_idx_ = snapshot_index;
  this->last_include_idx_ = last_included_idx;
  this->last_inclued_term_ = last_included_term;
  snap_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/snap_" +
                                            std::to_string(node_id_) + "_" +
                                            std::to_string(snapshot_idx_));
  return {current_term, vote_for, size};
}

template <typename Command>
void RaftLog<Command>::get_data(const int size) {
  int block_idx = 0;
  int used_bytes = 0;
  std::vector<u8> buffer(log_data_bm_->block_size());
  log_data_bm_->read_block(block_idx++, buffer.data());
  std::string str;
  std::stringstream ss;
  int term, value;
  str.assign(buffer.begin(), buffer.end());
  ss.str(str);
  for (int i = 0; i < size; i++) {
    if (used_bytes + per_entry_size > bm_->block_size()) {
      log_data_bm_->read_block(block_idx++, buffer.data());
      str.assign(buffer.begin(), buffer.end());
      used_bytes = 0;
    }
    ss >> term >> value;
    LOG_FORMAT_DEBUG("term {} value {}", term, value);
    logs_.emplace_back(LogEntry<Command>(term, Command(value)));
    used_bytes += per_entry_size;
  }
}

template <typename Command>
std::vector<u8> RaftLog<Command>::create_snap(const int commit_idx) {
  std::lock_guard lockGuard(mtx);
  auto [size, value] = get_snapshot_data();
  int i = 1;
  if (size == 0) {
    // no snapshot before
    size = 1;
    i = 0;
  }
  const auto idx = commit_idx - last_include_idx_;
  size += idx;
  for (; i <= idx; ++i) {
    if (i > logs_.size()) break;
    int val = logs_[i].command_.value;
    value.emplace_back(val);
  }
  LOG_FORMAT_WARN("snapshot entry cnt {}, size {}", value.size(), size);
  std::stringstream ss;
  ss << size;
  for (const auto v : value) {
    ss << ' ' << v;
  }
  std::string str = ss.str();
  std::vector<u8> data(str.begin(), str.end());
  return data;
}

template <typename Command>
void RaftLog<Command>::write_snapshot(
    const int offset, std::vector<u8> data) {
  std::lock_guard lockGuard(mtx);
  if (offset == 0) {
    // a new snapshot file
    snapshot_idx_++;
    snap_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/snap_" +
                                              std::to_string(node_id_) + "_" +
                                              std::to_string(snapshot_idx_));
  }
  assert(snap_bm_ != nullptr);
  // write data into snapshot file
  int block_idx = offset / snap_bm_->block_size();
  int block_offset = offset % snap_bm_->block_size();
  unsigned len = data.size();
  std::vector<u8> buffer(snap_bm_->block_size());
  while (len) {
    const unsigned write_len = std::min(
        len, snap_bm_->block_size() - block_offset);
    std::copy_n(data.begin(), write_len, buffer.begin());
    snap_bm_->write_partial_block(block_idx++, buffer.data(), block_offset,
                                  write_len);
    // update data
    data.erase(data.begin(), data.begin() + write_len);
    len -= write_len;
    block_offset = 0;
  }
}

template <typename Command>
void RaftLog<Command>::last_snapshot(
    term_id_t last_included_term, int last_included_idx) {
  std::lock_guard lockGuard(mtx);
  // delete other snapshot files
  for (int i = 0; i < snapshot_idx_; i++) {
    std::string path = "/tmp/raft_log/snap_" + std::to_string(node_id_) + "_" +
                       std::to_string(i);
    std::remove(path.c_str());
  }
  // check the logs_ entry
  auto op_idx = last_included_idx - this->last_include_idx_;
  if (logs_.size() < op_idx) {
    // reserve the first log entry
    LOG_FORMAT_INFO("log size not enough");
    logs_.erase(logs_.begin() + 1, logs_.end());
    this->last_include_idx_ = last_included_idx;
    this->last_inclued_term_ = last_included_term;
    return;
  }
  if (auto [term, _] = logs_[op_idx]; term == last_included_term) {
    delete_before_nth(op_idx);
  } else {
    logs_.erase(logs_.begin() + 1, logs_.end());
  }
  assert(logs_.size() > 0);
  this->last_include_idx_ = last_included_idx;
  this->last_inclued_term_ = last_included_term;
}

template <typename Command>
std::pair<int, std::vector<int>> RaftLog<Command>::get_snapshot_data() const {
  if (snap_bm_ == nullptr) return {0, {}};
  std::vector<u8> buffer(snap_bm_->block_size());
  std::vector<int> ret_val;
  std::stringstream ss, ret_ss;
  std::string str;
  int block_idx = 0;
  int size = 0, tmp;
  snap_bm_->read_block(block_idx++, buffer.data());
  str.assign(buffer.begin(), buffer.end());
  ss.str(str);
  ss >> size;
  ret_val.reserve(size);
  for (int i = 0; i < size; ++i) {
    if (ss.eof()) {
      snap_bm_->read_block(block_idx++, buffer.data());
      str.assign(buffer.begin(), buffer.end());
      ss.str(str);
    }
    ss >> tmp;
    ret_val.emplace_back(tmp);
  }
  // LOG_FORMAT_INFO("size: {}, val_size: {}", size, ret_val.size());
  return {size, ret_val};
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
void RaftLog<Command>::insert(term_id_t term, Command command) {
  std::lock_guard lockGuard(mtx);
  logs_.insert(logs_.begin() + 1, LogEntry<Command>(term, command));
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
size_t RaftLog<Command>::size() {
  std::lock_guard lockGuard(mtx);
  return logs_.size();
}

/* Lab3: Your code here */


class IntervalTimer {
public:
  explicit IntervalTimer(const long long interval) {
    fixed = true;
    start_time = std::chrono::steady_clock::now();
    gen = std::mt19937(rd());
    dist = std::uniform_int_distribution(500, 550);
    interval_ = interval;
  }

  IntervalTimer() {
    start_time = std::chrono::steady_clock::now();
    gen = std::mt19937(rd());
    dist = std::uniform_int_distribution(600, 1000);
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
