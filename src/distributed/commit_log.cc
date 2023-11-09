#include "distributed/commit_log.h"

#include <utility>

#include "common/logger.h"
#include "metadata/inode.h"

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(std::move(bm)) {
  usize block_cnt = 1024 - 2;
  this->block_sz = bm_->block_size();
  auto meta_per_block = block_sz / sizeof(LogInfo);
  // NOTICE: reserve 2 blocks for commit log
  this->entry_block_cnt_ = block_cnt / meta_per_block;
  if (block_cnt % meta_per_block != 0) {
    entry_block_cnt_++;
  }
  block_cnt -= entry_block_cnt_;
  auto num_bits_per_block = KBitsPerByte * block_sz;
  this->bitmap_block_cnt_ = block_cnt / num_bits_per_block;
  if (block_cnt % num_bits_per_block) {
    bitmap_block_cnt_++;
  }
  block_cnt -= bitmap_block_cnt_;
  this->logger_block_cnt_ = block_cnt;
  this->last_bitmap_num_ = block_cnt % num_bits_per_block;
  this->commit_block_id_ = bm_->get_log_id();
  this->entry_block_id_ = commit_block_id_ + 2;
  this->bitmap_block_id_ = entry_block_id_ + entry_block_cnt_;
  this->logger_block_id_ = bitmap_block_id_ + bitmap_block_cnt_;
}

CommitLog::~CommitLog() = default;

// {Your code here}
auto CommitLog::get_log_entry_num() const -> usize { return commit_num_; }

auto CommitLog::get_used_blocks_num() const -> usize {
  auto cnt = 0;
  for (int i = 0; i < this->bitmap_block_cnt_; i++) {
    auto curr_block_id = i + bitmap_block_id_;
    std::vector<u8> buffer(block_sz);
    bm_->read_block(curr_block_id, buffer.data());
    auto bitmap = Bitmap(buffer.data(), block_sz);
    cnt += bitmap.count_ones();
  }
  return cnt;
}

// {Your code here}
auto CommitLog::append_log(
    txn_id_t txn_id, const std::vector<std::shared_ptr<BlockOperation>> &ops)
    -> void {
  if (is_checkpoint_enabled_ &&
      this->get_used_blocks_num() >=
          this->logger_block_cnt_ - 150 - ops.size()) {
    checkpoint();
  }

  std::vector<u8> buffer(block_sz);
  std::vector<u8> logInfo(sizeof(LogInfo));
  block_id_t operation_block_id = 0;

  for (const auto &item : ops) {
    auto meta_block_id = item->block_id_;
    auto &new_data = item->new_block_state_;

    // get a free block from bitmap
    for (int i = 0; i < bitmap_block_cnt_; i++) {
      auto curr_block_id = i + bitmap_block_id_;
      bm_->read_block(curr_block_id, buffer.data());
      auto bitmap = Bitmap(buffer.data(), block_sz);
      std::optional<unsigned int> res;
      if (i == bitmap_block_cnt_ - 1 && last_bitmap_num_) {
        res = bitmap.find_first_free_w_bound(last_bitmap_num_);
      } else {
        res = bitmap.find_first_free();
      }
      if (res) {
        operation_block_id = res.value() + i * KBitsPerByte * block_sz;
        bitmap.set(res.value());
        bm_->write_block(curr_block_id, buffer.data());
        bm_->sync(curr_block_id);
        break;
      }
    }

    // write into entry_table
    auto num_info_per_block = block_sz / sizeof(LogInfo);
    auto entry_block_id =
        entry_block_id_ + operation_block_id / num_info_per_block;
    auto entry_block_offset =
        operation_block_id % num_info_per_block * sizeof(LogInfo);

    // calculate logger's block
    auto logger_block_id = logger_block_id_ + operation_block_id;
    bm_->read_block(entry_block_id, buffer.data());
    *((LogInfo *)logInfo.data()) =
        std::make_tuple(txn_id, meta_block_id, logger_block_id);
    bm_->write_partial_block(entry_block_id, logInfo.data(), entry_block_offset,
                             sizeof(LogInfo));
    // write into logger's block
    bm_->write_block(logger_block_id, new_data.data());
    // sync to disk
    bm_->sync(logger_block_id);
    bm_->sync(entry_block_id);
  }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  auto num_txn_per_block = block_sz / sizeof(txn_id_t);
  auto block_id = commit_block_id_ + commit_num_ / num_txn_per_block;
  auto offset = commit_num_ % num_txn_per_block * sizeof(txn_id_t);
  std::vector<u8> buffer(sizeof(txn_id_t));
  *(txn_id_t *)(buffer.data()) = txn_id;
  bm_->write_partial_block(block_id, buffer.data(), offset, sizeof(txn_id_t));
  bm_->sync(block_id);
  commit_num_++;
}

// {Your code here}
auto CommitLog::checkpoint() -> void { recover(); }

// {Your code here}
auto CommitLog::recover() -> void {
  // traverse the commits
  std::vector<u8> commit_buffer(block_sz);
  std::vector<u8> entry_buffer(block_sz);
  std::vector<u8> content_buffer(block_sz);
  std::vector<u8> zero_buffer(block_sz);
  bm_->read_block(this->commit_block_id_, commit_buffer.data());
  txn_id_t in_block_idx = 0;

  for (int i = 0; i < commit_num_; ++i, in_block_idx++) {
    // for each commit
    if (i == block_sz / sizeof(txn_id_t)) {
      bm_->read_block(this->commit_block_id_ + 1, commit_buffer.data());
      in_block_idx = 0;
    }
    txn_id_t trans_id =
        *(txn_id_t *)(commit_buffer.data() + in_block_idx * sizeof(txn_id_t));

    for (int j = 0; j < this->entry_block_cnt_; ++j) {
      // traverse the logs
      auto entry_block_id = j + this->entry_block_id_;
      bm_->read_block(entry_block_id, entry_buffer.data());
      auto *logInfo = (LogInfo *)entry_buffer.data();
      auto size = block_sz / sizeof(LogInfo);
      for (int k = 0; k < size; ++k) {
        auto [transaction_id, meta_block_id, logger_block_id] = *logInfo;
        if (transaction_id == trans_id) {
          // clear the bitmap
          auto num_bits_per_block = KBitsPerByte * block_sz;
          auto nth_logger_block = logger_block_id - this->logger_block_id_;
          auto bitmap_block_id =
              bitmap_block_id_ + nth_logger_block / num_bits_per_block;
          auto bitmap_block_offset = nth_logger_block % num_bits_per_block;

          bm_->read_block(bitmap_block_id, content_buffer.data());
          auto bitmap = Bitmap(content_buffer.data(), block_sz);
          bitmap.clear(bitmap_block_offset);
          bm_->write_block(bitmap_block_id, content_buffer.data());
          bm_->sync(bitmap_block_id);

          // read from logger and write to meta
          bm_->read_block(logger_block_id, content_buffer.data());
          bm_->write_block(meta_block_id, content_buffer.data());
          bm_->write_block(logger_block_id, zero_buffer.data());
          bm_->sync(meta_block_id);
          bm_->sync(logger_block_id);
        }
        logInfo++;
      }
    }
    // clear commit block field
    bm_->write_block(this->commit_block_id_, zero_buffer.data());
    bm_->write_block(this->commit_block_id_ + 1, zero_buffer.data());
    bm_->sync(this->commit_block_id_);
    bm_->sync(this->commit_block_id_ + 1);
  }
  commit_num_ = 0;
}
}  // namespace chfs